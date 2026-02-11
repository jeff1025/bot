"""
vol_paper_trader.py — Real-time volatility model + paper trading simulator

Connects to Chainlink (via Polymarket RTDS) for both spot prices AND vol
computation. Vol is computed from Chainlink ticks so it matches the settlement
oracle — no more Binance/Chainlink vol mismatch.

Prices binary options with Black-Scholes, and paper-trades PM markets when
model fair value diverges from market price.

Usage:
    python vol_paper_trader.py

Outputs:
    ~/.arb_data/sim_trades_v3.jsonl — Every simulated trade + outcome (bell curve confidence)
    ~/.arb_data/sim_summary_v3.json — Running P&L summary
    ~/.arb_data/vol_signals.jsonl   — All detected mispricings
    Console display                 — Live dashboard
"""

import asyncio
import json
import math
import os
import re
import sys
import random
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from datetime import timezone as _tz  # fallback

try:
    import aiohttp
except ImportError:
    aiohttp = None

try:
    import httpx
except ImportError:
    httpx = None

import base64

# Kalshi RSA signing (optional — only needed if scanning Kalshi)
_HAS_CRYPTO = False
try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
    from cryptography.hazmat.backends import default_backend
    _HAS_CRYPTO = True
except ImportError:
    pass


try:
    from scipy.stats import norm
    _norm_cdf = norm.cdf
except ImportError:
    # Pure-python fallback (Abramowitz & Stegun approximation, accurate to 1e-7)
    def _norm_cdf(x):
        if x < -8:
            return 0.0
        if x > 8:
            return 1.0
        a1, a2, a3, a4, a5 = 0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
        p = 0.3275911
        sign = 1 if x >= 0 else -1
        x = abs(x)
        t = 1.0 / (1.0 + p * x)
        y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(-x * x / 2)
        return 0.5 * (1.0 + sign * y)


# =============================================================================
# Constants
# =============================================================================

BINANCE_WS = "wss://stream.binance.com:9443/ws"
SYMBOLS = {
    "BTC": "btcusdt",
    "ETH": "ethusdt",
    "SOL": "solusdt",
}

# SOCKS5 proxy for Binance (geo-restricted from US)
# Set BINANCE_PROXY env var or it's set in run_vol_kalshi.bat
BINANCE_PROXY = os.environ.get("BINANCE_PROXY", None)

# Bell curve confidence — Gaussian centered on $0.50
# Replaces the old hard manipulation zone cutoff with a smooth confidence weight.
# Model accuracy is theoretically highest near ATM ($0.50) and degrades in tails.
# This is our PRIOR — empirical data will tell us if it's right.
BELL_K = 8.0  # Steepness: higher = sharper dropoff. 8.0 gives ~0.44 at $0.20/$0.80, ~0.12 at $0.10/$0.90

def bell_confidence(market_price: float) -> float:
    """
    Gaussian confidence weight centered on $0.50.
    
    Returns a 0-1 multiplier reflecting how much we trust our model
    at this contract price level. Peak confidence at ATM ($0.50),
    decays smoothly toward the tails.
    
    At k=8.0:
        $0.50 → 1.00  (full confidence — full Kelly fraction)
        $0.40/$0.60 → 0.92
        $0.35/$0.65 → 0.84
        $0.25/$0.75 → 0.61
        $0.20/$0.80 → 0.49
        $0.15/$0.85 → 0.38
        $0.10/$0.90 → 0.28
        $0.05/$0.95 → 0.20
    
    This is our PRIOR hypothesis. The dashboard's Confidence tab will
    show whether empirical win rates actually follow this curve.
    """
    return math.exp(-BELL_K * (market_price - 0.50) ** 2)

# Annualization factor: sqrt(minutes per year) for converting per-minute σ to annual
# 365.25 * 24 * 60 = 525,960 minutes/year
MINUTES_PER_YEAR = 525_960
SECONDS_PER_YEAR = MINUTES_PER_YEAR * 60

DATA_DIR = os.path.join(os.path.expanduser("~"), ".arb_data")
SIGNALS_FILE = os.path.join(DATA_DIR, "vol_signals.jsonl")
VOL_LOG_FILE = os.path.join(DATA_DIR, "vol_history.jsonl")


# =============================================================================
# VolTracker — Real-time volatility from Binance trade stream
# =============================================================================

@dataclass
class PriceTick:
    timestamp: float   # Unix seconds
    price: float


# =============================================================================
# CHAINLINK FEED — Real-time Chainlink prices via Polymarket RTDS WebSocket
# =============================================================================

class ChainlinkFeed:
    """Real-time Chainlink price feed from Polymarket's RTDS WebSocket.
    
    This is the AUTHORITATIVE price source for ALL trading decisions:
    - Spot prices for fair value calculations
    - Vol computation (ticks fed into VolTracker.feed_price())
    - Settlement checks
    
    Single source of truth — no Binance dependency. Vol computed from the
    same oracle that determines contract settlement.
    """
    
    WS_URL = "wss://ws-live-data.polymarket.com"
    
    SYMBOL_MAP = {
        "BTC": "btc/usd",
        "ETH": "eth/usd", 
        "SOL": "sol/usd",
    }
    
    def __init__(self, vol_tracker=None):
        self._prices: dict[str, float] = {}      # "BTC" -> price
        self._timestamps: dict[str, float] = {}   # "BTC" -> unix timestamp
        self._lock = threading.Lock()
        self._running = False
        self._thread = None
        self._connected = False
        self._update_count = 0
        self._vol_tracker = vol_tracker  # Feed ticks for vol computation
    
    def start(self):
        """Start RTDS WebSocket in background thread"""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
    
    def stop(self):
        self._running = False
        if hasattr(self, '_ws') and self._ws:
            try:
                self._ws.close()
            except:
                pass
    
    def _run(self):
        import websocket
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                topic = data.get("topic", "")
                
                if "crypto_prices_chainlink" not in topic:
                    return
                
                payload = data.get("payload", {})
                symbol = payload.get("symbol", "").lower()  # "btc/usd"
                value = payload.get("value")
                
                if symbol and value:
                    # Convert "btc/usd" → "BTC"
                    asset = symbol.split("/")[0].upper()
                    price_val = float(value)
                    with self._lock:
                        self._prices[asset] = price_val
                        self._timestamps[asset] = time.time()
                        self._update_count += 1
                    
                    # Feed into VolTracker for vol computation
                    if self._vol_tracker:
                        self._vol_tracker.feed_price(asset, price_val)
                        
            except Exception:
                pass
        
        def on_open(ws):
            self._connected = True
            print("[Chainlink] RTDS connected")
            ws.send(json.dumps({
                "action": "subscribe",
                "subscriptions": [
                    {"topic": "crypto_prices_chainlink", "type": "*", "filters": ""}
                ]
            }))
        
        def on_error(ws, error):
            pass
        
        def on_close(ws, code, msg):
            self._connected = False
            print(f"[Chainlink] RTDS disconnected: {code}")
        
        while self._running:
            try:
                self._ws = websocket.WebSocketApp(
                    self.WS_URL,
                    on_message=on_message,
                    on_open=on_open,
                    on_error=on_error,
                    on_close=on_close
                )
                self._ws.run_forever(ping_interval=5, ping_timeout=3)
            except:
                pass
            if self._running:
                time.sleep(1)
    
    def get_price(self, asset: str) -> Optional[float]:
        """Get current Chainlink price for asset (e.g. 'BTC')"""
        with self._lock:
            return self._prices.get(asset.upper())
    
    def get_all_prices(self) -> dict[str, float]:
        """Get all current Chainlink prices"""
        with self._lock:
            return dict(self._prices)
    
    def is_stale(self, asset: str, max_age_seconds: float = 30) -> bool:
        """Check if price is stale (no update in max_age_seconds)"""
        with self._lock:
            ts = self._timestamps.get(asset.upper())
            if not ts:
                return True
            return (time.time() - ts) > max_age_seconds
    
    def is_connected(self) -> bool:
        return self._connected
    
    def status(self) -> str:
        with self._lock:
            prices = ", ".join(f"{a}=${p:,.2f}" for a, p in sorted(self._prices.items()))
        return f"{'🟢' if self._connected else '🔴'} Chainlink: {prices or 'no data'} ({self._update_count} ticks)"


class VolTracker:
    """
    Maintains real-time price history and computes realized volatility
    using both rolling windows and EWMA (exponentially weighted moving average).
    
    Price data comes from Chainlink via ChainlinkFeed.feed_price(), ensuring
    vol estimates match the settlement oracle. No Binance dependency.
    
    EWMA responds faster to regime changes than flat windows:
        σ²_t = λ * σ²_{t-1} + (1-λ) * r²_t
    
    We run two EWMA speeds:
        fast (λ=0.90): ~10 observation half-life, catches spikes quickly
        slow (λ=0.97): ~33 observation half-life, smoother baseline
    
    Vol regime signal: fast_vol / slow_vol
        > 1.5 → vol expanding (contracts near strike are likely underpriced)
        < 0.7 → vol contracting (contracts near strike may be overpriced)
    """
    
    def __init__(self, max_history_minutes: int = 120):
        self.max_ticks = max_history_minutes * 60  # ~1 tick/sec assumed
        
        # Per-asset price history: deque of PriceTick
        self._prices: dict[str, deque] = {
            asset: deque(maxlen=self.max_ticks) for asset in SYMBOLS
        }
        
        # Per-asset 1-minute candle closes for cleaner vol calc
        self._minute_closes: dict[str, deque] = {
            asset: deque(maxlen=max_history_minutes) for asset in SYMBOLS
        }
        self._current_minute: dict[str, int] = {asset: 0 for asset in SYMBOLS}
        self._current_minute_last_price: dict[str, float] = {}
        
        # EWMA state per asset
        self._ewma_fast: dict[str, float] = {}  # σ² (variance), fast
        self._ewma_slow: dict[str, float] = {}  # σ² (variance), slow
        self._ewma_initialized: dict[str, bool] = {asset: False for asset in SYMBOLS}
        self._last_log_return: dict[str, float] = {}
        
        # EWMA decay factors
        self.lambda_fast = 0.90   # Half-life ≈ 7 observations
        self.lambda_slow = 0.97   # Half-life ≈ 23 observations
        
        # Latest price cache
        self.latest_price: dict[str, float] = {}
        
        # Connection state
        self._ws_task: Optional[asyncio.Task] = None
        self._running = False
        self._connected = False
        
        # Stats
        self.tick_count: dict[str, int] = {asset: 0 for asset in SYMBOLS}
        self._last_vol_log = 0
        
        # Tick interval tracking for EWMA annualization
        # Instead of assuming 1 tick/sec, track actual intervals
        self._last_tick_time: dict[str, float] = {}
        self._tick_interval_ewma: dict[str, float] = {}  # Smoothed avg seconds between ticks
        self._tick_interval_lambda = 0.95  # Smooth over ~20 ticks
    
    async def start(self):
        """Start websocket connection to Binance"""
        if self._running:
            return
        self._running = True
        self._ws_task = asyncio.create_task(self._ws_loop())
    
    async def stop(self):
        self._running = False
        if self._ws_task:
            self._ws_task.cancel()
    
    async def _ws_loop(self):
        """Main websocket loop — Binance via HTTP CONNECT proxy"""
        if aiohttp is None:
            print("[VolTracker] aiohttp not installed")
            return
        
        streams = "/".join(f"{sym}@trade" for sym in SYMBOLS.values())
        url = f"{BINANCE_WS}/{streams}"
        
        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    ws_kwargs = {"heartbeat": 20}
                    proxy_label = ""
                    if BINANCE_PROXY:
                        # Strip socks5:// prefix if present, use as http
                        proxy_url = BINANCE_PROXY
                        if proxy_url.startswith("socks"):
                            proxy_url = "http" + proxy_url[proxy_url.index("://"):]
                        ws_kwargs["proxy"] = proxy_url
                        proxy_label = " via proxy"
                    
                    async with session.ws_connect(url, **ws_kwargs) as ws:
                        self._connected = True
                        print(f"[VolTracker] Connected to Binance ({len(SYMBOLS)} streams){proxy_label}")
                        
                        async for msg in ws:
                            if not self._running:
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                self._process_trade(json.loads(msg.data))
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[VolTracker] WS error: {e}, reconnecting in 5s...")
                self._connected = False
                await asyncio.sleep(5)
    
    def _process_trade(self, data: dict):
        """Process a single Binance trade message"""
        symbol = data.get("s", "").upper()  # e.g. "BTCUSDT"
        
        # Map back to our asset names
        asset = None
        for a, sym in SYMBOLS.items():
            if sym.upper() == symbol:
                asset = a
                break
        if not asset:
            return
        
        price = float(data.get("p", 0))
        ts = float(data.get("T", 0)) / 1000  # Binance gives ms
        
        if price <= 0:
            return
        
        self.tick_count[asset] = self.tick_count.get(asset, 0) + 1
        prev_price = self.latest_price.get(asset)
        self.latest_price[asset] = price
        
        # Store tick
        self._prices[asset].append(PriceTick(ts, price))
        
        # Update minute candle
        current_min = int(ts // 60)
        if self._current_minute[asset] != current_min:
            # New minute — close previous candle
            if self._current_minute_last_price.get(asset) is not None:
                self._minute_closes[asset].append(
                    PriceTick(self._current_minute[asset] * 60, self._current_minute_last_price[asset])
                )
            self._current_minute[asset] = current_min
        self._current_minute_last_price[asset] = price
        
        # Update EWMA with log return
        if prev_price and prev_price > 0:
            log_return = math.log(price / prev_price)
            self._last_log_return[asset] = log_return
            
            r_sq = log_return ** 2
            
            if not self._ewma_initialized.get(asset):
                # Bootstrap: use first squared return as initial variance
                self._ewma_fast[asset] = r_sq
                self._ewma_slow[asset] = r_sq
                self._ewma_initialized[asset] = True
            else:
                self._ewma_fast[asset] = self.lambda_fast * self._ewma_fast[asset] + (1 - self.lambda_fast) * r_sq
                self._ewma_slow[asset] = self.lambda_slow * self._ewma_slow[asset] + (1 - self.lambda_slow) * r_sq
    
    def feed_price(self, asset: str, price: float, timestamp: float = None):
        """
        Manual price feed — primary input when using Chainlink as vol source.
        Tracks tick intervals for accurate EWMA annualization.
        """
        if timestamp is None:
            timestamp = time.time()
        
        if price <= 0:
            return
        
        # Track tick interval for EWMA annualization
        now = time.time()
        last_t = self._last_tick_time.get(asset)
        if last_t:
            dt = now - last_t
            if 0.01 < dt < 30:  # Ignore absurd intervals (reconnects, bursts)
                if asset in self._tick_interval_ewma:
                    lam = self._tick_interval_lambda
                    self._tick_interval_ewma[asset] = lam * self._tick_interval_ewma[asset] + (1 - lam) * dt
                else:
                    self._tick_interval_ewma[asset] = dt
        self._last_tick_time[asset] = now
        
        prev_price = self.latest_price.get(asset)
        self.latest_price[asset] = price
        self.tick_count[asset] = self.tick_count.get(asset, 0) + 1
        
        self._prices[asset].append(PriceTick(timestamp, price))
        
        # Minute candle
        current_min = int(timestamp // 60)
        if self._current_minute[asset] != current_min:
            if self._current_minute_last_price.get(asset) is not None:
                self._minute_closes[asset].append(
                    PriceTick(self._current_minute[asset] * 60, self._current_minute_last_price[asset])
                )
            self._current_minute[asset] = current_min
        self._current_minute_last_price[asset] = price
        
        # EWMA
        if prev_price and prev_price > 0:
            log_return = math.log(price / prev_price)
            r_sq = log_return ** 2
            
            if not self._ewma_initialized.get(asset):
                self._ewma_fast[asset] = r_sq
                self._ewma_slow[asset] = r_sq
                self._ewma_initialized[asset] = True
            else:
                self._ewma_fast[asset] = self.lambda_fast * self._ewma_fast[asset] + (1 - self.lambda_fast) * r_sq
                self._ewma_slow[asset] = self.lambda_slow * self._ewma_slow[asset] + (1 - self.lambda_slow) * r_sq
    
    def get_realized_vol(self, asset: str, window_minutes: int = 15) -> Optional[float]:
        """
        Compute annualized realized volatility from minute closes.
        
        Returns σ (annualized) or None if insufficient data.
        Uses simple rolling window of log returns.
        """
        closes = self._minute_closes.get(asset)
        if not closes or len(closes) < max(3, window_minutes // 2):
            return None
        
        # Use last N minute closes
        recent = list(closes)[-window_minutes:]
        if len(recent) < 3:
            return None
        
        # Compute log returns between consecutive minute closes
        log_returns = []
        for i in range(1, len(recent)):
            if recent[i].price > 0 and recent[i-1].price > 0:
                log_returns.append(math.log(recent[i].price / recent[i-1].price))
        
        if len(log_returns) < 2:
            return None
        
        # Standard deviation of per-minute returns
        mean_r = sum(log_returns) / len(log_returns)
        var = sum((r - mean_r) ** 2 for r in log_returns) / (len(log_returns) - 1)
        std_per_minute = math.sqrt(var)
        
        # Annualize: σ_annual = σ_minute * √(minutes_per_year)
        annualized = std_per_minute * math.sqrt(MINUTES_PER_YEAR)
        
        return annualized
    
    def get_ewma_vol(self, asset: str, speed: str = "fast") -> Optional[float]:
        """
        Get EWMA-based annualized volatility.
        
        speed: "fast" (λ=0.90, reactive) or "slow" (λ=0.97, smooth)
        
        Returns annualized σ or None if not initialized.
        
        Annualization uses tracked tick interval rather than assuming 1 tick/sec.
        ticks_per_year = seconds_per_year / avg_tick_interval
        σ_annual = σ_per_tick * √(ticks_per_year)
        """
        if not self._ewma_initialized.get(asset):
            return None
        
        var = self._ewma_fast.get(asset, 0) if speed == "fast" else self._ewma_slow.get(asset, 0)
        if var <= 0:
            return None
        
        per_tick_std = math.sqrt(var)
        
        # Use tracked tick interval for annualization (default 1.0s if not enough data)
        avg_interval = self._tick_interval_ewma.get(asset, 1.0)
        if avg_interval <= 0:
            avg_interval = 1.0
        ticks_per_year = SECONDS_PER_YEAR / avg_interval
        annualized = per_tick_std * math.sqrt(ticks_per_year)
        
        return annualized
    
    def get_vol_regime(self, asset: str) -> Optional[dict]:
        """
        Detect volatility regime by comparing fast vs slow EWMA.
        
        Returns dict with:
            ratio: fast_vol / slow_vol
                > 1.5 → "expanding" (vol spike, binary options near strike underpriced)
                < 0.7 → "contracting" (calm period, near-strike options overpriced)
                else  → "normal"
            fast_vol: annualized vol from fast EWMA
            slow_vol: annualized vol from slow EWMA
            regime: "expanding" | "contracting" | "normal"
            
        Also includes rolling window vols for cross-reference.
        """
        fast = self.get_ewma_vol(asset, "fast")
        slow = self.get_ewma_vol(asset, "slow")
        
        if fast is None or slow is None or slow <= 0:
            return None
        
        ratio = fast / slow
        
        if ratio > 1.5:
            regime = "expanding"
        elif ratio < 0.7:
            regime = "contracting"
        else:
            regime = "normal"
        
        # Also get rolling window vols for comparison
        vol_5m = self.get_realized_vol(asset, 5)
        vol_15m = self.get_realized_vol(asset, 15)
        vol_60m = self.get_realized_vol(asset, 60)
        
        return {
            "ratio": round(ratio, 3),
            "regime": regime,
            "fast_vol": round(fast, 4),
            "slow_vol": round(slow, 4),
            "vol_5m": round(vol_5m, 4) if vol_5m else None,
            "vol_15m": round(vol_15m, 4) if vol_15m else None,
            "vol_60m": round(vol_60m, 4) if vol_60m else None,
        }
    
    def get_best_vol(self, asset: str) -> Optional[float]:
        """
        Return the best available vol estimate for pricing.
        
        Priority:
        1. 15-min rolling window (most stable for 15-min binary pricing)
        2. 5-min rolling window (less data but still window-appropriate)
        3. Fast EWMA (available earliest, most reactive)
        
        Falls back through the chain if earlier options lack data.
        """
        vol = self.get_realized_vol(asset, 15)
        if vol and vol > 0.01:  # Sanity: >1% annualized
            return vol
        
        vol = self.get_realized_vol(asset, 5)
        if vol and vol > 0.01:
            return vol
        
        vol = self.get_ewma_vol(asset, "fast")
        if vol and vol > 0.01:
            return vol
        
        return None
    
    def get_status(self) -> dict:
        """Return tracker status for display"""
        status = {
            "connected": self._connected,
            "assets": {}
        }
        for asset in SYMBOLS:
            ticks = self.tick_count.get(asset, 0)
            minutes = len(self._minute_closes.get(asset, []))
            price = self.latest_price.get(asset)
            vol = self.get_best_vol(asset)
            regime = self.get_vol_regime(asset)
            
            status["assets"][asset] = {
                "ticks": ticks,
                "minutes": minutes,
                "price": price,
                "vol": round(vol, 4) if vol else None,
                "regime": regime["regime"] if regime else None,
            }
        return status


# =============================================================================
# BinaryPricer — Black-Scholes pricing for digital/binary options
# =============================================================================

class BinaryPricer:
    """
    Prices binary (digital) options using Black-Scholes.
    
    For a binary CALL paying $1 if S > K at expiry:
        price = N(d2)
    
    For a binary PUT paying $1 if S < K at expiry:
        price = N(-d2)
    
    where:
        d2 = [ln(S/K) + (r - σ²/2)T] / (σ√T)
        
    S = current underlying price
    K = strike price
    T = time to expiry in years
    σ = annualized volatility
    r = risk-free rate (≈0 for minutes/hours, negligible)
    
    For our 15-minute windows:
        T = minutes_left / 525960
        
    The key insight: if our σ estimate is more accurate than the market's
    implied σ, then our fair value is more accurate, and deviations = edge.
    """
    
    @staticmethod
    def price_binary_call(S: float, K: float, T_years: float, sigma: float) -> float:
        """
        Fair value of binary call: pays $1 if S > K at expiry.
        Equivalent to PM "UP" or K "YES" (for "above" markets).
        
        Returns: probability (0.0 to 1.0) = fair price in dollars
        """
        if T_years <= 0:
            # At expiry: worth $1 if S > K, else $0
            return 1.0 if S > K else 0.0
        
        if sigma <= 0:
            # Zero vol: deterministic
            return 1.0 if S > K else 0.0
        
        if S <= 0 or K <= 0:
            return 0.0
        
        sqrt_T = math.sqrt(T_years)
        d2 = (math.log(S / K) + (-0.5 * sigma ** 2) * T_years) / (sigma * sqrt_T)
        
        return _norm_cdf(d2)
    
    @staticmethod
    def price_binary_put(S: float, K: float, T_years: float, sigma: float) -> float:
        """
        Fair value of binary put: pays $1 if S < K at expiry.
        Equivalent to PM "DOWN" or K "NO" (for "above" markets).
        """
        return 1.0 - BinaryPricer.price_binary_call(S, K, T_years, sigma)
    
    @staticmethod
    def fair_value(S: float, K: float, seconds_left: float, sigma: float,
                   direction: str = "up") -> float:
        """
        Convenience method: price a binary option.
        
        Args:
            S: current underlying price (e.g. 65000 for BTC)
            K: strike price
            seconds_left: seconds until market settles
            sigma: annualized volatility
            direction: "up" (pays if S>K) or "down" (pays if S<K)
            
        Returns: fair price 0.00-1.00
        """
        T = seconds_left / SECONDS_PER_YEAR
        
        if direction in ("up", "yes", "call"):
            return BinaryPricer.price_binary_call(S, K, T, sigma)
        else:
            return BinaryPricer.price_binary_put(S, K, T, sigma)
    
    @staticmethod
    def implied_vol(market_price: float, S: float, K: float, seconds_left: float,
                    direction: str = "up", tol: float = 1e-6, max_iter: int = 50) -> Optional[float]:
        """
        Back out implied volatility from a market price using bisection.
        
        Useful for comparing: if implied_vol >> realized_vol, the market is
        pricing in more uncertainty than actually exists (option overpriced).
        
        Returns annualized implied σ, or None if no solution found.
        """
        if market_price <= 0.001 or market_price >= 0.999:
            return None
        
        T = seconds_left / SECONDS_PER_YEAR
        if T <= 0:
            return None
        
        # Bisection between 0.01 (1%) and 10.0 (1000%) annualized vol
        lo, hi = 0.01, 10.0
        
        for _ in range(max_iter):
            mid = (lo + hi) / 2
            model_price = BinaryPricer.fair_value(S, K, seconds_left, mid, direction)
            
            # For calls: higher vol → price moves toward 0.5 from either side
            # This makes bisection tricky near ATM. We handle it by checking direction of error.
            diff = model_price - market_price
            
            if abs(diff) < tol:
                return mid
            
            # Determine which direction to search
            # For deep ITM call (S >> K): higher vol → lower price (pulls toward 0.5)
            # For deep OTM call (S << K): higher vol → higher price (pulls toward 0.5)
            if S > K and direction in ("up", "yes", "call"):
                # ITM call: higher vol → lower price
                if diff > 0:
                    lo = mid  # Need higher vol to lower price
                else:
                    hi = mid
            elif S <= K and direction in ("up", "yes", "call"):
                # OTM call: higher vol → higher price
                if diff > 0:
                    hi = mid  # Need lower vol to lower price
                else:
                    lo = mid
            elif S < K and direction in ("down", "no", "put"):
                # ITM put
                if diff > 0:
                    lo = mid
                else:
                    hi = mid
            else:
                # OTM put
                if diff > 0:
                    hi = mid
                else:
                    lo = mid
        
        return (lo + hi) / 2  # Best estimate after max iterations


# =============================================================================
# MispricingScanner — Compare model to market, generate signals
# =============================================================================

@dataclass
class Signal:
    """A detected mispricing"""
    timestamp: str
    asset: str
    direction: str           # "up" or "down"
    strike: float
    spot: float
    seconds_left: float
    model_vol: float         # σ used for pricing
    model_fair: float        # our fair value
    market_price: float      # what the market is asking
    edge: float              # model_fair - market_price (positive = cheap)
    edge_pct: float          # edge as % of market price
    venue: str               # "PM" or "K"
    implied_vol: Optional[float]  # market's implied σ
    vol_regime: str           # "expanding" / "contracting" / "normal"
    confidence: float         # 0-1 signal quality score


class MispricingScanner:
    """
    Compares model fair values to market prices and logs signals.
    
    Signal quality scoring:
        - Higher edge_pct → stronger signal
        - More vol data (longer history) → higher confidence
        - Vol regime "expanding" + buying near-strike → bonus confidence
        - Agreement between rolling and EWMA vol → higher confidence
    """
    
    def __init__(self, vol_tracker: VolTracker, min_edge_pct: float = 3.0, chainlink_feed: ChainlinkFeed = None):
        self.vol = vol_tracker
        self.chainlink = chainlink_feed
        self.pricer = BinaryPricer()
        self.min_edge_pct = min_edge_pct  # Minimum edge % to log as signal
        self.signals: list[Signal] = []
        self._last_log = 0
        
        os.makedirs(DATA_DIR, exist_ok=True)
    
    def evaluate(self, asset: str, strike: float, seconds_left: float,
                 direction: str, market_price: float, venue: str = "PM") -> Optional[Signal]:
        """
        Evaluate a single contract for mispricing.
        
        Args:
            asset: "BTC", "ETH", "SOL"
            strike: strike price
            seconds_left: seconds until settlement
            direction: "up" or "down"
            market_price: current market ask/mid price (0.00-1.00)
            venue: "PM" or "K"
            
        Returns Signal if edge exceeds threshold, else None.
        """
        if market_price <= 0.01 or market_price >= 0.99:
            return None
        
        if seconds_left <= 30:
            return None  # Too close to expiry for model to be useful
        
        spot = self.chainlink.get_price(asset) if self.chainlink else None
        if not spot:
            return None
        
        sigma = self.vol.get_best_vol(asset)
        if not sigma:
            return None
        
        # Price the contract
        fair = self.pricer.fair_value(spot, strike, seconds_left, sigma, direction)
        
        # Edge: positive means market is cheap (we should buy)
        edge = fair - market_price
        edge_pct = (edge / market_price) * 100 if market_price > 0 else 0
        
        # Get implied vol from market price
        iv = self.pricer.implied_vol(market_price, spot, strike, seconds_left, direction)
        
        # Vol regime
        regime_data = self.vol.get_vol_regime(asset)
        regime = regime_data["regime"] if regime_data else "unknown"
        
        # Confidence scoring (0-1)
        confidence = self._score_confidence(asset, edge_pct, regime_data, sigma, iv)
        
        signal = Signal(
            timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            asset=asset,
            direction=direction,
            strike=strike,
            spot=spot,
            seconds_left=seconds_left,
            model_vol=round(sigma, 4),
            model_fair=round(fair, 4),
            market_price=round(market_price, 4),
            edge=round(edge, 4),
            edge_pct=round(edge_pct, 2),
            venue=venue,
            implied_vol=round(iv, 4) if iv else None,
            vol_regime=regime,
            confidence=round(confidence, 3),
        )
        
        # Log if edge is meaningful (even if below trading threshold — useful for analysis)
        if abs(edge_pct) >= 1.0:
            self._log_signal(signal)
        
        # Return only actionable signals
        if edge_pct >= self.min_edge_pct and confidence >= 0.3:
            self.signals.append(signal)
            return signal
        
        return None
    
    def _score_confidence(self, asset: str, edge_pct: float,
                          regime_data: Optional[dict], realized_vol: float,
                          implied_vol: Optional[float]) -> float:
        """
        Score signal confidence 0-1 based on:
        - Data quality (how much vol history we have)
        - Edge magnitude (bigger edge → but also check for stale data)
        - Vol agreement (rolling windows agree with EWMA)
        - Regime context (expanding vol + near-strike = real edge, not just noise)
        """
        score = 0.5  # Base confidence
        
        # Data quality: more minute candles = more confidence
        minutes_available = len(self.vol._minute_closes.get(asset, []))
        if minutes_available >= 30:
            score += 0.15
        elif minutes_available >= 15:
            score += 0.10
        elif minutes_available >= 5:
            score += 0.05
        else:
            score -= 0.2  # Very little data — low confidence
        
        # Edge magnitude: moderate edges are more believable than extreme ones
        abs_edge = abs(edge_pct)
        if 3 <= abs_edge <= 15:
            score += 0.1  # Sweet spot — real but not suspiciously large
        elif abs_edge > 30:
            score -= 0.15  # Probably stale data or model error
        
        # Vol agreement: if rolling and EWMA roughly agree, we're more confident
        if regime_data:
            ratio = regime_data["ratio"]
            if 0.8 <= ratio <= 1.3:
                score += 0.1  # Vols agree — stable estimate
            elif ratio > 2.0 or ratio < 0.5:
                score -= 0.1  # Vol in flux — less confident in any single estimate
        
        # Implied vol comparison
        if implied_vol and realized_vol and implied_vol > 0:
            iv_rv_ratio = implied_vol / realized_vol
            if 0.7 <= iv_rv_ratio <= 1.5:
                score += 0.05  # Market and model roughly agree on vol level
            # If IV >> RV: market pricing in more vol than realized → could be overpriced
            # If IV << RV: market ignoring real vol → could be underpriced
        
        return max(0.0, min(1.0, score))
    
    def _log_signal(self, signal: Signal):
        """Append signal to JSONL file"""
        try:
            entry = {
                "ts": signal.timestamp,
                "asset": signal.asset,
                "dir": signal.direction,
                "strike": signal.strike,
                "spot": signal.spot,
                "sec_left": signal.seconds_left,
                "model_vol": signal.model_vol,
                "model_fair": signal.model_fair,
                "mkt_price": signal.market_price,
                "edge": signal.edge,
                "edge_pct": signal.edge_pct,
                "venue": signal.venue,
                "iv": signal.implied_vol,
                "regime": signal.vol_regime,
                "confidence": signal.confidence,
            }
            with open(SIGNALS_FILE, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception:
            pass
    
    def log_vol_snapshot(self, asset: str):
        """Log current vol state for historical analysis"""
        now = time.time()
        if now - self._last_log < 60:
            return  # Throttle to once per minute
        self._last_log = now
        
        regime = self.vol.get_vol_regime(asset)
        if not regime:
            return
        
        try:
            entry = {
                "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "asset": asset,
                "spot": self.chainlink.get_price(asset) if self.chainlink else self.vol.latest_price.get(asset),
                **regime,
            }
            with open(VOL_LOG_FILE, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception:
            pass


# =============================================================================
# VolModel — Main orchestrator (import this from arb_scanner)
# =============================================================================

class VolModel:
    """
    Top-level interface for the volatility model.
    
    Vol is computed from Chainlink ticks (same oracle PM settles on).
    ChainlinkFeed receives RTDS ticks and feeds them into VolTracker.
    No Binance dependency — single price source for spot + vol.
    
    Usage:
    
        model = VolModel()
        await model.start()  # Connects to Chainlink RTDS
        
        # Evaluate contracts:
        signal = model.evaluate(
            asset="BTC",
            strike=65000,
            seconds_left=600,
            direction="up",
            market_price=0.45,
            venue="PM"
        )
        
        if signal:
            print(f"Edge: {signal.edge_pct:.1f}%")
        
        # Fair value directly:
        fair = model.fair_value("BTC", 65000, 600, "up")
    """
    
    def __init__(self, min_edge_pct: float = 3.0):
        self.vol_tracker = VolTracker()
        self.chainlink = ChainlinkFeed(vol_tracker=self.vol_tracker)  # Chainlink feeds vol ticks
        self.scanner = MispricingScanner(self.vol_tracker, min_edge_pct=min_edge_pct, chainlink_feed=self.chainlink)
        self.pricer = BinaryPricer()
    
    async def start(self):
        """Start Chainlink feed for both spot prices AND vol computation.
        
        Chainlink is the sole price source — no Binance WS needed.
        Vol is computed from Chainlink ticks, matching the settlement oracle.
        """
        self.chainlink.start()
        # Note: VolTracker.start() (Binance WS) is NOT called.
        # Vol ticks come from ChainlinkFeed → VolTracker.feed_price()
    
    async def stop(self):
        self.chainlink.stop()
        # VolTracker WS not started, no need to stop
    
    def feed_price(self, asset: str, price: float, timestamp: float = None):
        """
        Feed a price manually (from RTDS etc).
        Use this if Binance WS is unavailable or as supplementary data.
        """
        self.vol_tracker.feed_price(asset, price, timestamp)
    
    def fair_value(self, asset: str, strike: float, seconds_left: float,
                   direction: str = "up") -> Optional[float]:
        """
        Get model fair value for a binary option.
        Returns None if insufficient vol data.
        """
        spot = self.chainlink.get_price(asset)
        sigma = self.vol_tracker.get_best_vol(asset)
        
        if not spot or not sigma:
            return None
        
        return self.pricer.fair_value(spot, strike, seconds_left, sigma, direction)
    
    def evaluate(self, asset: str, strike: float, seconds_left: float,
                 direction: str, market_price: float, venue: str = "PM") -> Optional[Signal]:
        """
        Evaluate a contract for mispricing. Returns Signal if actionable edge found.
        """
        return self.scanner.evaluate(asset, strike, seconds_left, direction, market_price, venue)
    
    def get_vol(self, asset: str) -> Optional[float]:
        """Get current best vol estimate (annualized)"""
        return self.vol_tracker.get_best_vol(asset)
    
    def get_regime(self, asset: str) -> Optional[dict]:
        """Get vol regime for asset"""
        return self.vol_tracker.get_vol_regime(asset)
    
    def get_spot(self, asset: str) -> Optional[float]:
        """Get current Chainlink spot price (authoritative source for all price comparisons)"""
        return self.chainlink.get_price(asset)
    
    def get_status(self) -> dict:
        """Get full model status"""
        return self.vol_tracker.get_status()
    
    def format_status(self) -> str:
        """Human-readable status line for dashboard"""
        parts = []
        for asset in SYMBOLS:
            vol = self.vol_tracker.get_best_vol(asset)
            regime = self.vol_tracker.get_vol_regime(asset)
            cl_price = self.chainlink.get_price(asset)
            
            if vol:
                vol_str = f"{vol*100:.1f}%"
                regime_str = ""
                if regime:
                    r = regime["regime"]
                    if r == "expanding":
                        regime_str = "↑"
                    elif r == "contracting":
                        regime_str = "↓"
                price_str = f"${cl_price:,.2f}" if cl_price else "?"
                parts.append(f"{asset}:{price_str} σ={vol_str}{regime_str}")
        
        if not parts:
            return "Vol: warming up..."
        cl_status = "🟢" if self.chainlink.is_connected() else "🔴CL"
        
        # Show avg Chainlink tick rate
        intervals = [self.vol_tracker._tick_interval_ewma.get(a) for a in SYMBOLS if a in self.vol_tracker._tick_interval_ewma]
        tick_info = ""
        if intervals:
            avg_int = sum(intervals) / len(intervals)
            tick_info = f" | ~{1/avg_int:.1f} ticks/s" if avg_int > 0 else ""
        
        return f"{cl_status} " + " | ".join(parts) + tick_info
    
    # --- State persistence ---
    
    VOL_STATE_FILE = os.path.join(os.path.expanduser("~"), ".arb_data", "vol_state.json")
    
    def save_state(self):
        """Save current vol estimates to disk for fast restart"""
        try:
            state = {"saved_at": time.time(), "assets": {}}
            for asset in SYMBOLS:
                vol = self.vol_tracker.get_best_vol(asset)
                spot = self.chainlink.get_price(asset) or self.vol_tracker.latest_price.get(asset)
                regime = self.vol_tracker.get_vol_regime(asset)
                v5 = self.vol_tracker.get_realized_vol(asset, 5)
                v15 = self.vol_tracker.get_realized_vol(asset, 15)
                ewma_fast = self.vol_tracker.get_ewma_vol(asset, "fast")
                ewma_slow = self.vol_tracker.get_ewma_vol(asset, "slow")
                
                if vol and spot:
                    state["assets"][asset] = {
                        "vol": vol, "spot": spot,
                        "vol_5m": v5, "vol_15m": v15,
                        "ewma_fast": ewma_fast, "ewma_slow": ewma_slow,
                        "regime": regime["regime"] if regime else "unknown",
                    }
            
            os.makedirs(os.path.dirname(self.VOL_STATE_FILE), exist_ok=True)
            with open(self.VOL_STATE_FILE, "w") as f:
                json.dump(state, f)
        except Exception:
            pass
    
    def load_state(self) -> bool:
        """Load saved vol state. Returns True if loaded recent state."""
        try:
            if not os.path.exists(self.VOL_STATE_FILE):
                return False
            
            with open(self.VOL_STATE_FILE, "r") as f:
                state = json.load(f)
            
            age_min = (time.time() - state.get("saved_at", 0)) / 60
            if age_min > 60:  # Stale after 1 hour
                print(f"[Vol] Saved state is {age_min:.0f}min old — too stale, starting fresh")
                return False
            
            loaded = 0
            for asset, data in state.get("assets", {}).items():
                vol = data.get("vol")
                spot = data.get("spot")
                if vol and spot:
                    # Seed prices so pricing works immediately before WebSocket connects
                    self.vol_tracker.latest_price[asset] = spot  # For vol calc
                    self.chainlink._prices[asset] = spot  # For spot comparisons
                    self.chainlink._timestamps[asset] = time.time()
                    # Store saved vol as a reference for the floor
                    if not hasattr(self.vol_tracker, '_saved_vol'):
                        self.vol_tracker._saved_vol = {}
                    self.vol_tracker._saved_vol[asset] = data
                    loaded += 1
            
            if loaded > 0:
                print(f"[Vol] Loaded saved state ({age_min:.1f}min old): ", end="")
                for asset, data in state.get("assets", {}).items():
                    print(f"{asset}:σ={data['vol']*100:.0f}% ", end="")
                print()
                return True
            
            return False
        except Exception:
            return False
    
    def get_vol_with_saved_fallback(self, asset: str) -> Optional[float]:
        """Get vol, falling back to saved state during warmup"""
        live_vol = self.vol_tracker.get_best_vol(asset)
        
        # If live vol looks reasonable, use it
        floor = MIN_VOL_FLOOR.get(asset, 0.30)
        if live_vol and live_vol >= floor:
            return live_vol
        
        # Fall back to saved state
        if hasattr(self.vol_tracker, '_saved_vol') and asset in self.vol_tracker._saved_vol:
            saved = self.vol_tracker._saved_vol[asset]["vol"]
            if saved >= floor:
                return saved
        
        # Last resort: return None (will skip this asset)
        return None



# =============================================================================
# Config
# =============================================================================

GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"

DATA_DIR = os.path.join(os.path.expanduser("~"), ".arb_data")
SIM_TRADES_FILE = os.path.join(DATA_DIR, "sim_trades_v3.jsonl")
SIM_SUMMARY_FILE = os.path.join(DATA_DIR, "sim_summary_v3.json")


async def check_pm_resolution(client, asset: str, window_end_str: str, our_direction: str, our_token_id: str = "") -> Optional[bool]:
    """Check PM gamma API for market resolution.
    
    Returns True if we won, False if we lost, None if not yet resolved.
    
    PM markets resolve by setting winning token price to $1 and losing to $0.
    The gamma API shows 'resolved' status and outcome prices.
    """
    try:
        # Build the slug from asset and window start
        # window_end is the END of the 15-min window, we need the START
        end_dt = datetime.fromisoformat(window_end_str.replace("Z", "+00:00"))
        start_dt = end_dt - timedelta(minutes=15)
        ts = int(start_dt.timestamp())
        slug = f"{asset.lower()}-updown-15m-{ts}"
        
        r = await client.get(f"{GAMMA_URL}/markets", params={"slug": slug}, timeout=8)
        if r.status_code != 200:
            return None
        
        data = r.json()
        if not data:
            return None
        
        if isinstance(data, list):
            data = data[0]
        
        # Check if market is resolved
        resolved = data.get("resolved") or data.get("resolution_source")
        if not resolved:
            return None
        
        # Get outcome prices — winning outcome will have price near 1.0
        tokens = data.get("tokens", [])
        for token in tokens:
            outcome = (token.get("outcome", "") or "").lower()
            price = float(token.get("price", 0) or 0)
            winner = token.get("winner", False)
            
            # Determine which direction won
            if winner or price > 0.9:
                # This token won
                # For UP markets: YES token = UP wins, NO token = DOWN wins
                # For DOWN markets: YES token = DOWN wins, NO token = UP wins
                title = (data.get("question", "") or data.get("title", "")).lower()
                market_is_up = "up" in title
                
                if outcome == "yes":
                    winning_direction = "up" if market_is_up else "down"
                elif outcome == "no":
                    winning_direction = "down" if market_is_up else "up"
                else:
                    continue
                
                return our_direction == winning_direction
        
        # Market resolved but can't determine winner from tokens
        # Try outcome field directly
        outcome_str = (data.get("outcome", "") or "").lower()
        if outcome_str == "yes":
            title = (data.get("question", "") or data.get("title", "")).lower()
            market_is_up = "up" in title
            winning_direction = "up" if market_is_up else "down"
            return our_direction == winning_direction
        elif outcome_str == "no":
            title = (data.get("question", "") or data.get("title", "")).lower()
            market_is_up = "up" in title
            winning_direction = "down" if market_is_up else "up"
            return our_direction == winning_direction
        
        return None
        
    except Exception as e:
        return None


async def check_kalshi_resolution(kalshi_reader, ticker: str, our_direction: str) -> Optional[bool]:
    """Check Kalshi API for market resolution result.
    
    Kalshi markets have a 'result' field: "yes", "no", or "" (not yet resolved).
    For crypto 15-min markets:
      - An "above X" market: result="yes" means price was above strike
      - An "above X" market: result="no" means price was NOT above strike
    
    We map this to our direction ("up"/"down") to determine win/loss.
    
    Returns True if we won, False if we lost, None if not yet resolved.
    """
    if not kalshi_reader or not ticker:
        return None
    
    try:
        path = f"/markets/{ticker}"
        data = await kalshi_reader._req("GET", path)
        market = data.get("market", data)
        
        result = (market.get("result", "") or "").lower()
        if result not in ("yes", "no"):
            return None  # Not resolved yet
        
        # Kalshi crypto 15-min markets are "above X" style:
        #   result="yes" → price was above strike → UP won
        #   result="no"  → price was NOT above strike → DOWN won
        winning_direction = "up" if result == "yes" else "down"
        
        return our_direction == winning_direction
        
    except Exception:
        return None

# Trading parameters
MIN_EDGE_PCT = 2.0          # Minimum edge % to trigger paper trade
MAX_EDGE_PCT = 40.0         # Above this, assume stale data / model error
MIN_SECONDS_LEFT = 120      # Don't trade with < 2 min remaining
MAX_SECONDS_LEFT = 840      # Don't trade with > 14 min remaining (too early)
SLIPPAGE_CENTS = 3.0        # Assume 3¢ worse fill than displayed price
FILL_FAIL_RATE = 0.10       # 10% of orders fail to fill
FILL_DELAY_RANGE = (5, 45)  # Simulated seconds to get filled
FILL_DELAY_EXTRA_SLIP = 2.0 # Extra slippage (¢) from price movement during fill wait
SIM_STARTING_BALANCE = 200  # Simulated starting bankroll
KELLY_FRACTION = 0.25       # Quarter-Kelly for safety
BALANCE_RESERVE_PCT = 75    # Keep 75% as reserve
MIN_KELLY_SIZE = 5          # Polymarket minimum
MAX_KELLY_SIZE = 25         # Cap per trade

# Vol dampener — scales model vol down before pricing.
# Set to 1.0 (disabled) now that vol is computed from Chainlink ticks directly.
# Previously needed at 0.85x when using Binance (which runs ~1.2-2.5x hotter).
# Keep this lever available in case Chainlink vol still needs calibration.
VOL_DAMPENER = 1.0


def kelly_size(fair: float, market_price: float, balance: float) -> int:
    """Kelly criterion position sizing for binary contracts, weighted by bell curve confidence.
    
    The bell confidence weight scales down Kelly fraction for contracts
    far from $0.50 where our model is theoretically less reliable.
    Near ATM: full quarter-Kelly. Deep OTM/ITM: proportionally reduced.
    """
    if market_price <= 0.01 or market_price >= 0.99 or fair <= market_price:
        return 0
    
    p = fair
    q = 1 - p
    b = (1.0 - market_price) / market_price
    
    kelly_pct = (p * b - q) / b
    if kelly_pct <= 0:
        return 0
    
    # Apply bell curve confidence to Kelly fraction
    confidence = bell_confidence(market_price)
    adjusted_fraction = KELLY_FRACTION * confidence
    
    kelly_pct *= adjusted_fraction
    available = balance * (1 - BALANCE_RESERVE_PCT / 100)
    if available <= 0:
        return 0
    
    dollars = available * kelly_pct
    contracts = int(dollars / market_price)
    
    if contracts < MIN_KELLY_SIZE:
        if available >= MIN_KELLY_SIZE * market_price:
            return MIN_KELLY_SIZE
        return 0
    
    return min(contracts, MAX_KELLY_SIZE)
SCAN_INTERVAL = 15          # Seconds between scans
WARMUP_MINUTES = 3          # Minimum minutes of Binance data before trading
MIN_VOL_FLOOR = {           # Minimum plausible annualized vol per asset
    "BTC": 0.30,            # BTC never really below 30% vol
    "ETH": 0.40,            # ETH is more volatile
    "SOL": 0.50,            # SOL most volatile
}
CRYPTOS = ["btc", "eth", "sol"]

# PM API proxy — same proxy your main bot uses
# Set PM_PROXY env var, or it'll try to read from arb_scanner config
PM_PROXY = os.environ.get("PM_PROXY", None)
if not PM_PROXY:
    # Try reading from config.json (same dir as this script)
    _config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    if os.path.exists(_config_path):
        try:
            with open(_config_path) as _f:
                _cfg = json.load(_f)
            _px = _cfg.get("proxy", {})
            if _px.get("enabled"):
                PM_PROXY = f"http://{_px.get('username','')}:{_px.get('password','')}@{_px.get('host','')}:{_px.get('port','')}"
        except:
            pass

os.makedirs(DATA_DIR, exist_ok=True)


# =============================================================================
# Market data fetching (public APIs, no auth)
# =============================================================================

@dataclass
class MarketSnapshot:
    """A single market snapshot (PM or Kalshi)"""
    underlying: str         # "BTC", "ETH", "SOL"
    direction: str          # "up" or "down"
    slug: str
    condition_id: str
    token_id: str           # YES token (PM) or "" (Kalshi)
    no_token_id: str        # NO token (PM) or "" (Kalshi)
    strike_price: float
    end_time: datetime
    best_yes_ask: float     # Best ask for YES (= cost to buy UP)
    best_no_ask: float      # Best ask for NO (= cost to buy DOWN)
    yes_ask_size: float     # Depth at best ask
    no_ask_size: float
    strike_source: str = "" # "strikes.json", "kalshi_api", etc.
    venue: str = "pm"       # "pm" or "kalshi"
    kalshi_ticker: str = "" # Kalshi market ticker (e.g. KXBTC15M-26FEB071345-45)


async def fetch_pm_markets(client) -> list:
    """Fetch current 15-min crypto markets from PM gamma API"""
    markets = []
    now = datetime.now(timezone.utc)
    
    current_minute = now.minute
    current_slot_minute = (current_minute // 15) * 15
    slot_start = now.replace(minute=current_slot_minute, second=0, microsecond=0)
    
    for crypto in CRYPTOS:
        for offset in range(1):  # Current window only (future windows won't have collector strikes)
            window_start = slot_start + timedelta(minutes=15 * offset)
            ts = int(window_start.timestamp())
            slug = f"{crypto}-updown-15m-{ts}"
            
            try:
                r = await client.get(f"{GAMMA_URL}/markets", params={"slug": slug}, timeout=5)
                if r.status_code != 200:
                    if r.status_code != 404:
                        print(f"  [PM API] {slug}: HTTP {r.status_code}")
                    continue
                
                data = r.json()
                if not data:
                    continue
                
                if isinstance(data, list):
                    for item in data:
                        m = _parse_market(item, crypto.upper())
                        if m:
                            markets.append(m)
                        else:
                            print(f"  [PM API] Parse failed for {slug} (list item)")
                else:
                    m = _parse_market(data, crypto.upper())
                    if m:
                        markets.append(m)
                    else:
                        print(f"  [PM API] Parse failed for {slug}")
                        
            except Exception as e:
                print(f"  [PM API] {slug}: {type(e).__name__}: {e}")
                continue
    
    return markets


def _load_strikes_file():
    """Load strikes.json from home dir (same as main bot's strike_collector)"""
    if hasattr(_load_strikes_file, '_cache_time') and time.time() - _load_strikes_file._cache_time < 5:
        return _load_strikes_file._cache_data
    
    try:
        from pathlib import Path
        strikes_file = Path.home() / "strikes.json"
        if strikes_file.exists():
            with open(strikes_file, "r") as f:
                data = json.load(f)
            _load_strikes_file._cache_data = data.get("strikes", {})
            _load_strikes_file._cache_time = time.time()
            return _load_strikes_file._cache_data
    except:
        pass
    return {}





# Track Binance prices at window boundaries for strike approximation
_window_open_prices = {}  # "BTC_14:45" -> price


def record_window_open_price(asset: str, price: float):
    """Record price at each 15-min boundary for strike approximation"""
    now = datetime.now(timezone.utc)
    minute = now.minute
    slot_minute = (minute // 15) * 15
    key = f"{asset}_{now.strftime('%H')}:{slot_minute:02d}"
    if key not in _window_open_prices:
        _window_open_prices[key] = price


def _parse_market(d: dict, underlying: str) -> Optional[MarketSnapshot]:
    """Parse a PM gamma API response into MarketSnapshot"""
    try:
        title = d.get("question", "") or d.get("title", "")
        direction = "up" if "up" in title.lower() else "down"
        
        # Token IDs
        yes_tid = None
        no_tid = None
        tokens = d.get("tokens", [])
        for token in tokens:
            outcome = token.get("outcome", "").lower()
            tid = token.get("token_id") or token.get("tokenId")
            if tid:
                if outcome == "yes":
                    yes_tid = tid
                elif outcome == "no":
                    no_tid = tid
        
        if not yes_tid:
            clob_ids_raw = d.get("clobTokenIds") or d.get("clob_token_ids")
            if clob_ids_raw:
                if isinstance(clob_ids_raw, str):
                    clob_ids_raw = json.loads(clob_ids_raw)
                if len(clob_ids_raw) >= 1:
                    yes_tid = clob_ids_raw[0]
                if len(clob_ids_raw) >= 2:
                    no_tid = clob_ids_raw[1]
        
        if not yes_tid:
            return None
        
        # End time
        end_str = d.get("endDate") or d.get("endDateIso")
        if not end_str:
            return None
        end_time = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
        
        # Window start from eventStartTime or end_time - 15 min
        event_start_str = d.get("eventStartTime")
        if event_start_str:
            window_start = datetime.fromisoformat(event_start_str.replace("Z", "+00:00"))
        else:
            window_start = end_time - timedelta(minutes=15)
        
        # Strike price — these markets don't include it in the API.
        # The strike = Chainlink price at window open.
        strike = None
        strike_source = ""
        
        # strikes.json from strike_collector (only reliable source)
        strikes = _load_strikes_file()
        if strikes:
            key = f"{underlying}_{window_start.strftime('%H:%M')}"
            if key in strikes:
                data = strikes[key]
                if isinstance(data, dict):
                    strike = data.get("price")
                else:
                    strike = float(data)
                if strike:
                    strike_source = "strikes.json"
        
        if not strike:
            return None
        
        # Log strike source (once per market)
        slug = d.get("slug", "")
        if not hasattr(_parse_market, '_strike_logged'):
            _parse_market._strike_logged = set()
        if slug not in _parse_market._strike_logged:
            _parse_market._strike_logged.add(slug)
            print(f"  [Strike] {underlying} {direction.upper()} K={strike:,.2f} via {strike_source}")
        
        condition_id = d.get("conditionId") or d.get("condition_id") or ""
        slug = d.get("slug", "")
        
        mkt = MarketSnapshot(
            underlying=underlying,
            direction=direction,
            slug=slug,
            condition_id=condition_id,
            token_id=yes_tid or "",
            no_token_id=no_tid or "",
            strike_price=strike,
            end_time=end_time,
            best_yes_ask=0,
            best_no_ask=0,
            yes_ask_size=0,
            no_ask_size=0,
            strike_source=strike_source,
        )
        return mkt
    except Exception as e:
        print(f"  [Parse] Exception: {type(e).__name__}: {e}")
        return None


# Shared ref for current Binance spot prices (set by main loop)
_binance_spot_ref = {}


async def fetch_orderbook(client, token_id: str) -> tuple:
    """
    Fetch best ask price and size for a token from PM CLOB.
    Returns (best_ask_price, best_ask_size) or (0, 0) on failure.
    """
    try:
        r = await client.get(f"{CLOB_URL}/book", params={"token_id": token_id}, timeout=5)
        if r.status_code != 200:
            return (0, 0)
        
        d = r.json()
        asks = d.get("asks") or []
        if not asks:
            return (0, 0)
        
        # Sort by price ascending, get best (lowest) ask
        asks.sort(key=lambda x: float(x.get("price", 999)))
        best = asks[0]
        return (float(best.get("price", 0)), float(best.get("size", 0)))
    except Exception:
        return (0, 0)


# =============================================================================
# KalshiReader — Read-only Kalshi API client for market scanning
# =============================================================================

KALSHI_API_URL = "https://api.elections.kalshi.com/trade-api/v2"

def load_kalshi_credentials() -> Optional[dict]:
    """Load Kalshi API credentials from arb bot's config.json + PEM file.
    Returns dict with 'api_key_id' and 'private_key_pem', or None if missing."""
    script_dir = Path(__file__).parent
    config_path = script_dir / "config.json"
    pem_path = script_dir / "kalshi_private_key.pem"
    
    if not config_path.exists() or not pem_path.exists():
        return None
    
    try:
        with open(config_path) as f:
            keys = json.load(f)
        with open(pem_path) as f:
            pem_data = f.read()
        
        api_key_id = keys.get("kalshi_api_key_id", "")
        if not api_key_id or "YOUR_" in api_key_id:
            return None
        if "PASTE" in pem_data:
            return None
        
        return {"api_key_id": api_key_id, "private_key_pem": pem_data}
    except Exception:
        return None


class KalshiReader:
    """Read-only Kalshi API client for scanning markets and orderbooks.
    
    Uses the same RSA-PSS signing as the arb bot's KalshiConnector,
    but only supports GET requests (no trading).
    """
    
    def __init__(self, api_key_id: str, private_key_pem: str):
        if not _HAS_CRYPTO:
            raise ImportError("cryptography package required for Kalshi scanning")
        self.api_key_id = api_key_id
        self._private_key = serialization.load_pem_private_key(
            private_key_pem.encode(), password=None, backend=default_backend()
        )
        self._client = None
        self._ob_cache = {}       # ticker -> (time, result)
        self._market_cache = {}   # cache_key -> (time, markets)
        self._connected = False
    
    def _sign(self, ts: str, method: str, path: str) -> str:
        msg = f"{ts}{method}{path}".encode('utf-8')
        sig = self._private_key.sign(
            msg,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode('utf-8')
    
    def _headers(self, method: str, path: str) -> dict:
        ts = str(int(time.time() * 1000))
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-SIGNATURE": self._sign(ts, method, path),
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "Content-Type": "application/json",
        }
    
    async def connect(self) -> bool:
        """Initialize HTTP client and verify connection."""
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, connect=5.0), http2=True,
        )
        try:
            await self._req("GET", "/exchange/status")
            self._connected = True
            return True
        except Exception as e:
            print(f"  [Kalshi] Connection failed: {e}")
            return False
    
    async def close(self):
        if self._client:
            await self._client.aclose()
    
    async def _req(self, method: str, path: str, params=None) -> dict:
        # Signature must include /trade-api/v2 prefix
        full_path = f"/trade-api/v2{path}"
        headers = self._headers(method, full_path)
        r = await self._client.get(
            f"{KALSHI_API_URL}{path}", headers=headers, params=params
        )
        r.raise_for_status()
        return r.json()
    
    async def fetch_markets(self) -> list:
        """Fetch current 15-min crypto markets from Kalshi events API.
        Returns list of MarketSnapshot with venue='kalshi'."""
        import re
        markets = []
        now = datetime.now(timezone.utc)
        
        current_minute = now.minute
        current_slot_minute = (current_minute // 15) * 15
        slot_start = now.replace(minute=current_slot_minute, second=0, microsecond=0)
        
        min_close_ts = int(now.timestamp())
        max_close_ts = int((slot_start + timedelta(minutes=30)).timestamp())
        
        for crypto in ["BTC", "ETH", "SOL"]:
            series = f"KX{crypto}15M"
            try:
                r = await self._req("GET", "/events", params={
                    "series_ticker": series,
                    "with_nested_markets": "true",
                    "min_close_ts": min_close_ts,
                    "max_close_ts": max_close_ts,
                    "status": "open",
                    "limit": 20,
                })
                events = r.get("events", [])
                
                for evt in events:
                    event_sub_title = evt.get("sub_title", "")
                    event_meta = evt.get("product_metadata", {}) or {}
                    
                    for d in evt.get("markets", []):
                        try:
                            close = d.get("close_time") or d.get("expiration_time")
                            if not close:
                                continue
                            end_time = datetime.fromisoformat(close.replace("Z", "+00:00"))
                            
                            ticker = d.get("ticker", "")
                            if not ticker:
                                continue
                            
                            # Extract strike price from subtitle or metadata
                            strike = None
                            for field in ["subtitle", "yes_sub_title", "no_sub_title"]:
                                text = d.get(field, "")
                                if text and "$" in text:
                                    match = re.search(r'\$?([\d,]+\.?\d*)', text.replace(',', ''))
                                    if match:
                                        try:
                                            strike = float(match.group(1))
                                            break
                                        except:
                                            pass
                            
                            # Also check event-level subtitle
                            if not strike and event_sub_title and "$" in event_sub_title:
                                match = re.search(r'\$?([\d,]+\.?\d*)', event_sub_title.replace(',', ''))
                                if match:
                                    try:
                                        strike = float(match.group(1))
                                    except:
                                        pass
                            
                            # Check product_metadata
                            if not strike:
                                for key in ["strike_price", "strike", "reference_price"]:
                                    val = event_meta.get(key) or d.get(key)
                                    if val:
                                        try:
                                            strike = float(str(val).replace(',', '').replace('$', ''))
                                            break
                                        except:
                                            pass
                            
                            # Check numeric fields on market
                            if not strike:
                                for field in ["floor_strike", "cap_strike", "custom_strike"]:
                                    val = d.get(field)
                                    if val and isinstance(val, (int, float)) and val > 0:
                                        strike = float(val)
                                        break
                            
                            # NO FALLBACKS — if subtitle/metadata/numeric fields
                            # don't have the strike, skip this market entirely.
                            # Trading on stale prev-window data causes phantom edges.
                            if not strike:
                                continue
                            
                            # Kalshi markets are YES=above strike, so direction="up"
                            mkt = MarketSnapshot(
                                underlying=crypto,
                                direction="up",
                                slug=ticker,
                                condition_id="",
                                token_id="",
                                no_token_id="",
                                strike_price=strike,
                                end_time=end_time,
                                best_yes_ask=0,
                                best_no_ask=0,
                                yes_ask_size=0,
                                no_ask_size=0,
                                strike_source="kalshi_api",
                                venue="kalshi",
                                kalshi_ticker=ticker,
                            )
                            markets.append(mkt)
                            
                        except Exception as e:
                            continue
            
            except Exception as e:
                print(f"  [Kalshi] {series} fetch error: {e}")
                continue
        
        return markets
    
    async def fetch_orderbook(self, ticker: str) -> tuple:
        """Fetch Kalshi orderbook and return (yes_ask, yes_size, no_ask, no_size).
        
        Kalshi returns only bids. Asks are derived:
        - YES ask = (100 - highest NO bid) / 100
        - NO ask = (100 - highest YES bid) / 100
        """
        # Cache for 2 seconds
        cache_key = ticker
        if cache_key in self._ob_cache:
            cache_time, cached = self._ob_cache[cache_key]
            if time.time() - cache_time < 2:
                return cached
        
        try:
            r = await self._req("GET", f"/markets/{ticker}/orderbook")
            d = r.get("orderbook", {})
            yes_bids = d.get("yes") or []  # [[price_cents, qty], ...]
            no_bids = d.get("no") or []
            
            # YES ask = complement of highest NO bid
            yes_ask, yes_size = 0.0, 0.0
            if no_bids:
                # Sort NO bids descending by price
                no_bids.sort(key=lambda x: x[0], reverse=True)
                best_no_bid = no_bids[0]
                yes_ask = (100 - best_no_bid[0]) / 100.0
                yes_size = float(best_no_bid[1])
            
            # NO ask = complement of highest YES bid
            no_ask, no_size = 0.0, 0.0
            if yes_bids:
                yes_bids.sort(key=lambda x: x[0], reverse=True)
                best_yes_bid = yes_bids[0]
                no_ask = (100 - best_yes_bid[0]) / 100.0
                no_size = float(best_yes_bid[1])
            
            result = (yes_ask, yes_size, no_ask, no_size)
            self._ob_cache[cache_key] = (time.time(), result)
            return result
        except Exception:
            return (0.0, 0.0, 0.0, 0.0)
    
    async def place_limit_buy(self, ticker: str, direction: str, price: float, size: int) -> Optional[str]:
        """Place a limit buy order on Kalshi. Returns order_id or None."""
        side = "yes" if direction == "up" else "no"
        price_cents = int(round(price * 100))
        data = {
            "ticker": ticker,
            "action": "buy",
            "side": side,
            "type": "limit",
            "count": size,
            f"{side}_price": price_cents,
        }
        try:
            headers = self._headers("POST", "/trade-api/v2/portfolio/orders")
            r = await self._client.post(
                f"{KALSHI_API_URL}/portfolio/orders",
                headers=headers, json=data, timeout=10
            )
            r.raise_for_status()
            result = r.json()
            return result.get("order", {}).get("order_id")
        except Exception as e:
            print(f"  [Kalshi] Order failed: {e}")
            return None
    
    async def get_balance(self) -> Optional[float]:
        """Get Kalshi account balance in dollars."""
        try:
            r = await self._req("GET", "/portfolio/balance")
            bal = r.get("balance", 0)
            # Kalshi returns cents
            return bal / 100.0 if bal > 1 else float(bal)
        except:
            return None


async def fetch_kalshi_vol_markets(kalshi: KalshiReader) -> list:
    """Convenience wrapper — fetch Kalshi markets for vol scanning."""
    if not kalshi or not kalshi._connected:
        return []
    try:
        return await kalshi.fetch_markets()
    except Exception as e:
        print(f"  [Kalshi] Market fetch error: {e}")
        return []

def get_adaptive_vol(model: VolModel, asset: str, seconds_left: float) -> Optional[float]:
    """
    Select the best vol estimate based on time remaining in contract.
    
    >10 min: 15m rolling window (most stable)
    5-10 min: blend 5m (60%) and 15m (40%)
    2-5 min: 5m rolling window (recency matters)
    <2 min: fast EWMA (most reactive)
    
    If vol regime is "expanding", lean toward shorter window.
    Final result is scaled by VOL_DAMPENER to correct systematic overestimation.
    """
    tracker = model.vol_tracker
    regime = tracker.get_vol_regime(asset)
    expanding = regime and regime["ratio"] > 1.5
    
    vol_5m = tracker.get_realized_vol(asset, 5)
    vol_15m = tracker.get_realized_vol(asset, 15)
    ewma_fast = tracker.get_ewma_vol(asset, "fast")
    
    raw_vol = None
    
    if seconds_left > 600:  # >10 min
        if expanding and vol_5m:
            # Vol spiking — blend in shorter window
            base = vol_15m or vol_5m
            raw_vol = 0.6 * vol_5m + 0.4 * base if vol_5m and base else vol_5m or base
        else:
            raw_vol = vol_15m or vol_5m or ewma_fast
    
    elif seconds_left > 300:  # 5-10 min
        if vol_5m and vol_15m:
            raw_vol = 0.6 * vol_5m + 0.4 * vol_15m
        else:
            raw_vol = vol_5m or vol_15m or ewma_fast
    
    elif seconds_left > 120:  # 2-5 min
        raw_vol = vol_5m or ewma_fast or vol_15m
    
    else:  # <2 min
        raw_vol = ewma_fast or vol_5m
    
    if raw_vol is None:
        return None
    
    return raw_vol * VOL_DAMPENER


# =============================================================================
# Paper trade tracking
# =============================================================================

@dataclass
class PaperTrade:
    """A simulated trade"""
    trade_id: str
    timestamp: str
    asset: str
    direction: str          # "up" or "down" — what we're buying
    strike: float
    spot_at_entry: float
    seconds_left: float
    
    # Pricing
    model_vol: float
    model_fair: float       # Our fair value
    market_price: float     # What PM was showing
    fill_price: float       # market_price + slippage
    edge_pct: float         # (model_fair - fill_price) / fill_price * 100
    
    # Vol context
    vol_regime: str
    implied_vol: Optional[float]
    
    # Trade
    size: int
    cost: float             # fill_price * size
    bell_confidence: float = 1.0  # Bell curve confidence weight at entry price
    
    # Settlement (filled in later)
    settled: bool = False
    spot_at_settle: Optional[float] = None
    won: Optional[bool] = None
    payout: float = 0       # size * $1 if won, else $0
    pnl: float = 0          # payout - cost
    settle_time: Optional[str] = None
    window_end: Optional[str] = None
    venue: str = "pm"               # "pm" or "kalshi"
    kalshi_ticker: str = ""         # Kalshi market ticker for API resolution


class PaperTrader:
    """Manages simulated trades and tracks P&L with Kelly sizing"""
    
    def __init__(self):
        self.pending: list[PaperTrade] = []
        self.settled: list[PaperTrade] = []
        self.trade_count = 0
        self.sim_balance = SIM_STARTING_BALANCE
        
        # Running stats
        self.total_trades = 0
        self.wins = 0
        self.losses = 0
        self.total_pnl = 0
        self.total_cost = 0
        self.total_payout = 0
        
        # Per-venue stats: {"pm": {wins, losses, pnl}, "kalshi": {wins, losses, pnl}}
        self.venue_stats = {
            "pm": {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "cost": 0.0},
            "kalshi": {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "cost": 0.0},
        }
        
        # Dedup: don't trade same market twice
        self._traded_markets: set = set()
        
        # Load previous session data
        self._load_from_journal()
    
    def _load_from_journal(self):
        """Restore stats from sim_trades.jsonl on startup"""
        if not os.path.exists(SIM_TRADES_FILE):
            return
        
        settled_count = 0
        open_ids = set()
        settled_ids = set()
        max_trade_num = 0
        
        try:
            with open(SIM_TRADES_FILE, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    
                    event = entry.get("event")
                    trade_id = entry.get("trade_id", "")
                    
                    # Track highest trade number for continuing sequence
                    if trade_id.startswith("SIM-"):
                        try:
                            num = int(trade_id.split("-")[1])
                            max_trade_num = max(max_trade_num, num)
                        except (ValueError, IndexError):
                            pass
                    
                    if event == "OPEN":
                        open_ids.add(trade_id)
                        # Track market dedup
                        market_key = f"{entry.get('asset')}_{entry.get('window_end')}"
                        self._traded_markets.add(market_key)
                    
                    elif event == "SETTLE":
                        settled_ids.add(trade_id)
                        settled_count += 1
                        self.total_trades += 1
                        self.total_cost += entry.get("cost", 0)
                        self.total_payout += entry.get("payout", 0)
                        self.total_pnl += entry.get("pnl", 0)
                        if entry.get("won"):
                            self.wins += 1
                        else:
                            self.losses += 1
                        
                        # Per-venue stats
                        v = entry.get("venue", "pm")
                        if v not in self.venue_stats:
                            self.venue_stats[v] = {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "cost": 0.0}
                        vs = self.venue_stats[v]
                        vs["trades"] += 1
                        vs["pnl"] += entry.get("pnl", 0)
                        vs["cost"] += entry.get("cost", 0)
                        if entry.get("won"):
                            vs["wins"] += 1
                        else:
                            vs["losses"] += 1
                        
                        # Rebuild settled list (last 10 for display)
                        t = PaperTrade(
                            trade_id=trade_id,
                            timestamp=entry.get("ts", ""),
                            asset=entry.get("asset", ""),
                            direction=entry.get("dir", ""),
                            strike=entry.get("strike", 0),
                            spot_at_entry=entry.get("spot_entry", 0),
                            seconds_left=entry.get("sec_left", 0),
                            model_vol=entry.get("model_vol", 0),
                            model_fair=entry.get("model_fair", 0),
                            market_price=entry.get("mkt_price", 0),
                            fill_price=entry.get("fill_price", 0),
                            edge_pct=entry.get("edge_pct", 0),
                            vol_regime=entry.get("regime", ""),
                            implied_vol=entry.get("iv"),
                            size=entry.get("size", MIN_KELLY_SIZE),
                            cost=entry.get("cost", 0),
                            bell_confidence=entry.get("bell_confidence", 1.0),
                            settled=True,
                            spot_at_settle=entry.get("spot_settle"),
                            won=entry.get("won"),
                            payout=entry.get("payout", 0),
                            pnl=entry.get("pnl", 0),
                            settle_time=entry.get("ts"),
                            window_end=entry.get("window_end", ""),
                            venue=entry.get("venue", "pm"),
                            kalshi_ticker=entry.get("kalshi_ticker", ""),
                        )
                        self.settled.append(t)
            
            # Only keep last 20 settled for display
            if len(self.settled) > 20:
                self.settled = self.settled[-20:]
            
            # Continue trade numbering from where we left off
            self.trade_count = max_trade_num
            
            # Count unsettled trades (opened but never settled — lost on restart)
            unsettled = open_ids - settled_ids
            
            if settled_count > 0 or unsettled:
                # Reconstruct simulated balance
                self.sim_balance = SIM_STARTING_BALANCE - self.total_cost + self.total_payout
                win_rate = (self.wins / self.total_trades * 100) if self.total_trades > 0 else 0
                print(f"[Loaded] {settled_count} settled trades ({self.wins}W/{self.losses}L = {win_rate:.0f}%) P&L: ${self.total_pnl:+.2f} Bal: ${self.sim_balance:.2f}")
                # Show venue breakdown if we have both
                active_venues = {v: s for v, s in self.venue_stats.items() if s.get("trades", 0) > 0}
                if len(active_venues) > 1:
                    parts = []
                    for v, s in active_venues.items():
                        wr_v = (s["wins"] / s["trades"] * 100) if s["trades"] > 0 else 0
                        parts.append(f"{v.upper()}: {s['wins']}W/{s['losses']}L={wr_v:.0f}% ${s['pnl']:+.2f}")
                    print(f"[Loaded] By venue: {' | '.join(parts)}")
                if unsettled:
                    print(f"[Loaded] {len(unsettled)} trades from previous session expired unsettled (dropped)")
        
        except Exception as e:
            print(f"[Load] Error reading journal: {e}")
    
    def open_trade(self, asset: str, direction: str, strike: float,
                   spot: float, seconds_left: float, model_vol: float,
                   model_fair: float, market_price: float, implied_vol: float,
                   vol_regime: str, window_end: datetime,
                   venue: str = "pm", kalshi_ticker: str = "",
                   bell_confidence: float = 1.0) -> Optional[PaperTrade]:
        """Record a new paper trade with confidence-weighted Kelly sizing and fill simulation"""
        self.trade_count += 1
        
        # --- Fill simulation ---
        # 10% of orders fail to fill (book moves, gets sniped, etc.)
        if random.random() < FILL_FAIL_RATE:
            return "FILL_FAILED"  # Sentinel — caller checks for this
        
        # Simulate fill delay: 5-45 seconds of waiting
        fill_delay = random.uniform(*FILL_DELAY_RANGE)
        effective_seconds_left = seconds_left - fill_delay
        
        # If fill delay would push us past expiry, fail
        if effective_seconds_left < 60:
            return "FILL_FAILED"
        
        # Extra slippage from price movement during fill wait (0 to FILL_DELAY_EXTRA_SLIP ¢)
        extra_slip = random.uniform(0, FILL_DELAY_EXTRA_SLIP) / 100
        
        fill_price = market_price + (SLIPPAGE_CENTS / 100) + extra_slip
        fill_price = min(fill_price, 0.99)
        
        # Kelly-based position sizing
        size = kelly_size(model_fair, fill_price, self.sim_balance)
        if size == 0:
            return None  # Kelly says don't trade
        
        cost = round(fill_price * size, 4)
        edge_pct = ((model_fair - fill_price) / fill_price) * 100 if fill_price > 0 else 0
        
        trade = PaperTrade(
            trade_id=f"SIM-{self.trade_count:04d}",
            timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            asset=asset,
            direction=direction,
            strike=strike,
            spot_at_entry=spot,
            seconds_left=round(effective_seconds_left, 1),
            model_vol=round(model_vol, 4),
            model_fair=round(model_fair, 4),
            market_price=round(market_price, 4),
            fill_price=round(fill_price, 4),
            edge_pct=round(edge_pct, 2),
            vol_regime=vol_regime,
            implied_vol=round(implied_vol, 4) if implied_vol else None,
            size=size,
            cost=cost,
            bell_confidence=round(bell_confidence, 4),
            window_end=window_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            venue=venue,
            kalshi_ticker=kalshi_ticker,
        )
        
        # Deduct cost from simulated balance
        self.sim_balance -= cost
        
        self.pending.append(trade)
        market_key = f"{asset}_{window_end.isoformat()}"
        self._traded_markets.add(market_key)
        
        self._log_trade(trade, "OPEN")
        return trade
    
    def is_already_traded(self, asset: str, direction: str, window_end: datetime) -> bool:
        # One trade per asset per window — UP and DOWN are the same underlying bet
        market_key = f"{asset}_{window_end.isoformat()}"
        return market_key in self._traded_markets
    
    async def check_settlements(self, model: VolModel, http_client=None, kalshi_reader=None, cfb_rti=None):
        """Check if any pending trades have settled.
        
        Resolution priority by venue:
          PM trades:    1) PM API  2) Chainlink spot vs strike
          Kalshi trades: 1) Kalshi API result  2) CFB RTI estimate  3) Chainlink fallback
        """
        now = datetime.now(timezone.utc)
        still_pending = []
        newly_settled = False
        
        for trade in self.pending:
            end_time = datetime.fromisoformat(trade.window_end.replace("Z", "+00:00"))
            
            # Wait 90 seconds after window end for resolution
            if now < end_time + timedelta(seconds=90):
                still_pending.append(trade)
                continue
            
            won = None
            venue = getattr(trade, 'venue', 'pm')
            
            # ── Venue-specific resolution ──────────────────────────
            if venue == "kalshi":
                # 1) Try Kalshi API (authoritative for Kalshi trades)
                if kalshi_reader:
                    try:
                        ticker = getattr(trade, 'kalshi_ticker', '')
                        won = await check_kalshi_resolution(
                            kalshi_reader, ticker, trade.direction
                        )
                        if won is not None:
                            print(f"  [K-API] {trade.trade_id} resolved via Kalshi API: {'WON' if won else 'LOST'}")
                    except Exception:
                        pass
                
                # 2) Fallback: CFB RTI estimate (same source as Kalshi settlement)
                if won is None and cfb_rti:
                    rti_spot = cfb_rti.get_price(trade.asset)
                    if rti_spot and not cfb_rti.is_stale(trade.asset, max_age_seconds=60):
                        trade.spot_at_settle = rti_spot
                        if trade.direction == "up":
                            won = rti_spot > trade.strike
                        else:
                            won = rti_spot < trade.strike
                        
                        margin_pct = abs(rti_spot - trade.strike) / trade.strike * 100
                        source = "CFB-RTI"
                        if margin_pct < 0.15:
                            print(f"  ⚠️  {trade.trade_id} TIGHT margin {margin_pct:.3f}% — {source} may differ from actual Kalshi settlement")
                        else:
                            print(f"  [CFB-RTI] {trade.trade_id} resolved via RTI estimate: {'WON' if won else 'LOST'} (margin={margin_pct:.2f}%)")
            else:
                # PM venue: Try PM API first (authoritative)
                if http_client:
                    try:
                        won = await check_pm_resolution(
                            http_client, trade.asset, trade.window_end, trade.direction
                        )
                        if won is not None:
                            print(f"  [PM] {trade.trade_id} resolved via PM API: {'WON' if won else 'LOST'}")
                    except Exception:
                        pass
            
            # Record spot for logging (use venue-appropriate source)
            if venue == "kalshi" and cfb_rti:
                spot = cfb_rti.get_price(trade.asset) or model.get_spot(trade.asset)
            else:
                spot = model.get_spot(trade.asset)
            if spot and not trade.spot_at_settle:
                trade.spot_at_settle = spot
            
            # ── Universal fallback: Chainlink spot vs strike ──────
            if won is None:
                age_min = (now - end_time).total_seconds() / 60
                
                # Relax staleness for settlement — 300s is fine, we just need
                # a price that was valid around the settlement window
                stale = not spot or model.chainlink.is_stale(trade.asset, max_age_seconds=300)
                
                if stale and age_min < 60:
                    # Still early — keep waiting for fresh data
                    if age_min > 10:
                        print(f"  ⚠️  {trade.trade_id} unresolved after {age_min:.0f}min — waiting for spot data")
                    still_pending.append(trade)
                    continue
                
                if stale and age_min >= 60:
                    # Force-settle: use Binance spot as last resort rather
                    # than leaving trades unresolved forever
                    binance_spot = model.vol_tracker.latest_price.get(trade.asset)
                    if binance_spot:
                        spot = binance_spot
                        print(f"  ⚠️  {trade.trade_id} force-settling after {age_min:.0f}min using Binance spot ${spot:,.2f}")
                    else:
                        # Truly no data anywhere — give up and mark as loss
                        print(f"  ❌  {trade.trade_id} force-settling after {age_min:.0f}min — NO spot data, marking as loss")
                        won = False
                
                if won is None and spot:
                    if trade.direction == "up":
                        won = spot > trade.strike
                    else:
                        won = spot < trade.strike
                    
                    margin_pct = abs(spot - trade.strike) / trade.strike * 100
                    fallback_src = "Chainlink" if venue == "pm" else "Binance(⚠️approx)"
                    if margin_pct < 0.15:
                        print(f"  ⚠️  {trade.trade_id} TIGHT margin {margin_pct:.3f}% — {fallback_src} fallback")
                
                if won is None:
                    won = False  # Should not reach here, but don't leave unresolved
            
            trade.settled = True
            trade.settle_time = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            trade.won = won
            
            trade.payout = trade.size * 1.0 if trade.won else 0
            trade.pnl = round(trade.payout - trade.cost, 4)
            
            # Update simulated balance (cost already deducted at open)
            self.sim_balance += trade.payout
            
            # Update stats
            self.total_trades += 1
            self.total_cost += trade.cost
            self.total_payout += trade.payout
            self.total_pnl += trade.pnl
            if trade.won:
                self.wins += 1
            else:
                self.losses += 1
            
            # Per-venue stats
            v = getattr(trade, 'venue', 'pm')
            if v not in self.venue_stats:
                self.venue_stats[v] = {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "cost": 0.0}
            vs = self.venue_stats[v]
            vs["trades"] += 1
            vs["pnl"] += trade.pnl
            vs["cost"] += trade.cost
            if trade.won:
                vs["wins"] += 1
            else:
                vs["losses"] += 1
            
            self.settled.append(trade)
            self._log_trade(trade, "SETTLE")
            newly_settled = True
        
        self.pending = still_pending
        
        # Save summary whenever trades settle
        if newly_settled:
            self.save_summary()
    
    def _log_trade(self, trade: PaperTrade, event: str):
        """Log trade to JSONL file"""
        try:
            entry = {
                "event": event,
                "trade_id": trade.trade_id,
                "ts": trade.timestamp if event == "OPEN" else trade.settle_time,
                "asset": trade.asset,
                "dir": trade.direction,
                "strike": trade.strike,
                "spot_entry": trade.spot_at_entry,
                "sec_left": trade.seconds_left,
                "model_vol": trade.model_vol,
                "model_fair": trade.model_fair,
                "mkt_price": trade.market_price,
                "fill_price": trade.fill_price,
                "edge_pct": trade.edge_pct,
                "regime": trade.vol_regime,
                "iv": trade.implied_vol,
                "size": trade.size,
                "cost": trade.cost,
                "bell_confidence": getattr(trade, 'bell_confidence', 1.0),
                "window_end": trade.window_end,
                "venue": getattr(trade, 'venue', 'pm'),
                "kalshi_ticker": getattr(trade, 'kalshi_ticker', ''),
            }
            if event == "SETTLE":
                entry.update({
                    "spot_settle": trade.spot_at_settle,
                    "won": trade.won,
                    "payout": trade.payout,
                    "pnl": trade.pnl,
                })
            
            with open(SIM_TRADES_FILE, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception:
            pass
    
    def save_summary(self):
        """Save running summary to JSON"""
        try:
            win_rate = (self.wins / self.total_trades * 100) if self.total_trades > 0 else 0
            avg_edge = 0
            if self.settled:
                avg_edge = sum(t.edge_pct for t in self.settled) / len(self.settled)
            
            summary = {
                "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "strategy": "bell_curve_kelly_v2",
                "bell_k": BELL_K,
                "vol_dampener": VOL_DAMPENER,
                "vol_source": "chainlink",
                "total_trades": self.total_trades,
                "wins": self.wins,
                "losses": self.losses,
                "win_rate": round(win_rate, 1),
                "total_cost": round(self.total_cost, 2),
                "total_payout": round(self.total_payout, 2),
                "total_pnl": round(self.total_pnl, 2),
                "avg_edge_pct": round(avg_edge, 2),
                "pending_trades": len(self.pending),
                "slippage_assumption": SLIPPAGE_CENTS,
                "sizing": f"Kelly {KELLY_FRACTION*100:.0f}% × bell_conf(k={BELL_K}) ({MIN_KELLY_SIZE}-{MAX_KELLY_SIZE})",
                "sim_balance": round(self.sim_balance, 2),
                "min_edge_pct": MIN_EDGE_PCT,
                "venue_stats": {
                    v: {k: round(val, 2) if isinstance(val, float) else val for k, val in vs.items()}
                    for v, vs in self.venue_stats.items() if vs.get("trades", 0) > 0
                },
            }
            
            with open(SIM_SUMMARY_FILE, "w") as f:
                json.dump(summary, f, indent=2)
        except Exception:
            pass
    
    def format_dashboard(self) -> str:
        """Format stats for console display"""
        lines = []
        
        win_rate = (self.wins / self.total_trades * 100) if self.total_trades > 0 else 0
        
        lines.append(f"  Trades: {self.total_trades} settled ({self.wins}W/{self.losses}L = {win_rate:.0f}%) | {len(self.pending)} pending | Bal: ${self.sim_balance:.2f}")
        
        if self.total_trades > 0:
            lines.append(f"  P&L: ${self.total_pnl:+.2f} (cost: ${self.total_cost:.2f}, payout: ${self.total_payout:.2f})")
            
            # Per-venue breakdown (only show venues with trades)
            venue_parts = []
            for v in ["pm", "kalshi"]:
                vs = self.venue_stats.get(v, {})
                t = vs.get("trades", 0)
                if t > 0:
                    w = vs.get("wins", 0)
                    l = vs.get("losses", 0)
                    wr = (w / t * 100) if t > 0 else 0
                    pnl = vs.get("pnl", 0)
                    venue_parts.append(f"{v.upper()}: {w}W/{l}L={wr:.0f}% ${pnl:+.2f}")
            if len(venue_parts) > 1:
                lines.append(f"  By venue: {' | '.join(venue_parts)}")
            
            # Per-trade average
            avg_pnl = self.total_pnl / self.total_trades
            lines.append(f"  Avg P&L/trade: ${avg_pnl:+.2f} | Slippage assumption: {SLIPPAGE_CENTS}¢")
        
        # Show recent settled trades
        recent = self.settled[-3:]
        if recent:
            lines.append("  Recent:")
            for t in reversed(recent):
                icon = "✓" if t.won else "✗"
                vtag = f" [{t.venue.upper()}]" if getattr(t, 'venue', 'pm') != 'pm' else ""
                lines.append(f"    {icon} {t.trade_id}{vtag} {t.asset} {t.direction.upper()} | fair={t.model_fair:.3f} fill={t.fill_price:.3f} edge={t.edge_pct:+.1f}% → ${t.pnl:+.2f}")
        
        # Show pending
        if self.pending:
            lines.append("  Pending:")
            for t in self.pending:
                end = datetime.fromisoformat(t.window_end.replace("Z", "+00:00"))
                secs = (end - datetime.now(timezone.utc)).total_seconds()
                lines.append(f"    ⏳ {t.trade_id} {t.asset} {t.direction.upper()} @ {t.fill_price:.3f} | settles in {max(0,secs):.0f}s")
        
        return "\n".join(lines)


# =============================================================================
# Main simulation loop
# =============================================================================

async def run_simulator():
    print("=" * 60)
    print("  Vol Model Paper Trader v2 — Bell Curve Confidence")
    print("=" * 60)
    print(f"  Min edge: {MIN_EDGE_PCT}% | Slippage: {SLIPPAGE_CENTS}¢ | Kelly {KELLY_FRACTION*100:.0f}% × bell_conf(k={BELL_K}) | Vol source: Chainlink | Dampener: {VOL_DAMPENER}x")
    print(f"  Bell curve: @$0.50→100% @$0.35→{bell_confidence(0.35)*100:.0f}% @$0.20→{bell_confidence(0.20)*100:.0f}% @$0.10→{bell_confidence(0.10)*100:.0f}%")
    print(f"  Fill sim: {FILL_FAIL_RATE*100:.0f}% fail rate | {FILL_DELAY_RANGE[0]}-{FILL_DELAY_RANGE[1]}s delay | +0-{FILL_DELAY_EXTRA_SLIP}¢ slip")
    print(f"  Window: {MIN_SECONDS_LEFT}s - {MAX_SECONDS_LEFT}s remaining")
    print(f"  Scanning {', '.join(c.upper() for c in CRYPTOS)} on Polymarket + Kalshi")
    print(f"  Data: {SIM_TRADES_FILE}")
    print("=" * 60)
    print()
    
    # Start vol model
    model = VolModel()
    has_saved_state = model.load_state()
    await model.start()
    if not has_saved_state:
        print("[Vol] No saved state — need ~5 min of Chainlink ticks to warm up")
    else:
        print("[Vol] Using saved vol estimates — trading immediately, refining with live data")
    
    trader = PaperTrader()
    pricer = BinaryPricer()
    
    if httpx is None:
        print("ERROR: httpx not installed. Run: pip install httpx")
        return
    
    scan_count = 0
    last_dashboard = 0
    
    # Build httpx client with optional proxy
    client_kwargs = {"timeout": 10}
    if PM_PROXY:
        # httpx 0.28+ uses proxy=, older uses proxies=
        try:
            client_kwargs["proxy"] = PM_PROXY
        except:
            pass
        print(f"[PM] Using proxy: {PM_PROXY.split('@')[1] if '@' in PM_PROXY else PM_PROXY}")
    else:
        print("[PM] No proxy configured — PM API calls going direct")
        print("[PM] Set PM_PROXY env var if PM API isn't accessible from your IP")
    
    try:
        client = httpx.AsyncClient(**client_kwargs)
    except TypeError:
        # Older httpx — use proxies= instead
        if "proxy" in client_kwargs:
            p = client_kwargs.pop("proxy")
            client_kwargs["proxies"] = {"http://": p, "https://": p}
        client = httpx.AsyncClient(**client_kwargs)
    
    # Initialize Kalshi reader (optional — needs credentials + cryptography)
    kalshi = None
    kalshi_creds = load_kalshi_credentials()
    if kalshi_creds and _HAS_CRYPTO:
        try:
            kalshi = KalshiReader(kalshi_creds["api_key_id"], kalshi_creds["private_key_pem"])
        except Exception as e:
            print(f"[Kalshi] Init failed: {e} — scanning PM only")
            kalshi = None
    elif not _HAS_CRYPTO:
        print("[Kalshi] cryptography not installed — pip install cryptography — scanning PM only")
    else:
        print("[Kalshi] No credentials found (config.json / kalshi_private_key.pem) — scanning PM only")
    
    # Initialize CFB RTI Estimator (for Kalshi-venue pricing and settlement)
    cfb_rti = None
    try:
        from cfb_rti_estimator import CFBRTIEstimator
        cfb_rti = CFBRTIEstimator()
        cfb_rti.start()
        print("[CFB-RTI] ✅ Started — Coinbase/Kraken/Bitstamp feeds for Kalshi settlement")
    except ImportError:
        print("[CFB-RTI] cfb_rti_estimator.py not found — using Chainlink for all venues")
    except Exception as e:
        print(f"[CFB-RTI] Init failed: {e} — using Chainlink for all venues")
    
    async with client:
        # Connect Kalshi after entering async context
        if kalshi:
            if await kalshi.connect():
                bal = await kalshi.get_balance()
                bal_str = f" | Balance: ${bal:.2f}" if bal is not None else ""
                print(f"[Kalshi] ✅ Connected — scanning both PM and Kalshi{bal_str}")
            else:
                kalshi = None
                print("[Kalshi] Connection failed — scanning PM only")
        
        while True:
            try:
                scan_count += 1
                now = datetime.now(timezone.utc)
                
                # Check settlements first
                await trader.check_settlements(model, client, kalshi_reader=kalshi, cfb_rti=cfb_rti)
                
                # Dashboard every 30 seconds
                if time.time() - last_dashboard >= 30:
                    last_dashboard = time.time()
                    
                    print(f"\n{'='*60}")
                    print(f"  {now.strftime('%H:%M:%S')} UTC | Scan #{scan_count}")
                    print(f"  {model.format_status()}")
                    if cfb_rti:
                        print(f"  {cfb_rti.status()}")
                    print(f"{'='*60}")
                    print(trader.format_dashboard())
                    print()
                    
                    trader.save_summary()
                    model.save_state()  # Persist vol for fast restarts
                
                # Need vol data to trade
                has_vol = any(model.get_vol(a) for a in ["BTC", "ETH", "SOL"])
                has_usable_vol = any(model.get_vol_with_saved_fallback(a) for a in ["BTC", "ETH", "SOL"])
                
                # Start warmup timer from first tick data (regardless of vol level)
                any_ticks = any(model.vol_tracker.tick_count.get(a, 0) > 0 for a in ["BTC", "ETH", "SOL"])
                if any_ticks and not hasattr(model, '_first_tick_time'):
                    model._first_tick_time = time.time()
                
                if not has_usable_vol:
                    # Show warmup progress even when vol is below floor
                    if hasattr(model, '_first_tick_time') and scan_count % 4 == 0:
                        elapsed = (time.time() - model._first_tick_time) / 60
                        print(f"  [Warmup] {elapsed:.1f}/{WARMUP_MINUTES} min — vol below floor, building data...")
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                
                # Warmup: need enough tick data for vol estimates to be reliable
                if not has_saved_state and hasattr(model, '_first_tick_time'):
                    warmup_elapsed = (time.time() - model._first_tick_time) / 60
                    if warmup_elapsed < WARMUP_MINUTES:
                        if scan_count % 4 == 0:
                            print(f"  [Warmup] {warmup_elapsed:.1f}/{WARMUP_MINUTES} min — almost ready")
                        await asyncio.sleep(SCAN_INTERVAL)
                        continue
                
                # Fetch current markets (PM + Kalshi)
                markets = await fetch_pm_markets(client)
                k_markets = await fetch_kalshi_vol_markets(kalshi)
                if k_markets:
                    markets.extend(k_markets)
                
                pm_count = sum(1 for m in markets if m.venue == "pm")
                k_count = sum(1 for m in markets if m.venue == "kalshi")
                
                if scan_count % 2 == 0:
                    venue_str = f"{pm_count} PM"
                    if k_count:
                        venue_str += f" + {k_count} Kalshi"
                    print(f"  [{now.strftime('%H:%M:%S')}] Found {venue_str} markets")
                    for m in markets[:6]:
                        label = m.token_id[:20] if m.token_id else m.kalshi_ticker or 'NONE'
                        print(f"    [{m.venue.upper():2s}] {m.underlying} {m.direction} strike={m.strike_price} {label}... ends={m.end_time.strftime('%H:%M')}")
                
                if not markets:
                    if scan_count % 4 == 0:
                        print(f"  [Debug] No markets from PM or Kalshi")
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                
                # Evaluate each market — collect candidates, then pick best per asset/window
                evaluated = 0
                skipped_time = 0
                skipped_dedup = 0
                skipped_vol = 0
                skipped_strike = 0
                skipped_book = 0
                best_edge_seen = -999
                best_edge_label = ""
                
                # Candidates: keyed by (asset, window_end) → best candidate
                candidates = {}  # (asset, window_end_iso) → {edge, trade_params...}
                
                for mkt in markets:
                    # Time filter
                    seconds_left = (mkt.end_time - now).total_seconds()
                    if seconds_left < MIN_SECONDS_LEFT or seconds_left > MAX_SECONDS_LEFT:
                        skipped_time += 1
                        continue
                    
                    # Already traded this asset in this window?
                    if trader.is_already_traded(mkt.underlying, mkt.direction, mkt.end_time):
                        skipped_dedup += 1
                        continue
                    
                    # Strike check
                    if not mkt.strike_price:
                        skipped_strike += 1
                        continue
                    
                    # Get vol
                    vol = get_adaptive_vol(model, mkt.underlying, seconds_left)
                    vol_floor = MIN_VOL_FLOOR.get(mkt.underlying, 0.30)
                    
                    # If live vol is below floor, try saved state
                    if not vol or vol < vol_floor:
                        vol = model.get_vol_with_saved_fallback(mkt.underlying)
                    
                    if not vol or vol < vol_floor:
                        skipped_vol += 1
                        continue
                    
                    # Use venue-appropriate spot price:
                    # Kalshi settles on CF Benchmarks RTI, PM on Chainlink
                    if mkt.venue == "kalshi" and cfb_rti:
                        spot = cfb_rti.get_price(mkt.underlying)
                        if not spot or cfb_rti.is_stale(mkt.underlying):
                            spot = model.get_spot(mkt.underlying)  # Chainlink fallback
                    else:
                        spot = model.get_spot(mkt.underlying)
                    if not spot:
                        skipped_vol += 1
                        continue
                    
                    # Fetch orderbook — route based on venue
                    if mkt.venue == "kalshi" and kalshi:
                        yes_ask, yes_size, no_ask, no_size = await kalshi.fetch_orderbook(mkt.kalshi_ticker)
                    else:
                        yes_ask, yes_size = await fetch_orderbook(client, mkt.token_id)
                        no_ask, no_size = (0, 0)
                        if mkt.no_token_id:
                            no_ask, no_size = await fetch_orderbook(client, mkt.no_token_id)
                    
                    if yes_ask <= 0.01 and no_ask <= 0.01:
                        skipped_book += 1
                        continue
                    
                    # Determine what to evaluate
                    # For an "up" market: YES = pays if above strike, NO = pays if below
                    # For a "down" market: YES = pays if below, NO = pays if above
                    
                    evaluations = []
                    
                    if mkt.direction == "up":
                        if yes_ask > 0.01:
                            fair = pricer.fair_value(spot, mkt.strike_price, seconds_left, vol, "up")
                            evaluations.append(("up", yes_ask, yes_size, fair))
                        if no_ask > 0.01:
                            fair = pricer.fair_value(spot, mkt.strike_price, seconds_left, vol, "down")
                            evaluations.append(("down", no_ask, no_size, fair))
                    else:
                        if yes_ask > 0.01:
                            fair = pricer.fair_value(spot, mkt.strike_price, seconds_left, vol, "down")
                            evaluations.append(("down", yes_ask, yes_size, fair))
                        if no_ask > 0.01:
                            fair = pricer.fair_value(spot, mkt.strike_price, seconds_left, vol, "up")
                            evaluations.append(("up", no_ask, no_size, fair))
                    
                    for direction, market_price, depth, fair in evaluations:
                        evaluated += 1
                        
                        # Edge check
                        effective_price = market_price + (SLIPPAGE_CENTS / 100)
                        effective_edge_pct = ((fair - effective_price) / effective_price) * 100 if effective_price > 0 else 0
                        
                        # Track best edge for diagnostics
                        if effective_edge_pct > best_edge_seen:
                            best_edge_seen = effective_edge_pct
                            best_edge_label = f"{mkt.underlying} {direction.upper()} fair={fair:.3f} mkt={market_price:.3f} K={mkt.strike_price:,.0f} spot={spot:,.2f} σ={vol*100:.0f}% {seconds_left:.0f}s depth={depth:.0f}"
                        
                        if effective_edge_pct < MIN_EDGE_PCT:
                            continue
                        if effective_edge_pct > MAX_EDGE_PCT:
                            continue  # Suspiciously large — probably model error
                        
                        # Bell curve confidence — model trust decays away from $0.50
                        # (replaces hard manipulation zone cutoff)
                        confidence = bell_confidence(market_price)
                        
                        # Depth check
                        if depth < MIN_KELLY_SIZE:
                            continue
                        
                        # This is a valid candidate — keep if best for this asset+window+venue
                        cand_key = (mkt.underlying, mkt.end_time.isoformat(), mkt.venue)
                        existing = candidates.get(cand_key)
                        if not existing or effective_edge_pct > existing["edge_pct"]:
                            iv = pricer.implied_vol(market_price, spot, mkt.strike_price, seconds_left, direction)
                            regime_data = model.get_regime(mkt.underlying)
                            regime = regime_data["regime"] if regime_data else "unknown"
                            
                            candidates[cand_key] = {
                                "edge_pct": effective_edge_pct,
                                "asset": mkt.underlying,
                                "direction": direction,
                                "strike": mkt.strike_price,
                                "spot": spot,
                                "seconds_left": seconds_left,
                                "vol": vol,
                                "fair": fair,
                                "market_price": market_price,
                                "iv": iv or 0,
                                "regime": regime,
                                "window_end": mkt.end_time,
                                "venue": mkt.venue,
                                "kalshi_ticker": mkt.kalshi_ticker,
                                "bell_confidence": confidence,
                            }
                
                # Execute best candidate per asset per window (max ~3 per scan)
                for cand_key, c in sorted(candidates.items(), key=lambda x: -x[1]["edge_pct"]):
                    trade = trader.open_trade(
                        asset=c["asset"],
                        direction=c["direction"],
                        strike=c["strike"],
                        spot=c["spot"],
                        seconds_left=c["seconds_left"],
                        model_vol=c["vol"],
                        model_fair=c["fair"],
                        market_price=c["market_price"],
                        implied_vol=c["iv"],
                        vol_regime=c["regime"],
                        window_end=c["window_end"],
                        venue=c.get("venue", "pm"),
                        kalshi_ticker=c.get("kalshi_ticker", ""),
                        bell_confidence=c.get("bell_confidence", 1.0),
                    )
                    
                    if trade == "FILL_FAILED":
                        print(f"  ❌ FILL FAILED {c['asset']} {c['direction'].upper()} — order not filled (simulated)")
                        continue
                    
                    if trade is None:
                        print(f"  ⏭️  SKIP {c['asset']} {c['direction'].upper()} — Kelly size=0")
                        continue
                    
                    kelly_pct = ((c['fair'] * (1 - trade.fill_price) / trade.fill_price) - (1 - c['fair'])) / ((1 - trade.fill_price) / trade.fill_price)
                    delay = c['seconds_left'] - trade.seconds_left
                    conf = c.get('bell_confidence', 1.0)
                    venue_tag = f" [{c.get('venue','pm').upper()}]" if c.get('venue') != 'pm' else ""
                    print(f"\n  🔔 PAPER TRADE: {trade.trade_id}{venue_tag}")
                    print(f"     {c['asset']} {c['direction'].upper()} | strike={c['strike']:,.2f} spot={c['spot']:,.2f}")
                    print(f"     Fair={c['fair']:.3f} Mkt={c['market_price']:.3f} Fill={trade.fill_price:.3f} Edge={trade.edge_pct:+.1f}%")
                    print(f"     Kelly={kelly_pct*100:.1f}% × Conf={conf:.2f} → {trade.size} contracts (${trade.cost:.2f}) | Bal=${trader.sim_balance:.2f}")
                    print(f"     σ={c['vol']*100:.0f}% | {trade.seconds_left:.0f}s left (fill delay: {delay:.0f}s)")
                
                # Diagnostic output every other scan (~30s)
                if scan_count % 2 == 0:
                    print(f"  [{now.strftime('%H:%M:%S')}] Mkts:{len(markets)} Eval:{evaluated} Candidates:{len(candidates)} | Skip: time={skipped_time} strike={skipped_strike} vol={skipped_vol} book={skipped_book} dedup={skipped_dedup}")
                    if best_edge_seen > -999:
                        print(f"  Best edge: {best_edge_seen:+.1f}% → {best_edge_label}")
                    elif evaluated == 0 and len(markets) > 0:
                        sample = markets[0]
                        print(f"  Sample mkt: {sample.underlying} {sample.direction} strike={sample.strike_price} end={sample.end_time.strftime('%H:%M')}")
                
                await asyncio.sleep(SCAN_INTERVAL)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"\n  [Error] {type(e).__name__}: {e}")
                await asyncio.sleep(SCAN_INTERVAL)
    
    # Final summary
    print("\n" + "=" * 60)
    print("  FINAL RESULTS")
    print("=" * 60)
    print(trader.format_dashboard())
    trader.save_summary()
    model.save_state()
    
    await model.stop()
    if cfb_rti:
        cfb_rti.stop()


if __name__ == "__main__":
    print("Starting Vol Model Paper Trader...")
    print("Press Ctrl+C to stop.\n")
    asyncio.run(run_simulator())
