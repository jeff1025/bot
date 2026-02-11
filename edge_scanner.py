#!/usr/bin/env python3
"""
edge_scanner.py — Multi-Market Microedge Harvester for Kalshi

Scans hundreds of Kalshi markets simultaneously, identifies brief moments where
contract prices are significantly mispriced, and buys 1 contract at a time at
the best available opportunities. High volume across many markets rather than
large positions in a few.

Architecture:
    Single monolith file. Well-organized with clear sections.
    Uses Kalshi REST API for market discovery + polling.
    WebSocket for orderbook data + fill notifications.
    Continuous loop with configurable scan intervals.

V1 Scope:
    - Crypto hourly binary options (BTC, ETH, SOL) on Kalshi
    - Vol-based fair value model (Black-Scholes on binary options)
    - Realized vol from Chainlink (same oracle Kalshi settles against)
    - Full trade logging to SQLite
    - Per-asset P&L tracking and circuit breakers
    - Paper trade mode

SETUP:
    1. Place this file in your project folder
    2. config.json must have kalshi_api_key_id and kalshi_private_key_path
    3. pip install httpx cryptography aiohttp
    4. Run: python edge_scanner.py [--paper] [--verbose]

Settlement source: CF Benchmarks Real-Time Index (RTI) — averaged over 60s at
expiry. Chainlink price feeds track this closely. We compute vol from Chainlink
ticks so model vol matches the settlement oracle.
"""

# =============================================================================
# IMPORTS
# =============================================================================

import argparse
import asyncio
import base64
import json
import logging
import math
import os
import signal
import sqlite3
import sys
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

try:
    import httpx
except ImportError:
    print("[FATAL] httpx not installed. Run: pip install httpx")
    sys.exit(1)

try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
    from cryptography.hazmat.backends import default_backend
except ImportError:
    print("[FATAL] cryptography not installed. Run: pip install cryptography")
    sys.exit(1)

try:
    import aiohttp
except ImportError:
    aiohttp = None
    print("[WARN] aiohttp not installed — WebSocket features disabled. Run: pip install aiohttp")

try:
    from scipy.stats import norm
    _norm_cdf = norm.cdf
except ImportError:
    # Pure-python fallback (Abramowitz & Stegun, accurate to ~1e-7)
    def _norm_cdf(x):
        if x < -8:
            return 0.0
        if x > 8:
            return 1.0
        a1, a2, a3, a4, a5 = (
            0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
        )
        p = 0.3275911
        sign = 1 if x >= 0 else -1
        x = abs(x)
        t = 1.0 / (1.0 + p * x)
        y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(
            -x * x / 2
        )
        return 0.5 * (1.0 + sign * y)


# =============================================================================
# CONFIGURATION — All tunable parameters in one place.
# DO NOT change without explicit approval.
# =============================================================================

# ── Kalshi API ──────────────────────────────────────────────────────────────
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
KALSHI_DEMO_API_BASE = "https://demo-api.kalshi.co/trade-api/v2"
KALSHI_DEMO_WS_URL = "wss://demo-api.kalshi.co/trade-api/ws/v2"

# ── Scan timing ─────────────────────────────────────────────────────────────
SCAN_INTERVAL_SECONDS = 15      # Seconds between full market scans
RECONCILE_INTERVAL_SECONDS = 120  # Seconds between position reconciliation checks
SETTLEMENT_CHECK_INTERVAL = 30  # Seconds between settlement checks
STATS_PRINT_INTERVAL = 60       # Seconds between dashboard prints
VOL_WARMUP_MINUTES = 3          # Minutes of Chainlink data before trading

# ── Fee model (Kalshi taker) ────────────────────────────────────────────────
# Taker fee: roundup(0.07 * C * P * (1 - P)), max $0.02/contract
KALSHI_TAKER_FEE_RATE = 0.07
KALSHI_MAX_FEE_PER_CONTRACT = 0.02
# Maker fee (resting limit that gets filled): roundup(0.0175 * C * P * (1-P))
KALSHI_MAKER_FEE_RATE = 0.0175

# ── Global risk limits ──────────────────────────────────────────────────────
MAX_OPEN_POSITIONS_GLOBAL = 10     # Circuit breaker: stop trading above this
MAX_POSITIONS_PER_MARKET = 1       # Max concurrent positions in a single market
MAX_DAILY_LOSS_DOLLARS = 50.0      # Stop trading for the day if realized loss exceeds
MAX_TRADES_PER_HOUR = 30           # Rate limit on order placement
BALANCE_RESERVE_PCT = 50           # Keep this % of balance uninvested

# ── Vol model parameters ────────────────────────────────────────────────────
MINUTES_PER_YEAR = 525_960
SECONDS_PER_YEAR = MINUTES_PER_YEAR * 60
EWMA_LAMBDA_FAST = 0.90    # Half-life ~7 observations
EWMA_LAMBDA_SLOW = 0.97    # Half-life ~23 observations
VOL_DAMPENER = 1.0          # Scale factor on raw vol (1.0 = no dampening)

MIN_VOL_FLOOR = {
    "BTC": 0.30,
    "ETH": 0.40,
    "SOL": 0.50,
}

# ── Chainlink price feed ────────────────────────────────────────────────────
CHAINLINK_WS_URL = "wss://ws-live-data.polymarket.com"
CHAINLINK_SYMBOLS = {
    "BTC": "btc/usd",
    "ETH": "eth/usd",
    "SOL": "sol/usd",
}

# ── Rate limiting ───────────────────────────────────────────────────────────
API_READS_PER_SECOND = 10         # Conservative limit (basic tier allows ~20)
API_WRITES_PER_SECOND = 5         # Conservative limit (basic tier allows ~10)
MIN_ORDER_INTERVAL_SECONDS = 1.0  # Minimum time between order submissions

# =============================================================================
# CATEGORY CONFIGURATION — Each category defines its own fair value model,
# edge thresholds, and risk limits.
# =============================================================================

@dataclass
class CategoryConfig:
    """Configuration for a market category."""
    name: str
    enabled: bool
    # Series tickers to scan on Kalshi (e.g. ["KXBTC", "KXETH"])
    series_tickers: list
    # Timeframe filter
    timeframe: str                    # "hourly", "15min", "daily"
    # Fair value model
    fair_value_model: str             # "vol_bs" (Black-Scholes vol model)
    # Edge thresholds
    min_edge_pct: float               # Minimum (fair-ask)/fair to trade
    max_edge_pct: float               # Above this = assume model error, skip
    max_price: float                  # Don't buy contracts priced above this
    min_price: float                  # Don't buy contracts priced below this
    # Timing
    min_seconds_to_expiry: int        # Don't trade too close to settlement
    max_seconds_to_expiry: int        # Don't trade too far out
    # Risk
    max_positions_per_category: int   # Max open positions across this category
    max_positions_per_event: int = 2  # Max positions per event (correlated risk limit)
    contracts_per_trade: int          # Contracts to buy per opportunity
    # Circuit breaker
    loss_threshold_dollars: float     # Auto-disable if category P&L below this
    min_trades_for_breaker: int       # Minimum settled trades before breaker activates
    min_win_rate: float               # Auto-disable if win rate below this after N trades


CATEGORY_CONFIGS = {
    "crypto_hourly_btc": CategoryConfig(
        name="Crypto Hourly BTC",
        enabled=True,
        series_tickers=["KXBTC"],
        timeframe="hourly",
        fair_value_model="vol_bs",
        min_edge_pct=20.0,
        max_edge_pct=60.0,
        max_price=0.85,
        min_price=0.02,
        min_seconds_to_expiry=180,
        max_seconds_to_expiry=3300,
        max_positions_per_category=3,
        contracts_per_trade=1,
        loss_threshold_dollars=-25.0,
        min_trades_for_breaker=20,
        min_win_rate=0.30,
    ),
    "crypto_hourly_eth": CategoryConfig(
        name="Crypto Hourly ETH",
        enabled=True,
        series_tickers=["KXETH"],
        timeframe="hourly",
        fair_value_model="vol_bs",
        min_edge_pct=20.0,
        max_edge_pct=60.0,
        max_price=0.85,
        min_price=0.02,
        min_seconds_to_expiry=180,
        max_seconds_to_expiry=3300,
        max_positions_per_category=3,
        contracts_per_trade=1,
        loss_threshold_dollars=-25.0,
        min_trades_for_breaker=20,
        min_win_rate=0.30,
    ),
    "crypto_hourly_sol": CategoryConfig(
        name="Crypto Hourly SOL",
        enabled=True,
        series_tickers=["KXSOL"],
        timeframe="hourly",
        fair_value_model="vol_bs",
        min_edge_pct=20.0,
        max_edge_pct=60.0,
        max_price=0.85,
        min_price=0.02,
        min_seconds_to_expiry=180,
        max_seconds_to_expiry=3300,
        max_positions_per_category=3,
        contracts_per_trade=1,
        loss_threshold_dollars=-25.0,
        min_trades_for_breaker=20,
        min_win_rate=0.30,
    ),
    # ── 15-minute crypto markets ──────────────────────────────────────────
    "crypto_15min_btc": CategoryConfig(
        name="Crypto 15min BTC",
        enabled=True,
        series_tickers=["KXBTC15M"],
        timeframe="15min",
        fair_value_model="vol_bs",
        min_edge_pct=20.0,
        max_edge_pct=60.0,
        max_price=0.85,
        min_price=0.02,
        min_seconds_to_expiry=90,
        max_seconds_to_expiry=840,
        max_positions_per_category=2,
        contracts_per_trade=1,
        loss_threshold_dollars=-25.0,
        min_trades_for_breaker=20,
        min_win_rate=0.30,
    ),
    "crypto_15min_eth": CategoryConfig(
        name="Crypto 15min ETH",
        enabled=True,
        series_tickers=["KXETH15M"],
        timeframe="15min",
        fair_value_model="vol_bs",
        min_edge_pct=20.0,
        max_edge_pct=60.0,
        max_price=0.85,
        min_price=0.02,
        min_seconds_to_expiry=90,
        max_seconds_to_expiry=840,
        max_positions_per_category=2,
        contracts_per_trade=1,
        loss_threshold_dollars=-25.0,
        min_trades_for_breaker=20,
        min_win_rate=0.30,
    ),
    "crypto_15min_sol": CategoryConfig(
        name="Crypto 15min SOL",
        enabled=True,
        series_tickers=["KXSOL15M"],
        timeframe="15min",
        fair_value_model="vol_bs",
        min_edge_pct=20.0,
        max_edge_pct=60.0,
        max_price=0.85,
        min_price=0.02,
        min_seconds_to_expiry=90,
        max_seconds_to_expiry=840,
        max_positions_per_category=2,
        contracts_per_trade=1,
        loss_threshold_dollars=-25.0,
        min_trades_for_breaker=20,
        min_win_rate=0.30,
    ),
}

# =============================================================================
# LOGGING SETUP
# =============================================================================

LOG_DIR = os.path.join(os.path.expanduser("~"), ".edge_scanner")
os.makedirs(LOG_DIR, exist_ok=True)

DB_PATH = os.path.join(LOG_DIR, "trades.db")

logger = logging.getLogger("edge_scanner")
logger.setLevel(logging.DEBUG)

# Console handler — INFO level
_ch = logging.StreamHandler(sys.stdout)
_ch.setLevel(logging.INFO)
_ch.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
))
logger.addHandler(_ch)

# File handler — DEBUG level (everything)
_fh = logging.FileHandler(os.path.join(LOG_DIR, "edge_scanner.log"), encoding="utf-8")
_fh.setLevel(logging.DEBUG)
_fh.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
))
logger.addHandler(_fh)


# =============================================================================
# SQLITE TRADE JOURNAL
# =============================================================================

def init_db(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Initialize SQLite database for trade journal."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            trade_id TEXT PRIMARY KEY,
            timestamp TEXT NOT NULL,
            category TEXT NOT NULL,
            asset TEXT NOT NULL,
            ticker TEXT NOT NULL,
            direction TEXT NOT NULL,
            strike REAL NOT NULL,
            spot_entry REAL NOT NULL,
            seconds_left REAL NOT NULL,
            model_vol REAL NOT NULL,
            model_fair REAL NOT NULL,
            ask_price REAL NOT NULL,
            fill_price REAL,
            edge_pct REAL NOT NULL,
            implied_vol REAL,
            vol_regime TEXT,
            contracts INTEGER NOT NULL,
            cost REAL,
            fee REAL,
            order_id TEXT,
            order_status TEXT DEFAULT 'pending',
            -- Settlement
            settled INTEGER DEFAULT 0,
            settlement_result TEXT,
            spot_settle REAL,
            payout REAL DEFAULT 0,
            pnl REAL DEFAULT 0,
            settled_at TEXT,
            -- Metadata
            paper_trade INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')),
            notes TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS category_stats (
            category TEXT PRIMARY KEY,
            total_trades INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            total_pnl REAL DEFAULT 0,
            total_cost REAL DEFAULT 0,
            total_payout REAL DEFAULT 0,
            disabled_at TEXT,
            disable_reason TEXT,
            last_updated TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS scan_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            markets_scanned INTEGER,
            opportunities_found INTEGER,
            trades_executed INTEGER,
            best_edge_pct REAL,
            scan_duration_ms REAL
        )
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_trades_category ON trades(category)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_trades_settled ON trades(settled)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_trades_ticker ON trades(ticker)
    """)

    conn.commit()
    return conn


# =============================================================================
# BINARY OPTIONS PRICER — Black-Scholes for digital options
# =============================================================================

class BinaryPricer:
    """
    Prices binary (digital) options using Black-Scholes.

    For binary CALL paying $1 if S > K at expiry:
        price = N(d2)
    where d2 = [ln(S/K) + (-sigma^2/2)*T] / (sigma*sqrt(T))

    For binary PUT paying $1 if S < K at expiry:
        price = N(-d2) = 1 - N(d2)
    """

    @staticmethod
    def fair_value(
        S: float, K: float, seconds_left: float, sigma: float, direction: str = "up"
    ) -> float:
        """
        Price a binary option.

        Args:
            S: current underlying price
            K: strike price
            seconds_left: time to settlement
            sigma: annualized volatility
            direction: "up"/"yes" (pays if S>K) or "down"/"no" (pays if S<K)

        Returns: fair price 0.00-1.00
        """
        T = seconds_left / SECONDS_PER_YEAR
        if T <= 0:
            return 1.0 if (S > K and direction in ("up", "yes")) else 0.0
        if sigma <= 0 or S <= 0 or K <= 0:
            return 1.0 if (S > K and direction in ("up", "yes")) else 0.0

        sqrt_T = math.sqrt(T)
        d2 = (math.log(S / K) + (-0.5 * sigma**2) * T) / (sigma * sqrt_T)

        if direction in ("up", "yes", "call"):
            return _norm_cdf(d2)
        else:
            return 1.0 - _norm_cdf(d2)

    @staticmethod
    def implied_vol(
        market_price: float,
        S: float,
        K: float,
        seconds_left: float,
        direction: str = "up",
        tol: float = 1e-6,
        max_iter: int = 50,
    ) -> Optional[float]:
        """Back out implied vol from market price using bisection."""
        if market_price <= 0.001 or market_price >= 0.999:
            return None
        T = seconds_left / SECONDS_PER_YEAR
        if T <= 0:
            return None

        lo, hi = 0.01, 10.0
        for _ in range(max_iter):
            mid = (lo + hi) / 2
            model_price = BinaryPricer.fair_value(S, K, seconds_left, mid, direction)
            diff = model_price - market_price
            if abs(diff) < tol:
                return mid
            # Determine search direction based on moneyness
            if S > K and direction in ("up", "yes", "call"):
                if diff > 0:
                    lo = mid
                else:
                    hi = mid
            elif S <= K and direction in ("up", "yes", "call"):
                if diff > 0:
                    hi = mid
                else:
                    lo = mid
            elif S < K and direction in ("down", "no", "put"):
                if diff > 0:
                    lo = mid
                else:
                    hi = mid
            else:
                if diff > 0:
                    hi = mid
                else:
                    lo = mid
        return (lo + hi) / 2


# =============================================================================
# FEE CALCULATOR
# =============================================================================

def kalshi_taker_fee(contracts: int, price_dollars: float) -> float:
    """
    Kalshi taker fee: roundup(0.07 * C * P * (1-P)), max $0.02/contract.
    Price is in dollars (0.00-1.00).
    Returns fee in dollars.
    """
    if contracts <= 0 or price_dollars <= 0 or price_dollars >= 1:
        return 0.0
    raw = KALSHI_TAKER_FEE_RATE * contracts * price_dollars * (1 - price_dollars)
    fee = math.ceil(raw * 100) / 100  # Round up to nearest cent
    max_fee = contracts * KALSHI_MAX_FEE_PER_CONTRACT
    return min(fee, max_fee)


def net_edge_after_fees(
    fair_value: float, ask_price: float, contracts: int
) -> tuple:
    """
    Calculate edge net of fees.

    Returns (net_edge_pct, fee_dollars, net_expected_profit).
    """
    if fair_value <= 0 or ask_price <= 0:
        return 0.0, 0.0, 0.0

    fee = kalshi_taker_fee(contracts, ask_price)
    cost = contracts * ask_price
    expected_payout = contracts * fair_value  # E[payout] = fair_value * $1 * contracts
    gross_profit = expected_payout - cost
    net_profit = gross_profit - fee
    net_edge_pct = (net_profit / cost) * 100 if cost > 0 else 0.0

    return net_edge_pct, fee, net_profit


# =============================================================================
# CHAINLINK PRICE FEED — Real-time prices via Polymarket RTDS WebSocket
# =============================================================================

@dataclass
class PriceTick:
    timestamp: float
    price: float


class ChainlinkFeed:
    """
    Real-time Chainlink price feed from Polymarket RTDS WebSocket.
    This is the authoritative price source — Kalshi crypto markets settle
    against CF Benchmarks RTI which tracks closely with Chainlink.
    """

    def __init__(self):
        self._prices: dict[str, float] = {}
        self._timestamps: dict[str, float] = {}
        self._lock = threading.Lock()
        self._running = False
        self._connected = False
        self._update_count = 0
        self._callbacks: list = []  # (asset, price) callbacks

    def on_price(self, callback):
        """Register a callback: callback(asset: str, price: float)"""
        self._callbacks.append(callback)

    def start(self):
        if self._running:
            return
        self._running = True
        t = threading.Thread(target=self._run, daemon=True)
        t.start()

    def stop(self):
        self._running = False

    def _run(self):
        try:
            import websocket
        except ImportError:
            # Fallback: use aiohttp in a thread-local event loop
            if aiohttp is None:
                logger.error("Neither websocket-client nor aiohttp installed for Chainlink feed")
                return
            loop = asyncio.new_event_loop()
            loop.run_until_complete(self._aiohttp_ws_loop())
            return

        def on_message(ws, message):
            try:
                data = json.loads(message)
                topic = data.get("topic", "")
                if "crypto_prices_chainlink" not in topic:
                    return
                payload = data.get("payload", {})
                symbol = payload.get("symbol", "").lower()
                value = payload.get("value")
                if symbol and value:
                    asset = symbol.split("/")[0].upper()
                    price_val = float(value)
                    with self._lock:
                        self._prices[asset] = price_val
                        self._timestamps[asset] = time.time()
                        self._update_count += 1
                    for cb in self._callbacks:
                        try:
                            cb(asset, price_val)
                        except Exception:
                            pass
            except Exception:
                pass

        def on_open(ws):
            self._connected = True
            logger.info("Chainlink RTDS connected")
            ws.send(json.dumps({
                "action": "subscribe",
                "subscriptions": [
                    {"topic": "crypto_prices_chainlink", "type": "*", "filters": ""}
                ],
            }))

        def on_error(ws, error):
            logger.debug(f"Chainlink WS error: {error}")

        def on_close(ws, code, msg):
            self._connected = False
            logger.warning(f"Chainlink RTDS disconnected: {code}")

        while self._running:
            try:
                import websocket
                ws = websocket.WebSocketApp(
                    CHAINLINK_WS_URL,
                    on_message=on_message,
                    on_open=on_open,
                    on_error=on_error,
                    on_close=on_close,
                )
                ws.run_forever(ping_interval=5, ping_timeout=3)
            except Exception as e:
                logger.debug(f"Chainlink WS reconnect: {e}")
            if self._running:
                time.sleep(2)

    async def _aiohttp_ws_loop(self):
        """Fallback WS loop using aiohttp (if websocket-client not installed)."""
        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(
                        CHAINLINK_WS_URL, heartbeat=20
                    ) as ws:
                        self._connected = True
                        logger.info("Chainlink RTDS connected (aiohttp)")
                        await ws.send_json({
                            "action": "subscribe",
                            "subscriptions": [
                                {"topic": "crypto_prices_chainlink", "type": "*", "filters": ""}
                            ],
                        })
                        async for msg in ws:
                            if not self._running:
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                topic = data.get("topic", "")
                                if "crypto_prices_chainlink" not in topic:
                                    continue
                                payload = data.get("payload", {})
                                symbol = payload.get("symbol", "").lower()
                                value = payload.get("value")
                                if symbol and value:
                                    asset = symbol.split("/")[0].upper()
                                    price_val = float(value)
                                    with self._lock:
                                        self._prices[asset] = price_val
                                        self._timestamps[asset] = time.time()
                                        self._update_count += 1
                                    for cb in self._callbacks:
                                        try:
                                            cb(asset, price_val)
                                        except Exception:
                                            pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._connected = False
                logger.debug(f"Chainlink aiohttp WS error: {e}")
            if self._running:
                await asyncio.sleep(2)

    def get_price(self, asset: str) -> Optional[float]:
        with self._lock:
            return self._prices.get(asset.upper())

    def get_all_prices(self) -> dict:
        with self._lock:
            return dict(self._prices)

    def is_stale(self, asset: str, max_age: float = 30) -> bool:
        with self._lock:
            ts = self._timestamps.get(asset.upper())
            if not ts:
                return True
            return (time.time() - ts) > max_age

    def is_connected(self) -> bool:
        return self._connected

    @property
    def update_count(self) -> int:
        return self._update_count


# =============================================================================
# VOLATILITY TRACKER — EWMA + rolling window realized vol from Chainlink ticks
# =============================================================================

class VolTracker:
    """
    Real-time realized volatility from Chainlink price ticks.

    Maintains EWMA (fast/slow) and rolling-window vol estimates per asset.
    Vol is computed from the same oracle that Kalshi settles against.
    """

    def __init__(self, max_history_minutes: int = 120):
        self.max_ticks = max_history_minutes * 60
        assets = list(CHAINLINK_SYMBOLS.keys())

        self._prices: dict[str, deque] = {a: deque(maxlen=self.max_ticks) for a in assets}
        self._minute_closes: dict[str, deque] = {
            a: deque(maxlen=max_history_minutes) for a in assets
        }
        self._current_minute: dict[str, int] = {a: 0 for a in assets}
        self._current_minute_last_price: dict[str, float] = {}

        # EWMA state
        self._ewma_fast: dict[str, float] = {}
        self._ewma_slow: dict[str, float] = {}
        self._ewma_initialized: dict[str, bool] = {a: False for a in assets}

        # Tick interval tracking for annualization
        self._last_tick_time: dict[str, float] = {}
        self._tick_interval_ewma: dict[str, float] = {}
        self._tick_interval_lambda = 0.95

        self.latest_price: dict[str, float] = {}
        self.tick_count: dict[str, int] = {a: 0 for a in assets}

    def feed_price(self, asset: str, price: float, timestamp: float = None):
        """Feed a Chainlink price tick."""
        if timestamp is None:
            timestamp = time.time()
        if price <= 0:
            return

        now = time.time()
        last_t = self._last_tick_time.get(asset)
        if last_t:
            dt = now - last_t
            if 0.01 < dt < 30:
                if asset in self._tick_interval_ewma:
                    lam = self._tick_interval_lambda
                    self._tick_interval_ewma[asset] = (
                        lam * self._tick_interval_ewma[asset] + (1 - lam) * dt
                    )
                else:
                    self._tick_interval_ewma[asset] = dt
        self._last_tick_time[asset] = now

        prev_price = self.latest_price.get(asset)
        self.latest_price[asset] = price
        self.tick_count[asset] = self.tick_count.get(asset, 0) + 1

        if asset in self._prices:
            self._prices[asset].append(PriceTick(timestamp, price))

        # Minute candle
        current_min = int(timestamp // 60)
        if self._current_minute.get(asset) != current_min:
            if self._current_minute_last_price.get(asset) is not None and asset in self._minute_closes:
                self._minute_closes[asset].append(
                    PriceTick(self._current_minute[asset] * 60, self._current_minute_last_price[asset])
                )
            self._current_minute[asset] = current_min
        self._current_minute_last_price[asset] = price

        # EWMA update
        if prev_price and prev_price > 0:
            log_return = math.log(price / prev_price)
            r_sq = log_return**2

            if not self._ewma_initialized.get(asset):
                self._ewma_fast[asset] = r_sq
                self._ewma_slow[asset] = r_sq
                self._ewma_initialized[asset] = True
            else:
                self._ewma_fast[asset] = (
                    EWMA_LAMBDA_FAST * self._ewma_fast[asset]
                    + (1 - EWMA_LAMBDA_FAST) * r_sq
                )
                self._ewma_slow[asset] = (
                    EWMA_LAMBDA_SLOW * self._ewma_slow[asset]
                    + (1 - EWMA_LAMBDA_SLOW) * r_sq
                )

    def get_realized_vol(self, asset: str, window_minutes: int = 15) -> Optional[float]:
        """Annualized realized vol from minute closes (rolling window)."""
        closes = self._minute_closes.get(asset)
        if not closes or len(closes) < max(3, window_minutes // 2):
            return None

        recent = list(closes)[-window_minutes:]
        if len(recent) < 3:
            return None

        log_returns = []
        for i in range(1, len(recent)):
            if recent[i].price > 0 and recent[i - 1].price > 0:
                log_returns.append(math.log(recent[i].price / recent[i - 1].price))

        if len(log_returns) < 2:
            return None

        mean_r = sum(log_returns) / len(log_returns)
        var = sum((r - mean_r) ** 2 for r in log_returns) / (len(log_returns) - 1)
        std_per_minute = math.sqrt(var)
        return std_per_minute * math.sqrt(MINUTES_PER_YEAR)

    def get_ewma_vol(self, asset: str, speed: str = "fast") -> Optional[float]:
        """EWMA-based annualized vol. Speed: 'fast' or 'slow'."""
        if not self._ewma_initialized.get(asset):
            return None

        var = (
            self._ewma_fast.get(asset, 0)
            if speed == "fast"
            else self._ewma_slow.get(asset, 0)
        )
        if var <= 0:
            return None

        per_tick_std = math.sqrt(var)
        avg_interval = self._tick_interval_ewma.get(asset, 1.0)
        if avg_interval <= 0:
            avg_interval = 1.0
        ticks_per_year = SECONDS_PER_YEAR / avg_interval
        return per_tick_std * math.sqrt(ticks_per_year)

    def get_best_vol(self, asset: str) -> Optional[float]:
        """Best available vol estimate: 15m rolling > 5m rolling > fast EWMA."""
        for getter in [
            lambda: self.get_realized_vol(asset, 15),
            lambda: self.get_realized_vol(asset, 5),
            lambda: self.get_ewma_vol(asset, "fast"),
        ]:
            vol = getter()
            if vol and vol > 0.01:
                return vol
        return None

    def get_adaptive_vol(self, asset: str, seconds_left: float) -> Optional[float]:
        """
        Select vol estimate based on time remaining.
        >10 min: 15m rolling; 5-10 min: blend; 2-5 min: 5m; <2 min: fast EWMA.
        """
        vol_5m = self.get_realized_vol(asset, 5)
        vol_15m = self.get_realized_vol(asset, 15)
        ewma_fast = self.get_ewma_vol(asset, "fast")

        raw_vol = None
        if seconds_left > 600:
            raw_vol = vol_15m or vol_5m or ewma_fast
        elif seconds_left > 300:
            if vol_5m and vol_15m:
                raw_vol = 0.6 * vol_5m + 0.4 * vol_15m
            else:
                raw_vol = vol_5m or vol_15m or ewma_fast
        elif seconds_left > 120:
            raw_vol = vol_5m or ewma_fast or vol_15m
        else:
            raw_vol = ewma_fast or vol_5m

        if raw_vol is None:
            return None

        # Apply floor
        floor = MIN_VOL_FLOOR.get(asset, 0.30)
        raw_vol = max(raw_vol, floor)

        return raw_vol * VOL_DAMPENER

    def get_vol_regime(self, asset: str) -> Optional[str]:
        """Vol regime: 'expanding', 'contracting', or 'normal'."""
        fast = self.get_ewma_vol(asset, "fast")
        slow = self.get_ewma_vol(asset, "slow")
        if fast is None or slow is None or slow <= 0:
            return None
        ratio = fast / slow
        if ratio > 1.5:
            return "expanding"
        elif ratio < 0.7:
            return "contracting"
        return "normal"

    def is_warmed_up(self, asset: str) -> bool:
        """True if we have enough data for reliable vol estimates."""
        closes = self._minute_closes.get(asset)
        if not closes:
            return False
        return len(closes) >= VOL_WARMUP_MINUTES

    def status_str(self) -> str:
        parts = []
        for asset in CHAINLINK_SYMBOLS:
            vol = self.get_best_vol(asset)
            price = self.latest_price.get(asset)
            ticks = self.tick_count.get(asset, 0)
            if vol and price:
                parts.append(f"{asset}=${price:,.0f} vol={vol*100:.1f}% ({ticks}t)")
            elif price:
                parts.append(f"{asset}=${price:,.0f} (warming)")
        return " | ".join(parts) if parts else "Vol: warming up..."


# =============================================================================
# KALSHI API CONNECTOR — Auth, REST calls, order management
# =============================================================================

class KalshiAPI:
    """
    Handles all Kalshi API interactions: RSA auth signing, REST calls,
    market discovery, order placement, position/fill queries.
    """

    def __init__(self, api_key_id: str, private_key_pem: str, demo: bool = False):
        self.api_key_id = api_key_id
        self._private_key = serialization.load_pem_private_key(
            private_key_pem.encode(), password=None, backend=default_backend()
        )
        self.base_url = KALSHI_DEMO_API_BASE if demo else KALSHI_API_BASE
        self.ws_url = KALSHI_DEMO_WS_URL if demo else KALSHI_WS_URL
        self._client: Optional[httpx.AsyncClient] = None
        self._last_order_time = 0.0
        self._read_timestamps: deque = deque(maxlen=API_READS_PER_SECOND * 2)
        self._write_timestamps: deque = deque(maxlen=API_WRITES_PER_SECOND * 2)

    def _sign(self, ts_ms: str, method: str, path: str) -> str:
        """RSA-PSS signature for Kalshi auth."""
        msg = f"{ts_ms}{method}{path}".encode("utf-8")
        sig = self._private_key.sign(
            msg,
            asym_padding.PSS(
                mgf=asym_padding.MGF1(hashes.SHA256()),
                salt_length=asym_padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode("utf-8")

    def _headers(self, method: str, path: str) -> dict:
        ts = str(int(time.time() * 1000))
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-SIGNATURE": self._sign(ts, method, path),
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "Content-Type": "application/json",
        }

    async def connect(self):
        """Initialize HTTP client and verify connection."""
        limits = httpx.Limits(
            max_keepalive_connections=20, max_connections=50, keepalive_expiry=30
        )
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(15.0, connect=10.0),
            limits=limits,
            http2=True,
        )
        # Verify
        for attempt in range(3):
            try:
                bal = await self.get_balance()
                logger.info(f"Kalshi connected | Balance: ${bal:.2f}")
                self._client.timeout = httpx.Timeout(8.0, connect=5.0)
                return bal
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(2)
                else:
                    raise

    async def close(self):
        if self._client:
            await self._client.aclose()

    async def _rate_limit_read(self):
        """Enforce read rate limiting."""
        now = time.time()
        # Remove timestamps older than 1 second
        while self._read_timestamps and now - self._read_timestamps[0] > 1.0:
            self._read_timestamps.popleft()
        if len(self._read_timestamps) >= API_READS_PER_SECOND:
            sleep_time = 1.0 - (now - self._read_timestamps[0])
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
        self._read_timestamps.append(time.time())

    async def _rate_limit_write(self):
        """Enforce write rate limiting."""
        now = time.time()
        while self._write_timestamps and now - self._write_timestamps[0] > 1.0:
            self._write_timestamps.popleft()
        if len(self._write_timestamps) >= API_WRITES_PER_SECOND:
            sleep_time = 1.0 - (now - self._write_timestamps[0])
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
        self._write_timestamps.append(time.time())

    async def _req(
        self, method: str, path: str, params: dict = None, json_data: dict = None
    ) -> dict:
        """Make authenticated API request with rate limiting."""
        if method == "GET":
            await self._rate_limit_read()
        else:
            await self._rate_limit_write()

        url = f"{self.base_url}{path}"
        # Signature uses full path including /trade-api/v2
        full_path = f"/trade-api/v2{path}"
        headers = self._headers(method, full_path)

        for attempt in range(3):
            try:
                if method == "GET":
                    r = await self._client.get(url, headers=headers, params=params)
                elif method == "POST":
                    r = await self._client.post(url, headers=headers, json=json_data)
                elif method == "DELETE":
                    r = await self._client.delete(url, headers=headers)
                else:
                    raise ValueError(f"Unknown method: {method}")

                if r.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"Rate limited, waiting {wait}s...")
                    await asyncio.sleep(wait)
                    continue

                if r.status_code >= 400:
                    raise Exception(
                        f"Kalshi {method} {path}: HTTP {r.status_code} — {r.text[:300]}"
                    )

                return r.json() if r.text else {}

            except httpx.TimeoutException:
                if attempt < 2:
                    await asyncio.sleep(1)
                else:
                    raise

        return {}

    # ── Account ─────────────────────────────────────────────────────────────

    async def get_balance(self) -> float:
        r = await self._req("GET", "/portfolio/balance")
        return r.get("balance", 0) / 100  # Cents to dollars

    # ── Market Discovery ────────────────────────────────────────────────────

    async def get_events(
        self, series_ticker: str, status: str = "open", limit: int = 100
    ) -> list:
        """Get events for a series, with nested markets."""
        r = await self._req(
            "GET",
            "/events",
            params={
                "series_ticker": series_ticker,
                "with_nested_markets": "true",
                "status": status,
                "limit": limit,
            },
        )
        return r.get("events", [])

    async def get_markets(
        self, series_ticker: str = None, event_ticker: str = None, status: str = "open",
        limit: int = 200,
    ) -> list:
        """Get markets, optionally filtered by series or event."""
        params = {"status": status, "limit": limit}
        if series_ticker:
            params["series_ticker"] = series_ticker
        if event_ticker:
            params["event_ticker"] = event_ticker
        r = await self._req("GET", "/markets", params=params)
        return r.get("markets", [])

    async def get_market(self, ticker: str) -> dict:
        r = await self._req("GET", f"/markets/{ticker}")
        return r.get("market", r)

    async def get_orderbook(self, ticker: str, depth: int = 5) -> dict:
        """
        Get orderbook. Returns best YES ask and NO ask prices.
        Kalshi orderbook only has bids; asks are implied from opposite-side bids.
        """
        try:
            r = await self._req(
                "GET", f"/markets/{ticker}/orderbook", params={"depth": depth}
            )
            ob = r.get("orderbook", {})
            yes_bids = ob.get("yes") or []
            no_bids = ob.get("no") or []

            # Best YES ask = cheapest to buy YES = (100 - best_no_bid) / 100
            best_yes_ask = None
            best_yes_size = 0
            for level in no_bids:
                if len(level) >= 2:
                    price_cents, qty = level[0], level[1]
                    yes_ask = (100 - price_cents) / 100.0
                    if best_yes_ask is None or yes_ask < best_yes_ask:
                        best_yes_ask = yes_ask
                        best_yes_size = qty

            # Best NO ask = cheapest to buy NO = (100 - best_yes_bid) / 100
            best_no_ask = None
            best_no_size = 0
            for level in yes_bids:
                if len(level) >= 2:
                    price_cents, qty = level[0], level[1]
                    no_ask = (100 - price_cents) / 100.0
                    if best_no_ask is None or no_ask < best_no_ask:
                        best_no_ask = no_ask
                        best_no_size = qty

            return {
                "yes_ask": best_yes_ask,
                "yes_ask_size": best_yes_size,
                "no_ask": best_no_ask,
                "no_ask_size": best_no_size,
            }
        except Exception as e:
            logger.debug(f"Orderbook error for {ticker}: {e}")
            return {"yes_ask": None, "yes_ask_size": 0, "no_ask": None, "no_ask_size": 0}

    # ── Orders ──────────────────────────────────────────────────────────────

    async def place_limit_buy(
        self, ticker: str, side: str, price_dollars: float, count: int
    ) -> Optional[str]:
        """
        Place a limit buy order. Returns order_id or None.
        Uses IOC (immediate-or-cancel) to avoid stale resting orders.
        """
        elapsed = time.time() - self._last_order_time
        if elapsed < MIN_ORDER_INTERVAL_SECONDS:
            await asyncio.sleep(MIN_ORDER_INTERVAL_SECONDS - elapsed)

        price_cents = max(1, min(99, round(price_dollars * 100)))

        try:
            data = {
                "ticker": ticker,
                "action": "buy",
                "side": side,
                "type": "limit",
                "count": int(count),
                f"{side}_price": price_cents,
                "time_in_force": "ioc",
                "client_order_id": str(uuid.uuid4()),
            }

            r = await self._req("POST", "/portfolio/orders", json_data=data)
            self._last_order_time = time.time()

            order_data = r.get("order", {})
            order_id = order_data.get("order_id")

            if not order_id:
                logger.warning(f"Order rejected for {ticker}: {r}")
                return None

            logger.debug(f"Order placed: {ticker} {side} {count}@{price_cents}c -> {order_id}")
            return order_id

        except Exception as e:
            logger.error(f"Order error for {ticker}: {e}")
            return None

    async def get_order(self, order_id: str) -> dict:
        try:
            r = await self._req("GET", f"/portfolio/orders/{order_id}")
            return r.get("order", {})
        except Exception as e:
            logger.debug(f"Get order error: {e}")
            return {}

    async def get_fills(self, order_id: str = None, ticker: str = None) -> list:
        params = {}
        if order_id:
            params["order_id"] = order_id
        if ticker:
            params["ticker"] = ticker
        try:
            r = await self._req("GET", "/portfolio/fills", params=params)
            return r.get("fills", [])
        except Exception as e:
            logger.debug(f"Get fills error: {e}")
            return []

    async def cancel_order(self, order_id: str) -> bool:
        try:
            await self._req("DELETE", f"/portfolio/orders/{order_id}")
            return True
        except Exception as e:
            if "404" in str(e) or "not_found" in str(e).lower():
                return True
            logger.debug(f"Cancel error: {e}")
            return False

    # ── Positions ───────────────────────────────────────────────────────────

    async def get_positions(self) -> list:
        """Get all open positions."""
        try:
            positions = []
            cursor = None
            while True:
                params = {"limit": 200, "count_filter": "position"}
                if cursor:
                    params["cursor"] = cursor
                r = await self._req("GET", "/portfolio/positions", params=params)
                batch = r.get("market_positions", [])
                positions.extend(batch)
                cursor = r.get("cursor")
                if not cursor or not batch:
                    break
            return positions
        except Exception as e:
            logger.error(f"Get positions error: {e}")
            return []

    async def get_settlements(self, limit: int = 100) -> list:
        """Get recent settlements."""
        try:
            r = await self._req(
                "GET", "/portfolio/settlements", params={"limit": limit}
            )
            return r.get("settlements", [])
        except Exception as e:
            logger.debug(f"Get settlements error: {e}")
            return []

    async def get_market_settlement(self, ticker: str) -> dict:
        """Check if a specific market has settled."""
        try:
            r = await self._req("GET", f"/markets/{ticker}")
            market = r.get("market", r)
            status = market.get("status", "")
            result = market.get("result", "")
            if status == "settled" or result in ("yes", "no"):
                return {
                    "settled": True,
                    "result": result,
                    "settle_price": market.get("settlement_value"),
                }
            return {"settled": False}
        except Exception as e:
            return {"settled": False, "error": str(e)}


# =============================================================================
# MARKET SCANNER — Discover and parse Kalshi markets
# =============================================================================

@dataclass
class MarketOpportunity:
    """A scanned market with pricing data."""
    ticker: str
    event_ticker: str
    category: str           # Key into CATEGORY_CONFIGS
    asset: str              # "BTC", "ETH", "SOL"
    direction: str          # "up" (YES = above strike) or "down" (NO = below strike)
    strike: float
    close_time: datetime
    seconds_left: float
    # Pricing
    spot: float
    model_vol: float
    model_fair: float
    ask_price: float        # Best available ask
    ask_size: int
    # Edge (net of fees)
    gross_edge_pct: float
    net_edge_pct: float
    fee: float
    implied_vol: Optional[float]
    vol_regime: Optional[str]


import re

def _extract_strike_from_ticker(ticker: str) -> Optional[tuple]:
    """
    Extract strike price and direction from Kalshi market ticker.
    E.g. KXBTC-26FEB11-T100000 -> (100000.0, "up")
         KXBTC-26FEB11-B97000  -> (97000.0, "down")
    """
    # Match the strike part: T or B followed by digits (possibly with decimal)
    m = re.search(r'-([TB])(\d+(?:\.\d+)?)$', ticker)
    if not m:
        return None
    direction = "up" if m.group(1) == "T" else "down"
    strike = float(m.group(2))
    # Kalshi uses whole dollars in ticker for crypto
    return strike, direction


def _extract_strike_from_market_data(raw: dict) -> Optional[float]:
    """
    Extract strike price from event subtitle, product metadata, or market fields.
    Used for 15-min markets where the ticker doesn't encode the strike.
    Multi-fallback approach (matches vol_live_trader_kalshi.py).
    """
    # 1. Subtitle: "Price to beat: $XX,XXX.XX"
    for fld in ("_event_sub_title", "subtitle", "yes_sub_title", "no_sub_title"):
        text = raw.get(fld, "")
        if text and "$" in text:
            m = re.search(r'\$?([\d,]+\.?\d*)', text.replace(',', ''))
            if m:
                try:
                    return float(m.group(1))
                except (ValueError, TypeError):
                    pass

    # 2. Event product_metadata
    meta = raw.get("_event_product_metadata", {}) or {}
    for key in ("strike_price", "strike", "reference_price", "price_to_beat"):
        val = meta.get(key)
        if val:
            try:
                return float(str(val).replace(',', '').replace('$', ''))
            except (ValueError, TypeError):
                pass

    # 3. Numeric fields on the market dict itself
    for fld in ("floor_strike", "cap_strike", "strike_price", "strike",
                "custom_strike", "settlement_value"):
        val = raw.get(fld)
        if val and isinstance(val, (int, float)) and val > 0:
            return float(val)

    return None


def _extract_asset_from_series(series_ticker: str) -> Optional[str]:
    """Extract asset from series ticker. E.g. KXBTC -> BTC, KXETH -> ETH."""
    for asset in CHAINLINK_SYMBOLS:
        if asset in series_ticker.upper():
            return asset
    return None


class MarketScanner:
    """Discovers and catalogs Kalshi markets across all enabled categories."""

    def __init__(self, api: KalshiAPI):
        self.api = api
        self._market_cache: dict[str, dict] = {}  # ticker -> raw market data
        self._last_scan_time: dict[str, float] = {}  # series -> timestamp

    async def scan_category(self, cat_key: str, config: CategoryConfig) -> list:
        """
        Scan all markets for a category. Returns list of raw market dicts
        enriched with category info.
        """
        if not config.enabled:
            return []

        markets = []
        for series in config.series_tickers:
            try:
                events = await self.api.get_events(series)
                for evt in events:
                    for mkt in evt.get("markets", []):
                        mkt["_category"] = cat_key
                        mkt["_series"] = series
                        mkt["_event_sub_title"] = evt.get("sub_title", "")
                        mkt["_event_product_metadata"] = evt.get("product_metadata", {}) or {}
                        markets.append(mkt)
            except Exception as e:
                logger.debug(f"Scan error for {series}: {e}")

        self._last_scan_time[cat_key] = time.time()
        return markets

    async def scan_all(self) -> list:
        """Scan all enabled categories. Returns list of raw market dicts."""
        tasks = []
        for cat_key, config in CATEGORY_CONFIGS.items():
            if config.enabled:
                tasks.append(self.scan_category(cat_key, config))
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_markets = []
        for r in results:
            if isinstance(r, list):
                all_markets.extend(r)
            elif isinstance(r, Exception):
                logger.error(f"Category scan error: {r}")
        return all_markets

    def parse_market(
        self, raw: dict, now: datetime
    ) -> Optional[dict]:
        """
        Parse a raw Kalshi market dict into structured data.
        Returns dict with ticker, asset, strike, direction, close_time, seconds_left
        or None if unparseable.
        """
        ticker = raw.get("ticker", "")
        status = raw.get("status", "")
        if status != "open":
            return None

        close_str = raw.get("close_time") or raw.get("expiration_time")
        if not close_str:
            return None

        try:
            close_time = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None

        seconds_left = (close_time - now).total_seconds()
        if seconds_left <= 0:
            return None

        # Extract strike and direction from ticker
        parsed = _extract_strike_from_ticker(ticker)
        if parsed:
            strike, direction = parsed
        else:
            # Fallback: extract strike from subtitle/metadata (15-min markets)
            strike = _extract_strike_from_market_data(raw)
            if not strike:
                return None
            direction = None  # Signal: both sides should be evaluated

        # Extract asset
        series = raw.get("_series", "")
        asset = _extract_asset_from_series(series)
        if not asset:
            # Try from ticker directly
            for a in CHAINLINK_SYMBOLS:
                if a in ticker.upper():
                    asset = a
                    break
        if not asset:
            return None

        return {
            "ticker": ticker,
            "event_ticker": raw.get("event_ticker", ""),
            "category": raw.get("_category", ""),
            "asset": asset,
            "direction": direction,
            "strike": strike,
            "close_time": close_time,
            "seconds_left": seconds_left,
        }


# =============================================================================
# POSITION TRACKER — Airtight local position state + reconciliation
# =============================================================================

@dataclass
class OpenPosition:
    """Tracked open position."""
    trade_id: str
    ticker: str
    event_ticker: str
    category: str
    asset: str
    direction: str
    side: str           # "yes" or "no"
    contracts: int
    entry_price: float
    cost: float
    fee: float
    order_id: str
    opened_at: datetime
    close_time: datetime  # When the market settles


class PositionTracker:
    """
    Maintains local position state. All positions must be confirmed via API —
    never assumed from order placement alone.
    """

    def __init__(self):
        self._positions: dict[str, OpenPosition] = {}  # trade_id -> position
        self._lock = threading.Lock()

    def add(self, pos: OpenPosition):
        with self._lock:
            self._positions[pos.trade_id] = pos

    def remove(self, trade_id: str):
        with self._lock:
            self._positions.pop(trade_id, None)

    def get_all(self) -> list:
        with self._lock:
            return list(self._positions.values())

    def get_by_ticker(self, ticker: str) -> list:
        with self._lock:
            return [p for p in self._positions.values() if p.ticker == ticker]

    def get_by_category(self, category: str) -> list:
        with self._lock:
            return [p for p in self._positions.values() if p.category == category]

    def count_total(self) -> int:
        with self._lock:
            return len(self._positions)

    def count_by_category(self, category: str) -> int:
        with self._lock:
            return sum(1 for p in self._positions.values() if p.category == category)

    def count_by_ticker(self, ticker: str) -> int:
        with self._lock:
            return sum(1 for p in self._positions.values() if p.ticker == ticker)

    def count_by_event(self, event_ticker: str) -> int:
        with self._lock:
            return sum(1 for p in self._positions.values() if p.event_ticker == event_ticker)


# =============================================================================
# CATEGORY STATS TRACKER — Per-category P&L, win rate, circuit breakers
# =============================================================================

class CategoryStatsTracker:
    """
    Tracks running P&L and win rate per category.
    Auto-disables categories that breach loss or win-rate thresholds.
    """

    def __init__(self, db: sqlite3.Connection):
        self.db = db
        self._stats: dict[str, dict] = {}
        self._load_from_db()

    def _load_from_db(self):
        """Load persisted stats from SQLite."""
        rows = self.db.execute("SELECT * FROM category_stats").fetchall()
        cols = [d[0] for d in self.db.execute("SELECT * FROM category_stats").description]
        for row in rows:
            d = dict(zip(cols, row))
            self._stats[d["category"]] = d

    def record_settlement(
        self, category: str, won: bool, pnl: float, cost: float, payout: float
    ):
        """Record a settled trade. Returns True if category should be disabled."""
        if category not in self._stats:
            self._stats[category] = {
                "category": category,
                "total_trades": 0,
                "wins": 0,
                "losses": 0,
                "total_pnl": 0.0,
                "total_cost": 0.0,
                "total_payout": 0.0,
            }

        s = self._stats[category]
        s["total_trades"] += 1
        s["total_pnl"] += pnl
        s["total_cost"] += cost
        s["total_payout"] += payout
        if won:
            s["wins"] += 1
        else:
            s["losses"] += 1
        s["last_updated"] = datetime.now(timezone.utc).isoformat()

        # Persist to DB
        self.db.execute(
            """
            INSERT INTO category_stats (category, total_trades, wins, losses,
                total_pnl, total_cost, total_payout, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(category) DO UPDATE SET
                total_trades=excluded.total_trades,
                wins=excluded.wins,
                losses=excluded.losses,
                total_pnl=excluded.total_pnl,
                total_cost=excluded.total_cost,
                total_payout=excluded.total_payout,
                last_updated=excluded.last_updated
            """,
            (
                category, s["total_trades"], s["wins"], s["losses"],
                s["total_pnl"], s["total_cost"], s["total_payout"],
                s["last_updated"],
            ),
        )
        self.db.commit()

    def check_circuit_breaker(self, category: str) -> Optional[str]:
        """
        Check if a category should be auto-disabled.
        Returns reason string if should disable, None otherwise.
        """
        config = CATEGORY_CONFIGS.get(category)
        if not config:
            return None

        s = self._stats.get(category, {})
        total = s.get("total_trades", 0)
        pnl = s.get("total_pnl", 0.0)
        wins = s.get("wins", 0)

        # Only check after minimum trades
        if total < config.min_trades_for_breaker:
            return None

        # Loss threshold
        if pnl < config.loss_threshold_dollars:
            return f"P&L ${pnl:.2f} below threshold ${config.loss_threshold_dollars:.2f}"

        # Win rate
        win_rate = wins / total if total > 0 else 0
        if win_rate < config.min_win_rate:
            return f"Win rate {win_rate:.1%} below minimum {config.min_win_rate:.1%} ({total} trades)"

        return None

    def get_stats(self, category: str) -> dict:
        return self._stats.get(category, {})

    def get_all_stats(self) -> dict:
        return dict(self._stats)

    def get_daily_pnl(self) -> float:
        """Total P&L across all categories (from DB for today)."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        row = self.db.execute(
            "SELECT COALESCE(SUM(pnl), 0) FROM trades WHERE settled=1 AND settled_at LIKE ?",
            (f"{today}%",),
        ).fetchone()
        return row[0] if row else 0.0


# =============================================================================
# EDGE SCANNER ENGINE — The core scanning + execution loop
# =============================================================================

class EdgeScanner:
    """
    Main engine. Orchestrates:
    1. Market discovery (scan all enabled categories)
    2. Fair value estimation (vol-based BS model for crypto)
    3. Edge detection (compare fair value to best ask, net of fees)
    4. Opportunity ranking (best edge first)
    5. Order execution (1 contract per opportunity)
    6. Position tracking + fill confirmation
    7. Settlement monitoring + P&L accounting
    8. Circuit breakers (per-category and global)
    """

    def __init__(self, api: KalshiAPI, paper_mode: bool = False, verbose: bool = False):
        self.api = api
        self.paper_mode = paper_mode
        self.verbose = verbose

        # Components
        self.db = init_db()
        self.chainlink = ChainlinkFeed()
        self.vol_tracker = VolTracker()
        self.pricer = BinaryPricer()
        self.scanner = MarketScanner(api)
        self.positions = PositionTracker()
        self.cat_stats = CategoryStatsTracker(self.db)

        # Connect Chainlink price feed to vol tracker
        self.chainlink.on_price(self.vol_tracker.feed_price)

        # Trade counting for hourly rate limit
        self._hourly_trades: deque = deque()

        # Shutdown flag
        self._running = False

        # Track disabled categories (from circuit breakers)
        self._disabled_categories: set = set()

    # ── Fair Value Models ───────────────────────────────────────────────────

    def _fair_value_vol_bs(
        self, asset: str, strike: float, seconds_left: float, direction: str
    ) -> Optional[tuple]:
        """
        Black-Scholes vol-based fair value for crypto binary options.
        Returns (fair_value, vol_used, implied_vol) or None.
        """
        spot = self.chainlink.get_price(asset)
        if not spot:
            return None

        vol = self.vol_tracker.get_adaptive_vol(asset, seconds_left)
        if not vol:
            return None

        fair = self.pricer.fair_value(spot, strike, seconds_left, vol, direction)
        # Clamp to reasonable range
        fair = max(0.001, min(0.999, fair))

        return fair, vol, spot

    def compute_fair_value(
        self, category: str, asset: str, strike: float, seconds_left: float, direction: str
    ) -> Optional[tuple]:
        """
        Compute fair value using the model configured for this category.
        Returns (fair_value, vol, spot) or None.
        """
        config = CATEGORY_CONFIGS.get(category)
        if not config:
            return None

        if config.fair_value_model == "vol_bs":
            return self._fair_value_vol_bs(asset, strike, seconds_left, direction)

        logger.warning(f"Unknown fair value model: {config.fair_value_model}")
        return None

    # ── Opportunity Evaluation ──────────────────────────────────────────────

    async def evaluate_market(
        self, parsed: dict, config: CategoryConfig
    ) -> Optional[MarketOpportunity]:
        """
        Evaluate a single parsed market for trading opportunity.
        Returns MarketOpportunity if edge meets threshold, None otherwise.
        """
        ticker = parsed["ticker"]
        category = parsed["category"]
        asset = parsed["asset"]
        direction = parsed["direction"]
        strike = parsed["strike"]
        seconds_left = parsed["seconds_left"]
        close_time = parsed["close_time"]

        # Time filter
        if seconds_left < config.min_seconds_to_expiry:
            return None
        if seconds_left > config.max_seconds_to_expiry:
            return None

        # Check position limits
        if self.positions.count_by_ticker(ticker) >= MAX_POSITIONS_PER_MARKET:
            return None
        if self.positions.count_by_category(category) >= config.max_positions_per_category:
            return None
        # Event-level limit (correlated risk across strikes in same event)
        event_ticker = parsed.get("event_ticker", "")
        if event_ticker and self.positions.count_by_event(event_ticker) >= config.max_positions_per_event:
            return None

        # Compute fair value
        fv_result = self.compute_fair_value(category, asset, strike, seconds_left, direction)
        if not fv_result:
            return None
        fair, vol, spot = fv_result

        # Get orderbook
        ob = await self.api.get_orderbook(ticker)
        # For "up" direction, we buy YES side. For "down", we buy NO side.
        side = "yes" if direction == "up" else "no"
        ask_price = ob.get(f"{side}_ask")
        ask_size = ob.get(f"{side}_ask_size", 0)

        if not ask_price or ask_price <= 0:
            return None

        # Price filters
        if ask_price < config.min_price or ask_price > config.max_price:
            return None

        # Gross edge
        gross_edge_pct = ((fair - ask_price) / fair) * 100 if fair > 0 else 0

        # Net edge (after fees)
        contracts = config.contracts_per_trade
        net_edge_pct, fee, net_profit = net_edge_after_fees(fair, ask_price, contracts)

        # Edge filters
        if gross_edge_pct < config.min_edge_pct:
            return None
        if gross_edge_pct > config.max_edge_pct:
            logger.debug(
                f"SKIP {ticker}: edge {gross_edge_pct:.1f}% > max {config.max_edge_pct}% (model error?)"
            )
            return None

        # Implied vol for logging
        iv = self.pricer.implied_vol(ask_price, spot, strike, seconds_left, direction)
        vol_regime = self.vol_tracker.get_vol_regime(asset)

        return MarketOpportunity(
            ticker=ticker,
            event_ticker=parsed["event_ticker"],
            category=category,
            asset=asset,
            direction=direction,
            strike=strike,
            close_time=close_time,
            seconds_left=seconds_left,
            spot=spot,
            model_vol=vol,
            model_fair=fair,
            ask_price=ask_price,
            ask_size=ask_size,
            gross_edge_pct=gross_edge_pct,
            net_edge_pct=net_edge_pct,
            fee=fee,
            implied_vol=iv,
            vol_regime=vol_regime,
        )

    # ── Execution ───────────────────────────────────────────────────────────

    async def execute_opportunity(self, opp: MarketOpportunity) -> Optional[str]:
        """
        Execute a trade for an opportunity. Returns trade_id or None.
        """
        config = CATEGORY_CONFIGS.get(opp.category)
        if not config:
            return None

        contracts = config.contracts_per_trade
        side = "yes" if opp.direction == "up" else "no"
        trade_id = f"ES-{int(time.time())}-{uuid.uuid4().hex[:6]}"

        # Pre-execution logging
        logger.info(
            f"{'[PAPER] ' if self.paper_mode else ''}"
            f"TRADE {opp.asset} {opp.direction.upper()} | "
            f"{opp.ticker} | strike=${opp.strike:,.0f} | "
            f"fair={opp.model_fair:.3f} ask={opp.ask_price:.3f} | "
            f"edge={opp.gross_edge_pct:.1f}% (net={opp.net_edge_pct:.1f}%) | "
            f"vol={opp.model_vol*100:.1f}% | {opp.seconds_left:.0f}s left"
        )

        fill_price = None
        order_id = None
        order_status = "paper" if self.paper_mode else "pending"

        if not self.paper_mode:
            # Place IOC limit order at the displayed ask
            order_id = await self.api.place_limit_buy(
                ticker=opp.ticker,
                side=side,
                price_dollars=opp.ask_price,
                count=contracts,
            )

            if not order_id:
                logger.warning(f"Order rejected for {opp.ticker}")
                order_status = "rejected"
                # Still log the attempt
                self._log_trade_to_db(trade_id, opp, contracts, order_id, order_status, None)
                return None

            # Confirm fill via API (IOC either fills immediately or is cancelled)
            await asyncio.sleep(0.5)
            order_data = await self.api.get_order(order_id)
            status = order_data.get("status", "unknown")
            fill_count = order_data.get("fill_count", 0) or 0

            if fill_count <= 0:
                logger.info(f"Order {order_id} not filled (IOC expired)")
                order_status = "unfilled"
                self._log_trade_to_db(trade_id, opp, contracts, order_id, order_status, None)
                return None

            # Get actual fill price from fills endpoint
            fills = await self.api.get_fills(order_id=order_id)
            if fills:
                total_cost_cents = 0
                total_qty = 0
                for f in fills:
                    price_cents = f.get(f"{side}_price", 0) or 0
                    qty = f.get("count", 0) or 0
                    total_cost_cents += price_cents * qty
                    total_qty += qty
                if total_qty > 0:
                    fill_price = total_cost_cents / total_qty / 100
                    contracts = total_qty  # Actual filled quantity

            if fill_price is None:
                fill_price = opp.ask_price

            order_status = "filled"
            logger.info(
                f"FILLED {opp.ticker} {contracts}x @ ${fill_price:.2f} "
                f"(order {order_id})"
            )
        else:
            # Paper mode: simulate fill at ask price
            fill_price = opp.ask_price
            order_status = "paper_filled"

        # Record trade
        cost = contracts * fill_price
        fee = kalshi_taker_fee(contracts, fill_price) if not self.paper_mode else 0

        self._log_trade_to_db(
            trade_id, opp, contracts, order_id, order_status, fill_price
        )

        # Track position
        pos = OpenPosition(
            trade_id=trade_id,
            ticker=opp.ticker,
            event_ticker=opp.event_ticker,
            category=opp.category,
            asset=opp.asset,
            direction=opp.direction,
            side=side,
            contracts=contracts,
            entry_price=fill_price,
            cost=cost,
            fee=fee,
            order_id=order_id or "",
            opened_at=datetime.now(timezone.utc),
            close_time=opp.close_time,
        )
        self.positions.add(pos)

        # Track hourly rate
        self._hourly_trades.append(time.time())

        return trade_id

    def _log_trade_to_db(
        self,
        trade_id: str,
        opp: MarketOpportunity,
        contracts: int,
        order_id: Optional[str],
        order_status: str,
        fill_price: Optional[float],
    ):
        """Log trade to SQLite."""
        cost = contracts * (fill_price or opp.ask_price)
        fee = kalshi_taker_fee(contracts, fill_price or opp.ask_price)

        try:
            self.db.execute(
                """
                INSERT OR REPLACE INTO trades (
                    trade_id, timestamp, category, asset, ticker, direction,
                    strike, spot_entry, seconds_left, model_vol, model_fair,
                    ask_price, fill_price, edge_pct, implied_vol, vol_regime,
                    contracts, cost, fee, order_id, order_status, paper_trade
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade_id,
                    datetime.now(timezone.utc).isoformat(),
                    opp.category,
                    opp.asset,
                    opp.ticker,
                    opp.direction,
                    opp.strike,
                    opp.spot,
                    opp.seconds_left,
                    opp.model_vol,
                    opp.model_fair,
                    opp.ask_price,
                    fill_price,
                    opp.net_edge_pct,
                    opp.implied_vol,
                    opp.vol_regime,
                    contracts,
                    cost,
                    fee,
                    order_id or "",
                    order_status,
                    1 if self.paper_mode else 0,
                ),
            )
            self.db.commit()
        except Exception as e:
            logger.error(f"DB write error: {e}")

    # ── Settlement Monitoring ───────────────────────────────────────────────

    async def check_settlements(self):
        """Check all open positions for settlement."""
        positions = self.positions.get_all()
        now = datetime.now(timezone.utc)

        for pos in positions:
            # Only check after market close time
            if now < pos.close_time + timedelta(seconds=30):
                continue

            try:
                settlement = await self.api.get_market_settlement(pos.ticker)
                if not settlement.get("settled"):
                    # If it's been too long, log a warning
                    age = (now - pos.close_time).total_seconds()
                    if age > 600:  # 10 minutes past close
                        logger.warning(
                            f"Position {pos.trade_id} ({pos.ticker}) still unsettled "
                            f"{age:.0f}s after close"
                        )
                    continue

                result = settlement.get("result", "")
                won = False
                if pos.side == "yes" and result == "yes":
                    won = True
                elif pos.side == "no" and result == "no":
                    won = True

                payout = pos.contracts * 1.0 if won else 0.0
                pnl = payout - pos.cost - pos.fee

                # Update DB
                self.db.execute(
                    """
                    UPDATE trades SET
                        settled=1, settlement_result=?, payout=?, pnl=?,
                        settled_at=?
                    WHERE trade_id=?
                    """,
                    (result, payout, pnl, now.isoformat(), pos.trade_id),
                )
                self.db.commit()

                # Update category stats
                self.cat_stats.record_settlement(
                    pos.category, won, pnl, pos.cost, payout
                )

                # Check circuit breaker
                disable_reason = self.cat_stats.check_circuit_breaker(pos.category)
                if disable_reason and pos.category not in self._disabled_categories:
                    self._disabled_categories.add(pos.category)
                    CATEGORY_CONFIGS[pos.category].enabled = False
                    logger.warning(
                        f"CIRCUIT BREAKER: Disabled category '{pos.category}': {disable_reason}"
                    )
                    self.db.execute(
                        """
                        UPDATE category_stats SET disabled_at=?, disable_reason=?
                        WHERE category=?
                        """,
                        (now.isoformat(), disable_reason, pos.category),
                    )
                    self.db.commit()

                # Log settlement
                logger.info(
                    f"SETTLE {pos.ticker} | {'WIN' if won else 'LOSS'} | "
                    f"result={result} | payout=${payout:.2f} cost=${pos.cost:.2f} "
                    f"fee=${pos.fee:.2f} | P&L=${pnl:+.2f}"
                )

                # Remove from tracker
                self.positions.remove(pos.trade_id)

            except Exception as e:
                logger.debug(f"Settlement check error for {pos.ticker}: {e}")

    # ── Reconciliation ──────────────────────────────────────────────────────

    async def reconcile_positions(self):
        """
        Compare local position state to actual Kalshi portfolio.
        Detect phantom fills and missing positions.
        """
        try:
            api_positions = await self.api.get_positions()
            local_positions = self.positions.get_all()

            # Build set of tickers we think we have
            local_tickers = {p.ticker for p in local_positions}

            # Build set of tickers Kalshi says we have
            api_tickers = set()
            for p in api_positions:
                ticker = p.get("ticker", "")
                yes_count = p.get("market_position", {}).get("position", 0) if isinstance(p.get("market_position"), dict) else 0
                # Handle flat structure too
                if not yes_count:
                    yes_count = p.get("position", 0)
                if yes_count != 0:
                    api_tickers.add(ticker)

            # Phantom positions: we think we have it, Kalshi doesn't
            phantoms = local_tickers - api_tickers
            if phantoms:
                logger.warning(f"RECONCILE: Phantom positions detected: {phantoms}")
                for p in local_positions:
                    if p.ticker in phantoms:
                        logger.warning(
                            f"  Removing phantom: {p.trade_id} ({p.ticker})"
                        )
                        self.positions.remove(p.trade_id)

            # Missing positions: Kalshi has it, we don't track it
            # (This can happen after restart — we only track positions opened in this session)
            missing = api_tickers - local_tickers
            if missing:
                logger.info(
                    f"RECONCILE: Kalshi has positions we don't track: {missing} "
                    f"(may be from other bots or previous sessions)"
                )

        except Exception as e:
            logger.error(f"Reconciliation error: {e}")

    # ── Main Scan Loop ──────────────────────────────────────────────────────

    async def scan_once(self) -> dict:
        """
        Run one complete scan cycle:
        1. Discover markets across all categories
        2. Evaluate each for edge
        3. Rank opportunities
        4. Execute best ones (within limits)

        Returns scan stats dict.
        """
        scan_start = time.time()
        now = datetime.now(timezone.utc)

        # Global checks
        if self.positions.count_total() >= MAX_OPEN_POSITIONS_GLOBAL:
            logger.debug("Global position limit reached, skipping scan")
            return {"markets_scanned": 0, "opportunities": 0, "trades": 0, "skipped": "position_limit"}

        # Daily loss check
        daily_pnl = self.cat_stats.get_daily_pnl()
        if daily_pnl < -MAX_DAILY_LOSS_DOLLARS:
            logger.warning(f"Daily loss limit reached: ${daily_pnl:.2f}")
            return {"markets_scanned": 0, "opportunities": 0, "trades": 0, "skipped": "daily_loss"}

        # Hourly trade rate check
        cutoff = time.time() - 3600
        while self._hourly_trades and self._hourly_trades[0] < cutoff:
            self._hourly_trades.popleft()
        if len(self._hourly_trades) >= MAX_TRADES_PER_HOUR:
            logger.debug("Hourly trade limit reached")
            return {"markets_scanned": 0, "opportunities": 0, "trades": 0, "skipped": "hourly_limit"}

        # 1. Discover markets
        raw_markets = await self.scanner.scan_all()

        # 2. Parse and filter
        parsed_markets = []
        for raw in raw_markets:
            parsed = self.scanner.parse_market(raw, now)
            if not parsed:
                continue
            if parsed["direction"] is None:
                # 15-min market: evaluate both up (YES) and down (NO) sides
                for d in ("up", "down"):
                    entry = dict(parsed)
                    entry["direction"] = d
                    parsed_markets.append(entry)
            else:
                parsed_markets.append(parsed)

        # 3. Evaluate each market for edge
        opportunities = []
        for parsed in parsed_markets:
            config = CATEGORY_CONFIGS.get(parsed["category"])
            if not config or not config.enabled:
                continue
            if parsed["category"] in self._disabled_categories:
                continue

            # Vol warmup check
            if not self.vol_tracker.is_warmed_up(parsed["asset"]):
                continue

            opp = await self.evaluate_market(parsed, config)
            if opp:
                opportunities.append(opp)

        # 4. Rank by net edge (best first), with near-the-money preference as tiebreaker
        def _rank_key(opp):
            # Primary: net edge (higher is better)
            # Secondary: closeness to ATM (lower moneyness distance is better)
            if opp.spot > 0 and opp.strike > 0:
                moneyness_distance = abs(math.log(opp.spot / opp.strike))
            else:
                moneyness_distance = 999
            return (opp.net_edge_pct, -moneyness_distance)

        opportunities.sort(key=_rank_key, reverse=True)

        # 5. Execute (best first, within limits)
        trades_executed = 0
        for opp in opportunities:
            # Re-check limits before each trade
            if self.positions.count_total() >= MAX_OPEN_POSITIONS_GLOBAL:
                break
            config = CATEGORY_CONFIGS.get(opp.category)
            if config and self.positions.count_by_category(opp.category) >= config.max_positions_per_category:
                continue
            if self.positions.count_by_ticker(opp.ticker) >= MAX_POSITIONS_PER_MARKET:
                continue
            # Event-level limit (correlated risk)
            if config and opp.event_ticker and self.positions.count_by_event(opp.event_ticker) >= config.max_positions_per_event:
                continue

            trade_id = await self.execute_opportunity(opp)
            if trade_id:
                trades_executed += 1

        scan_duration_ms = (time.time() - scan_start) * 1000

        # Log scan
        best_edge = max((o.net_edge_pct for o in opportunities), default=0)
        try:
            self.db.execute(
                """
                INSERT INTO scan_log (timestamp, markets_scanned, opportunities_found,
                    trades_executed, best_edge_pct, scan_duration_ms)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    now.isoformat(),
                    len(parsed_markets),
                    len(opportunities),
                    trades_executed,
                    best_edge,
                    scan_duration_ms,
                ),
            )
            self.db.commit()
        except Exception:
            pass

        if opportunities or self.verbose:
            logger.info(
                f"SCAN: {len(parsed_markets)} markets | "
                f"{len(opportunities)} opportunities | "
                f"{trades_executed} trades | "
                f"best edge={best_edge:.1f}% | "
                f"{scan_duration_ms:.0f}ms"
            )

        return {
            "markets_scanned": len(parsed_markets),
            "opportunities": len(opportunities),
            "trades": trades_executed,
            "best_edge": best_edge,
            "duration_ms": scan_duration_ms,
        }

    # ── Dashboard ───────────────────────────────────────────────────────────

    def print_dashboard(self):
        """Print periodic stats summary."""
        now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        mode = "PAPER" if self.paper_mode else "LIVE"

        lines = [
            f"\n{'='*70}",
            f"  EDGE SCANNER [{mode}] — {now}",
            f"{'='*70}",
        ]

        # Vol status
        lines.append(f"  Vol: {self.vol_tracker.status_str()}")
        cl_status = "Connected" if self.chainlink.is_connected() else "DISCONNECTED"
        lines.append(f"  Chainlink: {cl_status} ({self.chainlink.update_count} ticks)")

        # Open positions
        positions = self.positions.get_all()
        lines.append(f"\n  Open Positions: {len(positions)}/{MAX_OPEN_POSITIONS_GLOBAL}")
        for pos in positions:
            age = (datetime.now(timezone.utc) - pos.opened_at).total_seconds()
            ttl = (pos.close_time - datetime.now(timezone.utc)).total_seconds()
            lines.append(
                f"    {pos.ticker} | {pos.direction.upper()} | "
                f"{pos.contracts}x @ ${pos.entry_price:.2f} | "
                f"settles in {max(0, ttl):.0f}s"
            )

        # Per-category stats
        lines.append(f"\n  Category Performance:")
        for cat_key, config in CATEGORY_CONFIGS.items():
            stats = self.cat_stats.get_stats(cat_key)
            total = stats.get("total_trades", 0)
            wins = stats.get("wins", 0)
            pnl = stats.get("total_pnl", 0.0)
            wr = (wins / total * 100) if total > 0 else 0
            status = "ON" if config.enabled and cat_key not in self._disabled_categories else "OFF"
            cat_pos = self.positions.count_by_category(cat_key)
            lines.append(
                f"    [{status}] {config.name}: "
                f"{total} trades | WR={wr:.0f}% | "
                f"P&L=${pnl:+.2f} | "
                f"positions={cat_pos}/{config.max_positions_per_category}"
            )

        # Global stats
        daily_pnl = self.cat_stats.get_daily_pnl()
        hourly_count = len(self._hourly_trades)
        lines.append(f"\n  Daily P&L: ${daily_pnl:+.2f} | Trades/hr: {hourly_count}/{MAX_TRADES_PER_HOUR}")

        # Disabled categories
        if self._disabled_categories:
            lines.append(f"\n  DISABLED: {', '.join(self._disabled_categories)}")

        lines.append(f"{'='*70}")
        print("\n".join(lines))

    # ── Main Run Loop ───────────────────────────────────────────────────────

    async def run(self):
        """Main async run loop."""
        self._running = True
        logger.info(f"Edge Scanner starting ({'PAPER' if self.paper_mode else 'LIVE'} mode)")
        logger.info(f"DB: {DB_PATH}")
        logger.info(f"Log: {os.path.join(LOG_DIR, 'edge_scanner.log')}")

        # Connect to Kalshi
        if not self.paper_mode:
            balance = await self.api.connect()
            logger.info(f"Kalshi balance: ${balance:.2f}")
        else:
            # Paper mode: still connect for market data
            try:
                balance = await self.api.connect()
                logger.info(f"Kalshi balance: ${balance:.2f} (paper mode — no real trades)")
            except Exception as e:
                logger.warning(f"Kalshi connection failed (paper mode OK): {e}")

        # Start Chainlink feed
        self.chainlink.start()
        logger.info("Chainlink price feed starting...")

        # Wait for initial vol warmup
        logger.info(f"Waiting {VOL_WARMUP_MINUTES} minutes for vol warmup...")
        warmup_start = time.time()
        while self._running:
            all_warmed = all(
                self.vol_tracker.is_warmed_up(a) for a in CHAINLINK_SYMBOLS
            )
            if all_warmed:
                logger.info("Vol warmup complete for all assets")
                break
            elapsed = time.time() - warmup_start
            if elapsed > VOL_WARMUP_MINUTES * 60 * 3:  # 3x timeout
                logger.warning("Vol warmup timeout — proceeding with available data")
                break
            # Print progress
            warmed = sum(1 for a in CHAINLINK_SYMBOLS if self.vol_tracker.is_warmed_up(a))
            if int(elapsed) % 30 == 0:
                logger.info(
                    f"Warmup: {warmed}/{len(CHAINLINK_SYMBOLS)} assets ready | "
                    f"{self.vol_tracker.status_str()}"
                )
            await asyncio.sleep(5)

        # Print enabled categories
        for cat_key, config in CATEGORY_CONFIGS.items():
            if config.enabled:
                logger.info(
                    f"Category '{cat_key}' enabled: "
                    f"min_edge={config.min_edge_pct}% "
                    f"max_pos={config.max_positions_per_category} "
                    f"model={config.fair_value_model}"
                )

        logger.info(f"Starting scan loop (interval={SCAN_INTERVAL_SECONDS}s)")
        self.print_dashboard()

        last_reconcile = time.time()
        last_settlement_check = time.time()
        last_dashboard = time.time()

        while self._running:
            try:
                # Main scan
                await self.scan_once()

                # Settlement checks
                if time.time() - last_settlement_check > SETTLEMENT_CHECK_INTERVAL:
                    await self.check_settlements()
                    last_settlement_check = time.time()

                # Reconciliation
                if time.time() - last_reconcile > RECONCILE_INTERVAL_SECONDS:
                    if not self.paper_mode:
                        await self.reconcile_positions()
                    last_reconcile = time.time()

                # Dashboard
                if time.time() - last_dashboard > STATS_PRINT_INTERVAL:
                    self.print_dashboard()
                    last_dashboard = time.time()

                # Sleep until next scan
                await asyncio.sleep(SCAN_INTERVAL_SECONDS)

            except asyncio.CancelledError:
                break
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Scan loop error: {e}", exc_info=True)
                await asyncio.sleep(5)

        logger.info("Edge Scanner shutting down...")
        self.chainlink.stop()
        if not self.paper_mode:
            await self.api.close()
        self.print_dashboard()
        logger.info("Shutdown complete.")


# =============================================================================
# CONFIG LOADING
# =============================================================================

def load_config() -> tuple:
    """Load Kalshi credentials from config.json + private key PEM."""
    script_dir = Path(__file__).parent
    config_path = script_dir / "config.json"

    if not config_path.exists():
        print("[FATAL] config.json not found!")
        print(f"  Expected at: {config_path}")
        print("  Create config.json with: kalshi_api_key_id and kalshi_private_key_path")
        sys.exit(1)

    with open(config_path) as f:
        cfg = json.load(f)

    api_key_id = cfg.get("kalshi_api_key_id", "")
    if not api_key_id or "YOUR_" in api_key_id:
        print("[FATAL] config.json missing kalshi_api_key_id")
        print("  Get it from: https://kalshi.com/account/api")
        sys.exit(1)

    # Find private key
    pk_path = cfg.get("kalshi_private_key_path", "")
    if pk_path:
        pk_full = script_dir / pk_path
    else:
        pk_full = script_dir / "kalshi_private_key.pem"

    if not pk_full.exists():
        print(f"[FATAL] Kalshi private key not found: {pk_full}")
        print("  Place kalshi_private_key.pem in the same folder, or set kalshi_private_key_path in config.json")
        sys.exit(1)

    with open(pk_full) as f:
        private_key_pem = f.read()

    if len(private_key_pem.strip()) < 100:
        print("[FATAL] Private key file appears empty or invalid")
        sys.exit(1)

    return api_key_id, private_key_pem


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Kalshi Multi-Market Microedge Harvester",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python edge_scanner.py --paper              # Paper trade mode
  python edge_scanner.py --paper --verbose    # Paper trade with extra logging
  python edge_scanner.py                      # Live trading
  python edge_scanner.py --demo               # Use Kalshi demo/sandbox API
        """,
    )
    parser.add_argument(
        "--paper", action="store_true",
        help="Paper trade mode — log what would happen without executing"
    )
    parser.add_argument(
        "--demo", action="store_true",
        help="Use Kalshi demo/sandbox API instead of production"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Verbose logging (show all scans, even empty ones)"
    )
    args = parser.parse_args()

    # Load config
    api_key_id, private_key_pem = load_config()

    # Create API client
    api = KalshiAPI(api_key_id, private_key_pem, demo=args.demo)

    # Create scanner
    scanner = EdgeScanner(api, paper_mode=args.paper, verbose=args.verbose)

    # Handle Ctrl+C gracefully
    def shutdown(sig, frame):
        logger.info("Shutdown signal received...")
        scanner._running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Print banner
    mode = "PAPER" if args.paper else "LIVE"
    env = "DEMO" if args.demo else "PRODUCTION"
    print(f"""
{'='*60}
  EDGE SCANNER — Kalshi Microedge Harvester
  Mode: {mode} | Environment: {env}
  Categories: {sum(1 for c in CATEGORY_CONFIGS.values() if c.enabled)} enabled
  DB: {DB_PATH}
  Log: {os.path.join(LOG_DIR, 'edge_scanner.log')}
{'='*60}
""")

    # Run
    asyncio.run(scanner.run())


if __name__ == "__main__":
    main()
