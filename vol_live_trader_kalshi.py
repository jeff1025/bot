#!/usr/bin/env python3
"""
Vol Model Live Trader — Directional bets on KALSHI
Uses Chainlink-derived realized vol to find mispriced binary options.
Vol computed from the same oracle that PM settles on — no Binance dependency.

Single-platform (Kalshi only), no hedging required.
Limit orders at displayed ask for fills with downside protection.

Can run simultaneously with arb_scanner.py — shares same Kalshi API
credentials but uses separate journal/state files. Rate limit headroom
is generous (20 reads/sec, 10 writes/sec basic tier).

SETUP:
1. Place this file + vol_paper_trader.py in your project folder
2. config.json must have kalshi_api_key_id and kalshi_private_key_path
3. Run via run_vol_kalshi.bat

STRATEGY:
- Chainlink tick data (via RTDS) → realized vol estimation (Black-Scholes)
- Compare model fair value to Kalshi market price
- Buy when model says contract is cheap (edge > threshold)
- Limit orders at displayed ask (fills immediately, no worse)
- Settlement: Kalshi API result field (CF Benchmarks RTI)
"""

# =============================================================================
# IMPORTS
# =============================================================================
import os
import sys
import math
import json
import re
import time
import uuid
import hashlib
import base64
import threading
import asyncio
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional
from decimal import Decimal

import httpx

# RSA signing for Kalshi auth
try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
    from cryptography.hazmat.backends import default_backend
except ImportError:
    print("[ERROR] cryptography package not installed!")
    print("  Run: pip install cryptography")
    input("\nPress Enter to exit...")
    sys.exit(1)

# Vol model — from paper trader (same folder)
try:
    from vol_paper_trader import (
        VolModel, BinaryPricer,
        get_adaptive_vol,
        MIN_VOL_FLOOR,
    )
except ImportError:
    print("[ERROR] vol_paper_trader.py not found in same folder!")
    print("  Both files must be in the same directory.")
    input("\nPress Enter to exit...")
    sys.exit(1)

# Bot coordinator — shared position registry for multi-bot operation
try:
    from bot_coordinator import register_position, unregister_position
    _HAS_COORDINATOR = True
except ImportError:
    _HAS_COORDINATOR = False
    def register_position(*a, **kw): pass
    def unregister_position(*a, **kw): pass

# =============================================================================
# CONFIGURATION
# =============================================================================

DATA_DIR = os.path.join(os.path.expanduser("~"), ".arb_data")
LIVE_JOURNAL_FILE = os.path.join(DATA_DIR, "vol_kalshi_trades_v2.jsonl")
LIVE_SUMMARY_FILE = os.path.join(DATA_DIR, "vol_kalshi_summary_v2.json")

# Trading parameters
MIN_EDGE_PCT = 20.0         # Flat 20% min edge: ~15% model overestimate buffer + ~5% fee coverage
MAX_EDGE_PCT = 60.0         # Above this, assume model error
MIN_SECONDS_LEFT = 180      # Don't trade with < 3 min remaining
MAX_SECONDS_LEFT = 840      # Full window minus ~1 min
SCAN_INTERVAL = 15          # Seconds between scans
WARMUP_MINUTES = 3          # Min Chainlink data before trading
ORDER_TIMEOUT_SEC = 30      # Cancel unfilled orders after this
CONFIRM_SCANS = 1           # Execute on first sighting
CRYPTOS = ["BTC", "ETH", "SOL"]

def get_min_edge(seconds_left: float, direction: str = "up", market_price: float = 0) -> float:
    """Flat 20% minimum edge for all trades.
    
    Derived from data: model overestimates by ~15% ROI on average,
    plus ~5-6% for Kalshi fees. Payout asymmetry naturally favors
    cheap contracts (one win at 25¢ covers 3 losses) while making
    expensive contracts nearly impossible to profit on.
    """
    return MIN_EDGE_PCT

# Vol regime filters
MAX_VOL_CEILING = 1.00
VOL_EDGE_MULTIPLIERS = {
    "contracting": 1.0,
    "normal": 1.0,
    "expanding": 1.5,
}

# Safety limits
MAX_PENDING_TRADES = 3
MAX_LOSS_PER_SESSION = 50.0
MAX_TRADES_PER_HOUR = 20
MAX_TRADES_PER_WINDOW = 3

# Position sizing — 10% Kelly Criterion
KELLY_FRACTION = 0.10        # 10% of full Kelly (conservative)
MAX_CONTRACTS_PER_TRADE = 10 # Hard cap regardless of Kelly
MAX_OPEN_TRADES = 3          # 1 per coin max
BALANCE_RESERVE_PCT = 75

# Kalshi fee formula (taker): ceil(0.07 × contracts × price × (1 - price))
# Maker (resting limit): free or ceil(0.0175 × C × P × (1 - P))
KALSHI_TAKER_FEE_RATE = 0.07
KALSHI_MAKER_FEE_RATE = 0.0  # Resting limits are free

KALSHI_API_URL = "https://api.elections.kalshi.com/trade-api/v2"

os.makedirs(DATA_DIR, exist_ok=True)

# ── Interactive strike input ──────────────────────────────────────────────
_input_queue = []
_input_lock = threading.Lock()

def _input_listener():
    while True:
        try:
            line = input()
            with _input_lock:
                _input_queue.append(line.strip().lower())
        except EOFError:
            break

def _check_for_strike_input():
    with _input_lock:
        cmds = list(_input_queue)
        _input_queue.clear()
    for cmd in cmds:
        if cmd == "s":
            _prompt_strikes()

def _prompt_strikes():
    """Interactive strike price entry"""
    print("\n── Manual Strike Entry ──")
    strikes_file = Path.home() / "strikes.json"
    try:
        if strikes_file.exists():
            with open(strikes_file) as f:
                data = json.load(f)
        else:
            data = {"strikes": {}}
        
        now = datetime.now(timezone.utc)
        current_minute = now.minute
        slot_minute = (current_minute // 15) * 15
        window_end = now.replace(minute=slot_minute, second=0, microsecond=0) + timedelta(minutes=15)
        window_key = window_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        print(f"Window ending: {window_key}")
        
        for crypto in CRYPTOS:
            existing = data.get("strikes", {}).get(window_key, {}).get(crypto)
            prompt = f"  {crypto} strike"
            if existing:
                prompt += f" (current: ${existing:,.2f})"
            prompt += ": "
            val = input(prompt).strip()
            if val:
                try:
                    price = float(val.replace(",", "").replace("$", ""))
                    if window_key not in data["strikes"]:
                        data["strikes"][window_key] = {}
                    data["strikes"][window_key][crypto] = price
                    print(f"    ✓ {crypto} = ${price:,.2f}")
                except ValueError:
                    print(f"    ✗ Invalid number: {val}")
        
        with open(strikes_file, "w") as f:
            json.dump(data, f, indent=2)
        print("── Strikes saved ──\n")
    except Exception as e:
        print(f"  Error: {e}")


def kalshi_taker_fee(contracts: int, price: float) -> float:
    """Kalshi taker fee: ceil(0.07 × C × P × (1 - P)), in dollars."""
    if contracts <= 0 or price <= 0 or price >= 1:
        return 0.0
    raw = KALSHI_TAKER_FEE_RATE * contracts * price * (1 - price)
    return math.ceil(raw * 100) / 100  # Round up to nearest cent


def kelly_size(fair: float, market_price: float, balance: float) -> int:
    """10% Kelly Criterion sizing. Returns number of contracts.
    
    Kelly fraction for binary bet: (p - price) / (1 - price)
    where p = model fair value, price = market ask.
    We use 10% of full Kelly for conservative sizing.
    """
    if market_price <= 0.01 or market_price >= 0.99 or fair <= market_price:
        return 0
    
    available = balance * (1 - BALANCE_RESERVE_PCT / 100)
    if available < market_price:
        return 0
    
    # Full Kelly fraction
    kelly_pct = (fair - market_price) / (1 - market_price)
    kelly_pct = max(0, min(kelly_pct, 1.0))  # Clamp 0-100%
    
    # Apply fractional Kelly
    bet_fraction = KELLY_FRACTION * kelly_pct
    dollars = bet_fraction * available
    
    contracts = int(dollars / market_price)
    contracts = max(contracts, 1)  # Kalshi minimum = 1 contract
    contracts = min(contracts, MAX_CONTRACTS_PER_TRADE)
    
    # Don't exceed available balance (contracts + fee)
    total_cost = contracts * market_price + kalshi_taker_fee(contracts, market_price)
    while total_cost > available and contracts > 1:
        contracts -= 1
        total_cost = contracts * market_price + kalshi_taker_fee(contracts, market_price)
    
    return contracts
    
    return contracts


# =============================================================================
# BLENDED VOL — Safety layer: dampen model when it overestimates vs market IV
# =============================================================================
# With Chainlink vol, the Binance-vs-Chainlink mismatch is eliminated.
# Blending is kept as a safety layer in case Chainlink vol still occasionally
# runs hotter than market IV (e.g. during microstructure noise events).
# Data from v1 (Binance era): model_vol > IV → 36% WR, model_vol < IV → 56% WR

BLEND_BASE_WEIGHT = 0.5    # How much of the excess vol to keep at ratio=1.0
BLEND_SENSITIVITY = 1.5    # How fast dampening increases with larger gaps

def compute_blended_vol(model_vol: float, market_price: float, spot: float,
                        strike: float, seconds_left: float, direction: str,
                        pricer) -> tuple:
    """
    Blend realized vol with market implied vol when model runs hot.
    
    Returns (blended_sigma, iv, blend_info_str)
    - If model <= IV: returns model vol unchanged (conservative = good)
    - If model > IV: dampens the excess proportionally
    - If IV can't be computed: returns model vol unchanged
    """
    iv = pricer.implied_vol(market_price, spot, strike, seconds_left, direction)
    
    if not iv or iv <= 0.01:
        return model_vol, None, "no_iv_fallback"
    
    ratio = model_vol / iv
    
    if ratio <= 1.0:
        # Model is conservative — trust it (this is where we win)
        return model_vol, iv, f"conservative({ratio:.2f}x)"
    
    # Model is hotter than market — dampen the excess
    excess = model_vol - iv
    excess_ratio = ratio - 1.0
    
    # Dampening: starts at base_weight, decays as gap widens
    dampen = BLEND_BASE_WEIGHT / (1.0 + excess_ratio * BLEND_SENSITIVITY)
    blended = iv + dampen * excess
    
    return blended, iv, f"blended({ratio:.2f}x,d={dampen:.2f})"


# =============================================================================
# CONFIG LOADING
# =============================================================================

def load_config():
    """Load Kalshi credentials from config.json + kalshi_private_key.pem"""
    script_dir = Path(__file__).parent
    config_path = script_dir / "config.json"
    if not config_path.exists():
        print("\n[ERROR] config.json not found!")
        input("\nPress Enter to exit...")
        sys.exit(1)
    
    with open(config_path) as f:
        cfg = json.load(f)
    
    api_key_id = cfg.get("kalshi_api_key_id", "")
    
    if not api_key_id or "YOUR_" in api_key_id:
        print("\n[ERROR] config.json missing kalshi_api_key_id")
        print("  Get it from: https://kalshi.com/account/api")
        input("\nPress Enter to exit...")
        sys.exit(1)
    
    # Find private key: config path → default filename
    pk_path = cfg.get("kalshi_private_key_path", "")
    if pk_path:
        pk_full = script_dir / pk_path
    else:
        pk_full = script_dir / "kalshi_private_key.pem"
    
    if not pk_full.exists():
        print(f"\n[ERROR] Kalshi private key not found: {pk_full}")
        print("  Place kalshi_private_key.pem in the same folder as this script")
        input("\nPress Enter to exit...")
        sys.exit(1)
    
    with open(pk_full, "r") as f:
        private_key_pem = f.read()
    
    if "PASTE" in private_key_pem or len(private_key_pem.strip()) < 100:
        print("\n[ERROR] kalshi_private_key.pem appears empty or placeholder")
        print("  Get your RSA key from: https://kalshi.com/account/api")
        input("\nPress Enter to exit...")
        sys.exit(1)
    
    return api_key_id, private_key_pem


# =============================================================================
# KALSHI CONNECTOR — Auth, orders, balance, market data
# =============================================================================

class KalshiConnector:
    """Handles Kalshi API: RSA auth, order placement, fills, cancellation, balance"""
    
    def __init__(self, api_key_id: str, private_key_pem: str):
        self.api_key_id = api_key_id
        self._private_key = serialization.load_pem_private_key(
            private_key_pem.encode(), password=None, backend=default_backend()
        )
        self._client: Optional[httpx.AsyncClient] = None
        self._last_order_time = 0
        self._order_count = 0
    
    def _sign(self, ts: str, method: str, path: str) -> str:
        msg = f"{ts}{method}{path}".encode('utf-8')
        sig = self._private_key.sign(
            msg,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
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
    
    async def connect(self):
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=50, keepalive_expiry=30)
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(15.0, connect=10.0),
            limits=limits,
            http2=True,
        )
        # Verify connection
        for attempt in range(3):
            try:
                bal = await self.get_balance()
                print(f"  [OK] Kalshi connected | Balance: ${bal:.2f}")
                self._client.timeout = httpx.Timeout(8.0, connect=5.0)
                return bal
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(2)
                else:
                    raise e
    
    async def _req(self, method: str, path: str, params: dict = None, json_data: dict = None) -> dict:
        url = f"{KALSHI_API_URL}{path}"
        # Signature must include /trade-api/v2 prefix
        full_path = f"/trade-api/v2{path}"
        headers = self._headers(method, full_path)
        
        if method == "GET":
            r = await self._client.get(url, headers=headers, params=params)
        elif method == "POST":
            r = await self._client.post(url, headers=headers, json=json_data)
        elif method == "DELETE":
            r = await self._client.delete(url, headers=headers)
        else:
            raise ValueError(f"Unknown method: {method}")
        
        if r.status_code >= 400:
            raise Exception(f"Kalshi API {method} {path}: HTTP {r.status_code} — {r.text[:200]}")
        
        return r.json() if r.text else {}
    
    async def get_balance(self) -> float:
        r = await self._req("GET", "/portfolio/balance")
        return r.get("balance", 0) / 100  # Cents to dollars
    
    async def get_markets(self) -> list:
        """Fetch current 15-min crypto markets from Kalshi events API"""
        markets = []
        now = datetime.now(timezone.utc)
        
        series_list = [f"KX{c}15M" for c in CRYPTOS]
        max_end_time = now + timedelta(minutes=30)
        min_close_ts = int(now.timestamp())
        max_close_ts = int(max_end_time.timestamp())
        
        async def fetch_series(series):
            try:
                r = await self._req("GET", "/events", params={
                    "series_ticker": series,
                    "with_nested_markets": "true",
                    "min_close_ts": min_close_ts,
                    "max_close_ts": max_close_ts,
                    "status": "open",
                    "limit": 50
                })
                events = r.get("events", [])
                result = []
                for evt in events:
                    event_sub_title = evt.get("sub_title", "")
                    event_product_meta = evt.get("product_metadata", {}) or {}
                    for mkt in evt.get("markets", []):
                        mkt["_event_sub_title"] = event_sub_title
                        mkt["_event_product_metadata"] = event_product_meta
                        result.append(mkt)
                if result:
                    return result
                
                # Fallback to markets endpoint
                r = await self._req("GET", "/markets", params={
                    "series_ticker": series,
                    "status": "open",
                    "min_close_ts": min_close_ts,
                    "max_close_ts": max_close_ts,
                    "limit": 50
                })
                return r.get("markets", [])
            except Exception as e:
                return []
        
        results = await asyncio.gather(*[fetch_series(s) for s in series_list])
        
        for series, market_data in zip(series_list, results):
            underlying = "BTC" if "BTC" in series else "ETH" if "ETH" in series else "SOL"
            for d in market_data:
                try:
                    close = d.get("close_time") or d.get("expiration_time")
                    if not close:
                        continue
                    end = datetime.fromisoformat(close.replace("Z", "+00:00"))
                    start = end - timedelta(minutes=15)
                    ticker = d.get("ticker", "")
                    
                    # Extract strike price
                    strike_price = None
                    strike_source = "unknown"
                    
                    # 1. Check subtitle for "Price to beat: $XX,XXX.XX"
                    for fld in ["subtitle", "yes_sub_title", "no_sub_title", "_event_sub_title"]:
                        text = d.get(fld, "")
                        if text and "$" in text:
                            match = re.search(r'\$?([\d,]+\.?\d*)', text.replace(',', ''))
                            if match:
                                try:
                                    strike_price = float(match.group(1))
                                    strike_source = "subtitle"
                                    break
                                except:
                                    pass
                    
                    # 2. Event product_metadata
                    if not strike_price:
                        meta = d.get("_event_product_metadata", {}) or {}
                        for key in ["strike_price", "strike", "reference_price", "price_to_beat"]:
                            val = meta.get(key)
                            if val:
                                try:
                                    strike_price = float(str(val).replace(',', '').replace('$', ''))
                                    strike_source = "product_metadata"
                                    break
                                except:
                                    pass
                    
                    # 3. Numeric fields on market
                    if not strike_price:
                        for fld in ["floor_strike", "cap_strike", "strike_price", "strike",
                                    "custom_strike", "settlement_value"]:
                            val = d.get(fld)
                            if val and isinstance(val, (int, float)) and val > 0:
                                strike_price = float(val)
                                strike_source = "market_field"
                                break
                    
                    # 4. Try strikes.json (from strike_collector, shared with arb bot)
                    if not strike_price:
                        strikes_data = _load_strikes_file()
                        window_key = end.strftime("%Y-%m-%dT%H:%M:%SZ")
                        if window_key in strikes_data and underlying in strikes_data[window_key]:
                            strike_price = strikes_data[window_key][underlying]
                            strike_source = "strikes.json"
                    
                    if not strike_price:
                        continue
                    
                    markets.append({
                        "ticker": ticker,
                        "underlying": underlying,
                        "strike_price": strike_price,
                        "strike_source": strike_source,
                        "end_time": end,
                        "start_time": start,
                    })
                except Exception:
                    continue
        
        return markets
    
    async def get_orderbook(self, ticker: str) -> dict:
        """Get orderbook for a market. Returns {yes_ask, yes_ask_size, no_ask, no_ask_size}"""
        try:
            r = await self._req("GET", f"/markets/{ticker}/orderbook")
            d = r.get("orderbook", {})
            
            yes_bids_data = d.get("yes") or []  # [[price_cents, qty], ...]
            no_bids_data = d.get("no") or []
            
            # Best YES ask = cheapest way to buy YES = lowest (100 - no_bid_price)
            # Because: NO bid at X cents → someone willing to buy NO at X
            #          which means they'll sell YES at (100-X) cents
            best_yes_ask = None
            best_yes_size = 0
            for level in no_bids_data:
                if len(level) >= 2:
                    price_cents, qty = level[0], level[1]
                    yes_ask = (100 - price_cents) / 100.0
                    if best_yes_ask is None or yes_ask < best_yes_ask:
                        best_yes_ask = yes_ask
                        best_yes_size = qty
            
            # Best NO ask = cheapest way to buy NO = lowest (100 - yes_bid_price)
            best_no_ask = None
            best_no_size = 0
            for level in yes_bids_data:
                if len(level) >= 2:
                    price_cents, qty = level[0], level[1]
                    no_ask = (100 - price_cents) / 100.0
                    if best_no_ask is None or no_ask < best_no_ask:
                        best_no_ask = no_ask
                        best_no_size = qty
            
            return {
                "yes_ask": best_yes_ask or 0,
                "yes_ask_size": best_yes_size,
                "no_ask": best_no_ask or 0,
                "no_ask_size": best_no_size,
            }
        except Exception as e:
            return {"yes_ask": 0, "yes_ask_size": 0, "no_ask": 0, "no_ask_size": 0}
    
    async def place_limit_buy(self, ticker: str, side: str, price: float, size: int) -> Optional[str]:
        """
        Place a limit buy order. side = "yes" or "no".
        Returns order_id or None on failure.
        """
        # Rate limit: minimum 1s between orders
        elapsed = time.time() - self._last_order_time
        if elapsed < 1.0:
            await asyncio.sleep(1.0 - elapsed)
        
        price_cents = max(1, min(99, round(price * 100)))
        
        try:
            data = {
                "ticker": ticker,
                "action": "buy",
                "side": side,
                "type": "limit",
                "count": int(size),
                f"{side}_price": price_cents,
            }
            
            r = await self._req("POST", "/portfolio/orders", json_data=data)
            self._last_order_time = time.time()
            self._order_count += 1
            
            order_data = r.get("order", {})
            order_id = order_data.get("order_id")
            
            if not order_id:
                print(f"    [K] Order rejected: {r}")
                return None
            
            return order_id
            
        except Exception as e:
            print(f"    [K] Order error: {e}")
            return None
    
    async def check_order(self, order_id: str) -> dict:
        """Check order fill status"""
        try:
            r = await self._req("GET", f"/portfolio/orders/{order_id}")
            order = r.get("order", {})
            
            fill_count = order.get("fill_count", 0) or 0
            remaining = order.get("remaining_count", 0) or 0
            status = order.get("status", "unknown")
            
            # Get actual fill price from fills endpoint
            avg_price = None
            if fill_count > 0:
                try:
                    fills = await self._req("GET", "/portfolio/fills", params={"order_id": order_id})
                    fill_list = fills.get("fills", [])
                    if fill_list:
                        total_cost = 0
                        total_qty = 0
                        for f in fill_list:
                            # Kalshi fills have yes_price and no_price in cents
                            side = order.get("side", "yes")
                            price_cents = f.get(f"{side}_price", 0) or 0
                            qty = f.get("count", 0) or 0
                            total_cost += price_cents * qty
                            total_qty += qty
                        if total_qty > 0:
                            avg_price = total_cost / total_qty / 100  # Convert cents to dollars
                except Exception:
                    pass
            
            return {
                "status": status,
                "filled": fill_count,
                "remaining": remaining,
                "avg_price": avg_price,
            }
        except Exception as e:
            return {"status": "error", "filled": 0, "remaining": 0, "error": str(e)}
    
    async def cancel_order(self, order_id: str) -> bool:
        try:
            await self._req("DELETE", f"/portfolio/orders/{order_id}")
            return True
        except Exception as e:
            if "404" in str(e) or "not_found" in str(e):
                return True
            print(f"    [K] Cancel error: {e}")
            return False
    
    async def cancel_all_open_orders(self) -> int:
        """Cancel all open orders on startup"""
        cancelled = 0
        try:
            r = await self._req("GET", "/portfolio/orders", params={"status": "resting"})
            orders = r.get("orders", [])
            for o in orders:
                oid = o.get("order_id")
                # Only cancel orders from our series (crypto 15M)
                ticker = o.get("ticker", "")
                if oid and any(f"KX{c}15M" in ticker for c in CRYPTOS):
                    await self.cancel_order(oid)
                    cancelled += 1
            if cancelled:
                print(f"[K] Cancelled {cancelled} stale open orders from previous session")
            else:
                print(f"[K] No stale open orders to cancel")
        except Exception as e:
            print(f"[K] Could not check for stale orders: {e}")
        return cancelled
    
    async def get_market_settlement(self, ticker: str) -> dict:
        """Check if market has settled. Returns {settled, result, settle_price}"""
        try:
            r = await self._req("GET", f"/markets/{ticker}")
            market = r.get("market", r)
            status = market.get("status", "")
            result = market.get("result", "")
            
            if status == "settled" or result in ["yes", "no"]:
                return {
                    "settled": True,
                    "result": result,
                    "settle_price": market.get("settlement_value", market.get("settle_price")),
                }
            return {"settled": False, "result": None, "settle_price": None}
        except Exception as e:
            return {"settled": False, "result": None, "settle_price": None, "error": str(e)}


def _load_strikes_file():
    """Load strikes.json (shared with arb bot's strike_collector)"""
    if hasattr(_load_strikes_file, '_cache_time') and time.time() - _load_strikes_file._cache_time < 5:
        return _load_strikes_file._cache_data
    try:
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


# =============================================================================
# TRADE DATA STRUCTURES
# =============================================================================

@dataclass
class LiveTrade:
    trade_id: str
    timestamp: str
    asset: str
    direction: str          # "up" or "down"
    strike: float
    spot_entry: float
    seconds_left: float
    model_vol: float
    model_fair: float
    market_price: float     # Displayed ask when we saw the opportunity
    limit_price: float      # Price we submitted
    fill_price: float       # Actual fill price
    edge_pct: float
    implied_vol: float
    vol_regime: str
    size: int
    cost: float
    fee: float              # Kalshi taker fee
    window_end: str         # ISO format
    order_id: str
    ticker: str = ""        # Kalshi market ticker for settlement
    raw_model_vol: float = 0    # Original unblended realized vol
    blended_vol: float = 0      # Vol after blending with IV
    blend_info: str = ""        # Diagnostic: "blended(1.4x,d=0.3)" or "conservative(0.8x)"
    # Post-fill
    filled: bool = False
    fill_size: int = 0
    # Settlement
    settled: bool = False
    spot_settle: float = 0
    won: bool = False
    payout: float = 0
    pnl: float = 0


# =============================================================================
# LIVE TRADER — Position management, journal, settlement
# =============================================================================

class LiveTrader:
    def __init__(self):
        self.pending_orders: list[LiveTrade] = []
        self.pending_settle: list[LiveTrade] = []
        self.total_trades = 0
        self.wins = 0
        self.losses = 0
        self.total_pnl = 0.0
        self.total_cost = 0.0
        self.total_payout = 0.0
        self._traded_markets: set = set()
        self._trade_counter = 0
        self._session_start = time.time()
        self._hourly_trades: list = []
        self._edge_sightings: dict = {}
        self._traded_windows: dict = {}
        
        self._load_journal()
    
    def _load_journal(self):
        if not os.path.exists(LIVE_JOURNAL_FILE):
            return
        
        try:
            settled_ids = set()
            cancelled_ids = set()
            filled_trades = {}
            max_num = 0
            
            with open(LIVE_JOURNAL_FILE, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    entry = json.loads(line)
                    event = entry.get("event")
                    trade_id = entry.get("trade_id", "")
                    
                    if trade_id.startswith("VK-"):
                        try:
                            num = int(trade_id.split("-")[1])
                            max_num = max(max_num, num)
                        except:
                            pass
                    
                    if event == "OPEN":
                        market_key = f"{entry.get('asset')}_{entry.get('window_end')}"
                        self._traded_markets.add(market_key)
                        self._traded_windows[entry.get('window_end', '')] = \
                            self._traded_windows.get(entry.get('window_end', ''), 0) + 1
                    
                    elif event == "FILLED":
                        filled_trades[trade_id] = entry
                    
                    elif event in ("CANCELLED", "CANCELLED_EDGE_GONE"):
                        cancelled_ids.add(trade_id)
                        market_key = f"{entry.get('asset')}_{entry.get('window_end')}"
                        self._traded_markets.discard(market_key)
                        if entry.get('window_end', '') in self._traded_windows:
                            self._traded_windows[entry.get('window_end', '')] = max(0,
                                self._traded_windows[entry.get('window_end', '')] - 1)
                    
                    elif event == "SETTLE":
                        if trade_id not in settled_ids:
                            settled_ids.add(trade_id)
                            self.total_trades += 1
                            self.total_cost += entry.get("cost", 0)
                            self.total_payout += entry.get("payout", 0)
                            self.total_pnl += entry.get("pnl", 0)
                            if entry.get("won"):
                                self.wins += 1
                            else:
                                self.losses += 1
            
            self._trade_counter = max_num
            
            # Rebuild pending_settle from filled but unsettled trades
            now = datetime.now(timezone.utc)
            orphaned = 0
            rebuilt = 0
            for trade_id, entry in filled_trades.items():
                if trade_id in settled_ids or trade_id in cancelled_ids:
                    continue
                
                window_end_str = entry.get("window_end", "")
                try:
                    window_end = datetime.fromisoformat(window_end_str.replace("Z", "+00:00"))
                except:
                    orphaned += 1
                    continue
                
                age_min = (now - window_end).total_seconds() / 60
                if age_min > 30:
                    orphaned += 1
                    continue
                
                trade = LiveTrade(
                    trade_id=trade_id,
                    timestamp=entry.get("timestamp", ""),
                    asset=entry.get("asset", ""),
                    direction=entry.get("dir", ""),
                    strike=entry.get("strike", 0),
                    spot_entry=entry.get("spot_entry", 0),
                    seconds_left=entry.get("seconds_left", 0),
                    model_vol=entry.get("model_vol", 0),
                    model_fair=entry.get("model_fair", 0),
                    market_price=entry.get("market_price", 0),
                    limit_price=entry.get("limit_price", 0),
                    fill_price=entry.get("fill_price", 0),
                    edge_pct=entry.get("edge_pct", 0),
                    implied_vol=entry.get("implied_vol", 0),
                    vol_regime=entry.get("vol_regime", ""),
                    size=entry.get("size", 1),
                    cost=entry.get("cost", 0),
                    fee=entry.get("fee", 0),
                    window_end=window_end_str,
                    order_id=entry.get("order_id", ""),
                    ticker=entry.get("ticker", ""),
                    filled=True,
                    fill_size=entry.get("fill_size", entry.get("size", 1)),
                )
                self.pending_settle.append(trade)
                rebuilt += 1
            
            if self.total_trades > 0:
                wr = (self.wins / self.total_trades * 100) if self.total_trades > 0 else 0
                print(f"[Loaded] {self.total_trades} settled ({self.wins}W/{self.losses}L = {wr:.0f}%) P&L: ${self.total_pnl:+.2f}")
            if rebuilt > 0:
                print(f"[Loaded] {rebuilt} filled trades awaiting settlement")
            if orphaned > 0:
                print(f"[Loaded] {orphaned} old trades too stale to settle (orphaned)")
        except Exception as e:
            print(f"[Warn] Journal load error: {e}")
    
    def _next_trade_id(self) -> str:
        self._trade_counter += 1
        return f"VK-{self._trade_counter:04d}"
    
    def can_trade(self) -> tuple:
        total_open = len(self.pending_orders) + len(self.pending_settle)
        if total_open >= MAX_PENDING_TRADES:
            return False, f"max_pending ({total_open}/{MAX_PENDING_TRADES})"
        
        session_pnl = self.total_pnl
        if session_pnl < -MAX_LOSS_PER_SESSION:
            return False, f"session_loss (${session_pnl:.2f} < -${MAX_LOSS_PER_SESSION})"
        
        now = time.time()
        self._hourly_trades = [t for t in self._hourly_trades if now - t < 3600]
        if len(self._hourly_trades) >= MAX_TRADES_PER_HOUR:
            return False, f"hourly_rate ({len(self._hourly_trades)}/{MAX_TRADES_PER_HOUR})"
        
        return True, ""
    
    def is_already_traded(self, asset: str, window_end: datetime) -> bool:
        end_str = window_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        if self._traded_windows.get(end_str, 0) >= MAX_TRADES_PER_WINDOW:
            return True
        market_key = f"{asset}_{end_str}"
        return market_key in self._traded_markets
    
    def record_edge_sighting(self, asset: str, window_end: datetime, scan_num: int,
                              edge_pct: float, candidate: dict) -> bool:
        end_str = window_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        key = f"{asset}_{end_str}"
        
        existing = self._edge_sightings.get(key)
        if existing and existing["last_scan"] == scan_num - 1:
            existing["count"] += 1
            existing["last_scan"] = scan_num
            if edge_pct > existing["best_edge"]:
                existing["best_edge"] = edge_pct
                existing["best_cand"] = candidate
        else:
            self._edge_sightings[key] = {
                "count": 1,
                "last_scan": scan_num,
                "best_edge": edge_pct,
                "best_cand": candidate,
            }
        
        sighting = self._edge_sightings[key]
        confirmed = sighting["count"] >= CONFIRM_SCANS
        
        if not confirmed and sighting["count"] == 1:
            print(f"    👀 {asset} {candidate.get('direction','').upper()} edge={edge_pct:+.1f}% — watching (1/{CONFIRM_SCANS})")
        elif not confirmed:
            print(f"    👀 {asset} {candidate.get('direction','').upper()} edge={edge_pct:+.1f}% — confirming ({sighting['count']}/{CONFIRM_SCANS})")
        
        return confirmed
    
    def get_confirmed_candidate(self, asset: str, window_end: datetime) -> Optional[dict]:
        end_str = window_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        key = f"{asset}_{end_str}"
        sighting = self._edge_sightings.get(key)
        if sighting and sighting["count"] >= CONFIRM_SCANS:
            return sighting["best_cand"]
        return None
    
    def cleanup_stale_sightings(self, scan_num: int):
        stale = [k for k, v in self._edge_sightings.items()
                 if scan_num - v["last_scan"] > 2]
        for k in stale:
            del self._edge_sightings[k]
    
    def open_trade(self, order_id: str, **kwargs) -> LiveTrade:
        trade = LiveTrade(
            trade_id=self._next_trade_id(),
            timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            order_id=order_id,
            **kwargs,
        )
        
        self.pending_orders.append(trade)
        market_key = f"{trade.asset}_{trade.window_end}"
        self._traded_markets.add(market_key)
        self._traded_windows[trade.window_end] = \
            self._traded_windows.get(trade.window_end, 0) + 1
        self._hourly_trades.append(time.time())
        
        self._log_event(trade, "OPEN")
        return trade
    
    async def check_order_fills(self, kalshi: KalshiConnector, model: VolModel = None, pricer: BinaryPricer = None):
        """Poll pending orders for fills, cancel if edge gone or timed out"""
        still_pending = []
        
        for trade in self.pending_orders:
            order_age = time.time() - datetime.fromisoformat(
                trade.timestamp.replace("Z", "+00:00")).timestamp()
            
            status = await kalshi.check_order(trade.order_id)
            filled = status.get("filled", 0)
            
            if filled >= trade.size:
                trade.filled = True
                trade.fill_size = int(filled)
                trade.fill_price = status.get("avg_price") or trade.limit_price
                trade.fee = round(kalshi_taker_fee(trade.fill_size, trade.fill_price), 4)
                trade.cost = round(trade.fill_price * trade.fill_size + trade.fee, 4)
                self.pending_settle.append(trade)
                self._log_event(trade, "FILLED")
                print(f"    ✅ {trade.trade_id} FILLED: {trade.asset} {trade.direction.upper()} "
                      f"x{trade.fill_size} @ ${trade.fill_price:.2f} (fee: ${trade.fee:.2f})")
            
            elif filled > 0:
                trade.filled = True
                trade.fill_size = int(filled)
                trade.fill_price = status.get("avg_price") or trade.limit_price
                trade.fee = round(kalshi_taker_fee(trade.fill_size, trade.fill_price), 4)
                trade.cost = round(trade.fill_price * trade.fill_size + trade.fee, 4)
                await kalshi.cancel_order(trade.order_id)
                self.pending_settle.append(trade)
                self._log_event(trade, "PARTIAL_FILL")
                print(f"    ⚠️ {trade.trade_id} PARTIAL: {trade.asset} {trade.direction.upper()} "
                      f"x{trade.fill_size}/{trade.size} @ ${trade.fill_price:.2f}")
            
            elif order_age > ORDER_TIMEOUT_SEC:
                await kalshi.cancel_order(trade.order_id)
                self._log_event(trade, "CANCELLED")
                print(f"    ❌ {trade.trade_id} CANCELLED: no fill in {ORDER_TIMEOUT_SEC}s")
                self._release_market_slot(trade)
            
            else:
                should_cancel = False
                
                if model and pricer:
                    spot = model.get_spot(trade.asset)
                    window_end = datetime.fromisoformat(trade.window_end.replace("Z", "+00:00"))
                    seconds_left = (window_end - datetime.now(timezone.utc)).total_seconds()
                    vol = get_adaptive_vol(model, trade.asset, seconds_left)
                    vol_floor = MIN_VOL_FLOOR.get(trade.asset, 0.30)
                    if not vol or vol < vol_floor:
                        vol = model.get_vol_with_saved_fallback(trade.asset)
                    
                    if spot and vol and seconds_left > 0:
                        # Use blended vol for revalidation (same as entry)
                        b_vol, _, _ = compute_blended_vol(vol, trade.limit_price, spot, trade.strike, seconds_left, trade.direction, pricer)
                        fair = pricer.fair_value(spot, trade.strike, seconds_left, b_vol, trade.direction)
                        new_edge = ((fair - trade.limit_price) / trade.limit_price) * 100
                        
                        if new_edge < 0:
                            should_cancel = True
                            print(f"    🔄 {trade.trade_id} EDGE GONE: {trade.asset} {trade.direction.upper()} "
                                  f"was {trade.edge_pct:+.1f}% now {new_edge:+.1f}% — cancelling")
                
                if should_cancel:
                    await kalshi.cancel_order(trade.order_id)
                    self._log_event(trade, "CANCELLED_EDGE_GONE")
                    self._release_market_slot(trade)
                else:
                    still_pending.append(trade)
        
        self.pending_orders = still_pending
    
    def _release_market_slot(self, trade: LiveTrade):
        market_key = f"{trade.asset}_{trade.window_end}"
        self._traded_markets.discard(market_key)
        if trade.window_end in self._traded_windows:
            self._traded_windows[trade.window_end] = max(0,
                self._traded_windows[trade.window_end] - 1)
        # Remove from shared position registry
        unregister_position(order_id=trade.order_id)
    
    async def check_settlements(self, model: VolModel, kalshi: KalshiConnector):
        """Check if filled trades have settled via Kalshi API result field"""
        now = datetime.now(timezone.utc)
        still_pending = []
        any_settled = False
        
        for trade in self.pending_settle:
            if trade.settled:
                continue
            
            window_end = datetime.fromisoformat(trade.window_end.replace("Z", "+00:00"))
            
            # Wait 60s after window end for Kalshi to settle
            if now < window_end + timedelta(seconds=60):
                still_pending.append(trade)
                continue
            
            # Get spot for logging
            spot = model.get_spot(trade.asset)
            if spot:
                trade.spot_settle = spot
            
            # Query Kalshi settlement (authoritative)
            won = None
            if trade.ticker:
                try:
                    settlement = await kalshi.get_market_settlement(trade.ticker)
                    if settlement.get("settled"):
                        result = settlement.get("result", "")
                        # result="yes" means price went UP, result="no" means DOWN
                        if result == "yes":
                            won = (trade.direction == "up")
                        elif result == "no":
                            won = (trade.direction == "down")
                        
                        if won is not None:
                            print(f"  [K] {trade.trade_id} resolved: result={result} → {'WON' if won else 'LOST'}")
                except Exception as e:
                    pass
            
            if won is None:
                age_min = (now - window_end).total_seconds() / 60
                if age_min > 30:
                    print(f"  ⚠️  {trade.trade_id} unresolved after {age_min:.0f}min — still waiting")
                still_pending.append(trade)
                continue
            
            trade.settled = True
            trade.won = won
            
            if trade.won:
                trade.payout = round(1.0 * trade.fill_size, 2)
            else:
                trade.payout = 0.0
            
            trade.pnl = round(trade.payout - trade.cost, 2)
            
            self.total_trades += 1
            self.total_cost += trade.cost
            self.total_payout += trade.payout
            self.total_pnl += trade.pnl
            if trade.won:
                self.wins += 1
            else:
                self.losses += 1
            
            self._log_event(trade, "SETTLE")
            
            # Remove from shared position registry
            unregister_position(order_id=trade.order_id)
            
            emoji = "✅" if trade.won else "❌"
            print(f"\n  {emoji} SETTLED: {trade.trade_id}")
            print(f"     {trade.asset} {trade.direction.upper()} | K={trade.strike:,.2f} → spot={spot:,.2f}")
            print(f"     {'WON' if trade.won else 'LOST'} | Cost: ${trade.cost:.2f} Payout: ${trade.payout:.2f} P&L: ${trade.pnl:+.2f}")
            
            any_settled = True
        
        self.pending_settle = still_pending
        return any_settled
    
    def _log_event(self, trade: LiveTrade, event: str):
        entry = {
            "event": event,
            "trade_id": trade.trade_id,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "asset": trade.asset,
            "dir": trade.direction,
            "strike": trade.strike,
            "spot_entry": trade.spot_entry,
            "seconds_left": trade.seconds_left,
            "model_vol": round(trade.model_vol, 4),
            "model_fair": round(trade.model_fair, 4),
            "market_price": trade.market_price,
            "limit_price": trade.limit_price,
            "fill_price": trade.fill_price,
            "edge_pct": round(trade.edge_pct, 2),
            "implied_vol": round(trade.implied_vol, 4),
            "vol_regime": trade.vol_regime,
            "size": trade.size,
            "fill_size": trade.fill_size,
            "cost": trade.cost,
            "fee": trade.fee,
            "window_end": trade.window_end,
            "order_id": trade.order_id,
            "ticker": trade.ticker,
            "raw_model_vol": round(getattr(trade, 'raw_model_vol', trade.model_vol), 4),
            "blended_vol": round(getattr(trade, 'blended_vol', trade.model_vol), 4),
            "blend_info": getattr(trade, 'blend_info', ''),
            "vol_source": "chainlink",
        }
        
        if event == "SETTLE":
            margin_pct = abs(trade.spot_settle - trade.strike) / trade.strike * 100 if trade.strike else 0
            entry.update({
                "spot_settle": trade.spot_settle,
                "won": trade.won,
                "payout": trade.payout,
                "pnl": trade.pnl,
                "margin_pct": round(margin_pct, 4),
                "tight_margin": margin_pct < 0.15,
            })
        
        try:
            with open(LIVE_JOURNAL_FILE, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            print(f"  [Warn] Journal write error: {e}")
    
    def save_summary(self):
        wr = (self.wins / self.total_trades * 100) if self.total_trades > 0 else 0
        summary = {
            "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "venue": "kalshi",
            "total_trades": self.total_trades,
            "wins": self.wins,
            "losses": self.losses,
            "win_rate": round(wr, 1),
            "total_cost": round(self.total_cost, 2),
            "total_payout": round(self.total_payout, 2),
            "total_pnl": round(self.total_pnl, 2),
            "pending_orders": len(self.pending_orders),
            "pending_settle": len(self.pending_settle),
            "trade_size": f"10% Kelly, max {MAX_CONTRACTS_PER_TRADE} contracts, max {MAX_OPEN_TRADES} open",
            "min_edge_pct": MIN_EDGE_PCT,
            "vol_source": "chainlink",
        }
        try:
            with open(LIVE_SUMMARY_FILE, "w") as f:
                json.dump(summary, f, indent=2)
        except:
            pass
    
    def format_dashboard(self) -> str:
        wr = (self.wins / self.total_trades * 100) if self.total_trades > 0 else 0
        pending_str = f"{len(self.pending_orders)} ordering, {len(self.pending_settle)} awaiting settle"
        return (
            f"  Trades: {self.total_trades} settled ({self.wins}W/{self.losses}L = {wr:.0f}%) "
            f"P&L: ${self.total_pnl:+.2f}\n"
            f"  Pending: {pending_str}"
        )


# =============================================================================
# MAIN LOOP
# =============================================================================

async def run_live_trader():
    print("=" * 60)
    print("  Vol Model LIVE Trader — KALSHI")
    print("  *** REAL MONEY ***")
    print("=" * 60)
    print(f"  Sizing: 10% Kelly (1-{MAX_CONTRACTS_PER_TRADE} contracts) | Max {MAX_OPEN_TRADES} open")
    print(f"  Edge: flat {MIN_EDGE_PCT}% minimum (all directions, all prices)")
    print(f"  Vol ceiling: {MAX_VOL_CEILING*100:.0f}% | Regime multipliers: contract={VOL_EDGE_MULTIPLIERS['contracting']}x normal={VOL_EDGE_MULTIPLIERS['normal']}x expand={VOL_EDGE_MULTIPLIERS['expanding']}x")
    print(f"  Filters: edge threshold only (no spot/strike filter) | Fees: taker {KALSHI_TAKER_FEE_RATE*100:.1f}%")
    print(f"  Reserve: {BALANCE_RESERVE_PCT}% | Window: {MIN_SECONDS_LEFT}s - {MAX_SECONDS_LEFT}s remaining")
    print(f"  Safety: max {MAX_PENDING_TRADES} open, ${MAX_LOSS_PER_SESSION} loss limit")
    print(f"  Scanning {', '.join(CRYPTOS)} on Kalshi")
    print(f"  Vol source: Chainlink (RTDS) — matches PM settlement oracle")
    print(f"  Logs: {LIVE_JOURNAL_FILE}")
    print("=" * 60)
    
    # Load config and connect
    api_key_id, private_key_pem = load_config()
    kalshi = KalshiConnector(api_key_id, private_key_pem)
    balance = await kalshi.connect()
    
    # Cancel stale orders
    await kalshi.cancel_all_open_orders()
    
    # Display balance
    if balance >= 0:
        available = balance * (1 - BALANCE_RESERVE_PCT / 100)
        print(f"[K] Balance: ${balance:.2f} | Available (after {BALANCE_RESERVE_PCT}% reserve): ${available:.2f}")
        print(f"[K] Sizing: 10% Kelly (1-{MAX_CONTRACTS_PER_TRADE}) | Max {MAX_OPEN_TRADES} open | Taker fee: {KALSHI_TAKER_FEE_RATE*100:.1f}%")
    
    # Start vol model
    model = VolModel()
    has_saved_state = model.load_state()
    await model.start()
    if has_saved_state:
        print("[Vol] Loaded saved state — trading immediately (Chainlink vol)")
    else:
        print("[Vol] No saved state — warming up on Chainlink ticks...")
    
    # Wait for Chainlink feed (required for spot pricing)
    print("[Chainlink] Waiting for RTDS connection...")
    for i in range(30):
        if model.chainlink.is_connected():
            prices = model.chainlink.get_all_prices()
            if len(prices) >= 2:
                print(f"[Chainlink] Connected — {', '.join(f'{a}=${p:,.2f}' for a, p in prices.items())}")
                break
        await asyncio.sleep(1)
    else:
        print("[Chainlink] ⚠️ RTDS not connected after 30s — will retry during operation")
    
    # Init trader and pricer
    trader = LiveTrader()
    pricer = BinaryPricer()
    
    # Start input listener thread
    input_thread = threading.Thread(target=_input_listener, daemon=True)
    input_thread.start()
    
    scan_count = 0
    last_balance_check = 0
    
    try:
        while True:
            try:
                now = datetime.now(timezone.utc)
                scan_count += 1
                
                _check_for_strike_input()
                
                # Refresh balance every 5 min
                if time.time() - last_balance_check > 300:
                    new_bal = await kalshi.get_balance()
                    if new_bal >= 0:
                        balance = new_bal
                    last_balance_check = time.time()
                
                # Dashboard every 30s
                if scan_count % 2 == 1:
                    print(f"\n{'=' * 60}")
                    avail = balance * (1 - BALANCE_RESERVE_PCT / 100)
                    print(f"  {now.strftime('%H:%M:%S')} UTC | Scan #{scan_count} | KALSHI LIVE | 10%K | ${avail:.2f} avail")
                    print(f"  {model.format_status()}")
                    print(f"{'=' * 60}")
                    print(trader.format_dashboard())
                
                # Check order fills
                if trader.pending_orders:
                    await trader.check_order_fills(kalshi, model, pricer)
                
                # Check settlements
                if trader.pending_settle:
                    settled = await trader.check_settlements(model, kalshi)
                    if settled:
                        trader.save_summary()
                        model.save_state()
                
                # Safety check
                can_trade, reason = trader.can_trade()
                if not can_trade:
                    if scan_count % 4 == 0:
                        print(f"  [Safety] Trading paused: {reason}")
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                
                # Need vol data
                has_usable_vol = any(model.get_vol_with_saved_fallback(a)
                                     for a in CRYPTOS)
                if not has_usable_vol:
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                
                # Need Chainlink prices
                if not model.chainlink.is_connected():
                    if scan_count % 4 == 0:
                        print(f"  [Chainlink] 🔴 RTDS disconnected — skipping trades")
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                
                # Warmup timer
                any_ticks = any(model.vol_tracker.tick_count.get(a, 0) > 0 for a in CRYPTOS)
                if any_ticks and not hasattr(model, '_first_tick_time'):
                    model._first_tick_time = time.time()
                
                if not has_saved_state and hasattr(model, '_first_tick_time'):
                    warmup = (time.time() - model._first_tick_time) / 60
                    if warmup < WARMUP_MINUTES:
                        if scan_count % 4 == 0:
                            print(f"  [Warmup] {warmup:.1f}/{WARMUP_MINUTES} min")
                        await asyncio.sleep(SCAN_INTERVAL)
                        continue
                
                # Fetch Kalshi markets
                markets = await kalshi.get_markets()
                
                if not markets:
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                
                # Strike source diagnostics
                strike_counts = {}
                for m in markets:
                    src = m.get("strike_source", "unknown")
                    strike_counts[src] = strike_counts.get(src, 0) + 1
                
                # Evaluate candidates
                candidates = {}
                evaluated = 0
                best_edge_seen = -999
                best_edge_label = ""
                
                for mkt in markets:
                    seconds_left = (mkt["end_time"] - now).total_seconds()
                    if seconds_left < MIN_SECONDS_LEFT or seconds_left > MAX_SECONDS_LEFT:
                        continue
                    
                    underlying = mkt["underlying"]
                    
                    if trader.is_already_traded(underlying, mkt["end_time"]):
                        continue
                    
                    strike = mkt["strike_price"]
                    if not strike:
                        continue
                    
                    # Only trade on verified strikes
                    if mkt.get("strike_source") not in ("strikes.json", "subtitle", "product_metadata", "market_field"):
                        continue
                    
                    # Get vol
                    vol = get_adaptive_vol(model, underlying, seconds_left)
                    vol_floor = MIN_VOL_FLOOR.get(underlying, 0.30)
                    if not vol or vol < vol_floor:
                        vol = model.get_vol_with_saved_fallback(underlying)
                    if not vol or vol < vol_floor:
                        continue
                    
                    if vol > MAX_VOL_CEILING:
                        continue
                    
                    spot = model.get_spot(underlying)
                    if not spot:
                        continue
                    
                    # Fetch orderbook
                    ob = await kalshi.get_orderbook(mkt["ticker"])
                    yes_ask = ob["yes_ask"]
                    yes_size = ob["yes_ask_size"]
                    no_ask = ob["no_ask"]
                    no_size = ob["no_ask_size"]
                    
                    if yes_ask <= 0.01 and no_ask <= 0.01:
                        continue
                    
                    # Evaluate both sides — using blended vol
                    evaluations = []
                    if yes_ask > 0.01:
                        b_vol, b_iv, b_info = compute_blended_vol(vol, yes_ask, spot, strike, seconds_left, "up", pricer)
                        fair = pricer.fair_value(spot, strike, seconds_left, b_vol, "up")
                        evaluations.append(("up", yes_ask, yes_size, fair, "yes", vol, b_vol, b_iv, b_info))
                    if no_ask > 0.01:
                        b_vol, b_iv, b_info = compute_blended_vol(vol, no_ask, spot, strike, seconds_left, "down", pricer)
                        fair = pricer.fair_value(spot, strike, seconds_left, b_vol, "down")
                        evaluations.append(("down", no_ask, no_size, fair, "no", vol, b_vol, b_iv, b_info))
                    
                    for direction, market_price, depth, fair, kalshi_side, raw_vol, blended_vol_val, blend_iv, blend_info in evaluations:
                        evaluated += 1
                        
                        effective_edge_pct = ((fair - market_price) / market_price) * 100 if market_price > 0 else 0
                        
                        if effective_edge_pct > best_edge_seen:
                            best_edge_seen = effective_edge_pct
                            best_edge_label = (
                                f"{underlying} {direction.upper()} fair={fair:.3f} "
                                f"mkt={market_price:.3f} K={strike:,.0f} "
                                f"spot={spot:,.2f} σ={vol*100:.0f}% {seconds_left:.0f}s"
                            )
                        
                        required_edge = get_min_edge(seconds_left, direction, market_price)
                        
                        regime_data = model.get_regime(underlying)
                        regime = regime_data["regime"] if regime_data else "normal"
                        regime_mult = VOL_EDGE_MULTIPLIERS.get(regime, 1.2)
                        required_edge *= regime_mult
                        
                        if effective_edge_pct < required_edge or effective_edge_pct > MAX_EDGE_PCT:
                            continue
                        
                        if depth < 1:  # Kalshi minimum = 1 contract
                            continue
                        
                        cand_key = (underlying, mkt["end_time"].isoformat())
                        existing = candidates.get(cand_key)
                        if not existing or effective_edge_pct > existing["edge_pct"]:
                            iv = blend_iv
                            if not iv:
                                iv = pricer.implied_vol(market_price, spot, strike, seconds_left, direction)
                            
                            candidates[cand_key] = {
                                "edge_pct": effective_edge_pct,
                                "asset": underlying,
                                "direction": direction,
                                "strike": strike,
                                "spot": spot,
                                "seconds_left": seconds_left,
                                "vol": blended_vol_val,
                                "raw_model_vol": raw_vol,
                                "blended_vol": blended_vol_val,
                                "blend_info": blend_info,
                                "fair": fair,
                                "market_price": market_price,
                                "iv": iv or 0,
                                "regime": regime,
                                "window_end": mkt["end_time"],
                                "ticker": mkt["ticker"],
                                "kalshi_side": kalshi_side,  # "yes" or "no"
                                "depth": depth,
                            }
                
                # Execute confirmed candidates
                trader.cleanup_stale_sightings(scan_count)
                
                for cand_key, c in sorted(candidates.items(), key=lambda x: -x[1]["edge_pct"]):
                    confirmed = trader.record_edge_sighting(
                        c["asset"], c["window_end"], scan_count,
                        c["edge_pct"], c
                    )
                    
                    if not confirmed:
                        continue
                    
                    best = trader.get_confirmed_candidate(c["asset"], c["window_end"])
                    if not best:
                        continue
                    c = best
                    
                    if trader.is_already_traded(c["asset"], c["window_end"]):
                        continue
                    
                    dedup_key = f"{c['asset']}_{c['window_end'].strftime('%Y-%m-%dT%H:%M:%SZ')}"
                    if dedup_key in trader._traded_markets:
                        print(f"    ⛔ BLOCKED duplicate: {c['asset']} already traded in this window")
                        continue
                    
                    window_str = c["window_end"].strftime("%Y-%m-%dT%H:%M:%SZ")
                    if trader._traded_windows.get(window_str, 0) >= MAX_TRADES_PER_WINDOW:
                        print(f"    ⛔ BLOCKED: window {window_str} already has {MAX_TRADES_PER_WINDOW} trades")
                        continue
                    
                    can_trade, reason = trader.can_trade()
                    if not can_trade:
                        break
                    
                    req_edge = get_min_edge(c['seconds_left'], c['direction'], c['market_price'])
                    r_mult = VOL_EDGE_MULTIPLIERS.get(c.get('regime', 'normal'), 1.2)
                    eff_req = req_edge * r_mult
                    
                    # 10% Kelly sizing
                    order_size = kelly_size(c['fair'], c['market_price'], balance)
                    if order_size == 0:
                        print(f"  ⏭️  SKIP {c['asset']} {c['direction'].upper()} — Kelly size=0 (edge too thin or low balance)")
                        continue
                    
                    if order_size > c.get('depth', order_size):
                        order_size = max(1, int(c['depth']))
                    
                    order_fee = kalshi_taker_fee(order_size, c["market_price"])
                    order_cost = c["market_price"] * order_size + order_fee
                    
                    # Compute Kelly % for display
                    full_kelly = (c['fair'] - c['market_price']) / (1 - c['market_price']) if c['market_price'] < 1 else 0
                    
                    print(f"\n  🎯 CONFIRMED — PLACING ORDER: {c['asset']} {c['direction'].upper()}")
                    print(f"     K={c['strike']:,.2f} spot={c['spot']:,.2f} | "
                          f"Fair={c['fair']:.3f} Ask={c['market_price']:.3f} Edge={c['edge_pct']:+.1f}% (need {eff_req:.0f}%)")
                    print(f"     Kelly={full_kelly*100:.1f}% → 10%K → {order_size} contracts @ ${c['market_price']:.2f} = ${order_cost:.2f}")
                    print(f"     σ={c['vol']*100:.0f}% IV={c['iv']*100:.0f}% | "
                          f"{c.get('blend_info', '')} | "
                          f"{c['seconds_left']:.0f}s left")
                    
                    # Place limit buy on Kalshi
                    order_id = await kalshi.place_limit_buy(
                        ticker=c["ticker"],
                        side=c["kalshi_side"],
                        price=c["market_price"],
                        size=order_size,
                    )
                    
                    if not order_id:
                        print(f"     ❌ Order failed!")
                        continue
                    
                    trade = trader.open_trade(
                        order_id=order_id,
                        asset=c["asset"],
                        direction=c["direction"],
                        strike=c["strike"],
                        spot_entry=c["spot"],
                        seconds_left=c["seconds_left"],
                        model_vol=c["vol"],
                        model_fair=c["fair"],
                        market_price=c["market_price"],
                        limit_price=c["market_price"],
                        fill_price=c["market_price"],
                        edge_pct=c["edge_pct"],
                        implied_vol=c["iv"],
                        vol_regime=c["regime"],
                        size=order_size,
                        cost=round(c["market_price"] * order_size + order_fee, 4),
                        fee=round(order_fee, 4),
                        window_end=c["window_end"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                        ticker=c["ticker"],
                        raw_model_vol=c.get("raw_model_vol", c["vol"]),
                        blended_vol=c.get("blended_vol", c["vol"]),
                        blend_info=c.get("blend_info", ""),
                    )
                    
                    print(f"     📋 {trade.trade_id} | Order: {order_id[:16]}...")
                    
                    # Register with shared position registry so arb bot doesn't interfere
                    register_position(
                        strategy="vol", ticker=c["ticker"], platform="kalshi",
                        asset=c["asset"], direction=c["kalshi_side"],
                        size=order_size, order_id=order_id,
                        window_end=c["window_end"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                    )
                    
                    sighting_key = f"{c['asset']}_{c['window_end'].strftime('%Y-%m-%dT%H:%M:%SZ')}"
                    trader._edge_sightings.pop(sighting_key, None)
                
                # Diagnostics
                if scan_count % 2 == 0 and evaluated > 0:
                    print(f"  [{now.strftime('%H:%M:%S')}] Eval:{evaluated} Candidates:{len(candidates)}")
                    if best_edge_seen > -999:
                        print(f"  Best edge: {best_edge_seen:+.1f}% → {best_edge_label}")
                
                # Periodic saves
                if scan_count % 4 == 0:
                    trader.save_summary()
                    model.save_state()
                
                await asyncio.sleep(SCAN_INTERVAL)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"\n  [Error] {type(e).__name__}: {e}")
                import traceback
                traceback.print_exc()
                await asyncio.sleep(SCAN_INTERVAL)
    except KeyboardInterrupt:
        pass
    
    # Shutdown
    print("\n" + "=" * 60)
    print("  SHUTTING DOWN")
    print("=" * 60)
    
    if trader.pending_orders:
        print(f"  Cancelling {len(trader.pending_orders)} unfilled orders...")
        for trade in trader.pending_orders:
            await kalshi.cancel_order(trade.order_id)
            print(f"    Cancelled {trade.trade_id}")
    
    print(f"\n  FINAL RESULTS:")
    print(trader.format_dashboard())
    trader.save_summary()
    model.save_state()
    
    await model.stop()


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    print("\nStarting Vol Model LIVE Trader (Kalshi)...")
    print("⚠️  This trades REAL money on Kalshi!")
    print("Press Ctrl+C to stop.\n")
    
    try:
        asyncio.run(run_live_trader())
    except KeyboardInterrupt:
        print("\n\nStopped by user.")
