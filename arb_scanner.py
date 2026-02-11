#!/usr/bin/env python3
"""
Crypto Arbitrage Scanner - Polymarket <-> Kalshi
15-minute BTC Up/Down markets

SETUP:
1. Edit config.json with your API keys
2. Save your Kalshi private key to kalshi_private_key.pem
3. Double-click run_bot.bat
"""

# =============================================================================
# PROXY SETUP - MUST BE BEFORE ANY OTHER IMPORTS
# =============================================================================
# This sets HTTP_PROXY/HTTPS_PROXY before requests/httpx are imported
# so that py_clob_client uses the proxy for all PM API calls
import os
import json as _json_early
# Load proxy config from config.json instead of hardcoding credentials
_script_dir = os.path.dirname(os.path.abspath(__file__))
_config_path = os.path.join(_script_dir, "config.json")
_PROXY_CONFIG = {"enabled": False}
if os.path.exists(_config_path):
    try:
        with open(_config_path) as _f:
            _cfg_data = _json_early.load(_f)
        _PROXY_CONFIG = _cfg_data.get("residential_proxy", {"enabled": False})
    except Exception:
        pass
if _PROXY_CONFIG.get("enabled", False):
    _proxy_url = f"http://{_PROXY_CONFIG['username']}:{_PROXY_CONFIG['password']}@{_PROXY_CONFIG['host']}:{_PROXY_CONFIG['port']}"
    os.environ["HTTP_PROXY"] = _proxy_url
    os.environ["HTTPS_PROXY"] = _proxy_url

# =============================================================================
# IMPORTS
# =============================================================================
import asyncio
import base64
import hashlib
import hmac
import json
import math
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor
import threading

# Bot coordinator — shared position registry for multi-bot operation
try:
    from bot_coordinator import filter_positions as _coord_filter_positions
    _HAS_COORDINATOR = True
except ImportError:
    _HAS_COORDINATOR = False

# Try to use uvloop for faster async (2-4x speedup)
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    UVLOOP_ENABLED = True
except ImportError:
    UVLOOP_ENABLED = False

# Suppress noisy websocket library logging
import logging
logging.getLogger('websocket').setLevel(logging.CRITICAL)
logging.getLogger('websockets').setLevel(logging.CRITICAL)

# =============================================================================
# CONFIGURATION - Edit these settings as needed
# =============================================================================

CONFIG = {
    # ==========================================================================
    # ARBITRAGE STRATEGY
    # ==========================================================================
    # Detect cross-venue price divergence and execute both legs
    
    "strategy": {
        "min_score_to_trade": 38,        # Minimum opportunity score to enter
        "min_edge_pct": 3.5,             # Minimum NET ROI % to trade (safety floor)
        "second_leg_delay_seconds": 0,   # No delay - execute both legs immediately
        "max_position_hold_seconds": 10, # Max time before forcing second leg
        "late_market_cutoff_pct": 85,    # Hard cutoff: don't enter trades after 85%
        
        # Late window trading - DISABLED
        "late_window_start_pct": 85,     # No late window penalty zone
        "late_window_roi_penalty": 0.0,  # No penalty
        "late_window_skip_reconciliation": True,
        
        # Scoring weights
        "divergence_weight": 3.0,        # Points per % divergence
        "early_market_bonus": 20,        # Bonus if < 2 min since open
        "mid_market_bonus": 10,          # Bonus if < 5 min since open  
        "extreme_price_bonus": 20,       # Bonus if price > 0.85 or < 0.15
        "moderate_extreme_bonus": 10,    # Bonus if price > 0.75 or < 0.25
    },
    
    "min_edge_pct": 3.5,           # Minimum ROI to consider (safety floor)
    "max_trade_fraction": 0.60,    # Max fraction of capital per trade
    
    # MARKET FOCUS
    "markets": {
        "cryptos": ["BTC", "ETH", "SOL"],
        "windows_minutes": [15],
        "max_windows_ahead": 2,
        
        # Strike price tolerance - PM uses Chainlink, Kalshi uses CF Benchmarks
        # Tiered system: higher ROI required for higher strike diff risk
        "min_strike_diff_pct": 0.02,    # Below this, use min_edge_pct (5%) - safe zone
        "max_strike_diff_pct": 1.0,    # Reference only — no longer used for pair matching (ROI model handles per direction)
        "min_strike_roi_pct": 8.0,      # ROI required at min_strike_diff (start of risky zone)
        "max_strike_roi_pct": 14.0,     # ROI required at max_strike_diff
        # Linear scale between min and max
        
        # Position limits per underlying per window - scales with strike diff risk
        # More aggressive dropoff to limit exposure when strikes diverge
        "mid_strike_diff_pct": 0.03,    # Threshold for dropping to mid zone
        
        # Early window trading - assume strikes match before Kalshi updates
        "allow_assumed_strike": True,        # Allow trading before Kalshi publishes strike (limited to 1 trade)
        "assumed_strike_window_secs": 60,    # Only assume within first 60s of window
        "assumed_strike_min_edge_pct": 15.0,  # Min ROI when K strike not confirmed (blind fallback - keep conservative)
        "exit_strike_diff_pct": 0.02,        # If actual diff exceeds this, exit position
        "max_trades_favorable": 4,           # Max favorable trades per window (no position cap)
        "max_trades_unfavorable": 2,         # Max unfavorable trades per window
        "max_trades_no_kalshi_strike": 1,    # Max trades when Kalshi strike is TBD (prev market usually provides it)
        "max_position_per_underlying": 30,   # HARD CAP for UNFAVORABLE only: max contracts per side
        "max_contracts_per_window_per_asset": 50,  # AGGREGATE CAP: max total contracts per underlying per window (all directions)
    },
    
    # ==========================================================================
    # LATE WINDOW ASYMMETRIC ARBITRAGE
    # ==========================================================================
    # Lottery-style trades in late market window (85% elapsed → 15s remaining)
    # PM buys favorite (≥91¢), K buys longshot (≤9¢) with asymmetric sizing
    # Breakeven when favorite wins, lottery payout when longshot wins
    
    "late_asymmetric": {
        "enabled": True,
        "window_start_pct": 85,              # Start at 85% elapsed (135s remaining)
        "window_end_seconds": 30,            # End 30 seconds before expiry (PM closes ~20s early)
        "pm_min_price": 0.91,                # PM must be ≥ 91¢ (favorite)
        "k_max_price": 0.09,                 # K must be ≤ 9¢ (longshot)
        "max_trades_per_window": 1,          # Limit per underlying (testing)
        
        # Dynamic ROI parameters (all tunable)
        "neutral_threshold_pct": 0.01,       # |diff| < 0.01% = neutral zone
        "neutral_roi": 6.0,                  # Flat ROI in neutral zone
        
        # Favorable curve: exponential decay from neutral_roi → floor
        "favorable_floor_roi": 0.5,          # Floor for huge favorable gaps
        "favorable_decay_rate": 25.0,        # Decay speed (higher = faster drop)
        
        # Unfavorable curve: gentle linear increase from neutral_roi
        "unfavorable_max_roi": 8.0,          # Cap (realistic for tight late spreads)
        "unfavorable_slope": 20.0,           # ROI increase per 1% strike diff
        "unfavorable_block_pct": 0.15,       # Block trades above this diff
    },
    
    # SELLING - disabled, let positions settle at expiration
    "selling": {
        "enabled": False,  # Never sell, always let positions settle
    },
    
    # LATENCY THRESHOLDS (based on testing)
    "latency": {
        "kalshi_order_warn_ms": 500,      # Kalshi orders usually ~100ms
        "kalshi_fill_warn_ms": 1500,      # Kalshi fills usually ~600ms
        "pm_order_warn_ms": 3000,         # PM orders usually ~1000ms
        "pm_fill_warn_ms": 2000,          # PM fills usually ~700ms
        "max_total_lag_ms": 15000,        # If total lag > 15s, something is very wrong (K needs up to 10s)
        
        # PM Signing Delay Compensation
        # VPN can cause 10-15 second delays in PM wallet signing
        # We compensate by: 1) starting PM leg first, 2) adding extra price buffer
        "pm_signing_delay_expected_ms": 12000,  # Expected PM signing delay (12s)
        "pm_signing_delay_buffer_pct": 5.0,     # Extra price buffer when high latency detected
        "pm_signing_delay_min_edge_pct": 10.0,  # Require higher edge when signing is slow
    },
    
    # FILL SETTINGS
    "aggressive_fill": {
        "enabled": True,
        "first_leg_buffer_pct": 1.0,    # 1% buffer for first leg (minimal)
        "second_leg_buffer_pct": 1.0,   # 1% buffer for second leg (minimal)
        "max_fill_wait_seconds": 5,
        "max_loss_to_cut_pct": 10.0,    # Accept up to 10% loss to close stuck position
        "cut_loss_after_attempts": 2,   # After 2 failed attempts, accept loss to exit
    },
    
    # LOGGING SETTINGS
    "logging": {
        "enabled": True,
        "log_file": "arb_log.jsonl",
        "log_all_scans": False,
        "log_opportunities": True,
        "log_trades": True,
        "min_edge_to_log": 1.0,
    },
    
    # SPEED OPTIMIZATIONS
    "speed": {
        "scan_interval_ms": 50,           # 50ms = 20 scans/sec (was 100ms)
        "parallel_orderbook_fetch": True,
        "cache_markets_seconds": 30,      # Normal cache (10s if strikes missing)
        "use_websocket": True,
        "batch_orderbook_requests": True,  # Batch multiple orderbook requests
        "skip_unchanged_books": True,      # Skip processing if orderbook unchanged
        "use_uvloop": True,                # Use faster event loop (Linux/Mac)
        "high_performance_mode": True,     # Enable all performance optimizations
        "concurrent_requests": 10,         # Max concurrent HTTP requests
    },
    
    # ==========================================================================
    # TIME-OF-DAY FILTER
    # ==========================================================================
    # Data shows 22:00-04:00 UTC consistently loses money (-$45 over 63 trades)
    # US market close volatility (17:00-18:00 ET = 22:00-23:00 UTC) breaks
    # the model, and overnight low-liquidity hours amplify slippage.
    "time_filter": {
        "enabled": True,
        "blocked_hours_utc": [22, 23, 0, 1, 2, 3],  # UTC hours to skip trading
        "log_blocked": True,  # Log when trades are blocked by time filter
    },

    # ==========================================================================
    # EXECUTION SLIPPAGE BUFFER
    # ==========================================================================
    # Real data shows Kalshi effective slippage of 11-16% vs expected 3%.
    # This buffer is SUBTRACTED from net edge before comparing to threshold.
    # It accounts for: orderbook movement between scan and fill, partial fills,
    # and Kalshi's slower execution vs PM.
    "execution_buffer": {
        "enabled": True,
        "kalshi_slippage_pct": 2.0,   # Extra % deducted from edge for K execution risk
        "pm_slippage_pct": 0.5,       # Extra % deducted for PM execution risk
    },

    "test_mode": {
        "enabled": True,
        "max_contracts_per_trade": 10,
        "one_trade_per_segment": False,  # Allow multiple trades per segment
        "min_trade_interval_seconds": 15,  # 15s cooldown between trades
    },
    
    "capital_protection": {
        "enabled": True,
        "min_balance_threshold": 5.0,    # If account would drop below this...
        "require_favorite_when_low": True, # ...only trade if that leg is the favorite (>50% to win)
    },
    
    "min_cash_balance": 5.0,
    "max_order_value_usd": 7.0,  # Max $ to spend on one leg of a trade
    "balance_refresh_seconds": 30,  # Refresh balance every 30s (was 10s - less API spam)
    
    # ==========================================================================
    # MINIMUM STRIKE MARGIN
    # ==========================================================================
    # Data shows ALL trades cluster within 0.2-0.4% of strike — pure coin flips.
    # Enforce a minimum distance from strike so the model has actual predictive value.
    "min_strike_margin": {
        "enabled": True,
        "min_pct": 0.5,           # Spot must be at least 0.5% from strike to trade
        "log_blocked": True,
    },

    # FEES (reference values — actual calculations use exact platform formulas)
    "polymarket_fee_pct": 2.0,  # Conservative estimate (actual varies 0-3%)
    "kalshi_fee_pct": 1.0,      # ~1% on contracts
    "min_depth_usd": 50,        # Lowered - we're trading small
    "max_slippage_bps": 50,
    
    "polymarket_clob_url": "https://clob.polymarket.com",
    "polymarket_gamma_url": "https://gamma-api.polymarket.com",
    "polygon_rpc_url": "https://polygon-rpc.com",
    "kalshi_api_url": "https://api.elections.kalshi.com/trade-api/v2",
    
    # ==========================================================================
    # RESIDENTIAL PROXY (for Polymarket)
    # ==========================================================================
    # Routes PM traffic through Webshare residential proxy to avoid VPN latency
    # Kalshi still uses VPN directly (they geo-block without it)
    
    # Proxy credentials loaded from config.json at startup (see top of file)
    # Add a "residential_proxy" section to config.json with:
    #   enabled, host, port, username, password
    "residential_proxy": _PROXY_CONFIG,
    
}

# =============================================================================
# LOAD API KEYS
# =============================================================================

def load_keys():
    script_dir = Path(__file__).parent
    config_path = script_dir / "config.json"
    pem_path = script_dir / "kalshi_private_key.pem"
    
    if not config_path.exists():
        print("\n[ERROR] config.json not found!")
        print("  Create it with your API keys")
        input("\nPress Enter to exit...")
        sys.exit(1)
    
    with open(config_path) as f:
        keys = json.load(f)
    
    if not pem_path.exists():
        print(f"\n[ERROR] kalshi_private_key.pem not found!")
        input("\nPress Enter to exit...")
        sys.exit(1)
    
    with open(pem_path) as f:
        keys["kalshi_private_key_pem"] = f.read()
    
    if "YOUR_" in keys.get("kalshi_api_key_id", "YOUR_"):
        print("\n[ERROR] Edit config.json - add your Kalshi API key ID")
        print("  Get it from: https://kalshi.com/account/api")
        input("\nPress Enter to exit...")
        sys.exit(1)
    
    if "YOUR_" in keys.get("polymarket_private_key", "YOUR_"):
        print("\n[ERROR] Edit config.json - add your Polymarket private key")
        print("  Export from MetaMask: Account Details -> Show Private Key")
        print("  Should be 64 hex characters (with or without 0x prefix)")
        input("\nPress Enter to exit...")
        sys.exit(1)
    
    if "YOUR_" in keys.get("polymarket_funder_address", "YOUR_"):
        print("\n[ERROR] Edit config.json - add your Polymarket funder/profile address")
        print("  This is shown on polymarket.com (e.g. 0x7dc4...9D07)")
        print("  It's where your USDC is held")
        input("\nPress Enter to exit...")
        sys.exit(1)
    
    if "PASTE" in keys.get("kalshi_private_key_pem", "PASTE"):
        print("\n[ERROR] Edit kalshi_private_key.pem - paste your Kalshi RSA key")
        print("  Get it from: https://kalshi.com/account/api")
        input("\nPress Enter to exit...")
        sys.exit(1)
    
    return keys

# =============================================================================
# CHECK DEPENDENCIES
# =============================================================================

def check_deps():
    missing = []
    for mod, pkg in [("httpx","httpx"),("eth_account","eth-account"),("web3","web3"),("cryptography","cryptography"),("pydantic","pydantic"),("py_clob_client","py-clob-client")]:
        try:
            __import__(mod)
        except ImportError:
            missing.append(pkg)
    
    if missing:
        print(f"\n[ERROR] Missing: {', '.join(missing)}")
        print(f"\n  Run: pip install {' '.join(missing)}")
        input("\nPress Enter to exit...")
        sys.exit(1)

check_deps()

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# =============================================================================
# LOGGER - Captures data for strategy analysis
# =============================================================================

class ArbLogger:
    """
    Session-based logging system.
    
    Structure:
        logs/
            session_001_2026-02-03_1830/
                session.log      # Human-readable text log
                trades.jsonl     # Trade records
                edges.jsonl      # Edge detection history
            session_002_2026-02-03_1900/
                ...
        data/
            profit_history.json   # Persistent across sessions
            pending_settlements.json
    """
    
    def __init__(self):
        self.enabled = CONFIG["logging"]["enabled"]
        self._setup_session_folder()
        
    def _setup_session_folder(self):
        """Create new session folder with ascending number"""
        base_dir = Path(__file__).parent / "logs"
        base_dir.mkdir(exist_ok=True)
        
        # Find next session number
        existing = list(base_dir.glob("session_*"))
        if existing:
            nums = []
            for p in existing:
                try:
                    nums.append(int(p.name.split("_")[1]))
                except:
                    pass
            next_num = max(nums) + 1 if nums else 1
        else:
            next_num = 1
        
        # Create session folder: session_001_2026-02-03_1645
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
        self.session_name = f"session_{next_num:03d}_{timestamp}"
        self.session_dir = base_dir / self.session_name
        self.session_dir.mkdir(exist_ok=True)
        
        # Session files
        self.text_log = self.session_dir / "session.log"
        self.trades_log = self.session_dir / "trades.jsonl"
        self.edges_log = self.session_dir / "edges.jsonl"
        
        # Initialize text log
        with open(self.text_log, "w", encoding="utf-8") as f:
            f.write(f"{'='*70}\n")
            f.write(f"Session: {self.session_name}\n")
            f.write(f"Started: {datetime.now(timezone.utc).isoformat()}\n")
            f.write(f"{'='*70}\n\n")
        
        # For throttling
        self._last_edges_log = 0
    
    def _write_jsonl(self, filepath: Path, record: dict):
        """Write a JSON record to a JSONL file"""
        if not self.enabled:
            return
        record["timestamp"] = datetime.now(timezone.utc).isoformat()
        try:
            with open(filepath, "a") as f:
                f.write(json.dumps(record, default=str) + "\n")
        except:
            pass
    
    def log_text(self, msg: str):
        """Write text message to session log"""
        if not self.enabled:
            return
        try:
            timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
            with open(self.text_log, "a", encoding="utf-8") as f:
                f.write(f"[{timestamp}] {msg}\n")
        except:
            pass
    
    def log_trade(self, opp, pm_result, k_result, pm_time_ms: float, k_time_ms: float, success: bool,
                  start_delta_ms: float = 0, end_delta_ms: float = 0, pm_finished_first: bool = None,
                  pm_fill_price: float = None, k_fill_price: float = None,
                  pm_filled: int = None, k_filled: int = None):
        """Log a trade attempt to trades.jsonl"""
        if not CONFIG["logging"]["log_trades"]:
            return
        
        data = {
            "type": "trade",
            "success": success,
            "underlying": opp.market_pair.polymarket.underlying,
            "direction": opp.direction,
            "raw_edge_pct": float(opp.raw_edge_pct),
            "net_edge_pct": float(opp.net_edge_pct),
            "pm_price": float(opp.first_leg_price if opp.first_leg_venue == Venue.POLYMARKET else opp.second_leg_price),
            "k_price": float(opp.second_leg_price if opp.first_leg_venue == Venue.POLYMARKET else opp.first_leg_price),
            "pm_result": str(pm_result),
            "k_result": str(k_result),
            "pm_latency_ms": pm_time_ms,
            "k_latency_ms": k_time_ms,
        }
        
        if pm_fill_price is not None:
            data["pm_fill_price"] = pm_fill_price
        if k_fill_price is not None:
            data["k_fill_price"] = k_fill_price
        if pm_filled is not None:
            data["pm_filled"] = pm_filled
        if k_filled is not None:
            data["k_filled"] = k_filled
        
        self._write_jsonl(self.trades_log, data)
    
    def log_edges(self, edges: list):
        """Log edge snapshots to session folder (throttled to once per 30s)"""
        if not edges:
            return
        now = time.time()
        if now - self._last_edges_log < 30:
            return
        self._last_edges_log = now
        
        for edge in edges:
            self._write_jsonl(self.edges_log, edge)


def log_print(msg: str, end: str = "\n", flush: bool = False):
    """Print to console AND log to session file"""
    print(msg, end=end, flush=flush)
    if end == "\n" and not msg.startswith("\r"):
        clean_msg = msg.strip()
        if clean_msg:
            logger.log_text(clean_msg)

# Global logger instance
logger = ArbLogger()
print(f"  [Log] Session: {logger.session_name}")

# =============================================================================
# TICKER WINDOW PARSING - Filter Kalshi positions by market window
# =============================================================================

_MONTH_MAP = {
    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
}

def get_ticker_window_end(ticker: str) -> datetime | None:
    """
    Parse the window close time from a Kalshi ticker.
    
    KXBTC15M-26FEB101715-15 → 2026-02-10 17:15:00 ET → UTC
    KXETH15M-26FEB101700-00 → 2026-02-10 17:00:00 ET → UTC
    
    Returns UTC datetime, or None if unparseable.
    """
    try:
        parts = ticker.split('-')
        if len(parts) < 2:
            return None
        
        date_time_str = parts[1]  # '26FEB101715'
        if len(date_time_str) < 11:
            return None
        
        year = 2000 + int(date_time_str[0:2])
        month_str = date_time_str[2:5].upper()
        day = int(date_time_str[5:7])
        hour = int(date_time_str[7:9])
        minute = int(date_time_str[9:11])
        
        month = _MONTH_MAP.get(month_str)
        if month is None:
            return None
        
        et = ZoneInfo("America/New_York")
        local_dt = datetime(year, month, day, hour, minute, tzinfo=et)
        return local_dt.astimezone(timezone.utc)
    except (ValueError, IndexError, KeyError):
        return None


def is_current_window_ticker(ticker: str, current_window_end_utc: datetime) -> bool:
    """
    Check if a Kalshi ticker belongs to the current trading window.
    
    Returns True if the ticker's window matches current, False if stale.
    Returns True if unparseable (fail-open to avoid blocking valid trades).
    """
    if current_window_end_utc is None:
        return True  # No window info → assume current
    
    ticker_end = get_ticker_window_end(ticker)
    if ticker_end is None:
        return True  # Can't parse → assume current (fail-open)
    
    # Allow 60-second tolerance for clock skew
    diff = abs((ticker_end - current_window_end_utc).total_seconds())
    return diff < 60


# =============================================================================
# STRIKE PRICE LOADING - Reads from strike_collector.py output
# =============================================================================

def load_strikes_from_file() -> dict:
    """Load strike prices from strikes.json (created by strike_collector.py)"""
    try:
        # Use user's home directory so both collector and bot find the same file
        strikes_file = Path.home() / "strikes.json"
        if strikes_file.exists():
            with open(strikes_file, "r") as f:
                data = json.load(f)
                return data.get("strikes", {})
    except Exception as e:
        pass
    return {}


def get_strike_from_file(symbol: str, window_start: datetime) -> Tuple[Optional[float], str]:
    """Get a strike price from the strikes.json file.
    
    Args:
        symbol: e.g., 'BTC', 'ETH', 'SOL'
        window_start: The start time of the 15-minute window
        
    Returns:
        (price, confidence) where confidence is HIGH/MED/LOW/NONE
    """
    strikes = load_strikes_from_file()
    
    # Build the key format used by strike_collector.py
    sym = symbol.upper().replace("/USD", "")
    key = f"{sym}_{window_start.strftime('%H:%M')}"
    
    if key in strikes:
        data = strikes[key]
        return data.get("price"), data.get("confidence", "LOW")
    
    return None, "NONE"


# =============================================================================
# POLYMARKET RTDS - Real-time Chainlink prices (fallback)
# =============================================================================

class PolymarketRTDS:
    """WebSocket client for real-time Chainlink price data from Polymarket RTDS.
    
    Tracks prices by 15-minute window so we can look up the "price to beat"
    for any given market window.
    """
    
    WS_URL = "wss://ws-live-data.polymarket.com"
    
    def __init__(self):
        self.ws = None
        self._thread = None
        self._running = False
        self._lock = threading.Lock()
        
        # Current prices
        self._prices = {}  # symbol -> {value, timestamp}
        
        # Historical prices by window start time
        # Key: (symbol, window_start_iso) -> {first_price, first_time, prices: [...]}
        self._window_prices = {}
        
        # Rolling price history for realized volatility (last ~60 min, sampled every ~10s)
        # Key: symbol -> deque of (timestamp_float, price)
        self._price_history = {}       # symbol -> deque(maxlen=360)  # 360 × 10s = 60 min
        self._last_history_sample = {}  # symbol -> last sample time (throttle to ~10s)
        
        # Stats
        self._update_count = 0
        self._connected = False
    
    def start(self):
        """Start RTDS WebSocket in background thread"""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
    
    def stop(self):
        """Stop the WebSocket"""
        self._running = False
        if self.ws:
            try:
                self.ws.close()
            except:
                pass
    
    def _get_window_start(self, dt: datetime) -> datetime:
        """Get the start of the 15-minute window for a given datetime"""
        minute = dt.minute
        window_minute = (minute // 15) * 15
        return dt.replace(minute=window_minute, second=0, microsecond=0)
    
    def _run(self):
        """Main WebSocket loop"""
        import websocket
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                topic = data.get("topic", "")
                
                # Only track Chainlink prices (used for settlement)
                if "crypto_prices_chainlink" in topic:
                    payload = data.get("payload", {})
                    symbol = payload.get("symbol", "").lower()  # e.g., "btc/usd"
                    value = payload.get("value")
                    payload_ts = payload.get("timestamp")  # ms timestamp from Chainlink
                    msg_ts = data.get("timestamp")  # ms timestamp from server
                    
                    if symbol and value:
                        now = datetime.now(timezone.utc)
                        window_start = self._get_window_start(now)
                        window_key = (symbol, window_start.isoformat())
                        seconds_into_window = (now - window_start).total_seconds()
                        
                        with self._lock:
                            # Update current price
                            self._prices[symbol] = {
                                "value": float(value),
                                "timestamp": payload_ts,
                                "local_time": now.isoformat()
                            }
                            
                            # Track by window
                            is_new_window = window_key not in self._window_prices
                            
                            if is_new_window:
                                # First price in this window - this might be the "price to beat"
                                self._window_prices[window_key] = {
                                    "first_price": float(value),
                                    "first_time": now.isoformat(),
                                    "first_seconds": seconds_into_window,
                                    "payload_ts": payload_ts,
                                    "msg_ts": msg_ts,
                                    "prices": []
                                }
                                
                                # Log new window to bot_log.txt if captured early (good strike)
                                sym_upper = symbol.upper().replace("/USD", "")
                                confidence = "HIGH" if seconds_into_window <= 5 else "MED" if seconds_into_window <= 30 else "LOW"
                                logger.log_text(f"RTDS {sym_upper} @ {window_start.strftime('%H:%M')} UTC: ${float(value):,.2f} ({seconds_into_window:.1f}s - {confidence})")
                            
                            # Keep some price history
                            if seconds_into_window <= 30:
                                self._window_prices[window_key]["prices"].append({
                                    "price": float(value),
                                    "seconds": seconds_into_window,
                                })
                            
                            # Keep only recent prices per window to save memory
                            if len(self._window_prices[window_key]["prices"]) > 50:
                                self._window_prices[window_key]["prices"] = \
                                    self._window_prices[window_key]["prices"][-50:]
                            
                            self._update_count += 1
                            
                            # Sample price for realized volatility (~every 10 seconds per symbol)
                            from collections import deque
                            now_ts = time.time()
                            last_sample = self._last_history_sample.get(symbol, 0)
                            if now_ts - last_sample >= 10.0:
                                if symbol not in self._price_history:
                                    self._price_history[symbol] = deque(maxlen=360)
                                self._price_history[symbol].append((now_ts, float(value)))
                                self._last_history_sample[symbol] = now_ts
                            
                            # Clean up old windows (keep last 2 hours = 8 windows per symbol)
                            if len(self._window_prices) > 30:
                                sorted_keys = sorted(self._window_prices.keys(), key=lambda x: x[1])
                                for old_key in sorted_keys[:-24]:
                                    del self._window_prices[old_key]
                            
            except Exception as e:
                pass  # Silently handle parse errors
        
        def on_open(ws):
            self._connected = True
            logger.log_text("RTDS connected")
            # Subscribe to Chainlink prices
            subscribe_msg = {
                "action": "subscribe",
                "subscriptions": [
                    {"topic": "crypto_prices_chainlink", "type": "*", "filters": ""}
                ]
            }
            ws.send(json.dumps(subscribe_msg))
        
        def on_error(ws, error):
            logger.log_text(f"RTDS error: {error}")
        
        def on_close(ws, close_status_code, close_msg):
            self._connected = False
            logger.log_text(f"RTDS disconnected: {close_status_code}")
        
        while self._running:
            try:
                self.ws = websocket.WebSocketApp(
                    self.WS_URL,
                    on_message=on_message,
                    on_open=on_open,
                    on_error=on_error,
                    on_close=on_close
                )
                self.ws.run_forever(ping_interval=5, ping_timeout=3)
            except:
                pass
            if self._running:
                time.sleep(1)  # Reconnect delay
    
    def get_current_price(self, symbol: str) -> Optional[float]:
        """Get current Chainlink price for symbol (e.g., 'btc/usd')"""
        with self._lock:
            data = self._prices.get(symbol.lower())
            return data["value"] if data else None
    
    def get_realized_volatility(self, symbol: str, lookback_minutes: float = 60.0) -> Optional[float]:
        """
        Calculate trailing realized annualized volatility from price history.
        
        Uses log returns of ~10-second price samples over the lookback window.
        Returns annualized volatility (e.g., 0.70 = 70%).
        Returns None if insufficient data (< 20 samples).
        """
        import math
        sym = symbol.lower()
        with self._lock:
            history = self._price_history.get(sym)
            if not history or len(history) < 20:
                return None
            
            # Filter to lookback window
            cutoff = time.time() - (lookback_minutes * 60)
            samples = [(t, p) for t, p in history if t >= cutoff]
            
            if len(samples) < 20:
                return None
            
            # Calculate log returns
            log_returns = []
            for i in range(1, len(samples)):
                dt = samples[i][0] - samples[i-1][0]
                if dt > 0 and samples[i-1][1] > 0:
                    r = math.log(samples[i][1] / samples[i-1][1])
                    log_returns.append((r, dt))
            
            if len(log_returns) < 15:
                return None
            
            # Variance of returns, normalized to per-second
            # Each return covers ~dt seconds, so per-second variance = var(r/sqrt(dt))
            normalized = [r / math.sqrt(dt) for r, dt in log_returns if dt > 0]
            mean_r = sum(normalized) / len(normalized)
            var_per_sec = sum((r - mean_r) ** 2 for r in normalized) / (len(normalized) - 1)
            
            # Annualize: vol_annual = sqrt(var_per_sec * seconds_per_year)
            vol_annual = math.sqrt(var_per_sec * 365.25 * 24 * 3600)
            
            return vol_annual
    
    def get_strike_price(self, symbol: str, window_start: datetime) -> Optional[float]:
        """Get the strike price (first Chainlink price) for a specific 15-min window.
        
        This is the "Price to Beat" that Polymarket uses for settlement.
        The price must be captured within the first few seconds of the window
        to be accurate.
        
        Args:
            symbol: e.g., 'btc/usd', 'eth/usd', 'sol/usd'
            window_start: The start time of the 15-minute window
            
        Returns:
            The first Chainlink price recorded for that window, or None if not available
        """
        window_key = (symbol.lower(), window_start.isoformat())
        with self._lock:
            data = self._window_prices.get(window_key)
            if data:
                return data["first_price"]
            return None
    
    def get_strike_price_with_confidence(self, symbol: str, window_start: datetime) -> Tuple[Optional[float], str]:
        """Get strike price with confidence level.
        
        Returns:
            (price, confidence) where confidence is:
            - "high": captured within first 5 seconds of window
            - "medium": captured within first 30 seconds  
            - "low": captured later in window
            - "none": no data for this window
        """
        window_key = (symbol.lower(), window_start.isoformat())
        with self._lock:
            data = self._window_prices.get(window_key)
            if not data:
                return None, "none"
            
            first_seconds = data.get("first_seconds", 999)
            price = data["first_price"]
            
            if first_seconds <= 5:
                return price, "high"
            elif first_seconds <= 30:
                return price, "medium"
            else:
                return price, "low"
    
    def is_connected_and_ready(self) -> bool:
        """Check if RTDS is connected and receiving data"""
        return self._connected and self._update_count > 0
    
    def get_window_data(self, symbol: str, window_start: datetime) -> Optional[dict]:
        """Get all data for a specific window (for debugging)"""
        window_key = (symbol.lower(), window_start.isoformat())
        with self._lock:
            return self._window_prices.get(window_key)
    
    def get_stats(self) -> dict:
        """Get connection stats"""
        with self._lock:
            return {
                "connected": self._connected,
                "update_count": self._update_count,
                "windows_tracked": len(self._window_prices),
                "symbols": list(self._prices.keys())
            }
    
    def print_status(self):
        """Print current status (for debugging)"""
        stats = self.get_stats()
        print(f"  [RTDS] Connected: {stats['connected']} | Updates: {stats['update_count']} | Windows: {stats['windows_tracked']}")
        with self._lock:
            for symbol, data in self._prices.items():
                print(f"         {symbol}: ${data['value']:,.2f}")

# Global RTDS instance for strike prices
_rtds_client: Optional[PolymarketRTDS] = None

def get_rtds_client() -> PolymarketRTDS:
    """Get or create global RTDS client"""
    global _rtds_client
    if _rtds_client is None:
        _rtds_client = PolymarketRTDS()
        _rtds_client.start()
    return _rtds_client

# =============================================================================
# POLYMARKET WEBSOCKET - Real-time orderbook streaming
# =============================================================================

class PolymarketWebSocket:
    """WebSocket client for real-time Polymarket orderbook updates"""
    
    WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    
    def __init__(self):
        self.ws = None
        self.orderbooks = {}  # token_id -> {bids, asks}
        self.token_to_market = {}  # token_id -> market_id
        self._running = False
        self._thread = None
        self._lock = threading.Lock()
    
    def start(self, token_ids: list, token_to_market: dict):
        """Start WebSocket connection and subscribe to token orderbooks"""
        import websocket
        
        self.token_to_market = token_to_market
        self._running = True
        
        def on_message(ws, message):
            try:
                # Skip non-JSON messages (like pong responses)
                if not message or message == "pong" or not message.startswith("{"):
                    return
                    
                data = json.loads(message)
                event_type = data.get("event_type")
                
                if event_type == "book":
                    # Full orderbook snapshot
                    token_id = data.get("asset_id")
                    if token_id:
                        with self._lock:
                            self.orderbooks[token_id] = {
                                "bids": data.get("bids", []),
                                "asks": data.get("asks", []),
                                "timestamp": data.get("timestamp")
                            }
                elif event_type == "price_change":
                    # Price update
                    for change in data.get("price_changes", []):
                        token_id = change.get("asset_id")
                        if token_id:
                            with self._lock:
                                if token_id not in self.orderbooks:
                                    self.orderbooks[token_id] = {"bids": [], "asks": []}
                                # Update best bid/ask from price change
                                self.orderbooks[token_id]["best_bid"] = change.get("best_bid")
                                self.orderbooks[token_id]["best_ask"] = change.get("best_ask")
            except json.JSONDecodeError:
                pass  # Ignore non-JSON messages
            except Exception as e:
                pass  # Silently ignore parsing errors
        
        def on_error(ws, error):
            # Log to file only - don't spam console with connection errors
            pass
        
        def on_close(ws, close_status, close_msg):
            if self._running:
                # Silent reconnect - no console spam
                time.sleep(1)
                self._connect(token_ids, on_message, on_error, on_close, on_open)
        
        def on_open(ws):
            # Subscribe to market channel with all token IDs
            sub_msg = {
                "assets_ids": token_ids,
                "type": "market"
            }
            ws.send(json.dumps(sub_msg))
            print(f"  [WS] Subscribed to {len(token_ids)} tokens")
            
            # Start ping thread to keep connection alive
            def ping():
                while self._running and ws.sock:
                    try:
                        ws.send("ping")
                        time.sleep(30)
                    except:
                        break
            threading.Thread(target=ping, daemon=True).start()
        
        def run():
            self._connect(token_ids, on_message, on_error, on_close, on_open)
        
        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()
    
    def _connect(self, token_ids, on_message, on_error, on_close, on_open):
        import websocket
        self.ws = websocket.WebSocketApp(
            self.WS_URL,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )
        self.ws.run_forever()
    
    def get_orderbook(self, token_id: str) -> dict:
        """Get cached orderbook for a token"""
        with self._lock:
            return self.orderbooks.get(token_id, {})
    
    def stop(self):
        """Stop WebSocket connection"""
        self._running = False
        if self.ws:
            self.ws.close()


class KalshiWebSocket:
    """WebSocket client for real-time Kalshi orderbook updates"""
    
    WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
    
    def __init__(self, api_key_id: str, private_key):
        self.api_key_id = api_key_id
        self._private_key = private_key
        self.ws = None
        self.orderbooks = {}  # market_ticker -> {yes_bids, no_bids}
        self._running = False
        self._thread = None
        self._lock = threading.Lock()
    
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
    
    def _get_headers(self) -> dict:
        ts = str(int(time.time() * 1000))
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-SIGNATURE": self._sign(ts, "GET", "/trade-api/ws/v2"),
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }
    
    def start(self, market_tickers: list):
        """Start WebSocket connection and subscribe to orderbooks"""
        import websocket
        
        self._running = True
        self._market_tickers = market_tickers
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                msg_type = data.get("type")
                
                if msg_type == "orderbook_snapshot":
                    # Full orderbook snapshot
                    ticker = data.get("data", {}).get("market_ticker")
                    if ticker:
                        with self._lock:
                            self.orderbooks[ticker] = {
                                "yes": data["data"].get("yes", []),
                                "no": data["data"].get("no", []),
                                "timestamp": time.time()
                            }
                
                elif msg_type == "orderbook_delta":
                    # Incremental update
                    ticker = data.get("data", {}).get("market_ticker")
                    if ticker and ticker in self.orderbooks:
                        with self._lock:
                            # Apply delta updates
                            for side in ["yes", "no"]:
                                for update in data["data"].get(side, []):
                                    price = update.get("price")
                                    delta = update.get("delta", 0)
                                    # Update the orderbook (simplified - just track best prices)
                                    self.orderbooks[ticker]["timestamp"] = time.time()
                
                elif msg_type == "ticker":
                    # Ticker update with best bid/ask
                    ticker = data.get("data", {}).get("market_ticker")
                    if ticker:
                        with self._lock:
                            if ticker not in self.orderbooks:
                                self.orderbooks[ticker] = {"yes": [], "no": [], "timestamp": 0}
                            self.orderbooks[ticker]["yes_bid"] = data["data"].get("yes_bid")
                            self.orderbooks[ticker]["yes_ask"] = data["data"].get("yes_ask")
                            self.orderbooks[ticker]["timestamp"] = time.time()
                            
            except Exception as e:
                pass  # Silently ignore parsing errors
        
        def on_error(ws, error):
            # Log to file only - don't spam console
            pass
        
        def on_close(ws, close_status, close_msg):
            if self._running:
                # Silent reconnect
                time.sleep(2)
                self._connect(on_message, on_error, on_close, on_open)
        
        def on_open(ws):
            # Subscribe to ticker for all markets (gets best bid/ask)
            sub_msg = {
                "id": 1,
                "cmd": "subscribe",
                "params": {
                    "channels": ["ticker"],
                    "market_tickers": self._market_tickers
                }
            }
            ws.send(json.dumps(sub_msg))
            
            # Also subscribe to orderbook deltas
            sub_msg2 = {
                "id": 2,
                "cmd": "subscribe", 
                "params": {
                    "channels": ["orderbook_delta"],
                    "market_tickers": self._market_tickers
                }
            }
            ws.send(json.dumps(sub_msg2))
            print(f"  [K-WS] Subscribed to {len(self._market_tickers)} markets")
        
        def run():
            self._connect(on_message, on_error, on_close, on_open)
        
        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()
    
    def _connect(self, on_message, on_error, on_close, on_open):
        import websocket
        headers = self._get_headers()
        header_list = [f"{k}: {v}" for k, v in headers.items()]
        self.ws = websocket.WebSocketApp(
            self.WS_URL,
            header=header_list,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )
        self.ws.run_forever()
    
    def get_orderbook(self, market_ticker: str) -> dict:
        """Get cached orderbook for a market"""
        with self._lock:
            return self.orderbooks.get(market_ticker, {})
    
    def stop(self):
        """Stop WebSocket connection"""
        self._running = False
        if self.ws:
            self.ws.close()

import threading
from cryptography.hazmat.backends import default_backend
from eth_account import Account
from eth_account.messages import encode_typed_data
from web3 import Web3

# =============================================================================
# DATA MODELS
# =============================================================================

class Venue(str, Enum):
    POLYMARKET = "polymarket"
    KALSHI = "kalshi"

class Direction(str, Enum):
    UP = "up"
    DOWN = "down"

class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"

class OrderStatus(str, Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    FILLED = "filled"
    REJECTED = "rejected"

@dataclass
class Market:
    venue: Venue
    market_id: str
    title: str
    underlying: str
    direction: Direction
    interval_minutes: int
    start_time: datetime
    end_time: datetime
    yes_price: Optional[Decimal] = None
    no_price: Optional[Decimal] = None
    token_id: Optional[str] = None  # YES token
    no_token_id: Optional[str] = None  # NO token
    strike_price: Optional[float] = None  # Reference price to beat
    strike_uncertain: bool = False  # True if strike came from previous market's expiration value
    
    @property
    def time_to_close(self) -> float:
        return max(0, (self.end_time - datetime.now(timezone.utc)).total_seconds())
    
    @property
    def is_tradeable(self) -> bool:
        return self.time_to_close > 0

@dataclass
class MarketPair:
    polymarket: Market
    kalshi: Market
    confidence: float = 0.0
    assumed_strike: bool = False  # True if we're using PM strike as assumed Kalshi strike
    strike_diff_pct: float = 0.0  # Strike difference percentage for tiered ROI requirement
    
    @property
    def is_valid(self) -> bool:
        return self.polymarket.is_tradeable and self.kalshi.is_tradeable and self.confidence >= 0.80

@dataclass
class OrderbookLevel:
    price: Decimal
    size: Decimal

@dataclass
class Orderbook:
    venue: Venue
    market_id: str
    yes_bids: list = field(default_factory=list)  # People wanting to BUY YES
    yes_asks: list = field(default_factory=list)  # People wanting to SELL YES (we buy from them)
    
    @property
    def best_yes_bid(self):
        """Best price someone will pay for YES"""
        return self.yes_bids[0].price if self.yes_bids else None
    
    @property
    def best_yes_ask(self):
        """Best price we can buy YES for"""
        return self.yes_asks[0].price if self.yes_asks else None
    
    @property
    def best_no_bid(self):
        """Best price someone will pay for NO = 1 - best_yes_ask"""
        return Decimal("1") - self.best_yes_ask if self.best_yes_ask else None
    
    @property
    def best_no_ask(self):
        """Best price we can buy NO for = 1 - best_yes_bid"""
        return Decimal("1") - self.best_yes_bid if self.best_yes_bid else None

@dataclass
class Order:
    order_id: str
    venue: Venue
    market_id: str
    side: OrderSide
    direction: Direction
    price: Decimal
    size: Decimal
    status: OrderStatus = OrderStatus.PENDING

@dataclass
class Opportunity:
    market_pair: MarketPair
    direction: str
    raw_edge_pct: Decimal
    net_edge_pct: Decimal
    max_size_usd: Decimal
    pm_price: Decimal
    k_price: Decimal

@dataclass
class ScoredOpportunity:
    """An opportunity scored by the momentum + delayed arb strategy"""
    market_pair: MarketPair
    
    # Which side to buy first (the underpriced/lagging one)
    first_leg_venue: Venue
    first_leg_direction: Direction
    first_leg_price: Decimal
    first_leg_market_id: str
    
    # Second leg to complete the arb
    second_leg_venue: Venue
    second_leg_direction: Direction
    second_leg_price: Decimal
    second_leg_market_id: str
    
    # Scoring components
    divergence_pct: Decimal
    minutes_since_open: float
    price_extremity: Decimal  # Distance from 0.50
    score: float
    
    # Price info
    pm_implied_prob: Decimal  # PM's view of UP probability
    k_implied_prob: Decimal   # Kalshi's view of UP probability
    lagging_venue: Venue      # Which venue hasn't caught up
    
    # Profit estimates
    raw_edge_pct: Decimal     # Edge before fees/buffer
    net_edge_pct: Decimal     # Edge AFTER fees and buffer
    est_profit_per_contract: Decimal  # Estimated $ profit per contract
    
    # Late window flag - skip reconciliation for late trades
    is_late_window_trade: bool = False
    
    # Late arb flag - trade entered in 85-95% of window, reduced size
    is_late_arb: bool = False
    
    # Strike favorability: True=win zone (both legs pay between strikes), 
    # False=loss zone or unknown, None=neutral (equal strikes)
    strike_favorable: bool = False
    
    # Strike diff percentage for ROI/size adjustments
    strike_diff_pct: float = 0.0
    
    # Late asymmetric arbitrage fields
    is_asymmetric: bool = False  # True for late window lottery trades
    asymmetric_sizing: dict = field(default_factory=dict)  # Sizing details if asymmetric
    
    # Keep for backward compat
    @property
    def potential_edge_pct(self) -> Decimal:
        return self.net_edge_pct

@dataclass
class ActivePosition:
    """Tracks a position waiting for second leg"""
    opportunity: ScoredOpportunity
    first_leg_order: Order
    first_leg_fill_price: Decimal
    first_leg_fill_size: int
    entry_time: datetime
    fill_attempts: int = 0  # Track how many times we've tried to fill second leg
    is_partial_fill: bool = False  # True if first leg was partial (more urgent to hedge)
    
    @property
    def seconds_held(self) -> float:
        return (datetime.now(timezone.utc) - self.entry_time).total_seconds()

@dataclass
class Balance:
    venue: Venue
    cash: Decimal  # Available to spend
    portfolio: Decimal = Decimal("0")  # Value of positions
    
    @property
    def available(self) -> Decimal:
        """Alias for cash for backward compatibility"""
        return self.cash
    
    @property
    def total(self) -> Decimal:
        return self.cash + self.portfolio

@dataclass
class ConfirmedTrade:
    """A trade where both legs have confirmed fills"""
    timestamp: datetime
    underlying: str
    direction: str
    size: int
    pm_fill_price: Decimal
    k_fill_price: Decimal
    total_cost: Decimal  # What we actually paid
    guaranteed_payout: Decimal  # Always $1 per contract
    expected_profit: Decimal  # payout - cost


class ProfitTracker:
    """
    Tracks profit accurately with persistence to file.
    
    Profit calculation for arb trades:
    - We buy contracts on BOTH sides (PM + Kalshi)
    - One side is guaranteed to win and pay $1
    - Profit = $1 × contracts - (PM cost + Kalshi cost) - fees
    
    Fees:
    - PM: Variable based on price (~2-3% at mid prices, lower at extremes)
    - Kalshi: ~1% on profit (not on volume)
    """
    
    # Data file location
    DATA_DIR = os.path.join(os.path.expanduser("~"), ".arb_data")
    PROFIT_FILE = os.path.join(DATA_DIR, "profit_history.json")
    JOURNAL_FILE = os.path.join(DATA_DIR, "trade_journal.jsonl")
    
    def __init__(self):
        self._lock = threading.Lock()
        self.session_start = datetime.now(timezone.utc)
        self.session_trades = []  # List of trade records this session
        self.session_profit = Decimal("0")
        self.all_time_profit = Decimal("0")
        self.all_time_trades = 0
        
        # Balance-based P&L (source of truth for display)
        self.balance_baseline = None       # Set on first run: PM + K total
        self.session_start_balance = None  # Set when session starts
        
        # Pending settlements - trades waiting for market to settle
        self.PENDING_FILE = os.path.join(self.DATA_DIR, "pending_settlements.json")
        self.pending_settlements = []  # List of pending trade records
        
        # Ensure data directory exists
        os.makedirs(self.DATA_DIR, exist_ok=True)
        
        # Load historical data
        self._load_history()
        self._load_pending_settlements()
    
    def _load_history(self):
        """Load profit history from file"""
        import math
        try:
            if Path(self.PROFIT_FILE).exists():
                with open(self.PROFIT_FILE, "r") as f:
                    data = json.load(f)
                    self.all_time_profit = Decimal(str(data.get("all_time_profit", 0)))
                    self.all_time_trades = data.get("all_time_trades", 0)
                    
                    # Balance-based P&L: load baseline if it exists
                    if data.get("balance_baseline") is not None:
                        self.balance_baseline = float(data["balance_baseline"])
                        print(f"  [Profit] Balance baseline: ${self.balance_baseline:.2f}")
                    else:
                        # No baseline yet — will be set on first balance check
                        print(f"  [Profit] No balance baseline set — will initialize on first run")
                    
                    print(f"  [Profit] Loaded history: {self.all_time_trades} trades")
        except Exception as e:
            print(f"  [Profit] Could not load history: {e}")
    
    def _load_pending_settlements(self):
        """Load pending settlements from file"""
        try:
            if Path(self.PENDING_FILE).exists():
                with open(self.PENDING_FILE, "r") as f:
                    self.pending_settlements = json.load(f)
                    if self.pending_settlements:
                        print(f"  [Profit] {len(self.pending_settlements)} trades pending settlement")
        except Exception as e:
            self.pending_settlements = []
    
    def _save_pending_settlements(self):
        """Save pending settlements to file"""
        try:
            with open(self.PENDING_FILE, "w") as f:
                json.dump(self.pending_settlements, f, indent=2)
        except Exception as e:
            pass
    
    def initialize_balance_baseline(self, total_balance: float):
        """
        Set the balance baseline on first run after update.
        All-time P&L = current_balance - baseline.
        Only sets if no baseline exists yet.
        """
        if self.balance_baseline is not None:
            return  # Already set
        
        self.balance_baseline = total_balance
        self.session_start_balance = total_balance
        print(f"  [Profit] ✅ Balance baseline set: ${total_balance:.2f}")
        print(f"  [Profit] All-time P&L will track from this point forward")
        self._save_history()
    
    def update_session_start_balance(self, total_balance: float):
        """Set session start balance (called once per session start)"""
        if self.session_start_balance is None:
            self.session_start_balance = total_balance
    
    def get_balance_pnl(self, current_total_balance: float) -> tuple:
        """
        Get P&L based on actual balance changes.
        Returns (all_time_pnl, session_pnl).
        """
        if self.balance_baseline is None:
            return (Decimal("0"), Decimal("0"))
        
        all_time = Decimal(str(round(current_total_balance - self.balance_baseline, 2)))
        
        if self.session_start_balance is not None:
            session = Decimal(str(round(current_total_balance - self.session_start_balance, 2)))
        else:
            session = Decimal("0")
        
        return (all_time, session)
    
    def log_to_journal(self, underlying: str, strategy: str, size: int,
                       pm_dir: str, k_dir: str,
                       pm_expected: float, pm_actual: float,
                       k_expected: float, k_actual: float,
                       strike_diff_pct: float = 0, strike_favorable: bool = True,
                       k_ticker: str = "", window_end: str = "",
                       pm_fill_ms: float = 0, k_fill_ms: float = 0,
                       session_name: str = "", notes: str = ""):
        """
        Append one trade record to ~/.arb_data/trade_journal.jsonl.
        
        Fields per line:
          ts               – UTC timestamp
          session          – session folder name
          underlying       – BTC / ETH / SOL
          strategy         – arb | late_asym
          size             – contracts (hedged min of both legs)
          pm_dir / k_dir   – up/down, yes/no
          pm_expected      – orderbook snapshot price per contract
          pm_actual        – exchange-reported avg fill price
          pm_slippage      – actual - expected (positive = paid more)
          k_expected / k_actual / k_slippage – same for Kalshi
          total_expected   – pm_expected + k_expected
          total_actual     – pm_actual + k_actual
          profit_per_contract – 1.00 - total_actual
          profit_total     – profit_per_contract * size
          strike_diff_pct  – strike price difference between platforms
          strike_favorable – True if diff is in our favor
          k_ticker         – Kalshi market ticker
          window_end       – market expiry time
          pm_fill_ms / k_fill_ms – fill latency
        """
        import json
        
        pm_slip = pm_actual - pm_expected
        k_slip = k_actual - k_expected
        total_expected = pm_expected + k_expected
        total_actual = pm_actual + k_actual
        profit_per = 1.0 - total_actual
        
        entry = {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "session": session_name,
            "underlying": underlying,
            "strategy": strategy,
            "size": size,
            "pm_dir": pm_dir,
            "k_dir": k_dir,
            "pm_expected": round(pm_expected, 4),
            "pm_actual": round(pm_actual, 4),
            "pm_slippage": round(pm_slip, 4),
            "k_expected": round(k_expected, 4),
            "k_actual": round(k_actual, 4),
            "k_slippage": round(k_slip, 4),
            "total_expected": round(total_expected, 4),
            "total_actual": round(total_actual, 4),
            "profit_per_contract": round(profit_per, 4),
            "profit_total": round(profit_per * size, 4),
            "strike_diff_pct": round(strike_diff_pct, 4),
            "strike_favorable": strike_favorable,
            "k_ticker": k_ticker,
            "window_end": window_end,
            "pm_fill_ms": round(pm_fill_ms),
            "k_fill_ms": round(k_fill_ms),
            "notes": notes,
            "settled": False,
            "settlement_payout": None,
            "settlement_profit": None,
            "settlement_outcome": None,
        }
        
        try:
            with open(self.JOURNAL_FILE, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            print(f"  [Journal] Write error: {e}")
    
    def update_journal_settlement(self, trade_timestamp: str, underlying: str, size: int,
                                   payout: float, actual_profit: float, outcome: str):
        """
        Update a journal entry with actual settlement outcome.
        
        Matches by underlying + size + timestamp proximity (within 5 seconds).
        Updates settled, settlement_payout, settlement_profit, settlement_outcome,
        and corrects profit_per_contract and profit_total to reflect reality.
        """
        import json
        from datetime import datetime as dt
        
        try:
            # Parse the trade timestamp for matching
            # Handle both ISO formats: with/without fractional seconds
            ts_clean = trade_timestamp.replace("+00:00", "").replace("Z", "")
            try:
                trade_dt = dt.fromisoformat(ts_clean)
            except ValueError:
                log_print(f"  [Journal] Could not parse timestamp: {trade_timestamp}")
                return False
            
            # Read all journal entries
            if not os.path.exists(self.JOURNAL_FILE):
                return False
            
            with open(self.JOURNAL_FILE, "r") as f:
                lines = f.readlines()
            
            # Find matching entry (search from end since recent trades settle first)
            best_idx = -1
            best_diff = 10.0  # Max 10 second gap
            
            for i in range(len(lines) - 1, -1, -1):
                try:
                    entry = json.loads(lines[i])
                except (json.JSONDecodeError, ValueError):
                    continue
                
                # Skip already settled
                if entry.get("settled", False):
                    continue
                
                # Match underlying and size
                if entry.get("underlying") != underlying or entry.get("size") != size:
                    continue
                
                # Match timestamp within 5 seconds
                try:
                    entry_ts = entry["ts"].replace("Z", "")
                    entry_dt = dt.fromisoformat(entry_ts)
                    diff = abs((trade_dt - entry_dt).total_seconds())
                    if diff < best_diff:
                        best_diff = diff
                        best_idx = i
                except (ValueError, KeyError):
                    continue
            
            if best_idx < 0:
                log_print(f"  [Journal] No matching unsettled entry for {underlying} x{size} @ {trade_timestamp}")
                return False
            
            # Update the entry
            entry = json.loads(lines[best_idx])
            entry["settled"] = True
            entry["settlement_payout"] = round(payout, 4)
            entry["settlement_profit"] = round(actual_profit, 4)
            entry["settlement_outcome"] = outcome
            # Correct the P&L fields to reflect actual outcome
            entry["profit_total"] = round(actual_profit, 4)
            entry["profit_per_contract"] = round(actual_profit / size, 4) if size > 0 else 0
            
            lines[best_idx] = json.dumps(entry) + "\n"
            
            # Write back
            with open(self.JOURNAL_FILE, "w") as f:
                f.writelines(lines)
            
            log_print(f"  [Journal] Updated {underlying} x{size}: {outcome} → P&L ${actual_profit:+.2f} (payout ${payout:.2f})")
            return True
            
        except Exception as e:
            log_print(f"  [Journal] Settlement update error: {e}")
            return False
    
    def record_pending_settlement(self, underlying: str, pm_price: float, k_price: float,
                                   size: int, pm_direction: str, k_direction: str,
                                   k_ticker: str, pm_token_id: str, 
                                   pm_strike: float, k_strike: float,
                                   expected_profit: float, notes: str = ""):
        """
        Record a trade that needs settlement verification.
        
        For arb trades, we assume $1 payout, but if strikes differ and price lands between,
        both sides could lose. This tracks the trade for later settlement check.
        """
        with self._lock:
            record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "underlying": underlying,
                "size": size,
                "pm_price": pm_price,
                "k_price": k_price,
                "pm_direction": pm_direction,  # "up" or "down"
                "k_direction": k_direction,    # "yes" or "no"
                "k_ticker": k_ticker,
                "pm_token_id": pm_token_id,
                "pm_strike": pm_strike,
                "k_strike": k_strike,
                "total_cost": pm_price * size + k_price * size,
                "expected_profit": expected_profit,
                "actual_profit": None,  # Filled in after settlement
                "settled": False,
                "notes": notes
            }
            self.pending_settlements.append(record)
            self._save_pending_settlements()
    
    async def check_settlements(self, kalshi_connector) -> list:
        """
        Check if any pending settlements have resolved.
        Returns list of settled trades with actual profit/loss.
        """
        if not self.pending_settlements:
            return []
        
        settled_trades = []
        still_pending = []
        
        for trade in self.pending_settlements:
            if trade.get("settled"):
                continue
            
            k_ticker = trade.get("k_ticker")
            if not k_ticker:
                still_pending.append(trade)
                continue
            
            try:
                settlement = await kalshi_connector.get_market_settlement(k_ticker)
                
                if settlement.get("settled"):
                    # Market has settled - calculate actual profit
                    k_result = settlement.get("result")  # "yes" or "no"
                    k_direction = trade.get("k_direction")  # What we bought
                    pm_direction = trade.get("pm_direction")  # What we bought on PM
                    size = trade.get("size", 0)
                    total_cost = trade.get("total_cost", 0)
                    
                    # Determine who won
                    k_won = (k_result == k_direction)
                    
                    # Check if this trade might have landed between strikes
                    # If so, the scanner's check_settlement_with_new_strikes handles P&L adjustment
                    # We just mark it settled here and skip the adjustment to avoid double-counting
                    pm_strike = trade.get("pm_strike", 0)
                    k_strike = trade.get("k_strike", 0)
                    has_strike_diff = pm_strike and k_strike and abs(pm_strike - k_strike) > 0.01
                    
                    if has_strike_diff and not k_won:
                        # K lost but strikes differ - PM might also have lost (between strikes)
                        # Let check_settlement_with_new_strikes handle the P&L adjustment
                        # Just mark settled, don't adjust profit here
                        trade["settled"] = True
                        trade["k_result"] = k_result
                        trade["settlement_time"] = datetime.now(timezone.utc).isoformat()
                        trade["deferred_to_strike_check"] = True
                        settled_trades.append(trade)
                        continue
                    
                    if k_won:
                        # Kalshi paid out $1 per contract
                        payout = size * 1.0
                    else:
                        # Kalshi lost - PM should have won
                        payout = size * 1.0
                    
                    actual_profit = payout - total_cost
                    
                    # Update trade record
                    trade["settled"] = True
                    trade["actual_profit"] = actual_profit
                    trade["k_result"] = k_result
                    trade["settlement_time"] = datetime.now(timezone.utc).isoformat()
                    
                    # Adjust all-time profit if different from expected
                    expected = trade.get("expected_profit", 0)
                    diff = actual_profit - expected
                    if abs(diff) > 0.01:  # Only if meaningful difference
                        self.all_time_profit += Decimal(str(diff))
                        log_print(f"  [Settlement] {trade['underlying']}: Expected ${expected:.2f}, Actual ${actual_profit:.2f} (diff: ${diff:+.2f})")
                        self._save_history()
                    
                    # Update trade journal with settlement outcome
                    outcome_str = "K won" if k_won else "PM won"
                    self.update_journal_settlement(
                        trade_timestamp=trade.get("timestamp", ""),
                        underlying=trade["underlying"],
                        size=size,
                        payout=payout,
                        actual_profit=actual_profit,
                        outcome=outcome_str
                    )
                    
                    settled_trades.append(trade)
                else:
                    still_pending.append(trade)
                    
            except Exception as e:
                still_pending.append(trade)
        
        # Update pending list
        self.pending_settlements = still_pending + [t for t in settled_trades]
        self._save_pending_settlements()
        
        return settled_trades
    
    def _save_history(self, reset_flag=False, fee_correction_flag=False):
        """Save profit history to file"""
        try:
            # Load existing data first to preserve trade log and flags
            existing_trades = []
            existing_flags = {}
            if Path(self.PROFIT_FILE).exists():
                try:
                    with open(self.PROFIT_FILE, "r") as f:
                        data = json.load(f)
                        existing_trades = data.get("trade_log", [])
                        # Preserve all existing flags
                        if data.get("profit_reset_v2_done"):
                            existing_flags["profit_reset_v2_done"] = True
                        if data.get("fee_correction_v1_done"):
                            existing_flags["fee_correction_v1_done"] = True
                except:
                    pass
            
            # Add new trades from this session
            new_trades = []
            for trade in self.session_trades:
                if trade not in existing_trades:  # Avoid duplicates
                    new_trades.append(trade)
            
            all_trades = existing_trades + new_trades
            
            # Keep only last 500 trades in log
            if len(all_trades) > 500:
                all_trades = all_trades[-500:]
            
            data = {
                "updated": datetime.now(timezone.utc).isoformat(),
                "all_time_profit": str(self.all_time_profit),
                "all_time_trades": self.all_time_trades,
                "balance_baseline": self.balance_baseline,
                "trade_log": all_trades
            }
            
            # Apply all flags
            data.update(existing_flags)
            if reset_flag:
                data["profit_reset_v2_done"] = True
            if fee_correction_flag:
                data["fee_correction_v1_done"] = True
            
            with open(self.PROFIT_FILE, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"  [Profit] Save error: {e}")
    
    def calculate_arb_profit(self, pm_price: float, k_price: float, size: int, 
                              pm_direction: str = None, k_direction: str = None) -> dict:
        """
        Calculate profit for an arbitrage trade.
        
        PM fee: feeRateBps/10000 * price * (1-price) per contract (default 200bps)
        K fee: roundup(0.07 * P * (1-P)) per contract - taker fee charged on trade
        """
        import math
        
        # Total cost per contract
        cost_per_contract = pm_price + k_price
        total_cost = cost_per_contract * size
        
        # Payout is guaranteed $1 per contract (one side always wins)
        payout = 1.0 * size
        
        # Gross profit before fees
        gross_profit = payout - total_cost
        
        # PM fee: feeRateBps/10000 * price * (1-price)
        pm_fee_bps = 200  # 2% default for 15-min crypto
        pm_fee_per = (pm_fee_bps / 10000) * pm_price * (1 - pm_price)
        pm_fee = pm_fee_per * size
        
        # Kalshi taker fee: roundup(0.07 * P * (1-P)) per contract
        k_fee_per = math.ceil(0.07 * k_price * (1 - k_price) * 100) / 100
        k_fee = k_fee_per * size
        
        # Total fees
        total_fees = pm_fee + k_fee
        
        # Net profit
        net_profit = gross_profit - total_fees
        
        return {
            "cost_per_contract": cost_per_contract,
            "total_cost": total_cost,
            "payout": payout,
            "gross_profit": gross_profit,
            "pm_fee": pm_fee,
            "k_fee": k_fee,
            "total_fees": total_fees,
            "net_profit": net_profit,
            "net_per_contract": net_profit / size if size > 0 else 0
        }
    
    def record_trade(self, underlying: str, pm_price: float, k_price: float, 
                     size: int, source: str = "arb", notes: str = ""):
        """
        Record a completed trade with profit calculation.
        
        Args:
            underlying: "BTC", "ETH", "SOL"
            pm_price: Fill price on PM
            k_price: Fill price on Kalshi
            size: Number of contracts filled on BOTH sides
            source: "arb", "reconcile", "hedge", etc.
            notes: Additional info
        """
        with self._lock:
            # Calculate profit
            profit_calc = self.calculate_arb_profit(pm_price, k_price, size)
            net_profit = Decimal(str(round(profit_calc["net_profit"], 4)))
            
            # Update totals
            self.session_profit += net_profit
            self.all_time_profit += net_profit
            self.all_time_trades += 1
            
            # Create trade record
            trade_record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "underlying": underlying,
                "size": size,
                "pm_price": pm_price,
                "k_price": k_price,
                "total_cost": profit_calc["total_cost"],
                "gross_profit": profit_calc["gross_profit"],
                "fees": profit_calc["total_fees"],
                "net_profit": float(net_profit),
                "source": source,
                "notes": notes
            }
            self.session_trades.append(trade_record)
            
            # Log to file (per-trade estimate — dashboard shows authoritative balance-based P&L)
            log_print(f"  [Profit] +${net_profit:.2f} ({underlying} x{size}) | Session: ${self.session_profit:.2f} | All-time: ${self.all_time_profit:.2f}")
            
            # Save to file
            self._save_history()
            
            return net_profit
    
    def record_hedge_cost(self, underlying: str, venue: str, price: float, size: int, notes: str = ""):
        """
        Record a hedge order that adds cost without completing an arb.
        This is a COST not profit - used when reconciliation buys more contracts.
        
        The profit will be realized when the market settles.
        We don't count this as profit yet - just track it.
        """
        with self._lock:
            # This is just tracking - profit calculated at settlement
            trade_record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "underlying": underlying,
                "size": size,
                "venue": venue,
                "price": price,
                "cost": price * size,
                "source": "hedge",
                "notes": notes,
                "pending_settlement": True  # Will be resolved at market close
            }
            self.session_trades.append(trade_record)
            
            log_print(f"  [Profit] Hedge recorded: {venue} {underlying} x{size} @ ${price:.2f} (settles at market close)")
    
    def record_settlement_loss(self, underlying: str, expected_profit: float, actual_profit: float, size: int, notes: str = ""):
        """
        Record a loss from a between-strikes settlement.
        The original trade was recorded with expected_profit, but actual outcome was worse.
        Adjusts both session and all-time P&L.
        """
        with self._lock:
            # The original trade already added expected_profit to P&L
            # We need to subtract expected and add actual
            adjustment = Decimal(str(round(actual_profit - expected_profit, 4)))
            
            self.session_profit += adjustment
            self.all_time_profit += adjustment
            
            trade_record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "underlying": underlying,
                "size": size,
                "expected_profit": expected_profit,
                "actual_profit": actual_profit,
                "adjustment": float(adjustment),
                "source": "settlement_loss",
                "notes": notes
            }
            self.session_trades.append(trade_record)
            
            if float(adjustment) >= 0:
                log_print(f"  [Profit] 🎉 SETTLEMENT WIN: {underlying} x{size} | Adjustment: ${adjustment:+.2f} | Session: ${self.session_profit:.2f} | All-time: ${self.all_time_profit:.2f}")
            else:
                log_print(f"  [Profit] ⚠️ SETTLEMENT LOSS: {underlying} x{size} | Adjustment: ${adjustment:.2f} | Session: ${self.session_profit:.2f} | All-time: ${self.all_time_profit:.2f}")
            
            self._save_history()
            
            return adjustment
    
    def get_session_summary(self) -> str:
        """Get session profit summary for display"""
        runtime = (datetime.now(timezone.utc) - self.session_start).total_seconds() / 60
        session_trades = len([t for t in self.session_trades if t.get("source") != "hedge"])
        return f"Session: ${self.session_profit:+.2f} ({session_trades} trades, {runtime:.0f}m)"
    
    def get_alltime_summary(self) -> str:
        """Get all-time profit summary for display"""
        return f"All-time: ${self.all_time_profit:+.2f} ({self.all_time_trades} trades)"
    
    def get_dashboard_line(self) -> str:
        """Get compact profit line for dashboard"""
        return f"${self.session_profit:+.2f} session | ${self.all_time_profit:+.2f} total"
    
    def reset_all(self):
        """Reset all profit tracking to zero"""
        with self._lock:
            old_profit = self.all_time_profit
            old_trades = self.all_time_trades
            
            self.session_profit = Decimal("0")
            self.all_time_profit = Decimal("0")
            self.all_time_trades = 0
            self.session_trades = []
            self.pending_settlements = []
            
            # Save the reset state
            self._save_history()
            
            # Also clear pending settlements file
            try:
                if Path(self.PENDING_FILE).exists():
                    os.remove(self.PENDING_FILE)
            except:
                pass
            
            print(f"  [Profit] ✅ RESET COMPLETE: ${old_profit:.2f} ({old_trades} trades) → $0.00 (0 trades)")
            return True


# Global profit tracker
profit_tracker = ProfitTracker()
    
class SessionTracker:
    """Tracks confirmed trades and calculates session P&L"""
    def __init__(self):
        self.trades: list[ConfirmedTrade] = []
        self.pending_orders: list[dict] = []  # Orders waiting for fill confirmation
        self.start_time = datetime.now(timezone.utc)
    
    def add_pending(self, pm_order: Order, k_order: Order, opp: Opportunity):
        """Add orders that need fill confirmation"""
        self.pending_orders.append({
            "pm_order": pm_order,
            "k_order": k_order,
            "opp": opp,
            "timestamp": datetime.now(timezone.utc),
        })
    
    def confirm_trade(self, pm_fill: dict, k_fill: dict, opp: Opportunity) -> Optional[ConfirmedTrade]:
        """Confirm a trade with actual fill prices"""
        pm_filled = pm_fill.get("filled_count", 0)
        k_filled = k_fill.get("filled_count", 0)
        
        if pm_filled == 0 or k_filled == 0:
            return None
        
        # Use actual fill prices, or submitted price if not available
        pm_fallback = opp.first_leg_price if opp.first_leg_venue == Venue.POLYMARKET else opp.second_leg_price
        k_fallback = opp.second_leg_price if opp.first_leg_venue == Venue.POLYMARKET else opp.first_leg_price
        pm_price = Decimal(str(pm_fill.get("avg_fill_price") or pm_fallback))
        k_price = Decimal(str(k_fill.get("avg_fill_price") or k_fallback))
        
        size = min(int(pm_filled), int(k_filled))
        total_cost = (pm_price + k_price) * size
        payout = Decimal(size)  # $1 per contract guaranteed
        profit = payout - total_cost
        
        trade = ConfirmedTrade(
            timestamp=datetime.now(timezone.utc),
            underlying=opp.market_pair.polymarket.underlying,
            direction=opp.direction,
            size=size,
            pm_fill_price=pm_price,
            k_fill_price=k_price,
            total_cost=total_cost,
            guaranteed_payout=payout,
            expected_profit=profit,
        )
        self.trades.append(trade)
        return trade
    
    @property
    def total_trades(self) -> int:
        return len(self.trades)
    
    @property
    def total_profit(self) -> Decimal:
        return sum(t.expected_profit for t in self.trades) if self.trades else Decimal("0")
    
    @property
    def total_volume(self) -> Decimal:
        return sum(t.total_cost for t in self.trades) if self.trades else Decimal("0")
    
    @property
    def win_rate(self) -> float:
        if not self.trades:
            return 0.0
        wins = sum(1 for t in self.trades if t.expected_profit > 0)
        return wins / len(self.trades) * 100
    
    def summary(self) -> str:
        runtime = (datetime.now(timezone.utc) - self.start_time).total_seconds() / 60
        return (
            f"Trades: {self.total_trades} | "
            f"P&L: ${self.total_profit:.2f} | "
            f"Volume: ${self.total_volume:.2f} | "
            f"Runtime: {runtime:.1f}m"
        )

# Global session tracker
session = SessionTracker()

# =============================================================================
# KALSHI CONNECTOR
# =============================================================================

class KalshiConnector:
    def __init__(self, api_key_id: str, private_key_pem: str, api_url: str):
        self.api_key_id = api_key_id
        self.api_url = api_url
        self._private_key = serialization.load_pem_private_key(
            private_key_pem.encode(), password=None, backend=default_backend()
        )
        self._client = None
        self._ws = None  # WebSocket for real-time data
        self._internal_positions = {}  # ticker -> {"size": X, "direction": "yes"/"no"}
        
        # Latency tracking (like PM)
        self._latency_log = []  # List of {"order_ms": X, "fill_ms": Y, "timestamp": Z}
        self._max_latency_samples = 10
    
    def record_latency(self, order_ms: float = 0, fill_ms: float = 0):
        """Record latency for adaptive behavior"""
        self._latency_log.append({
            "order_ms": order_ms,
            "fill_ms": fill_ms,
            "timestamp": time.time()
        })
        # Keep only recent samples
        if len(self._latency_log) > self._max_latency_samples:
            self._latency_log = self._latency_log[-self._max_latency_samples:]
    
    def get_avg_latency(self) -> dict:
        """Get average latency from recent samples"""
        if not self._latency_log:
            return {"order_ms": 100, "fill_ms": 600}  # Default fast values
        
        order_times = [l["order_ms"] for l in self._latency_log if l["order_ms"] > 0]
        fill_times = [l["fill_ms"] for l in self._latency_log if l["fill_ms"] > 0]
        
        return {
            "order_ms": sum(order_times) / len(order_times) if order_times else 100,
            "fill_ms": sum(fill_times) / len(fill_times) if fill_times else 600
        }
    
    def is_slow(self) -> bool:
        """Check if Kalshi is currently slow (fill > 5s average)"""
        avg = self.get_avg_latency()
        return avg["fill_ms"] > 5000  # 5 seconds is slow
    
    def record_fill(self, ticker: str, size: int, direction: str, window_end: datetime = None):
        """Record a fill internally for position tracking.
        
        Handles offsetting: if you have NO and buy YES, they cancel out.
        """
        opposite = "no" if direction == "yes" else "yes"
        opposite_key = f"{ticker}_{opposite}"
        same_key = f"{ticker}_{direction}"
        
        # Check if we have an opposite position to offset
        if opposite_key in self._internal_positions:
            opposite_pos = self._internal_positions[opposite_key]
            if opposite_pos["size"] > 0:
                # Offset the positions
                offset_amount = min(size, opposite_pos["size"])
                opposite_pos["size"] -= offset_amount
                size -= offset_amount
                print(f"    [Internal] K position offset: {ticker} {opposite.upper()} reduced by {offset_amount} (now {opposite_pos['size']})")
                
                # Remove if fully offset
                if opposite_pos["size"] <= 0:
                    del self._internal_positions[opposite_key]
        
        # Add remaining size to same direction (if any left after offsetting)
        if size > 0:
            current = self._internal_positions.get(same_key, {"size": 0, "direction": direction, "ticker": ticker, "time": time.time(), "window_end": None})
            current["size"] += size
            current["time"] = time.time()
            current["window_end"] = window_end  # Tag with window
            self._internal_positions[same_key] = current
            print(f"    [Internal] K position updated: {ticker} {direction.upper()} = {current['size']}")
    
    def get_internal_positions(self) -> dict:
        """Get internally tracked positions"""
        result = {}
        for key, data in self._internal_positions.items():
            if data["size"] > 0:
                result[data["ticker"]] = {
                    "size": data["size"],
                    "direction": data["direction"],
                    "avg_price": 0,
                    "time": data.get("time", 0)
                }
        return result
    
    def _sign(self, ts: str, method: str, path: str) -> str:
        # Kalshi requires RSA-PSS signature, not PKCS1v15
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
        # Optimized HTTP client with connection pooling
        # Use longer timeout for initial connection, then we'll reduce for ongoing requests
        limits = httpx.Limits(max_keepalive_connections=50, max_connections=100, keepalive_expiry=30)
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=20.0),  # Generous initial timeout
            limits=limits,
            http2=True,  # HTTP/2 for multiplexing
            proxy=None   # BYPASS global proxy - Kalshi doesn't need it, only PM does
        )
        
        # Retry connection up to 3 times silently
        last_error = None
        for attempt in range(3):
            try:
                await self._req("GET", "/portfolio/balance")
                print("  [OK] Kalshi connected (direct, no proxy)")
                # Now reduce timeout for faster ongoing requests
                self._client.timeout = httpx.Timeout(8.0, connect=5.0)
                return
            except Exception as e:
                last_error = e
                if attempt < 2:
                    await asyncio.sleep(2)
        
        # All retries failed
        print(f"  [ERROR] Kalshi connection failed after 3 attempts: {last_error}")
        raise last_error
    
    def start_websocket(self, market_tickers: list):
        """Start WebSocket for real-time orderbook updates"""
        if CONFIG["speed"]["use_websocket"] and market_tickers:
            self._ws = KalshiWebSocket(self.api_key_id, self._private_key)
            self._ws.start(market_tickers)
    
    async def disconnect(self):
        if self._client:
            await self._client.aclose()
        if self._ws:
            self._ws.stop()
    
    async def _req(self, method: str, path: str, params=None, json_data=None) -> dict:
        # Rate limiting - track last request time
        if not hasattr(self, '_last_req_time'):
            self._last_req_time = 0
        
        # Minimum 50ms between requests (20 req/sec max)
        elapsed = time.time() - self._last_req_time
        if elapsed < 0.05:
            await asyncio.sleep(0.05 - elapsed)
        
        # Signature must include /trade-api/v2 prefix
        full_path = f"/trade-api/v2{path}"
        url = f"{self.api_url}{path}"
        
        try:
            r = await self._client.request(method, url, params=params, json=json_data, headers=self._headers(method, full_path))
            self._last_req_time = time.time()
            
            if r.status_code == 429:
                # Rate limited - back off and retry once
                if not hasattr(self, '_rate_limit_warned') or time.time() - self._rate_limit_warned > 60:
                    self._rate_limit_warned = time.time()
                    print(f"\n  [Kalshi] Rate limited - backing off")
                await asyncio.sleep(2)
                r = await self._client.request(method, url, params=params, json=json_data, headers=self._headers(method, full_path))
            
            # Retry on 502/503/504 (server errors) - up to 3 times
            if r.status_code in (502, 503, 504):
                for retry in range(3):
                    await asyncio.sleep(2 * (retry + 1))  # Exponential backoff: 2s, 4s, 6s
                    r = await self._client.request(method, url, params=params, json=json_data, headers=self._headers(method, full_path))
                    if r.status_code < 500:
                        break
            
            if r.status_code >= 400:
                raise Exception(f"Kalshi error {r.status_code}: {r.text}")
            return r.json() if r.text else {}
        except Exception as e:
            self._last_req_time = time.time()
            raise
    
    async def get_markets(self) -> list:
        markets = []
        now = datetime.now(timezone.utc)
        
        # Build series list from config
        cryptos = CONFIG["markets"]["cryptos"]
        series_list = [f"KX{c}15M" for c in cryptos]
        
        max_ahead = CONFIG["markets"]["max_windows_ahead"]
        max_end_time = now + timedelta(minutes=15 * (max_ahead + 1))
        
        min_close_ts = int(now.timestamp())
        max_close_ts = int(max_end_time.timestamp())
        
        # Fetch all series via events endpoint (has strike_date/strike_period + nested markets)
        async def fetch_series(series):
            try:
                # Try events endpoint first - it has strike info at event level
                r = await self._req("GET", "/events", params={
                    "series_ticker": series,
                    "with_nested_markets": "true",
                    "min_close_ts": min_close_ts,
                    "max_close_ts": max_close_ts,
                    "status": "open",
                    "limit": 50
                })
                events = r.get("events", [])
                
                # Flatten: extract markets with event-level strike info attached
                result = []
                for evt in events:
                    event_sub_title = evt.get("sub_title", "")
                    event_strike_date = evt.get("strike_date", "")
                    event_strike_period = evt.get("strike_period", "")
                    event_product_meta = evt.get("product_metadata", {}) or {}
                    
                    for mkt in evt.get("markets", []):
                        # Attach event-level fields to each market dict
                        mkt["_event_sub_title"] = event_sub_title
                        mkt["_event_strike_date"] = event_strike_date
                        mkt["_event_strike_period"] = event_strike_period
                        mkt["_event_product_metadata"] = event_product_meta
                        result.append(mkt)
                
                # If events endpoint returned markets, use them
                if result:
                    return result
                
                # Fallback to markets endpoint if events returned nothing
                r = await self._req("GET", "/markets", params={
                    "series_ticker": series,
                    "status": "open",
                    "min_close_ts": min_close_ts,
                    "max_close_ts": max_close_ts,
                    "limit": 50
                })
                return r.get("markets", [])
            except Exception as e:
                # If events endpoint fails, fall back to markets endpoint
                try:
                    r = await self._req("GET", "/markets", params={
                        "series_ticker": series,
                        "status": "open",
                        "min_close_ts": min_close_ts,
                        "max_close_ts": max_close_ts,
                        "limit": 50
                    })
                    return r.get("markets", [])
                except Exception as e2:
                    print(f"  [Debug] Kalshi {series} error: {e2}")
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
                    yp = Decimal(str(d.get("yes_bid", 50))) / 100
                    
                    # Extract strike price - check multiple sources
                    strike_price = None
                    is_tbd = False
                    
                    # 1. Check subtitle/yes_sub_title for "Price to beat: $XX,XXX.XX"
                    import re
                    strike_from_prev_market = False
                    for field in ["subtitle", "yes_sub_title", "no_sub_title", "_event_sub_title"]:
                        text = d.get(field, "")
                        if text and "TBD" in text.upper():
                            is_tbd = True
                        if text and "$" in text:
                            match = re.search(r'\$?([\d,]+\.?\d*)', text.replace(',', ''))
                            if match:
                                try:
                                    strike_price = float(match.group(1))
                                    break
                                except:
                                    pass
                    
                    # 2. Check event-level product_metadata for strike info
                    if not strike_price:
                        meta = d.get("_event_product_metadata", {}) or {}
                        for key in ["strike_price", "strike", "reference_price", "price_to_beat"]:
                            val = meta.get(key)
                            if val:
                                try:
                                    strike_price = float(str(val).replace(',', '').replace('$', ''))
                                    break
                                except:
                                    pass
                    
                    # 3. Check numeric fields on the market
                    if not strike_price:
                        for field in ["floor_strike", "cap_strike", "strike_price", "strike", 
                                      "custom_strike", "settlement_value"]:
                            val = d.get(field)
                            if val and isinstance(val, (int, float)) and val > 0:
                                strike_price = float(val)
                                break
                    
                    # 4. If still no strike, get expiration_value from the PREVIOUS window's market
                    #    The "price to beat" = CF Benchmarks average at previous window close
                    if not strike_price:
                        try:
                            ticker = d.get("ticker", "")
                            # Parse ticker: KXBTC15M-26FEB011345-45
                            # Previous window closed 15 min earlier
                            prev_start = start - timedelta(minutes=15)
                            prev_close = start  # Previous window closes when current opens
                            
                            # Build previous ticker from the close time in EST
                            from zoneinfo import ZoneInfo
                            prev_close_est = prev_close.astimezone(ZoneInfo("America/New_York"))
                            prev_hhmm = prev_close_est.strftime("%H%M")  # e.g. "1330"
                            prev_mm = prev_close_est.strftime("%M")      # e.g. "30"
                            date_part = prev_close_est.strftime("%y%b%d").upper()  # e.g. "26FEB01"
                            
                            prev_ticker = f"KX{underlying}15M-{date_part}{prev_hhmm}-{prev_mm}"
                            
                            # Cache to avoid re-fetching same previous market
                            if not hasattr(self, '_prev_market_cache'):
                                self._prev_market_cache = {}
                            
                            if prev_ticker in self._prev_market_cache:
                                strike_price = self._prev_market_cache[prev_ticker]
                                if strike_price:
                                    strike_from_prev_market = True
                            else:
                                prev_data = await self._req("GET", f"/markets/{prev_ticker}")
                                if prev_data and "market" in prev_data:
                                    pm = prev_data["market"]
                                    exp_val = pm.get("expiration_value", "")
                                    if exp_val:
                                        # expiration_value might be a string like "78180.13"
                                        try:
                                            strike_price = float(str(exp_val).replace(',', '').replace('$', ''))
                                            self._prev_market_cache[prev_ticker] = strike_price
                                            strike_from_prev_market = True
                                            
                                            if not hasattr(self, '_prev_strike_logged'):
                                                self._prev_strike_logged = set()
                                            if underlying not in self._prev_strike_logged:
                                                print(f"  [K] {underlying} strike from prev market ({prev_ticker}): ${strike_price:,.2f}")
                                                self._prev_strike_logged.add(underlying)
                                        except:
                                            pass
                                    else:
                                        # Previous market not settled yet - DON'T cache None
                                        # so we retry on next pair refresh (10s for assumed mode)
                                        pass
                        except Exception as e:
                            pass
                    
                    # Log event-level data once per asset if strike still missing
                    if not strike_price and is_tbd:
                        if not hasattr(self, '_kalshi_event_logged'):
                            self._kalshi_event_logged = set()
                        if underlying not in self._kalshi_event_logged:
                            evt_sub = d.get("_event_sub_title", "")
                            evt_strike = d.get("_event_strike_date", "")
                            evt_period = d.get("_event_strike_period", "")
                            evt_meta = d.get("_event_product_metadata", {})
                            if evt_sub or evt_strike or evt_period or evt_meta:
                                print(f"  [K Event] {underlying}: sub='{evt_sub}' strike_date='{evt_strike}' period='{evt_period}' meta={evt_meta}")
                            self._kalshi_event_logged.add(underlying)
                    
                    markets.append(Market(
                        venue=Venue.KALSHI, market_id=d["ticker"], title=d.get("title", ""),
                        underlying=underlying, direction=Direction.UP, interval_minutes=15,
                        start_time=start, end_time=end, yes_price=yp, no_price=Decimal("1")-yp,
                        strike_price=strike_price, strike_uncertain=strike_from_prev_market
                    ))
                except Exception as e:
                    pass
        
        if not markets:
            # Debug: try without filters to see what's available
            try:
                r = await self._req("GET", "/markets", params={
                    "series_ticker": series_list[0],
                    "limit": 5
                })
                sample = r.get("markets", [])
                if sample:
                    d = sample[0]
                    status = d.get('status', 'unknown')
                    if status == 'initialized':
                        print(f"  ⏳ New markets loading (status: initialized) - waiting...")
                    else:
                        print(f"  [Debug] Sample Kalshi market: status={status}, close_time={d.get('close_time')}")
            except:
                pass
        
        markets.sort(key=lambda m: m.end_time)
        return markets
    
    async def get_all_markets(self, max_minutes_to_close: int = 60) -> list:
        """Fetch ALL open markets closing within max_minutes_to_close.
        
        Returns raw market data for cross-venue matching.
        """
        markets = []
        now = datetime.now(timezone.utc)
        
        min_close_ts = int(now.timestamp())
        max_close_ts = int((now + timedelta(minutes=max_minutes_to_close)).timestamp())
        
        cursor = None
        fetched = 0
        
        while fetched < 1000:  # Safety limit
            try:
                params = {
                    "status": "open",
                    "min_close_ts": min_close_ts,
                    "max_close_ts": max_close_ts,
                    "limit": 100
                }
                if cursor:
                    params["cursor"] = cursor
                    
                r = await self._req("GET", "/markets", params=params)
                batch = r.get("markets", [])
                
                if not batch:
                    break
                    
                for d in batch:
                    try:
                        close = d.get("close_time") or d.get("expiration_time")
                        open_time = d.get("open_time") or d.get("created_time")
                        if not close:
                            continue
                        
                        end = datetime.fromisoformat(close.replace("Z", "+00:00"))
                        start = datetime.fromisoformat(open_time.replace("Z", "+00:00")) if open_time else end - timedelta(hours=24)
                        
                        duration_minutes = int((end - start).total_seconds() / 60)
                        yp = Decimal(str(d.get("yes_bid", 50))) / 100
                        
                        markets.append({
                            "venue": "kalshi",
                            "market_id": d["ticker"],
                            "event_ticker": d.get("event_ticker", ""),
                            "title": d.get("title", ""),
                            "subtitle": d.get("subtitle", ""),
                            "category": d.get("category", ""),
                            "start_time": start,
                            "end_time": end,
                            "duration_minutes": duration_minutes,
                            "yes_price": yp,
                            "no_price": Decimal("1") - yp,
                            "volume": d.get("volume", 0),
                            "raw": d
                        })
                    except Exception as e:
                        pass
                
                fetched += len(batch)
                cursor = r.get("cursor")
                if not cursor:
                    break
                    
            except Exception as e:
                print(f"  [Kalshi] Error fetching markets: {e}")
                break
        
        return markets
    
    async def get_orderbook(self, market_id: str) -> Orderbook:
        # Cache orderbooks for 2 seconds to reduce API calls
        cache_key = f"ob_{market_id}"
        if hasattr(self, '_ob_cache') and cache_key in self._ob_cache:
            cache_time, cached_ob = self._ob_cache[cache_key]
            if time.time() - cache_time < 2:
                return cached_ob
        
        # For now, just use REST API - Kalshi WS needs more work
        try:
            r = await self._req("GET", f"/markets/{market_id}/orderbook")
            d = r.get("orderbook", {})
            yes_bids_data = d.get("yes") or []
            no_bids_data = d.get("no") or []
            
            # Kalshi returns BIDS only - [price_cents, quantity]
            # YES bids: highest price first
            yes_bids = [OrderbookLevel(Decimal(str(l[0]))/100, Decimal(str(l[1]))) for l in yes_bids_data if len(l)>=2]
            yes_bids.sort(key=lambda x: x.price, reverse=True)
            
            # NO bids at price P = YES asks at price (1-P)
            # Convert and sort: lowest ask first
            yes_asks = [OrderbookLevel(Decimal("1") - Decimal(str(l[0]))/100, Decimal(str(l[1]))) for l in no_bids_data if len(l)>=2]
            yes_asks.sort(key=lambda x: x.price)
            
            ob = Orderbook(Venue.KALSHI, market_id, yes_bids, yes_asks)
            
            # Cache it
            if not hasattr(self, '_ob_cache'):
                self._ob_cache = {}
            self._ob_cache[cache_key] = (time.time(), ob)
            
            return ob
        except Exception as e:
            # Return empty orderbook on error
            return Orderbook(Venue.KALSHI, market_id)
    
    async def get_depth_at_price(self, market_id: str, direction: Direction, max_price: float) -> int:
        """
        Get total contracts available up to max_price.
        For buying YES: sum quantities where ask price <= max_price
        For buying NO: sum quantities where (1-bid) <= max_price, i.e. bid >= (1-max_price)
        Returns total contracts available.
        """
        try:
            # Use cached orderbook if available and fresh
            cache_key = f"ob_{market_id}"
            if hasattr(self, '_ob_cache') and cache_key in self._ob_cache:
                cache_time, cached_ob = self._ob_cache[cache_key]
                if time.time() - cache_time < 5:  # Use cache if < 5s old
                    # Calculate depth from cached orderbook
                    total_depth = 0
                    if direction == Direction.UP:
                        # Sum YES asks where price <= max_price
                        for level in cached_ob.yes_asks:
                            if float(level.price) <= max_price:
                                total_depth += int(level.size)
                    else:
                        # Sum NO asks (from yes_bids converted)
                        for level in cached_ob.yes_bids:
                            no_price = 1.0 - float(level.price)
                            if no_price <= max_price:
                                total_depth += int(level.size)
                    return total_depth
            
            # Fallback to API call
            r = await self._req("GET", f"/markets/{market_id}/orderbook")
            d = r.get("orderbook", {})
            
            total_depth = 0
            
            if direction == Direction.UP:
                # Buying YES - need to look at NO bids (which become YES asks)
                no_bids = d.get("no") or []
                for level in no_bids:
                    if len(level) >= 2:
                        price_cents, qty = level[0], level[1]
                        # NO bid at X cents = YES ask at (100-X) cents
                        yes_ask_price = (100 - price_cents) / 100.0
                        if yes_ask_price <= max_price:
                            total_depth += qty
            else:
                # Buying NO - need to look at YES bids (which become NO asks)
                yes_bids = d.get("yes") or []
                for level in yes_bids:
                    if len(level) >= 2:
                        price_cents, qty = level[0], level[1]
                        # YES bid at X cents = NO ask at (100-X) cents
                        no_ask_price = (100 - price_cents) / 100.0
                        if no_ask_price <= max_price:
                            total_depth += qty
            
            return total_depth
        except Exception as e:
            log_print(f"      [Depth] Kalshi depth check error: {e}")
            return 0  # Return 0 on error (will skip trade)
    
    async def get_balance(self) -> Balance:
        r = await self._req("GET", "/portfolio/balance")
        cash = Decimal(str(r.get("balance", 0))) / 100
        # Kalshi also has portfolio_value in the response
        portfolio = Decimal(str(r.get("portfolio_value", 0))) / 100
        return Balance(Venue.KALSHI, cash, portfolio)
    
    async def place_order(self, market_id: str, side: OrderSide, direction: Direction, price: Decimal, size: Decimal) -> Order:
        ks = "yes" if direction == Direction.UP else "no"
        act = "buy" if side == OrderSide.BUY else "sell"
        data = {"ticker": market_id, "action": act, "side": ks, "type": "limit", "count": int(size), f"{ks}_price": int(price*100)}
        r = await self._req("POST", "/portfolio/orders", json_data=data)
        return Order(r.get("order",{}).get("order_id",str(uuid.uuid4())), Venue.KALSHI, market_id, side, direction, price, size, OrderStatus.SUBMITTED)
    
    async def place_market_order(self, market_id: str, side: OrderSide, direction: Direction, size: int, max_price: float = None) -> Order:
        """
        Place a market-like order on Kalshi.
        Kalshi doesn't support native market orders, so we use a limit order.
        If max_price is provided, use it as the ceiling instead of $0.95.
        This prevents catastrophic fills in thin/empty orderbooks.
        """
        ks = "yes" if direction == Direction.UP else "no"
        act = "buy" if side == OrderSide.BUY else "sell"
        
        if side == OrderSide.BUY:
            if max_price is not None:
                # Use the provided max price as ceiling (capped at 92¢)
                price_cents = min(int(round(max_price * 100)), 92)
                price_cents = max(price_cents, 1)  # Minimum 1¢
            else:
                price_cents = 95  # Legacy fallback
        else:
            price_cents = 5
        
        data = {
            "ticker": market_id,
            "action": act,
            "side": ks,
            "type": "limit",
            "count": int(size),
            f"{ks}_price": price_cents,
        }
        log_print(f"  [K MARKET ORDER] {act} {ks} x{size} on {market_id} @ ${price_cents/100:.2f} ceiling")
        r = await self._req("POST", "/portfolio/orders", json_data=data)
        order_data = r.get("order", {})
        order_id = order_data.get("order_id", str(uuid.uuid4()))
        # Get the fill price from response
        fill_price = None
        if ks == "yes" and order_data.get("yes_price"):
            fill_price = Decimal(str(order_data["yes_price"])) / 100
        elif ks == "no" and order_data.get("no_price"):
            fill_price = Decimal(str(order_data["no_price"])) / 100
        return Order(order_id, Venue.KALSHI, market_id, side, direction, fill_price or Decimal(str(price_cents/100)), Decimal(str(size)), OrderStatus.SUBMITTED)
    
    async def get_order(self, order_id: str) -> dict:
        """Get order status and fill information"""
        try:
            r = await self._req("GET", f"/portfolio/orders/{order_id}")
            order = r.get("order", {})
            
            fill_count = order.get("fill_count", 0)
            avg_price = None
            
            if fill_count and fill_count > 0:
                # ═══════════════════════════════════════════════════════════
                # CRITICAL: Do NOT use yes_price/no_price from the order!
                # Those are the LIMIT prices we set, not the execution prices.
                # For market orders at $0.95 ceiling, those always return 95.
                # Instead, query the fills endpoint for actual execution prices.
                # ═══════════════════════════════════════════════════════════
                order_side = order.get("side", "")  # "yes" or "no"
                
                try:
                    fills = await self.get_fills_for_order(order_id)
                    if fills:
                        total_cost = 0
                        total_count = 0
                        for f in fills:
                            count = f.get("count", 0)
                            # Try side-specific price first, then generic "price"
                            if order_side == "no":
                                price = f.get("no_price", 0) or f.get("price", 0)
                            elif order_side == "yes":
                                price = f.get("yes_price", 0) or f.get("price", 0)
                            else:
                                price = f.get("price", 0) or f.get("no_price", 0) or f.get("yes_price", 0)
                            
                            if count > 0 and price > 0:
                                total_cost += price * count
                                total_count += count
                        
                        if total_count > 0:
                            avg_price = (total_cost / total_count) / 100  # Convert cents to dollars
                except Exception as e:
                    pass  # Fall through to legacy method
                
                # Legacy fallback: read from order fields if fills endpoint failed
                # WARNING: These are likely LIMIT prices, not fill prices for market orders
                if avg_price is None:
                    yes_price = order.get("yes_price", 0)
                    no_price = order.get("no_price", 0)
                    
                    if order_side == "no" and no_price and no_price > 0:
                        avg_price = no_price / 100
                    elif order_side == "yes" and yes_price and yes_price > 0:
                        avg_price = yes_price / 100
                    elif no_price and no_price > 0:
                        avg_price = min(no_price, yes_price) / 100 if yes_price else no_price / 100
                    elif yes_price and yes_price > 0:
                        avg_price = yes_price / 100
                
                # Sanity check
                if avg_price is not None and not (0.01 <= avg_price <= 0.99):
                    avg_price = None
            
            return {
                "order_id": order_id,
                "status": order.get("status", "unknown"),
                "filled_count": fill_count,
                "remaining_count": order.get("remaining_count", 0),
                "initial_count": order.get("initial_count", 0),
                "avg_fill_price": avg_price,
            }
        except Exception as e:
            # If order not found, check fills to see if it completed
            if "404" in str(e) or "not_found" in str(e):
                fills = await self.get_fills_for_order(order_id)
                if fills:
                    total_filled = sum(f.get("count", 0) for f in fills)
                    avg_price = sum(f.get("price", 0) * f.get("count", 0) for f in fills) / max(total_filled, 1) / 100
                    return {
                        "order_id": order_id,
                        "status": "filled",
                        "filled_count": total_filled,
                        "remaining_count": 0,
                        "avg_fill_price": avg_price,
                    }
            return {"order_id": order_id, "status": "error", "error": str(e), "filled_count": 0}
    
    async def get_fills_for_order(self, order_id: str) -> list:
        """Get fills for a specific order"""
        try:
            r = await self._req("GET", f"/portfolio/fills?order_id={order_id}")
            return r.get("fills", [])
        except Exception as e:
            return []
    
    async def get_market_settlement(self, ticker: str) -> dict:
        """
        Get settlement result for a market.
        Returns: {"settled": bool, "result": "yes"/"no"/None, "settle_price": float/None}
        """
        try:
            r = await self._req("GET", f"/markets/{ticker}")
            market = r.get("market", r)  # API might return {"market": {...}} or just {...}
            
            status = market.get("status", "")
            result = market.get("result", "")  # "yes" or "no" for settled markets
            
            # Check if settled
            if status == "settled" or result in ["yes", "no"]:
                return {
                    "settled": True,
                    "result": result,  # "yes" or "no"
                    "settle_price": market.get("settlement_value", market.get("settle_price")),
                }
            else:
                return {
                    "settled": False,
                    "result": None,
                    "settle_price": None,
                }
        except Exception as e:
            return {"settled": False, "result": None, "settle_price": None, "error": str(e)}
    
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an unfilled order"""
        try:
            await self._req("DELETE", f"/portfolio/orders/{order_id}")
            return True
        except Exception as e:
            # 404 means order already gone (filled, cancelled, or expired) - that's fine
            if "404" in str(e) or "not_found" in str(e):
                return True
            print(f"      K cancel error: {e}")
            return False
    
    async def amend_order(self, order_id: str, ticker: str, direction: Direction, new_price: Decimal, 
                          client_order_id: str = None) -> dict:
        """
        Amend an existing order's price without cancelling.
        This is atomic - no race condition between cancel and new order.
        
        Returns: {"success": bool, "order_id": str, "new_price": float, "error": str or None}
        """
        try:
            side = "yes" if direction == Direction.UP else "no"
            price_field = f"{side}_price"
            
            # Generate new client_order_id if not provided
            if not client_order_id:
                client_order_id = str(uuid.uuid4())
            new_client_order_id = str(uuid.uuid4())
            
            data = {
                "ticker": ticker,
                "side": side,
                "action": "buy",
                "client_order_id": client_order_id,
                "updated_client_order_id": new_client_order_id,
                price_field: int(new_price * 100),  # Price in cents
            }
            
            r = await self._req("POST", f"/portfolio/orders/{order_id}/amend", json_data=data)
            
            amended_order = r.get("order", {})
            return {
                "success": True,
                "order_id": amended_order.get("order_id", order_id),
                "new_price": float(new_price),
                "client_order_id": new_client_order_id,
                "error": None
            }
        except Exception as e:
            error_str = str(e)
            # If order already filled, that's actually good news
            if "filled" in error_str.lower() or "cannot_update_filled" in error_str.lower():
                return {
                    "success": False,
                    "order_id": order_id,
                    "new_price": None,
                    "error": "already_filled"
                }
            # If order not found, it may have been cancelled or filled
            if "404" in error_str or "not_found" in error_str.lower():
                return {
                    "success": False,
                    "order_id": order_id,
                    "new_price": None,
                    "error": "not_found"
                }
            return {
                "success": False,
                "order_id": order_id,
                "new_price": None,
                "error": error_str
            }
    
    async def get_open_orders(self) -> list:
        """Get all open/resting orders"""
        try:
            r = await self._req("GET", "/portfolio/orders?status=resting")
            return r.get("orders", [])
        except Exception as e:
            # Try without status filter
            try:
                r = await self._req("GET", "/portfolio/orders")
                orders = r.get("orders", [])
                # Filter to only open/resting orders
                return [o for o in orders if o.get("status") in ("resting", "pending", "open")]
            except:
                return []
    
    async def cancel_all_orders(self) -> int:
        """Cancel all open orders on Kalshi. Safe to call anytime."""
        cancelled = 0
        try:
            orders = await self.get_open_orders()
            if not orders:
                print(f"  [K] No open orders to cancel")
                return 0
            
            print(f"  [K] Found {len(orders)} open order(s), cancelling...")
            for order in orders:
                order_id = order.get("order_id")
                if order_id:
                    try:
                        await self.cancel_order(order_id)
                        cancelled += 1
                        print(f"    ✓ Cancelled K order {order_id[:16]}...")
                    except Exception as e:
                        print(f"    ✗ Failed to cancel {order_id[:16]}: {e}")
            
            return cancelled
        except Exception as e:
            print(f"  [K] Error cancelling orders: {e}")
            return cancelled
    
    async def get_positions(self) -> dict:
        """Get current positions on Kalshi
        Returns: {ticker: {"size": X, "direction": "yes"/"no", "avg_price": Y}}
        """
        positions = {}
        try:
            r = await self._req("GET", "/portfolio/positions")
            for pos in r.get("market_positions", []):
                ticker = pos.get("ticker", "")
                # Positive = yes position, negative = no position
                position_count = pos.get("position", 0)
                if position_count != 0:
                    positions[ticker] = {
                        "size": abs(position_count),
                        "direction": "yes" if position_count > 0 else "no",
                        "avg_price": pos.get("average_price", 0) / 100 if pos.get("average_price") else 0
                    }
        except Exception as e:
            pass
        
        # Clean up internal positions not in API response
        # Instead of checking settlement (expensive), just remove if API doesn't have it
        # The API is the source of truth for what positions actually exist
        stale_tickers = [t for t in self._internal_positions if t not in positions]
        for ticker in stale_tickers:
            # Only remove if the position is old (> 20 minutes = past settlement)
            pos_data = self._internal_positions.get(ticker, {})
            pos_time = pos_data.get("time", 0)
            if time.time() - pos_time > 1200:  # 20 minutes
                del self._internal_positions[ticker]
                logger.log_text(f"[Internal] K position {ticker} stale - removing from tracking")
        
        # Merge with internal tracking (internal takes precedence for current session)
        internal = self.get_internal_positions()
        for ticker, data in internal.items():
            if ticker not in positions:
                positions[ticker] = data
                logger.log_text(f"[DEBUG] Using internal K position: {ticker} = {data}")
        
        return positions
    
    async def get_position_for_ticker(self, ticker: str) -> tuple:
        """Get position for a specific ticker
        Returns: (size, direction) where direction is "yes" or "no" or None
        """
        positions = await self.get_positions()
        pos = positions.get(ticker, {})
        if pos:
            return (pos.get("size", 0), pos.get("direction"))
        return (0, None)

# =============================================================================
# POLYMARKET CONNECTOR (using official py-clob-client)
# =============================================================================

class PolymarketConnector:
    # USDC.e (bridged) on Polygon - this is what Polymarket uses
    USDC = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
    
    def __init__(self, private_key: str, funder_address: str, clob_url: str, gamma_url: str, rpc_url: str):
        from py_clob_client.client import ClobClient
        
        self.clob_url = clob_url
        self.gamma_url = gamma_url
        self._private_key = private_key
        self._funder = funder_address
        self._w3 = Web3(Web3.HTTPProvider(rpc_url))
        self._cache = {}
        self._gamma = None
        self._ws = None  # WebSocket for real-time orderbooks
        self._internal_positions = {}  # Track positions internally {token_id: size}
        self._latency_log = []  # Track API latencies for diagnostics
        self._fill_latency_log = []  # Track fill times for step interval tuning
        self._max_fill_samples = 10
        
        # Initialize py-clob-client with signature_type=2 for MetaMask/Browser wallet
        # signature_type=0: Direct EOA 
        # signature_type=1: Magic/Email wallet
        # signature_type=2: Browser wallet (MetaMask) with Gnosis Safe proxy
        # The funder is your Polymarket profile address where USDC is held
        self._client = ClobClient(
            host=clob_url,
            key=private_key,
            chain_id=137,
            signature_type=2,  # Browser wallet (MetaMask) uses Safe proxy
            funder=funder_address
        )
        
        # Store the signing address (derived from private key)
        self.address = self._client.get_address()
        
        # Order failure tracking for safety system
        self._last_order_failed = False
        self._last_order_error = ""
    
    async def connect(self):
        # Optimized HTTP client with connection pooling for high performance
        # Use longer timeout for initial connection, then reduce for ongoing requests
        limits = httpx.Limits(max_keepalive_connections=50, max_connections=100, keepalive_expiry=30)
        
        # Build headers
        headers = {}
        
        self._gamma = httpx.AsyncClient(
            base_url=self.gamma_url,
            timeout=httpx.Timeout(15.0, connect=10.0),  # Initial connection needs more time
            limits=limits,
            http2=True,
            headers=headers
        )
        
        # Start RTDS for Chainlink prices (used for strike prices)
        self._rtds = get_rtds_client()
        
        # Create or derive API credentials
        try:
            self._client.set_api_creds(self._client.create_or_derive_api_creds())
            print(f"  [OK] Polymarket connected (signer: {self.address[:6]}...{self.address[-4:]})")
            print(f"       Funder: {self._funder[:6]}...{self._funder[-4:]}")
            
            # Now reduce timeout for faster ongoing requests
            self._gamma.timeout = httpx.Timeout(8.0, connect=5.0)
        except Exception as e:
            print(f"  [WARN] Polymarket auth issue: {e}")
            print(f"         Will work for reading, but trading may fail")
    
    def start_websocket(self, markets: list):
        """Start WebSocket for real-time orderbook updates"""
        if not CONFIG["speed"]["use_websocket"]:
            return
        
        # Collect all token IDs (both YES and NO) and create mapping
        token_ids = []
        token_to_market = {}
        for m in markets:
            if m.token_id:
                token_ids.append(m.token_id)
                token_to_market[m.token_id] = m.market_id
            if m.no_token_id:
                token_ids.append(m.no_token_id)
                token_to_market[m.no_token_id] = m.market_id
        
        if token_ids:
            self._ws = PolymarketWebSocket()
            self._ws.start(token_ids, token_to_market)
            print(f"  [WS] Started streaming {len(token_ids)} orderbooks")
    
    async def disconnect(self):
        if self._gamma: await self._gamma.aclose()
        if self._ws: self._ws.stop()
    
    async def get_markets(self) -> list:
        markets = []
        now = datetime.now(timezone.utc)
        
        # Use config for which cryptos and windows to scan
        cryptos = [c.lower() for c in CONFIG["markets"]["cryptos"]]
        max_ahead = CONFIG["markets"]["max_windows_ahead"]
        
        # Slug-based discovery for LIVE markets
        current_minute = now.minute
        current_slot_minute = (current_minute // 15) * 15
        slot_start = now.replace(minute=current_slot_minute, second=0, microsecond=0)
        
        # Build all slugs to fetch
        slugs_to_fetch = []
        for crypto in cryptos:
            for offset in range(0, max_ahead + 1):
                window_start = slot_start + timedelta(minutes=15 * offset)
                ts = int(window_start.timestamp())
                slugs_to_fetch.append((crypto, f"{crypto}-updown-15m-{ts}"))
        
        # Fetch all in parallel for speed (with rate limit handling)
        async def fetch_slug(crypto, slug):
            try:
                r = await self._gamma.get("/markets", params={"slug": slug})
                if r.status_code == 200:
                    data = r.json()
                    if data:
                        return (crypto, data if isinstance(data, list) else [data])
                elif r.status_code == 429:
                    # Rate limited - back off
                    if not hasattr(self, '_rate_limit_warned') or time.time() - self._rate_limit_warned > 60:
                        self._rate_limit_warned = time.time()
                        print(f"\n  [PM API] Rate limited - backing off")
                    await asyncio.sleep(2)  # Back off 2 seconds
                elif r.status_code != 404:
                    # Log non-404 errors (404 just means market doesn't exist yet)
                    if not hasattr(self, '_gamma_error_logged'):
                        self._gamma_error_logged = True
                        print(f"\n  [PM API] Error {r.status_code} fetching {slug}")
            except Exception as e:
                if not hasattr(self, '_gamma_error_logged'):
                    self._gamma_error_logged = True
                    print(f"\n  [PM API] Exception: {type(e).__name__}: {e}")
            return (crypto, [])
        
        # Don't fetch too frequently - cache results for 5 seconds
        cache_key = f"markets_{slot_start.isoformat()}"
        if hasattr(self, '_market_cache') and cache_key in self._market_cache:
            cache_time, cached_markets = self._market_cache[cache_key]
            if time.time() - cache_time < 5:  # 5 second cache
                return cached_markets
        
        results = await asyncio.gather(*[fetch_slug(c, s) for c, s in slugs_to_fetch])
        
        for crypto, market_list in results:
            for md in market_list:
                m = self._parse(md, crypto.upper())
                if m and m.market_id not in [x.market_id for x in markets]:
                    markets.append(m)
        
        markets.sort(key=lambda m: m.end_time)
        
        # Cache the results
        if not hasattr(self, '_market_cache'):
            self._market_cache = {}
        self._market_cache[cache_key] = (time.time(), markets)
        
        return markets
    
    def _parse(self, d: dict, underlying: str) -> Optional[Market]:
        try:
            cid = d.get("conditionId") or d.get("condition_id")
            
            # Token ID extraction - need BOTH yes and no tokens
            yes_tid = None
            no_tid = None
            
            # First try direct token fields
            yes_tid = d.get("tokenId") or d.get("token_id")
            
            # Try tokens array FIRST - it has outcome field to reliably determine YES vs NO
            if not yes_tid:
                tokens = d.get("tokens", [])
                for token in tokens:
                    outcome = token.get("outcome", "").lower()
                    tid = token.get("token_id") or token.get("tokenId")
                    if tid:
                        if outcome == "yes":
                            yes_tid = tid
                        elif outcome == "no":
                            no_tid = tid
            
            # Fallback to clobTokenIds - assumes [YES_token, NO_token] order
            if not yes_tid:
                clob_ids_raw = d.get("clobTokenIds") or d.get("clob_token_ids")
                if clob_ids_raw:
                    clob_ids = None
                    if isinstance(clob_ids_raw, str):
                        try:
                            clob_ids = json.loads(clob_ids_raw)
                        except:
                            pass
                    elif isinstance(clob_ids_raw, list):
                        clob_ids = clob_ids_raw
                    
                    if clob_ids and len(clob_ids) >= 1:
                        yes_tid = clob_ids[0]
                    if clob_ids and len(clob_ids) >= 2:
                        no_tid = clob_ids[1]
            
            if not cid: 
                return None
                
            title = d.get("question","") or d.get("title","")
            description = d.get("description", "") or d.get("details", "")
            dir = Direction.UP if "up" in title.lower() else Direction.DOWN
            
            # Extract strike price - for crypto up/down markets
            strike_price = None
            
            # Try 'line' field first - this is commonly used for strike/spread in sports/crypto markets
            line_val = d.get("line")
            if line_val and line_val != 0:
                try:
                    strike_price = float(line_val)
                except:
                    pass
            
            # Try groupItemThreshold 
            if not strike_price:
                threshold = d.get("groupItemThreshold")
                if threshold and threshold != "0" and threshold != 0:
                    try:
                        strike_price = float(str(threshold).replace(',', '').replace('$', ''))
                    except:
                        pass
            
            # For crypto up/down markets, get strike from strikes.json file
            # (created by strike_collector.py running in background)
            # NO FALLBACK - collector is required for accurate strikes
            if not strike_price:
                symbol_map = {"BTC": "BTC", "ETH": "ETH", "SOL": "SOL"}
                symbol = symbol_map.get(underlying)
                if symbol:
                    # Get the market's end time and calculate start as the 15-min boundary
                    end_time_str = d.get('endDate') or d.get('endDateIso')
                    
                    if end_time_str:
                        try:
                            end_dt = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
                            # Start time is exactly 15 min before end, rounded to boundary
                            # Market ending at 23:15 started at 23:00
                            start_time = end_dt - timedelta(minutes=15)
                            # Round to 15-min boundary (should already be, but ensure it)
                            start_minute = (start_time.minute // 15) * 15
                            start_time = start_time.replace(minute=start_minute, second=0, microsecond=0)
                            
                            # Read from strikes.json file (from strike_collector.py)
                            strike_price, confidence = get_strike_from_file(symbol, start_time)
                            
                            # Only use HIGH or MED confidence strikes
                            if strike_price and confidence in ["HIGH", "MED"]:
                                pass  # Good strike, use it
                            else:
                                strike_price = None  # Reject LOW or missing
                        except Exception as e:
                            pass
            
            ets = d.get("endDate") or d.get("end_date_iso") or d.get("endDateIso")
            if not ets:
                return None
            
            end = datetime.fromisoformat(ets.replace("Z","+00:00"))
            
            now = datetime.now(timezone.utc)
            if end <= now:
                return None
            
            start = end - timedelta(minutes=15)
            ps = d.get("outcomePrices", [])
            
            if isinstance(ps, str):
                try:
                    ps = json.loads(ps)
                except:
                    ps = []
            
            yp = Decimal("0.5")
            if ps:
                try:
                    yp = Decimal(str(ps[0])) if isinstance(ps[0], (int, float, str)) else Decimal("0.5")
                except:
                    pass
            
            m = Market(Venue.POLYMARKET, cid, title, underlying, dir, 15, start, end, yp, Decimal("1")-yp, yes_tid, no_tid, strike_price)
            self._cache[cid] = m
            return m
        except Exception as e:
            return None
    
    async def get_all_markets(self, max_minutes_to_close: int = 60) -> list:
        """Fetch ALL active markets closing within max_minutes_to_close.
        
        Returns raw market data for cross-venue matching.
        """
        markets = []
        now = datetime.now(timezone.utc)
        
        total_markets = 0
        
        try:
            # Use the /markets endpoint directly with date filters
            # This is more efficient than fetching all events
            params = {
                "active": "true",
                "closed": "false",
                "end_date_min": now.isoformat(),  # End date must be in the future
                "end_date_max": (now + timedelta(minutes=max_minutes_to_close)).isoformat(),
                "limit": 100
            }
            
            r = await self._gamma.get("/markets", params=params)
            if r.status_code != 200:
                print(f"     [PM Debug] Markets API returned {r.status_code}")
            else:
                data = r.json()
                market_list = data if isinstance(data, list) else data.get("markets", [])
                print(f"     [PM Debug] Markets with end_date filter: {len(market_list)}")
                
                for m in market_list:
                    total_markets += 1
                    try:
                        end_str = m.get("endDate") or m.get("endDateIso")
                        if not end_str:
                            continue
                        end = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                        
                        start_str = m.get("startDate") or m.get("startDateIso") or m.get("eventStartTime")
                        start = datetime.fromisoformat(start_str.replace("Z", "+00:00")) if start_str else end - timedelta(hours=1)
                        
                        duration_minutes = int((end - start).total_seconds() / 60)
                        time_remaining = (end - now).total_seconds() / 60
                        
                        # Get token IDs
                        yes_tid = None
                        no_tid = None
                        clob_ids_raw = m.get("clobTokenIds") or m.get("clob_token_ids")
                        if clob_ids_raw:
                            clob_ids = json.loads(clob_ids_raw) if isinstance(clob_ids_raw, str) else clob_ids_raw
                            if clob_ids and len(clob_ids) >= 1:
                                yes_tid = clob_ids[0]
                            if clob_ids and len(clob_ids) >= 2:
                                no_tid = clob_ids[1]
                        
                        # Get prices
                        ps = m.get("outcomePrices", [])
                        if isinstance(ps, str):
                            try:
                                ps = json.loads(ps)
                            except:
                                ps = []
                        
                        yp = Decimal(str(ps[0])) if ps else Decimal("0.5")
                        
                        title = m.get("question", "") or m.get("title", "")
                        
                        markets.append({
                            "venue": "polymarket",
                            "market_id": m.get("conditionId") or m.get("condition_id"),
                            "slug": m.get("slug", ""),
                            "event_title": "",  # Not available from /markets directly
                            "title": title,
                            "description": m.get("description", ""),
                            "category": m.get("category", ""),
                            "start_time": start,
                            "end_time": end,
                            "duration_minutes": duration_minutes,
                            "time_remaining_minutes": time_remaining,
                            "yes_price": yp,
                            "no_price": Decimal("1") - yp,
                            "yes_token_id": yes_tid,
                            "no_token_id": no_tid,
                            "volume": m.get("volume", 0),
                            "raw": m
                        })
                        
                    except Exception as e:
                        pass
            
            # Also try fetching by eventStartTime (for mentions/speech markets)
            # These have an event that starts soon even if endDate is later
            event_params = {
                "active": "true",
                "closed": "false",
                "limit": 100,
                "order": "eventStartTime",
                "ascending": "true"
            }
            
            r2 = await self._gamma.get("/markets", params=event_params)
            if r2.status_code == 200:
                data2 = r2.json()
                market_list2 = data2 if isinstance(data2, list) else data2.get("markets", [])
                
                event_markets_count = 0
                for m in market_list2:
                    try:
                        # Check if event starts soon
                        event_start_str = m.get("eventStartTime") or m.get("gameStartTime")
                        if not event_start_str:
                            continue
                        
                        event_start = datetime.fromisoformat(event_start_str.replace("Z", "+00:00"))
                        
                        # Only include if event starts within our window
                        minutes_until_event = (event_start - now).total_seconds() / 60
                        if minutes_until_event < 0 or minutes_until_event > max_minutes_to_close:
                            continue
                        
                        # Check if we already have this market
                        market_id = m.get("conditionId") or m.get("condition_id")
                        if any(x["market_id"] == market_id for x in markets):
                            continue
                        
                        event_markets_count += 1
                        
                        end_str = m.get("endDate") or m.get("endDateIso")
                        end = datetime.fromisoformat(end_str.replace("Z", "+00:00")) if end_str else event_start + timedelta(hours=2)
                        
                        time_remaining = (end - now).total_seconds() / 60
                        duration_minutes = int((end - event_start).total_seconds() / 60)
                        
                        # Get token IDs
                        yes_tid = None
                        no_tid = None
                        clob_ids_raw = m.get("clobTokenIds") or m.get("clob_token_ids")
                        if clob_ids_raw:
                            clob_ids = json.loads(clob_ids_raw) if isinstance(clob_ids_raw, str) else clob_ids_raw
                            if clob_ids and len(clob_ids) >= 1:
                                yes_tid = clob_ids[0]
                            if clob_ids and len(clob_ids) >= 2:
                                no_tid = clob_ids[1]
                        
                        ps = m.get("outcomePrices", [])
                        if isinstance(ps, str):
                            try:
                                ps = json.loads(ps)
                            except:
                                ps = []
                        
                        yp = Decimal(str(ps[0])) if ps else Decimal("0.5")
                        title = m.get("question", "") or m.get("title", "")
                        
                        markets.append({
                            "venue": "polymarket",
                            "market_id": market_id,
                            "slug": m.get("slug", ""),
                            "event_title": "",
                            "title": title,
                            "description": m.get("description", ""),
                            "category": m.get("category", ""),
                            "start_time": event_start,
                            "end_time": end,
                            "duration_minutes": duration_minutes,
                            "time_remaining_minutes": time_remaining,
                            "event_start": event_start,
                            "minutes_until_event": minutes_until_event,
                            "yes_price": yp,
                            "no_price": Decimal("1") - yp,
                            "yes_token_id": yes_tid,
                            "no_token_id": no_tid,
                            "volume": m.get("volume", 0),
                            "raw": m
                        })
                        
                    except Exception as e:
                        pass
                
                print(f"     [PM Debug] Markets with upcoming events: {event_markets_count}")
            
            print(f"     [PM Debug] Total PM markets found: {len(markets)}")
            
            # Show closest markets for debugging
            if markets:
                markets.sort(key=lambda x: x["time_remaining_minutes"])
                print(f"     [PM Debug] Closest PM markets:")
                for m in markets[:5]:
                    time_rem = m["time_remaining_minutes"]
                    if time_rem < 60:
                        time_str = f"{time_rem:.0f}min"
                    elif time_rem < 1440:
                        time_str = f"{time_rem/60:.1f}hr"
                    else:
                        time_str = f"{time_rem/1440:.0f}days"
                    print(f"        [{time_str}] {m['title'][:50]}")
            
            return markets
                    
        except Exception as e:
            print(f"  [PM] Error fetching all markets: {e}")
            import traceback
            traceback.print_exc()
        
        return markets
    
    async def get_orderbook(self, market_id: str) -> Orderbook:
        m = self._cache.get(market_id)
        if not m or not m.token_id:
            return Orderbook(Venue.POLYMARKET, market_id)
        
        # Try WebSocket cache first (instant, no network call)
        if self._ws and CONFIG["speed"]["use_websocket"]:
            ws_book = self._ws.get_orderbook(m.token_id)
            if ws_book and (ws_book.get("bids") or ws_book.get("asks")):
                bids_data = ws_book.get("bids", [])
                asks_data = ws_book.get("asks", [])
                
                # WebSocket data can be dicts {"price": x, "size": y} or lists [price, size]
                yes_bids = []
                yes_asks = []
                
                for l in bids_data:
                    try:
                        if isinstance(l, dict):
                            yes_bids.append(OrderbookLevel(Decimal(str(l.get("price", 0))), Decimal(str(l.get("size", 0)))))
                        elif isinstance(l, (list, tuple)) and len(l) >= 2:
                            yes_bids.append(OrderbookLevel(Decimal(str(l[0])), Decimal(str(l[1]))))
                    except:
                        pass
                
                for l in asks_data:
                    try:
                        if isinstance(l, dict):
                            yes_asks.append(OrderbookLevel(Decimal(str(l.get("price", 0))), Decimal(str(l.get("size", 0)))))
                        elif isinstance(l, (list, tuple)) and len(l) >= 2:
                            yes_asks.append(OrderbookLevel(Decimal(str(l[0])), Decimal(str(l[1]))))
                    except:
                        pass
                
                yes_bids.sort(key=lambda x: x.price, reverse=True)
                yes_asks.sort(key=lambda x: x.price)
                
                return Orderbook(Venue.POLYMARKET, market_id, yes_bids, yes_asks)
        
        # Fallback to REST API
        try:
            book = self._client.get_order_book(m.token_id)
            
            bids_data = book.bids if hasattr(book, 'bids') else []
            asks_data = book.asks if hasattr(book, 'asks') else []
            
            yes_bids = [OrderbookLevel(Decimal(str(l.price)), Decimal(str(l.size))) for l in bids_data]
            yes_asks = [OrderbookLevel(Decimal(str(l.price)), Decimal(str(l.size))) for l in asks_data]
            
            yes_bids.sort(key=lambda x: x.price, reverse=True)
            yes_asks.sort(key=lambda x: x.price)
            
            return Orderbook(Venue.POLYMARKET, market_id, yes_bids, yes_asks)
        except Exception as e:
            # Fallback to direct HTTP request
            async with httpx.AsyncClient() as client:
                r = await client.get(f"{self.clob_url}/book", params={"token_id": m.token_id})
                if r.status_code != 200:
                    return Orderbook(Venue.POLYMARKET, market_id)
                
                d = r.json()
                bids_data = d.get("bids") or []
                asks_data = d.get("asks") or []
                
                yes_bids = [OrderbookLevel(Decimal(str(l.get("price",0))), Decimal(str(l.get("size",0)))) for l in bids_data]
                yes_asks = [OrderbookLevel(Decimal(str(l.get("price",0))), Decimal(str(l.get("size",0)))) for l in asks_data]
                
                yes_bids.sort(key=lambda x: x.price, reverse=True)
                yes_asks.sort(key=lambda x: x.price)
                
                return Orderbook(Venue.POLYMARKET, market_id, yes_bids, yes_asks)
    
    async def get_balance(self) -> Balance:
        try:
            # Get USDC cash balance
            abi = [{"constant":True,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"type":"function"}]
            c = self._w3.eth.contract(address=self.USDC, abi=abi)
            # Check balance at the FUNDER address (where USDC actually is)
            b = c.functions.balanceOf(self._funder).call()
            cash = Decimal(str(b)) / Decimal("1000000")
            
            # Cache the successful balance read
            self._last_known_balance = cash
            
            # Try to get portfolio value from CLOB API
            portfolio = Decimal("0")
            try:
                # Get open positions - this may not work depending on API
                positions = self._client.get_positions() if hasattr(self._client, 'get_positions') else []
                for pos in positions:
                    # Estimate position value (size * current price)
                    size = Decimal(str(pos.get("size", 0)))
                    price = Decimal(str(pos.get("avgPrice", 0.5)))
                    portfolio += size * price
            except:
                pass  # Portfolio estimation is optional
            
            return Balance(Venue.POLYMARKET, cash, portfolio)
        except Exception as e:
            # On error, return last known balance instead of 0
            if hasattr(self, '_last_known_balance') and self._last_known_balance > 0:
                print(f"  [PM] Balance check failed, using cached: ${self._last_known_balance:.2f}")
                return Balance(Venue.POLYMARKET, self._last_known_balance, Decimal("0"))
            # Only return 0 if we've never had a successful read
            return Balance(Venue.POLYMARKET, Decimal("0"), Decimal("0"))
    
    async def get_depth_at_price(self, market_id: str, direction: Direction, max_price: float) -> int:
        """
        Get total contracts available up to max_price.
        Uses WebSocket cache for speed, falls back to REST.
        Returns total contracts available.
        """
        try:
            ob = await self.get_orderbook(market_id)
            total_depth = 0
            
            if direction == Direction.UP:
                # Buying YES - look at asks
                for level in ob.yes_asks:
                    if float(level.price) <= max_price:
                        total_depth += int(level.quantity)
            else:
                # Buying NO - need to look at NO token orderbook
                # Get NO token orderbook
                m = self._cache.get(market_id)
                if m and m.no_token_id and self._ws:
                    ws_book = self._ws.get_orderbook(m.no_token_id)
                    if ws_book:
                        asks_data = ws_book.get("asks", [])
                        for l in asks_data:
                            try:
                                if isinstance(l, dict):
                                    price = float(l.get("price", 0))
                                    size = int(float(l.get("size", 0)))
                                elif isinstance(l, (list, tuple)) and len(l) >= 2:
                                    price = float(l[0])
                                    size = int(float(l[1]))
                                else:
                                    continue
                                if price <= max_price:
                                    total_depth += size
                            except:
                                pass
            
            return total_depth
        except Exception as e:
            return 0  # Return 0 on error
    
    async def place_order(self, market_id: str, side: OrderSide, direction: Direction, price: Decimal, size: Decimal, is_reorder: bool = False) -> Order:
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY, SELL
        import time as _time
        
        # LOG EVERY ORDER ATTEMPT for debugging phantom orders
        log_print(f"  [PM ORDER] {side.value} {direction.value} x{size} @ ${price:.2f}{' (reorder)' if is_reorder else ''}")
        
        # DEDUP CHECK: Prevent duplicate orders on same market within 3 seconds
        # This catches race conditions where two scan loops fire simultaneously
        # Reorders (from stepping logic) bypass this check since they cancelled the previous order
        if not is_reorder:
            if not hasattr(self, '_recent_pm_orders'):
                self._recent_pm_orders = {}
            
            dedup_key = f"{market_id}:{side.value}:{direction.value}"
            now = _time.time()
            last_order_time = self._recent_pm_orders.get(dedup_key, 0)
            if now - last_order_time < 3.0:
                log_print(f"  [PM ORDER] ❌ DEDUP BLOCKED: Same order placed {now - last_order_time:.1f}s ago")
                return None
            self._recent_pm_orders[dedup_key] = now
        
        m = self._cache.get(market_id)
        if not m:
            raise Exception(f"Market {market_id} not found")
        
        # Select correct token based on direction
        if direction == Direction.UP:
            token_id = m.token_id  # YES token
            if not token_id:
                raise Exception(f"Market {market_id} has no YES token")
        else:
            token_id = m.no_token_id  # NO token
            if not token_id:
                raise Exception(f"Market {market_id} has no NO token")
        
        try:
            order_side = BUY if side == OrderSide.BUY else SELL
            
            order_args = OrderArgs(
                token_id=token_id,
                price=float(price),
                size=float(size),
                side=order_side
            )
            
            # Rate limit PM requests to avoid Cloudflare blocks
            if not hasattr(self, '_last_pm_order_time'):
                self._last_pm_order_time = 0
            
            elapsed = _time.time() - self._last_pm_order_time
            if elapsed < 1.0:  # Minimum 1 second between PM orders
                await asyncio.sleep(1.0 - elapsed)
            
            # Capture balance before order for phantom fill detection
            try:
                self._pre_order_balance = await self.get_balance()
            except:
                self._pre_order_balance = None
            
            # Retry up to 3 times on network errors
            last_error = None
            for attempt in range(3):
                try:
                    start_time = _time.time()
                    
                    # Run signing in thread pool to avoid blocking event loop
                    # This allows Kalshi orders to be placed in parallel
                    signed_order = await asyncio.to_thread(self._client.create_order, order_args)
                    sign_time = _time.time() - start_time
                    
                    # Extract fee info from signed order (client fetches this automatically)
                    pm_fee_bps = None
                    
                    # Debug: log what the signed_order actually is
                    log_print(f"      [DEBUG] signed_order type: {type(signed_order).__name__}")
                    if hasattr(signed_order, '__dict__'):
                        log_print(f"      [DEBUG] signed_order attrs: {list(signed_order.__dict__.keys())[:10]}")
                    if isinstance(signed_order, dict):
                        log_print(f"      [DEBUG] signed_order keys: {list(signed_order.keys())[:10]}")
                    
                    # Try various ways to get feeRateBps
                    if hasattr(signed_order, 'feeRateBps'):
                        pm_fee_bps = signed_order.feeRateBps
                    elif hasattr(signed_order, 'fee_rate_bps'):
                        pm_fee_bps = signed_order.fee_rate_bps
                    elif isinstance(signed_order, dict):
                        pm_fee_bps = signed_order.get('feeRateBps') or signed_order.get('fee_rate_bps')
                    # Try nested in 'order' key
                    elif hasattr(signed_order, 'order') and isinstance(signed_order.order, dict):
                        pm_fee_bps = signed_order.order.get('feeRateBps')
                    
                    if pm_fee_bps is not None:
                        # feeRateBps is basis points (100 = 1%)
                        pm_fee_pct = float(pm_fee_bps) / 10000
                        # Actual fee = price * (1-price) * feeRateBps / 10000
                        actual_pm_fee = float(price) * (1 - float(price)) * pm_fee_pct
                        log_print(f"      [PM Fee] {pm_fee_bps}bps = ${actual_pm_fee:.4f}/contract @ ${price:.2f}")
                        # Cache the fee rate for this token
                        if not hasattr(self, '_token_fee_rates'):
                            self._token_fee_rates = {}
                        self._token_fee_rates[token_id] = pm_fee_bps
                    
                    post_start = _time.time()
                    # Also run post in thread pool
                    resp = await asyncio.to_thread(self._client.post_order, signed_order, OrderType.GTC)
                    post_time = _time.time() - post_start
                    
                    self._last_pm_order_time = _time.time()  # Update last order time
                    total_time = _time.time() - start_time
                    
                    # Log latency
                    self._latency_log.append({
                        "op": "place_order",
                        "sign_ms": int(sign_time * 1000),
                        "post_ms": int(post_time * 1000),
                        "total_ms": int(total_time * 1000),
                        "attempt": attempt + 1
                    })
                    
                    # Keep only last 20 entries
                    if len(self._latency_log) > 20:
                        self._latency_log = self._latency_log[-20:]
                    
                    # Print latency for debugging
                    log_print(f"      [Latency] PM order: sign={int(sign_time*1000)}ms post={int(post_time*1000)}ms total={int(total_time*1000)}ms")
                    
                    order_id = resp.get("orderID") or resp.get("order_id") or str(uuid.uuid4())
                    
                    if not resp.get("success"):
                        error_msg = resp.get("errorMsg") or resp.get("error") or resp.get("message") or str(resp)
                        log_print(f"      PM order rejected: {error_msg}")
                        return Order(order_id, Venue.POLYMARKET, market_id, side, direction, price, size, OrderStatus.REJECTED)
                    
                    # Clear any previous Cloudflare block flag on success
                    self._cloudflare_blocked = False
                    return Order(order_id, Venue.POLYMARKET, market_id, side, direction, price, size, OrderStatus.SUBMITTED)
                except Exception as e:
                    last_error = e
                    error_str = str(e)
                    
                    # Detect Cloudflare block - this is CRITICAL
                    if "403" in error_str and ("cloudflare" in error_str.lower() or "blocked" in error_str.lower() or "Attention Required" in error_str):
                        log_print(f"\n  🚨 CLOUDFLARE BLOCK DETECTED - PM API is blocked!")
                        log_print(f"  🚨 PAUSING BOT - Change VPN server or wait before retrying")
                        self._cloudflare_blocked = True
                        self._cloudflare_block_time = time.time()
                        # Don't retry on Cloudflare blocks
                        return Order(str(uuid.uuid4()), Venue.POLYMARKET, market_id, side, direction, price, size, OrderStatus.REJECTED)
                    
                    if attempt < 2:
                        log_print(f"      PM order attempt {attempt+1} failed: {e}, retrying...")
                        await asyncio.sleep(2.0)  # Longer delay between retries
                    continue
            
            # All retries failed - BUT the order might have actually gone through!
            # "Request exception" often means the order succeeded but response failed
            log_print(f"      PM order error: {last_error}")
            
            # CRITICAL: Check if our balance changed - this indicates the order went through
            # even though we got an error response
            try:
                await asyncio.sleep(1.0)  # Brief wait for blockchain to settle
                post_balance = await self.get_balance()
                if hasattr(self, '_pre_order_balance'):
                    balance_change = float(self._pre_order_balance.cash) - float(post_balance.cash)
                    expected_cost = float(price) * float(size)
                    
                    # If balance dropped by approximately the order cost, the order went through!
                    if balance_change > expected_cost * 0.8:  # Allow 20% variance for fees
                        log_print(f"      ⚠️ PHANTOM FILL DETECTED: Balance dropped ${balance_change:.2f} (expected ~${expected_cost:.2f})")
                        log_print(f"      Order likely went through despite error - marking as success!")
                        
                        # Generate a pseudo order ID (we don't have the real one)
                        phantom_order_id = f"PHANTOM_{uuid.uuid4()}"
                        
                        # Track failure for safety system - but this is actually a hidden success
                        self._last_order_failed = False
                        self._phantom_fill_detected = True
                        self._phantom_fill_size = int(size)
                        
                        return Order(phantom_order_id, Venue.POLYMARKET, market_id, side, direction, price, size, OrderStatus.SUBMITTED)
            except Exception as balance_check_err:
                log_print(f"      Balance check failed: {balance_check_err}")
            
            # Track failure for safety system
            self._last_order_failed = True
            self._last_order_error = str(last_error)
            return Order(str(uuid.uuid4()), Venue.POLYMARKET, market_id, side, direction, price, size, OrderStatus.REJECTED)
        except Exception as e:
            log_print(f"      PM order error: {e}")
            self._last_order_failed = True
            self._last_order_error = str(e)
            return Order(str(uuid.uuid4()), Venue.POLYMARKET, market_id, side, direction, price, size, OrderStatus.REJECTED)
    
    async def place_market_order(self, market_id: str, side: OrderSide, direction: Direction, size: int) -> Order:
        """
        Place a market order on Polymarket.
        Since PM doesn't have native market orders, we place a limit order at 95¢ (buy) or 5¢ (sell).
        The limit price is just a ceiling/floor - we'll get filled at the best available price.
        """
        if side == OrderSide.BUY:
            price = Decimal("0.95")  # Max we're willing to pay
        else:
            price = Decimal("0.05")  # Min we're willing to receive
        
        log_print(f"  [PM MARKET ORDER] {side.value} {direction.value} x{size} @ ${price:.2f} ceiling")
        return await self.place_order(market_id, side, direction, price, Decimal(str(size)))
    
    def is_cloudflare_blocked(self) -> bool:
        """Check if we're currently Cloudflare blocked"""
        if not hasattr(self, '_cloudflare_blocked'):
            return False
        if not self._cloudflare_blocked:
            return False
        # Auto-clear after 5 minutes
        if hasattr(self, '_cloudflare_block_time') and time.time() - self._cloudflare_block_time > 300:
            self._cloudflare_blocked = False
            return False
        return True
    
    async def get_order(self, order_id: str) -> dict:
        """Get order status and fill information"""
        import time as _time
        try:
            start_time = _time.time()
            # Polymarket CLOB client get_order method
            resp = self._client.get_order(order_id)
            latency = int((_time.time() - start_time) * 1000)
            
            # Only log very slow get_order calls (>2s) - normal polls are silent
            if latency > 2000:
                log_print(f"      [Latency] PM get_order: {latency}ms (SLOW)")
            
            if resp:
                size_matched = float(resp.get("size_matched", 0) or 0)
                original_size = float(resp.get("original_size", 0) or 0)
                limit_price = float(resp.get("price", 0) or 0)
                # Note: PM doesn't return actual fill price in get_order
                # We get price improvement but can't report it accurately
                # associate_trades field or separate trades API would be needed
                # For now, return None so caller knows we don't have real fill price
                return {
                    "order_id": order_id,
                    "status": resp.get("status", "unknown"),
                    "filled_count": size_matched,
                    "size_matched": size_matched,
                    "remaining_count": original_size - size_matched,
                    "avg_fill_price": None,  # PM doesn't expose this in get_order
                    "limit_price": limit_price,  # The price we submitted, NOT fill price
                }
            return {"order_id": order_id, "status": "unknown"}
        except Exception as e:
            return {"order_id": order_id, "status": "error", "error": str(e)}
    
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an unfilled order"""
        try:
            self._client.cancel(order_id)
            return True
        except Exception as e:
            # Order already gone (filled, cancelled, expired) - that's fine
            err_str = str(e).lower()
            if "not found" in err_str or "not_found" in err_str or "404" in err_str:
                return True
            print(f"      PM cancel error: {e}")
            return False
    
    def get_fee_rate_bps(self, token_id: str = None) -> int:
        """
        Get the fee rate in basis points for a token.
        Returns cached value if available, otherwise default estimate.
        Fee formula: fee = feeRateBps/10000 * price * (1-price)
        """
        if token_id and hasattr(self, '_token_fee_rates') and token_id in self._token_fee_rates:
            return self._token_fee_rates[token_id]
        # Default for 15-min crypto markets (typically 200bps = 2% max at 50/50)
        # This is an estimate - actual rate logged when orders are placed
        return 200  # 2% max fee rate
    
    def calculate_pm_fee(self, price: float, token_id: str = None) -> float:
        """Calculate PM fee per contract at given price"""
        fee_bps = self.get_fee_rate_bps(token_id)
        return (fee_bps / 10000) * price * (1 - price)
    
    async def get_trade_fills(self, order_id: str) -> list:
        """
        Get actual fill prices for an order from the trades endpoint.
        Returns list of fills: [{"price": 0.XX, "size": N}, ...]
        PM doesn't return fill prices in get_order, so we need this separate call.
        """
        try:
            # Try to get trades for this order
            if hasattr(self._client, 'get_trades'):
                trades = self._client.get_trades(order_id=order_id)
                if trades:
                    return [
                        {"price": float(t.get("price", 0)), "size": float(t.get("size", 0))}
                        for t in trades
                        if t.get("size", 0) > 0
                    ]
        except Exception as e:
            # This endpoint may not exist or may require different params
            pass  # Silently fail - we'll use estimated price
        return []
    
    async def get_positions(self) -> dict:
        """Get current positions on Polymarket
        Returns: {token_id: {"size": X, "avg_price": Y}}
        """
        positions = {}
        
        # Method 1: Try py-clob-client's get_positions (usually doesn't work for browser wallets)
        try:
            if hasattr(self._client, 'get_positions'):
                pos_list = self._client.get_positions()
                if pos_list:
                    logger.log_text(f"[DEBUG] PM API positions: {len(pos_list)} found")
                    for pos in pos_list or []:
                        token_id = pos.get("asset") or pos.get("token_id") or pos.get("market")
                        size = float(pos.get("size", 0) or pos.get("position", 0) or 0)
                        avg_price = float(pos.get("avgPrice", 0) or pos.get("average_price", 0) or 0)
                        if token_id and size > 0:
                            positions[token_id] = {"size": size, "avg_price": avg_price}
        except Exception as e:
            logger.log_text(f"[DEBUG] PM get_positions client error: {e}")
        
        # Method 2: Try Polymarket Data API (https://data-api.polymarket.com/positions?user=ADDRESS)
        if not positions:
            try:
                import httpx
                # Use the funder address (proxy wallet that holds funds)
                url = "https://data-api.polymarket.com/positions"
                params = {"user": self._funder, "sizeThreshold": 0}
                
                async with httpx.AsyncClient(timeout=5.0) as client:
                    r = await client.get(url, params=params)
                    if r.status_code == 200:
                        data = r.json()
                        for pos in data if isinstance(data, list) else []:
                            # Data API returns 'asset' as the token_id
                            token_id = pos.get("asset")
                            size = float(pos.get("size", 0))
                            avg_price = float(pos.get("avgPrice", 0) or 0)
                            if token_id and size > 0:
                                positions[token_id] = {
                                    "size": size, 
                                    "avg_price": avg_price,
                                    "title": pos.get("title", ""),
                                    "outcome": pos.get("outcome", "")
                                }
                        if positions:
                            logger.log_text(f"[DEBUG] PM Data API positions: {len(positions)} found")
                    else:
                        logger.log_text(f"[DEBUG] PM Data API HTTP {r.status_code}")
            except Exception as e:
                logger.log_text(f"[DEBUG] PM Data API error: {e}")
        
        # Method 3: Merge with internal tracking (internal tracking for very recent trades)
        for token_id, size in self._internal_positions.items():
            if size > 0:
                if token_id not in positions:
                    positions[token_id] = {"size": size, "avg_price": 0}
                    logger.log_text(f"[DEBUG] PM internal position: {token_id} = {size}")
        
        return positions
    
    def record_fill(self, token_id: str, size: float, underlying: str = None, direction: str = None, window_end: datetime = None):
        """Record a fill internally for position tracking"""
        current = self._internal_positions.get(token_id, 0)
        self._internal_positions[token_id] = current + size
        
        # Also track metadata if provided
        if underlying and direction:
            if not hasattr(self, '_position_metadata'):
                self._position_metadata = {}
            self._position_metadata[token_id] = {
                "underlying": underlying, 
                "direction": direction,
                "window_end": window_end  # Track which window this position belongs to
            }
        
        # Log to file only (console print breaks dashboard)
        logger.log_text(f"[Internal] PM position: {token_id[:20]}... = {self._internal_positions[token_id]}")
    
    def get_internal_positions_with_metadata(self) -> dict:
        """Get internal positions with underlying/direction info"""
        result = {}
        if not hasattr(self, '_position_metadata'):
            self._position_metadata = {}
        
        for token_id, size in self._internal_positions.items():
            if size > 0:
                meta = self._position_metadata.get(token_id, {})
                result[token_id] = {
                    "size": size,
                    "underlying": meta.get("underlying"),
                    "direction": meta.get("direction"),
                    "window_end": meta.get("window_end"),  # Include window info
                    "avg_price": 0
                }
        return result
    
    def clear_position(self, token_id: str):
        """Clear internal position (e.g., after market settles)"""
        if token_id in self._internal_positions:
            del self._internal_positions[token_id]
    
    def record_fill_latency(self, fill_ms: float):
        """Record how long a PM fill took for step interval tuning"""
        self._fill_latency_log.append({"fill_ms": fill_ms, "timestamp": time.time()})
        if len(self._fill_latency_log) > self._max_fill_samples:
            self._fill_latency_log = self._fill_latency_log[-self._max_fill_samples:]
    
    def get_avg_fill_latency(self) -> float:
        """Get average PM fill time in ms. Returns 0 if no data."""
        if not self._fill_latency_log:
            return 0
        fill_times = [l["fill_ms"] for l in self._fill_latency_log if l["fill_ms"] > 0]
        return sum(fill_times) / len(fill_times) if fill_times else 0
    
    async def get_position_for_token(self, token_id: str) -> float:
        """Get position size for a specific token"""
        positions = await self.get_positions()
        pos = positions.get(token_id, {})
        return pos.get("size", 0)

    async def check_and_cancel_open_orders(self) -> int:
        """Check for open orders and cancel them. Returns count of cancelled orders."""
        try:
            # Get open orders from the CLOB client
            open_orders = self._client.get_orders()
            if not open_orders:
                print(f"  [PM] No open orders found")
                return 0
            
            print(f"  [PM] Found {len(open_orders)} order(s), checking status...")
            cancelled = 0
            for order in open_orders:
                order_id = order.get("id") or order.get("order_id") or order.get("orderID")
                if order_id:
                    status = order.get("status", "").lower()
                    size = order.get("original_size") or order.get("size") or "?"
                    filled = order.get("size_matched") or order.get("filled") or 0
                    print(f"    Order {order_id[:20]}... status={status} size={size} filled={filled}")
                    
                    # Cancel ANY order that's not explicitly filled/cancelled/matched
                    if status not in ["filled", "cancelled", "matched", "expired"]:
                        try:
                            self._client.cancel(order_id)
                            print(f"    ✓ Cancelled order {order_id[:20]}...")
                            cancelled += 1
                        except Exception as e:
                            print(f"    ✗ Failed to cancel {order_id[:20]}: {e}")
            
            return cancelled
        except Exception as e:
            # get_orders might not exist or fail - that's ok
            print(f"  [Startup] Could not check open orders: {e}")
            return 0
    
    async def cancel_all_orders(self) -> int:
        """Aggressively cancel ALL orders. Call on shutdown or emergency."""
        try:
            open_orders = self._client.get_orders()
            if not open_orders:
                return 0
            
            cancelled = 0
            for order in open_orders:
                order_id = order.get("id") or order.get("order_id") or order.get("orderID")
                if order_id:
                    try:
                        self._client.cancel(order_id)
                        cancelled += 1
                    except:
                        pass
            
            if cancelled > 0:
                print(f"  [PM] Emergency cancelled {cancelled} order(s)")
            return cancelled
        except:
            return 0

# =============================================================================
# SCANNER LOGIC - MOMENTUM + DELAYED ARB STRATEGY
# =============================================================================

class Scanner:
    def __init__(self, pm: PolymarketConnector, kalshi: KalshiConnector):
        self.pm = pm
        self.kalshi = kalshi
        self._last_trade = None
        self._last_trade_attempt = None  # Only set when orders are actually sent
        self._traded_segments = set()
        self._orderbook_cache = {}
        self._last_edges = []
        self._active_position: Optional[ActivePosition] = None  # Track single-leg position
        self._pm_balance: Optional[Balance] = None  # Track balances for capital protection
        self._k_balance: Optional[Balance] = None
        self._reconciliation_in_progress = False  # Prevent trades during reconciliation
        self._has_orphaned_positions = False  # Track unhedged positions
        self._startup_time = datetime.now(timezone.utc)  # Track when scanner started
        self._trading_enabled = False  # MUST be explicitly enabled after startup checks
        
        # Recent edges history for dashboard display (last 3 significant edges)
        self._recent_edges_history = []  # List of {"time": datetime, "asset": str, "raw_edge": float, "net_edge": float, "score": float}
        
        # Window tracking for order clearing
        self._current_window_end = None  # Track which window we're in
        self._orders_cleared_for_window = False  # Only clear once per window transition
        
        # Trade count per underlying per window - for position limit enforcement
        # Resets on window transition
        self._window_trade_counts = {}  # {"BTC": 2, "ETH": 1, ...}
        self._window_contract_counts = {}  # {"BTC": 48, "ETH": 24, ...} total contracts this window
        
        # Late asymmetric trade counter - separate from main arb
        self._late_asymmetric_trade_counts = {}  # {"BTC": 1, ...}
        
        # =======================================================================
        # SAFETY KILL SWITCH - Pause trading after repeated errors
        # =======================================================================
        self._safety_paused = False           # True = ALL trading halted
        self._safety_pause_reason = ""        # Why we paused
        self._safety_pause_time = 0           # When the pause started
        self._safety_pause_window = None      # Which window caused the pause
        self._consecutive_pm_errors = 0       # PM API errors in a row
        self._consecutive_k_errors = 0        # K API errors in a row
        self._max_consecutive_errors = 2      # Pause after this many
        self._unhedged_position = None        # Track if we have unbalanced exposure
        # Format: {"underlying": "BTC", "venue": "PM"/"K", "size": 21, "direction": "up"}
        
        # CUMULATIVE position tracker - does NOT reset on window transition
        # Tracks total contracts held across all unsettled windows
        # Only decrements when positions actually settle (verified via exchange API)
        # Format: {"BTC": {"size": 28, "direction": "bearish"}, "ETH": {...}}
        self._cumulative_positions = {}
        
        # RTDS reference — points to the same client as PM connector
        # Used for current price lookups and realized volatility in the scan loop
        self._rtds = getattr(pm, '_rtds', None) or get_rtds_client()
        
        # Execution lock - prevents double trades from race conditions
        self._executing = False
        
        # Pending PM fill tracker - for fills that may not have loaded yet
        self._pending_pm_fills = []  # List of {"order_id": str, "market_id": str, "size": int, "time": float, "underlying": str, "direction": str}
        
        # Settlement tracking - trades from previous window awaiting settlement check
        # When new window starts, we use its strike prices as settlement prices for previous window
        self._trades_awaiting_settlement = []  # List of trade records from current window
        self._previous_window_trades = []  # Trades from last window, waiting for settlement prices
        self._previous_window_strikes = {}  # {"BTC": {"pm": 80000, "k": 80010}, ...} - strikes we traded at
        
        # Failed trades log file
        self._failed_trades_file = Path(os.path.dirname(os.path.abspath(__file__))) / "failed_trades.jsonl"
        
        log_print(f"  [Scanner] Initialized - trading DISABLED until safety checks complete")
    
    # ==========================================================================
    # SAFETY KILL SWITCH METHODS
    # ==========================================================================
    
    def trigger_safety_pause(self, reason: str, unhedged: dict = None):
        """
        EMERGENCY STOP - Halt all trading immediately.
        
        Args:
            reason: Why we're pausing (logged and displayed)
            unhedged: Optional dict with unhedged position details
        """
        self._safety_paused = True
        self._safety_pause_reason = reason
        self._safety_pause_time = time.time()
        self._safety_pause_window = self._current_window_end  # Track which window caused it
        if unhedged:
            self._unhedged_position = unhedged
        
        # Big visible warning
        print(f"\n{'='*60}")
        print(f"  🛑 SAFETY PAUSE TRIGGERED")
        print(f"  Reason: {reason}")
        if unhedged:
            print(f"  Unhedged: {unhedged['venue']} {unhedged['underlying']} "
                  f"{unhedged['direction'].upper()} x{unhedged['size']}")
        print(f"  ALL TRADING HALTED - will auto-resume on next window transition")
        print(f"{'='*60}\n")
        
        log_print(f"  🛑 SAFETY PAUSE: {reason}")
        if unhedged:
            log_print(f"      Unhedged position: {unhedged}")
    
    def record_venue_error(self, venue: str, error: str):
        """
        Record an API error for a venue. Triggers pause after consecutive errors.
        
        Args:
            venue: "PM" or "K"
            error: Error message
        """
        if venue == "PM":
            self._consecutive_pm_errors += 1
            self._consecutive_k_errors = 0  # Reset other venue
            log_print(f"  [ERROR] PM error #{self._consecutive_pm_errors}: {error}")
            
            if self._consecutive_pm_errors >= self._max_consecutive_errors:
                self.trigger_safety_pause(
                    f"PM API failed {self._consecutive_pm_errors}x consecutively: {error}"
                )
        else:
            self._consecutive_k_errors += 1
            self._consecutive_pm_errors = 0  # Reset other venue
            log_print(f"  [ERROR] K error #{self._consecutive_k_errors}: {error}")
            
            if self._consecutive_k_errors >= self._max_consecutive_errors:
                self.trigger_safety_pause(
                    f"Kalshi API failed {self._consecutive_k_errors}x consecutively: {error}"
                )
    
    def record_venue_success(self, venue: str):
        """Reset error counter for a venue after successful operation."""
        if venue == "PM":
            self._consecutive_pm_errors = 0
        else:
            self._consecutive_k_errors = 0
    
    def is_safety_paused(self) -> tuple[bool, str]:
        """Check if trading is paused. Returns (is_paused, reason)."""
        return self._safety_paused, self._safety_pause_reason
    
    def resume_trading(self):
        """Manually resume trading after safety pause (call from console if needed)."""
        if self._safety_paused:
            print(f"  ✅ Resuming trading - was paused for: {self._safety_pause_reason}")
            log_print(f"  [SAFETY] Resumed trading - was: {self._safety_pause_reason}")
        self._safety_paused = False
        self._safety_pause_reason = ""
        self._safety_pause_time = 0
        self._safety_pause_window = None
        self._unhedged_position = None
        self._consecutive_pm_errors = 0
        self._consecutive_k_errors = 0
    
    async def check_window_transition(self, pairs: list) -> bool:
        """
        Check if we've transitioned to a new market window.
        If so, clear all open orders (safe - doesn't affect filled positions).
        Returns True if orders were cleared.
        """
        if not pairs:
            return False
        
        # Get current window end time from first pair
        current_end = pairs[0].polymarket.end_time
        
        # Check if window changed
        if self._current_window_end is None:
            # First time - just record the window
            self._current_window_end = current_end
            self._orders_cleared_for_window = False
            return False
        
        if current_end != self._current_window_end:
            # Window changed! Clear orders from old window
            print(f"\n  🔄 New window detected - clearing stale orders...")
            log_print(f"  [Window] Transition: {self._current_window_end} → {current_end}")
            
            pm_cancelled = await self.pm.cancel_all_orders()
            k_cancelled = await self.kalshi.cancel_all_orders()
            
            if pm_cancelled + k_cancelled > 0:
                print(f"  [Window] Cleared {pm_cancelled} PM + {k_cancelled} K orders from previous window")
            else:
                print(f"  [Window] No stale orders to clear")
            
            # Clear internal position tracking for old window
            self.pm._internal_positions = {}
            if hasattr(self.pm, '_position_metadata'):
                self.pm._position_metadata = {}
            self.kalshi._internal_positions = {}
            
            # Reconcile cumulative positions against exchange reality
            # Previous window's positions should have settled, reducing our cumulative count
            # IMPORTANT: K API may still show stale positions from the just-expired window
            # (settlement isn't instant), so we filter to only count positions from the NEW window.
            try:
                k_real_positions = await self.kalshi.get_positions()
                
                # Build a ticker prefix for the NEW window to filter by
                # Kalshi tickers look like: KXBTC15M-26FEB092045-45
                # We extract the time component from current_end to match
                new_window_tickers = set()
                for pair in pairs:
                    if hasattr(pair, 'kalshi') and pair.kalshi.market_id:
                        new_window_tickers.add(pair.kalshi.market_id)
                
                # Count real positions per underlying, ONLY from new window
                real_counts = {}
                for ticker, pos in k_real_positions.items():
                    # Skip positions from old/settling windows
                    if ticker not in new_window_tickers:
                        continue
                    for asset in ["BTC", "ETH", "SOL"]:
                        if asset in ticker.upper():
                            real_counts[asset] = real_counts.get(asset, 0) + pos.get("size", 0)
                            break
                
                # Update cumulative tracker to match exchange reality
                for asset in list(self._cumulative_positions.keys()):
                    real_size = real_counts.get(asset, 0)
                    old_size = self._cumulative_positions[asset].get("size", 0)
                    if real_size < old_size:
                        log_print(f"  [Window] {asset} cumulative: {old_size} → {real_size} (settled)")
                    if real_size == 0:
                        del self._cumulative_positions[asset]
                    else:
                        self._cumulative_positions[asset]["size"] = real_size
                
                if self._cumulative_positions:
                    cum_str = ", ".join(f"{a}: {d['size']} {d['direction']}" for a, d in self._cumulative_positions.items())
                    log_print(f"  [Window] Cumulative positions after settlement: {cum_str}")
                else:
                    log_print(f"  [Window] All positions settled - cumulative tracker clear")
            except Exception as e:
                log_print(f"  [Window] ⚠️ Could not verify positions from exchange: {e}")
                # Don't clear cumulative on error - safer to keep the higher count
            
            # Reset trade counts for new window
            self._window_trade_counts = {}
            self._window_contract_counts = {}
            self._late_asymmetric_trade_counts = {}  # Reset late asymmetric counter too
            if hasattr(self, '_logged_trade_limits'):
                self._logged_trade_limits = set()
            
            # Move current window's trades to previous window for settlement check
            # (settlement will be checked when we get the new strike prices)
            if self._trades_awaiting_settlement:
                self._previous_window_trades = self._trades_awaiting_settlement.copy()
                log_print(f"  [Settlement] {len(self._previous_window_trades)} trades awaiting settlement check")
            self._trades_awaiting_settlement = []
            
            # Update tracking
            self._current_window_end = current_end
            self._orders_cleared_for_window = True
            self._has_orphaned_positions = False  # Reset orphan flag for new window
            
            # Auto-clear safety pause if unhedged position has settled
            # Kalshi auto-settles at window end, so if cumulative positions are clear,
            # the unhedged exposure from the previous window is resolved.
            if self._safety_paused:
                pause_window = getattr(self, '_safety_pause_window', None)
                pause_age = time.time() - getattr(self, '_safety_pause_time', time.time())
                
                if not self._cumulative_positions:
                    # Positions confirmed settled by exchange
                    log_print(f"  [SAFETY] Auto-resuming: unhedged position settled on window transition")
                    self.resume_trading()
                elif pause_window and pause_window != current_end and pause_age > 900:
                    # Pause is from a previous window AND >15min old — force clear
                    # The position auto-settled on Kalshi even if we can't verify
                    log_print(f"  [SAFETY] Force-resuming: pause from previous window ({pause_age:.0f}s old)")
                    self.resume_trading()
            
            return True
        
        return False
    
    def update_balances(self, pm_bal: Balance, k_bal: Balance):
        """Update stored balances for capital protection checks"""
        self._pm_balance = pm_bal
        self._k_balance = k_bal
    
    def record_trade_for_settlement(self, underlying: str, pm_strike: float, k_strike: float,
                                     pm_direction: str, k_direction: str, size: int,
                                     pm_price: float, k_price: float, expected_profit: float):
        """
        Record a trade for settlement verification at next window.
        
        When the next window starts, we'll use its strike prices as settlement prices
        to determine if the trade landed between strikes (both sides lose).
        """
        trade = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "underlying": underlying,
            "pm_strike": pm_strike,  # Strike we traded at
            "k_strike": k_strike,    # Strike we traded at
            "pm_direction": pm_direction,  # "up" or "down"
            "k_direction": k_direction,    # "yes" or "no"
            "size": size,
            "pm_price": pm_price,
            "k_price": k_price,
            "total_cost": (pm_price + k_price) * size,
            "expected_profit": expected_profit,
        }
        self._trades_awaiting_settlement.append(trade)
        
        # Also track the strikes we used
        self._previous_window_strikes[underlying] = {
            "pm": pm_strike,
            "k": k_strike
        }
        
        log_print(f"  [Settlement] Tracking {underlying} trade for settlement check")
    
    def check_settlement_with_new_strikes(self, new_strikes: dict):
        """
        Check previous window's trades against settlement prices.
        
        new_strikes: {"BTC": {"pm": 80100, "k": 80095}, "ETH": {...}, ...}
        
        KEY INSIGHT: PM and K have DIFFERENT settlement prices (different oracles).
        - PM uses Chainlink price
        - K uses CF Benchmarks price
        
        4 possible outcomes:
        1. Above both strikes → UP leg wins → $1 payout
        2. Below both strikes → DOWN leg wins → $1 payout
        3. Between strikes + FAVORABLE → Both legs win → $2 payout
        4. Between strikes + UNFAVORABLE → Both legs lose → $0 payout
        
        FAVORABLE means: we hold UP on the LOWER strike AND DOWN on the HIGHER strike
        So if price lands between, both pay out.
        
        UNFAVORABLE means: we hold UP on the HIGHER strike AND DOWN on the LOWER strike
        So if price lands between, neither pays out.
        """
        if not self._previous_window_trades:
            return
        
        for trade in self._previous_window_trades:
            underlying = trade["underlying"]
            if underlying not in new_strikes:
                continue
            
            # Get settlement prices from both oracles
            # We use the new window's strike prices as proxy for settlement
            pm_settlement_price = new_strikes[underlying].get("pm")
            k_settlement_price = new_strikes[underlying].get("k")
            
            if pm_settlement_price is None or k_settlement_price is None:
                continue
            
            # Get the strikes we traded at
            pm_strike = trade["pm_strike"]
            k_strike = trade["k_strike"]
            
            # Get our positions
            pm_direction = trade["pm_direction"]  # "up" or "down"
            k_direction = trade["k_direction"]    # "yes" (up) or "no" (down)
            size = trade["size"]
            total_cost = trade["total_cost"]
            expected_profit = trade["expected_profit"]
            
            # Determine if each leg won, using EACH PLATFORM'S OWN settlement price
            # PM: check PM settlement price against PM strike
            if pm_direction == "up":
                pm_won = pm_settlement_price > pm_strike
            else:  # down
                pm_won = pm_settlement_price < pm_strike
            
            # K: check K settlement price against K strike
            if k_direction == "yes":  # yes = up
                k_won = k_settlement_price > k_strike
            else:  # no = down
                k_won = k_settlement_price < k_strike
            
            # Calculate actual payout
            payout = 0
            if pm_won:
                payout += size * 1.0
            if k_won:
                payout += size * 1.0
            
            actual_profit = payout - total_cost
            diff = actual_profit - expected_profit
            
            # Determine outcome type for logging
            if pm_won and k_won:
                outcome = "BOTH WON (favorable)"
                outcome_emoji = "🎉"
            elif not pm_won and not k_won:
                outcome = "BOTH LOST (unfavorable)"
                outcome_emoji = "💀"
            elif pm_won:
                outcome = "PM won, K lost (normal)"
                outcome_emoji = "✓"
            else:
                outcome = "K won, PM lost (normal)"
                outcome_emoji = "✓"
            
            # Log the settlement
            log_print(f"\n  {outcome_emoji} SETTLEMENT: {underlying}")
            log_print(f"      PM: strike ${pm_strike:,.2f} | settlement ${pm_settlement_price:,.2f} | {pm_direction} {'WON' if pm_won else 'LOST'}")
            log_print(f"      K:  strike ${k_strike:,.2f} | settlement ${k_settlement_price:,.2f} | {k_direction} {'WON' if k_won else 'LOST'}")
            log_print(f"      Payout: ${payout:.2f} | Cost: ${total_cost:.2f} | Profit: ${actual_profit:.2f}")
            log_print(f"      Outcome: {outcome}")
            
            # Log settlement divergence for calibration
            settle_divergence = abs(pm_settlement_price - k_settlement_price)
            settle_divergence_pct = (settle_divergence / pm_settlement_price * 100) if pm_settlement_price else 0
            log_print(f"      Divergence: ${settle_divergence:,.2f} ({settle_divergence_pct:.4f}%)")
            self._log_settlement_divergence(
                underlying=underlying,
                pm_settlement=pm_settlement_price,
                k_settlement=k_settlement_price,
                divergence_usd=settle_divergence,
                divergence_pct=settle_divergence_pct,
                pm_strike=pm_strike,
                k_strike=k_strike,
                had_trade=True,
                outcome=outcome
            )
            
            # Adjust P&L if different from expected
            if abs(diff) > 0.01:
                log_print(f"      Expected: ${expected_profit:.2f} | Actual: ${actual_profit:.2f} | Adjustment: ${diff:+.2f}")
                profit_tracker.record_settlement_loss(
                    underlying=underlying,
                    expected_profit=expected_profit,
                    actual_profit=actual_profit,
                    size=size,
                    notes=f"{outcome}: PM settle ${pm_settlement_price:,.2f} vs strike ${pm_strike:,.2f}, K settle ${k_settlement_price:,.2f} vs strike ${k_strike:,.2f}"
                )
            
            # Update trade journal with actual settlement outcome
            profit_tracker.update_journal_settlement(
                trade_timestamp=trade["timestamp"],
                underlying=underlying,
                size=size,
                payout=payout,
                actual_profit=actual_profit,
                outcome=outcome
            )
            
            # Log failed trades (both lost) to separate file
            if not pm_won and not k_won:
                failed_record = {
                    "timestamp": trade["timestamp"],
                    "settlement_time": datetime.now(timezone.utc).isoformat(),
                    "underlying": underlying,
                    "pm_strike": pm_strike,
                    "k_strike": k_strike,
                    "pm_settlement": pm_settlement_price,
                    "k_settlement": k_settlement_price,
                    "pm_direction": pm_direction,
                    "k_direction": k_direction,
                    "pm_won": pm_won,
                    "k_won": k_won,
                    "size": size,
                    "total_cost": total_cost,
                    "expected_profit": expected_profit,
                    "actual_profit": actual_profit,
                    "loss": -actual_profit if actual_profit < 0 else 0,
                }
                self._log_failed_trade(failed_record)
        
        # Clear previous window trades after processing
        self._previous_window_trades = []
    
    def _log_failed_trade(self, record: dict):
        """Append failed trade to JSONL log file"""
        try:
            with open(self._failed_trades_file, "a") as f:
                f.write(json.dumps(record) + "\n")
            log_print(f"  [Settlement] Failed trade logged to {self._failed_trades_file}")
        except Exception as e:
            log_print(f"  [Settlement] Error logging failed trade: {e}")
    
    def _log_settlement_divergence(self, underlying: str, pm_settlement: float, k_settlement: float,
                                     divergence_usd: float, divergence_pct: float,
                                     pm_strike: float = None, k_strike: float = None,
                                     had_trade: bool = False, outcome: str = ""):
        """
        Log settlement divergence data for calibrating the divergence model.
        
        Writes to logs/settlement_divergence.jsonl — a persistent file across sessions.
        Each record captures both settlement prices, the divergence, and context.
        
        Use this data to update divergence_dollars in _estimate_settlement_divergence_pct():
          - After 50+ observations, replace hardcoded values with 75th percentile of |PM - K|
        """
        try:
            log_dir = Path(__file__).parent / "logs"
            log_dir.mkdir(exist_ok=True)
            divergence_file = log_dir / "settlement_divergence.jsonl"
            
            record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "underlying": underlying,
                "pm_settlement": pm_settlement,
                "k_settlement": k_settlement,
                "divergence_usd": round(divergence_usd, 4),
                "divergence_pct": round(divergence_pct, 6),
                "pm_strike": pm_strike,
                "k_strike": k_strike,
                "strike_diff_usd": round(abs(pm_strike - k_strike), 4) if pm_strike and k_strike else None,
                "had_trade": had_trade,
                "outcome": outcome,
            }
            
            with open(divergence_file, "a") as f:
                f.write(json.dumps(record) + "\n")
        except Exception as e:
            log_print(f"  [Divergence] Error logging: {e}")
    
    def get_active_position(self) -> Optional[ActivePosition]:
        """Get current active position if any"""
        return self._active_position
    
    def has_active_position(self) -> bool:
        """Check if there's an active position"""
        return self._active_position is not None
    
    def add_pending_pm_fill(self, order_id: str, market_id: str, size: int, underlying: str, 
                           direction: str, k_market_id: str, k_direction: Direction, k_price: float):
        """
        Track a PM order that we need to verify filled.
        If it fills < 5, we'll need to hedge on Kalshi.
        """
        self._pending_pm_fills.append({
            "order_id": order_id,
            "market_id": market_id,
            "size": size,
            "time": time.time(),
            "underlying": underlying,
            "direction": direction,
            "k_market_id": k_market_id,
            "k_direction": k_direction,
            "k_price": k_price,
            "checked": False
        })
    
    async def check_pending_pm_fills(self, pairs: list) -> None:
        """
        Check pending PM fills and hedge on Kalshi if needed.
        Called from reconciliation flow - this is URGENT if PM filled < trade_size.
        
        If PM filled partial contracts, we buy matching contracts on Kalshi to hedge
        (since we already have NO on Kalshi from leg 2).
        """
        if not self._pending_pm_fills:
            return
        
        now = time.time()
        fills_to_remove = []
        
        for i, pending in enumerate(self._pending_pm_fills):
            # Skip if already checked recently (within 3 seconds)
            if pending.get("checked") and now - pending.get("last_check", 0) < 3:
                continue
            
            # Give up after 2 minutes
            if now - pending["time"] > 120:
                print(f"  [PendingFill] Giving up on {pending['underlying']} order after 2min")
                fills_to_remove.append(i)
                continue
            
            try:
                # Check the order status
                status = await self.pm.get_order(pending["order_id"])
                pending["checked"] = True
                pending["last_check"] = now
                
                if not status:
                    continue
                
                filled = float(status.get("size_matched", 0) or status.get("filled", 0) or 0)
                
                expected_size = pending.get("size", 5)  # Flat 5 contracts
                
                if filled >= expected_size:
                    # Good - got full fill, remove from pending
                    print(f"  [PendingFill] ✓ {pending['underlying']} PM filled {int(filled)}/{expected_size} contracts")
                    fills_to_remove.append(i)
                    
                elif filled > 0 and filled < expected_size:
                    # Partial fill - need to hedge on Kalshi NOW
                    print(f"  [PendingFill] ⚠️ {pending['underlying']} PM only filled {int(filled)}/{expected_size} - hedging on Kalshi")
                    
                    # Find the matching pair for this underlying
                    matching_pair = None
                    time_remaining = None
                    for pair in pairs:
                        if pair.polymarket.underlying == pending["underlying"]:
                            matching_pair = pair
                            time_remaining = pair.polymarket.time_to_close
                            break
                    
                    if matching_pair and time_remaining:
                        # Calculate aggressive price based on time remaining
                        # More aggressive (higher price) when less time remains
                        market_duration = 15 * 60
                        time_elapsed = market_duration - time_remaining
                        pct_elapsed = time_elapsed / market_duration
                        
                        # Base price from the pending order's Kalshi price
                        base_price = pending["k_price"]
                        
                        # Add aggressiveness: +5% at start, +20% near end
                        aggression = 0.05 + (pct_elapsed * 0.15)
                        hedge_price = min(base_price * (1 + aggression), 0.95)
                        
                        # CAP at breakeven: never pay more than (1.00 - PM_paid) for hedge
                        pm_paid = pending.get("pm_price", 0)
                        if pm_paid > 0:
                            breakeven = round(1.00 - pm_paid - 0.01, 2)  # 1¢ below breakeven
                            if hedge_price > breakeven:
                                print(f"    [Hedge] Price ${hedge_price:.2f} > breakeven ${breakeven:.2f} — capping")
                                hedge_price = max(breakeven, 0.01)
                        
                        # We need to buy the OPPOSITE side on Kalshi to hedge
                        # If we bought PM DOWN, we have K NO from leg 2
                        # So we need to buy K YES to hedge the partial PM fill
                        hedge_direction = Direction.UP if pending["k_direction"] == Direction.DOWN else Direction.DOWN
                        
                        print(f"    [Hedge] Buying {int(filled)} K {hedge_direction.value} @ ${hedge_price:.2f} ({pct_elapsed*100:.0f}% through market)")
                        
                        try:
                            order = await self.kalshi.place_order(
                                pending["k_market_id"],
                                OrderSide.BUY,
                                hedge_direction,
                                Decimal(str(hedge_price)),
                                Decimal(str(int(filled)))
                            )
                            
                            if order:
                                print(f"    [Hedge] ✓ Kalshi hedge order placed: {order.order_id[:16]}...")
                                # Wait briefly for fill
                                await asyncio.sleep(2)
                                hedge_status = await self.kalshi.get_order(order.order_id)
                                if hedge_status:
                                    hedge_filled = int(hedge_status.get("filled", 0) or hedge_status.get("count", 0) or 0)
                                    if hedge_filled > 0:
                                        print(f"    [Hedge] ✓ Kalshi hedged {hedge_filled} contracts")
                                    else:
                                        print(f"    [Hedge] ⚠️ Kalshi hedge didn't fill immediately")
                            else:
                                print(f"    [Hedge] ❌ Kalshi hedge order failed")
                        except Exception as e:
                            print(f"    [Hedge] ❌ Kalshi hedge error: {e}")
                    
                    fills_to_remove.append(i)
                    
                elif filled == 0 and now - pending["time"] > 30:
                    # No fill after 30 seconds - the order probably failed or was cancelled
                    print(f"  [PendingFill] {pending['underlying']} PM no fill after 30s - removing")
                    
                    # CRITICAL: Remove the internal position we incorrectly recorded
                    # Find and remove the PM position for this order
                    token_id = pending.get("market_id")
                    if token_id and token_id in self.pm._internal_positions:
                        old_size = self.pm._internal_positions[token_id]
                        del self.pm._internal_positions[token_id]
                        print(f"  [PendingFill] ⚠️ Removed phantom PM position: {token_id[:20]}... (was {old_size})")
                        logger.log_text(f"[PendingFill] Removed phantom PM position {token_id[:20]}... size={old_size}")
                    
                    # Also remove from metadata
                    if hasattr(self.pm, '_position_metadata') and token_id in self.pm._position_metadata:
                        del self.pm._position_metadata[token_id]
                    
                    fills_to_remove.append(i)
                    
            except Exception as e:
                # API error - will retry next time
                print(f"  [PendingFill] Check error: {e}")
        
        # Remove processed fills (in reverse order to preserve indices)
        for i in sorted(fills_to_remove, reverse=True):
            self._pending_pm_fills.pop(i)
    
    async def check_assumed_strike_position(self, pairs: list) -> bool:
        """
        Check if active position was an assumed-strike trade that needs emergency exit.
        Returns True if we should exit immediately due to strike divergence.
        """
        if not self._active_position:
            return False
        
        # Check if this position was made with an assumed strike
        pos = self._active_position
        if not hasattr(pos, 'assumed_strike') or not pos.assumed_strike:
            return False
        
        # Find the matching pair to check current strike diff
        underlying = pos.opportunity.market_pair.polymarket.underlying
        for pair in pairs:
            if pair.polymarket.underlying == underlying:
                strike_diff = getattr(pair, 'strike_diff_pct', 0.0)
                exit_threshold = CONFIG["markets"].get("assumed_strike_exit_pct", 0.02)
                
                if strike_diff > exit_threshold:
                    print(f"  ⚠️ Strike divergence {strike_diff:.4f}% > {exit_threshold}% on assumed-strike trade")
                    return True
                break
        
        return False
    
    def _log_filtered(self, underlying: str, score: float, edge: float, reason: str):
        """Log when a decent opportunity gets filtered out (to file only, not console)"""
        if not hasattr(self, '_last_filter_log'):
            self._last_filter_log = {}
        now = time.time()
        key = f"{underlying}_{reason.split()[0]}"
        if key not in self._last_filter_log or now - self._last_filter_log[key] > 30:
            # Log to file only - don't print to console as it interferes with dashboard
            logger.log_text(f"[FILTERED] {underlying}: score={score:.0f} edge={edge:.1f}% - {reason}")
            self._last_filter_log[key] = now
    
    async def find_pairs(self, force_refresh: bool = False) -> list:
        """Find matching market pairs. Caches results for 30 seconds, less if strikes missing."""
        now = time.time()
        
        # Check if we have cached pairs with assumed/missing/uncertain Kalshi strikes
        has_assumed_strikes = False
        has_uncertain_strikes = False
        if hasattr(self, '_cached_pairs') and self._cached_pairs:
            for p in self._cached_pairs:
                if getattr(p, 'assumed_strike', False) or p.kalshi.strike_price is None:
                    has_assumed_strikes = True
                    break
                if getattr(p.kalshi, 'strike_uncertain', False):
                    has_uncertain_strikes = True
        
        # Shorter cache if we have assumed/uncertain strikes (want to check for real strike)
        if has_assumed_strikes:
            cache_duration = 10
        elif has_uncertain_strikes:
            cache_duration = 15
        else:
            cache_duration = 30
        
        # Return cached pairs if fresh and not forcing refresh
        if not force_refresh and hasattr(self, '_cached_pairs') and self._cached_pairs:
            cache_age = now - getattr(self, '_pairs_cache_time', 0)
            if cache_age < cache_duration:
                # But check if any pairs have expired (< 30s left)
                valid_pairs = [p for p in self._cached_pairs if p.polymarket.time_to_close > 30]
                if valid_pairs:
                    return valid_pairs
        
        # Fetch fresh markets
        pm_mkts = await self.pm.get_markets()
        k_mkts = await self.kalshi.get_markets()
        
        # Debug: show market counts if none found (overwrite same line)
        if not pm_mkts or not k_mkts:
            msg = f"  [Waiting] PM markets: {len(pm_mkts)}, K markets: {len(k_mkts)}    "
            print(f"\r{msg}", end="", flush=True)
            # Store that we printed a waiting message so we can clear it later
            self._waiting_msg_shown = True
        elif hasattr(self, '_waiting_msg_shown') and self._waiting_msg_shown:
            # Clear the waiting message line when markets are found
            print(f"\r{' ' * 60}\r", end="", flush=True)
            self._waiting_msg_shown = False
        
        pairs = []
        for pm in pm_mkts:
            for k in k_mkts:
                if pm.underlying != k.underlying:
                    continue
                
                # Check both END time and START time match
                end_diff = abs((pm.end_time - k.end_time).total_seconds())
                start_diff = abs((pm.start_time - k.start_time).total_seconds())
                
                if end_diff <= 120 and start_diff <= 120:  # Both within 2 minutes
                    # IMPORTANT: PM and Kalshi use DIFFERENT data sources!
                    # - PM uses Chainlink Data Streams (captured via RTDS WebSocket)
                    # - Kalshi uses CF Benchmarks RTI
                    # We need BOTH strikes and they must be close for safe arb
                    
                    pm_strike = pm.strike_price  # From RTDS Chainlink
                    k_strike = k.strike_price    # From Kalshi API (CF Benchmarks)
                    
                    # Log strike prices - track separately for "assumed" vs "uncertain" vs "confirmed"
                    window_key = f"{pm.underlying}_{pm.start_time.strftime('%H:%M')}"
                    confirmed_key = f"{window_key}_confirmed"
                    uncertain_key = f"{window_key}_uncertain"
                    
                    if not hasattr(self, '_logged_strikes'):
                        self._logged_strikes = set()
                    
                    k_uncertain = getattr(k, 'strike_uncertain', False)
                    
                    # Print if: (1) never logged this window, (2) was assumed but now has value,
                    # or (3) was uncertain but now confirmed from real API
                    should_print = False
                    is_upgrade = False
                    if window_key not in self._logged_strikes:
                        should_print = True
                    elif k_strike and confirmed_key not in self._logged_strikes:
                        if not k_uncertain:
                            # Real strike from API - upgraded from assumed or uncertain
                            should_print = True
                            if uncertain_key in self._logged_strikes:
                                is_upgrade = True
                        elif uncertain_key not in self._logged_strikes:
                            # First time seeing uncertain strike
                            should_print = True
                        
                    if should_print:
                        pm_str = f"${pm_strike:,.2f}" if pm_strike else "waiting..."
                        k_label = " [uncertain]" if k_uncertain else ""
                        upgrade_label = " ✓ CONFIRMED" if is_upgrade else ""
                        
                        # Staleness check for display
                        is_stale = False
                        if pm_strike and k_strike and k_strike > 0:
                            display_diff = abs(pm_strike - k_strike) / k_strike * 100
                            if display_diff > 5.0:
                                k_label = " ⚠️ STALE"
                                is_stale = True
                        
                        k_str = f"${k_strike:,.2f}{k_label}{upgrade_label}" if k_strike else "None"
                        
                        if pm_strike and k_strike:
                            strike_diff_pct = abs(pm_strike - k_strike) / k_strike * 100
                            diff_str = f"{strike_diff_pct:.4f}%"
                        else:
                            strike_diff_pct = None
                            diff_str = "N/A"
                        
                        print(f"  [Strike] {pm.underlying}:")
                        print(f"           PM (Chainlink): {pm_str}")
                        print(f"           K (CF Bench):   {k_str}")
                        print(f"           Diff: {diff_str}")
                        
                        # Log to file
                        logger.log_text(f"Strike {pm.underlying} @ {pm.start_time.strftime('%H:%M')} UTC:")
                        logger.log_text(f"  PM: {pm_str} | K: {k_str} | Diff: {diff_str}")
                        
                        self._logged_strikes.add(window_key)
                        if k_strike and k_uncertain:
                            self._logged_strikes.add(uncertain_key)
                        if k_strike and not k_uncertain:
                            self._logged_strikes.add(confirmed_key)
                    
                    # Calculate strike price difference
                    strike_diff_pct = None
                    if pm_strike and k_strike and pm_strike > 0 and k_strike > 0:
                        strike_diff_pct = abs(pm_strike - k_strike) / k_strike * 100
                        
                        # STALENESS DETECTION: If K and PM differ by > 5%, K strike is almost
                        # certainly stale (from a prior session or an unsettled market).
                        # Real divergence is never more than ~0.5%. Treat as "no K strike".
                        if strike_diff_pct > 5.0:
                            stale_key = f"{pm.underlying}_{pm.start_time.strftime('%H:%M')}_stale"
                            if not hasattr(self, '_logged_stale'):
                                self._logged_stale = set()
                            if stale_key not in self._logged_stale:
                                log_print(f"  [STALE] {pm.underlying}: K strike ${k_strike:,.2f} vs PM ${pm_strike:,.2f} "
                                         f"({strike_diff_pct:.1f}% diff) — treating as TBD")
                                self._logged_stale.add(stale_key)
                            # Reset to treat as missing K strike
                            k_strike = None
                            strike_diff_pct = None
                            k.strike_price = None
                    
                    # If PM strike is missing, we can't safely compare - skip for now
                    # (Will be available after next window boundary)
                    if not pm_strike:
                        continue
                    
                    # Check if we're in early window period where we can assume strikes match
                    time_into_window = 900 - pm.time_to_close  # seconds since window opened
                    allow_assumed = CONFIG["markets"].get("allow_assumed_strike", False)
                    assumed_window = CONFIG["markets"].get("assumed_strike_window_secs", 60)
                    max_tbd_window = 90  # If Kalshi strike still TBD after 90s, something's wrong
                    
                    # If Kalshi strike is missing...
                    if not k_strike:
                        # Safety check: if we're past 90s and still no Kalshi strike, skip entirely
                        if time_into_window > max_tbd_window:
                            skip_key = f"{pm.underlying}_{pm.start_time.strftime('%H:%M')}_late_tbd"
                            if not hasattr(self, '_logged_skips'):
                                self._logged_skips = set()
                            if skip_key not in self._logged_skips:
                                print(f"  [Skip] {pm.underlying}: Kalshi strike still TBD after {time_into_window:.0f}s - something wrong")
                                self._logged_skips.add(skip_key)
                            continue
                        
                        # Allow trading with assumed strike in early window
                        if allow_assumed and time_into_window <= assumed_window:
                            k_strike = pm_strike  # Assume same as PM
                            strike_diff_pct = 0.0
                            assumed_strike = True
                            
                            # Log this once per window
                            if not hasattr(self, '_logged_assumed'):
                                self._logged_assumed = set()
                            assume_key = f"{pm.underlying}_{pm.start_time.strftime('%H:%M')}"
                            if assume_key not in self._logged_assumed:
                                logger.log_text(f"ASSUMED: {pm.underlying} @ {pm.start_time.strftime('%H:%M')} using PM strike ${pm_strike:,.2f}")
                                print(f"  [ASSUME] {pm.underlying} using PM strike ${pm_strike:,.2f} (Kalshi TBD)")
                                self._logged_assumed.add(assume_key)
                        else:
                            continue
                    else:
                        assumed_strike = False
                    
                    # NOTE: No blanket hard cap on strike diff here.
                    # The dynamic ROI model handles large diffs correctly per direction:
                    #   - Favorable: 3% floor (large diff = big jackpot zone, no added risk)
                    #   - Unfavorable: scales to 80% for large gaps (effectively blocks)
                    #   - Neutral: returns 999% for >0.5% (blocks unknown direction)
                    # Staleness (>5% diff) is caught above and treated as "no K strike".
                    
                    # Good pair - log it only once per window
                    if not hasattr(self, '_logged_pairs'):
                        self._logged_pairs = set()
                    pair_key = f"{pm.underlying}_{pm.start_time.strftime('%H:%M')}"
                    if pair_key not in self._logged_pairs:
                        diff_str = f"{strike_diff_pct:.4f}%" if strike_diff_pct is not None else "N/A"
                        status = " (ASSUMED)" if assumed_strike else ""
                        # Show zone label
                        if strike_diff_pct is not None:
                            if strike_diff_pct <= 0.02:
                                zone = "safe"
                            elif strike_diff_pct <= 0.03:
                                zone = "mid"
                            elif strike_diff_pct <= 0.15:
                                zone = "risky"
                            elif strike_diff_pct <= 0.5:
                                zone = "DANGER"
                            else:
                                zone = "EXTREME"
                        else:
                            zone = "?"
                        logger.log_text(f"PAIRED: {pm.underlying} @ {pm.start_time.strftime('%H:%M')} (diff: {diff_str} [{zone}]){status}")
                        self._logged_pairs.add(pair_key)
                    
                    pair = MarketPair(pm, k, 0.90)
                    pair.assumed_strike = assumed_strike  # Track if we assumed the strike
                    pair.strike_diff_pct = strike_diff_pct if strike_diff_pct is not None else 0.0  # Store for ROI calculation
                    pairs.append(pair)
                    break
        
        # Check if we have new confirmed strikes to verify previous window's settlements
        # Only do this once per window when we get confirmed K strikes
        if pairs and self._previous_window_trades:
            new_strikes = {}
            have_all_k_strikes = True
            
            for pair in pairs:
                underlying = pair.polymarket.underlying
                pm_strike = pair.polymarket.strike_price
                k_strike = pair.kalshi.strike_price
                
                if k_strike is None:
                    have_all_k_strikes = False
                    break
                
                new_strikes[underlying] = {
                    "pm": pm_strike,
                    "k": k_strike
                }
            
            if have_all_k_strikes and new_strikes:
                # Check previous window's trades against these settlement prices
                self.check_settlement_with_new_strikes(new_strikes)
                
                # Log settlement divergence for ALL assets for calibration (even without trades)
                for ul, strikes in new_strikes.items():
                    pm_s = strikes.get("pm")
                    k_s = strikes.get("k")
                    if pm_s and k_s:
                        div_usd = abs(pm_s - k_s)
                        div_pct = (div_usd / pm_s * 100) if pm_s else 0
                        self._log_settlement_divergence(
                            underlying=ul,
                            pm_settlement=pm_s,
                            k_settlement=k_s,
                            divergence_usd=div_usd,
                            divergence_pct=div_pct,
                            had_trade=False,
                            outcome="window_boundary"
                        )
        
        # Cache the pairs
        self._cached_pairs = pairs
        self._pairs_cache_time = now
        
        return pairs
    
    def _calculate_score(self, divergence_pct: float, minutes_since_open: float, 
                         price: float) -> Tuple[float, dict]:
        """Calculate opportunity score based on multiple factors"""
        cfg = CONFIG["strategy"]
        components = {}
        score = 0
        
        # 1. Divergence is king
        div_score = abs(divergence_pct) * cfg["divergence_weight"]
        components["divergence"] = div_score
        score += div_score
        
        # 2. Time-based bonus - gradual decay from max at open to min at 50% through
        # For 15-min markets: max bonus at 0 min, min bonus at 7.5 min, stays at min after
        market_duration = 15.0  # minutes
        half_market = market_duration / 2  # 7.5 minutes
        
        max_time_bonus = 20.0  # Full bonus at market open
        min_time_bonus = 10.0  # Minimum bonus at 50% through and beyond (raised from 5)
        
        if minutes_since_open <= 0:
            time_bonus = max_time_bonus
        elif minutes_since_open >= half_market:
            time_bonus = min_time_bonus
        else:
            # Linear decay from max to min over first half of market
            decay_pct = minutes_since_open / half_market
            time_bonus = max_time_bonus - (max_time_bonus - min_time_bonus) * decay_pct
        
        components["time_bonus"] = time_bonus
        score += time_bonus
        
        # 3. Extreme price bonus (likely to revert toward 0.50)
        # This is separate from timing - extreme prices are risky but potentially profitable
        extremity = abs(price - 0.50)
        if extremity > 0.35:  # Price > 0.85 or < 0.15
            components["extreme_bonus"] = cfg["extreme_price_bonus"]
            score += cfg["extreme_price_bonus"]
        elif extremity > 0.25:  # Price > 0.75 or < 0.25
            components["extreme_bonus"] = cfg["moderate_extreme_bonus"]
            score += cfg["moderate_extreme_bonus"]
        else:
            components["extreme_bonus"] = 0
        
        components["total"] = score
        return score, components
    
    def _get_max_trades_for_window(self, strike_favorable: bool = False) -> int:
        """
        Max trades per underlying per market window.
        
        Favorable: 4 trades (no position cap though)
        Unfavorable: 2 trades + 30 contract cap
        """
        if strike_favorable:
            return CONFIG["markets"].get("max_trades_favorable", 4)
        return CONFIG["markets"].get("max_trades_unfavorable", 2)
    
    def _get_volatility(self, underlying: str) -> float:
        """
        Get best available annualized volatility estimate.
        
        Uses max(hardcoded_floor, trailing_1h_realized × 1.2) to ensure the model
        never UNDERESTIMATES volatility during volatile periods.
        
        The 1.2× multiplier provides a buffer since realized vol looks backward
        and current conditions may be more volatile than the trailing average.
        """
        # Hardcoded floors — reasonable baseline for normal conditions
        vol_floors = {"BTC": 0.70, "ETH": 0.80, "SOL": 0.90}
        floor = vol_floors.get(underlying, 0.80)
        
        # Try to get trailing realized vol from RTDS price history
        if hasattr(self, '_rtds') and self._rtds:
            sym = f"{underlying.lower()}/usd"
            realized = self._rtds.get_realized_volatility(sym, lookback_minutes=60.0)
            if realized is not None:
                return max(floor, realized * 1.2)
        
        return floor
    
    def _estimate_settlement_divergence_pct(self, underlying: str, ref_price: float = None) -> float:
        """
        Estimate settlement divergence risk between PM (Chainlink spot) and 
        Kalshi (CF Benchmarks 60-second TWAP).
        
        PM settles on a single Chainlink price at the exact expiry instant.
        Kalshi settles on the AVERAGE of 60 per-second CF Benchmarks RTI 
        observations in the final minute before expiry.
        
        Returns divergence as a percentage of price — this represents the 
        expected (not max) difference between spot and 60s TWAP.
        
        The EXPECTED spot-vs-TWAP divergence for a random walk is:
            E[|spot - TWAP|] ≈ sigma_1min / sqrt(3)
        
        Both Chainlink and CFB are exchange aggregates, so they track closely.
        The primary divergence source is temporal (spot vs average), not 
        data source differences. Using conservative estimates.
        
        TODO: Calibrate with real settlement data from price logs.
        """
        # Expected spot-vs-60s-TWAP divergence in dollar terms
        # Derived from: sigma_1min / sqrt(3) for each asset
        # BTC: ~$94/1.73 ≈ $54 expected, but both feeds are aggregates 
        # so actual divergence is lower. Using ~$30 as practical estimate.
        divergence_dollars = {
            "BTC": 30.0,   # ~$30 expected spot-vs-TWAP divergence
            "ETH": 2.50,   # ~$2.50 for ETH  
            "SOL": 0.25,   # ~$0.25 for SOL
        }
        
        div_usd = divergence_dollars.get(underlying, 20.0)
        
        # Convert to percentage using reference price
        if ref_price and ref_price > 0:
            return (div_usd / ref_price) * 100.0
        
        # Fallback percentages
        fallback_pct = {"BTC": 0.03, "ETH": 0.07, "SOL": 0.12}
        return fallback_pct.get(underlying, 0.05)

    def _calculate_favorable_roi(self, gap_pct: float, time_remaining_minutes: float = None,
                                   underlying: str = None, safety_buffer_pct: float = 3.0,
                                   current_price: float = None, avg_strike: float = None) -> float:
        """
        Calculate dynamic ROI threshold for FAVORABLE trades using three-outcome model,
        accounting for WHERE the current price is relative to the jackpot zone.
        
        Favorable means the strike gap creates a JACKPOT ZONE where both legs pay $2.
        Three possible outcomes at settlement:
          1. Normal win: price outside both strikes → one leg wins → payout $1
          2. Jackpot: price between strikes → BOTH legs win → payout $2
          3. Divergence loss: price at boundary + PM/K settlement disagree → payout $0
        
        The jackpot probability depends critically on how far the current price has
        drifted from the gap. At window open, price ≈ gap center → high jackpot chance.
        Ten minutes later with price $400 away → jackpot chance near zero.
        
        Expected payout = $1 + p_jackpot - p_divergence_loss
        Required ROI = safety + div_penalty - jackpot_bonus
        """
        import math
        
        if time_remaining_minutes is None or time_remaining_minutes <= 0:
            time_remaining_minutes = 15.0
        time_remaining_minutes = max(0.5, min(15.0, time_remaining_minutes))
        
        vol_annual = self._get_volatility(underlying)
        vol_minute = vol_annual / math.sqrt(525600)
        sigma_t = vol_minute * math.sqrt(time_remaining_minutes)
        
        def norm_pdf(x):
            return math.exp(-x * x / 2) / math.sqrt(2 * math.pi)
        
        def norm_cdf(x):
            return 0.5 * (1 + math.erf(x / math.sqrt(2)))
        
        # ── P(jackpot): probability price at expiry lands INSIDE the favorable gap ──
        # 
        # The gap runs from strike_low to strike_high (width = gap_pct % of price).
        # Price at expiry follows: P_expiry = P_current * (1 + X), X ~ N(0, sigma_t)
        #
        # P(jackpot) = P(strike_low < P_expiry < strike_high)
        #            = Φ(z_upper) - Φ(z_lower)
        # where z = (strike/current_price - 1) / sigma_t
        
        ref_price = avg_strike or current_price
        
        # If no current spot price from RTDS, use avg_strike as best estimate.
        # Price is always near the strike at window open, and the centered
        # approximation gives wildly wrong results for small gaps (div_sigmas
        # dominates gap_sigmas, causing 17%+ required ROI on tiny favorable gaps).
        effective_price = current_price or avg_strike
        
        if effective_price and avg_strike and sigma_t > 0:
            # Exact CDF calculation using actual price position
            gap_half = avg_strike * (gap_pct / 200.0)  # half gap in dollar terms
            strike_low = avg_strike - gap_half
            strike_high = avg_strike + gap_half
            
            z_lower = (strike_low / effective_price - 1.0) / sigma_t
            z_upper = (strike_high / effective_price - 1.0) / sigma_t
            
            p_jackpot = norm_cdf(z_upper) - norm_cdf(z_lower)
            p_jackpot = min(0.50, max(0.0, p_jackpot))
            
            # ── P(divergence loss): settlement disagree at outer boundaries ──
            # Divergence zone extends beyond each strike boundary.
            # Price landing in the divergence zone near a boundary could see
            # PM and K settle on different sides → one flips from win to loss.
            divergence_pct = self._estimate_settlement_divergence_pct(underlying, ref_price)
            div_half = avg_strike * (divergence_pct / 200.0)
            
            # Outer divergence zones: just outside each strike boundary
            # Near strike_low: price in [strike_low - div, strike_low]
            # Near strike_high: price in [strike_high, strike_high + div]
            z_div_low_outer = (((strike_low - div_half) / effective_price) - 1.0) / sigma_t
            z_div_low_inner = ((strike_low / effective_price) - 1.0) / sigma_t
            z_div_high_inner = ((strike_high / effective_price) - 1.0) / sigma_t
            z_div_high_outer = (((strike_high + div_half) / effective_price) - 1.0) / sigma_t
            
            p_near_low = norm_cdf(z_div_low_inner) - norm_cdf(z_div_low_outer)
            p_near_high = norm_cdf(z_div_high_outer) - norm_cdf(z_div_high_inner)
            p_near_boundary = max(0, p_near_low) + max(0, p_near_high)
            
            # 50% chance divergence flips the outcome against you
            p_div_loss = min(0.50, p_near_boundary * 0.5)
        
        elif sigma_t > 0:
            # Fallback: no current price, assume centered (original model)
            gap_sigmas = (gap_pct / 100.0) / sigma_t
            p_jackpot = min(0.50, gap_sigmas * norm_pdf(0))
            
            divergence_pct = self._estimate_settlement_divergence_pct(underlying, ref_price)
            div_sigmas = (divergence_pct / 100.0) / sigma_t
            p_near_boundary = min(0.50, 2 * div_sigmas * norm_pdf(0))
            p_div_loss = p_near_boundary * 0.5
        
        else:
            p_jackpot = 0.0
            p_div_loss = 0.05  # conservative
        
        # ── Expected payout and required ROI ──
        # Standard arb assumes $1 payout: ROI = ($1 - cost) / cost
        # Real expected payout includes jackpot: EP = 1 + p_jackpot - p_div_loss
        # Required standard ROI = safety + div_penalty - jackpot_bonus
        jackpot_bonus_roi = p_jackpot * 100.0
        div_penalty_roi = p_div_loss * 100.0
        
        required_roi = safety_buffer_pct + div_penalty_roi - jackpot_bonus_roi
        
        # Floor at 3% — always want meaningful edge even on "free money" trades
        required_roi = max(3.0, required_roi)
        
        return required_roi

    def _calculate_dynamic_unfavorable_roi(self, gap_pct: float, time_remaining_minutes: float = None,
                                           underlying: str = None, safety_buffer_pct: float = 3.0,
                                           current_price: float = None, avg_strike: float = None) -> float:
        """
        Calculate dynamic ROI threshold for UNFAVORABLE trades based on probability theory.
        
        Uses the probability that price will cross the strike gap to determine fair ROI.
        Key insight: With less time, price is less likely to move enough to cross the gap,
        so we need higher ROI to compensate for the higher probability of total loss.
        
        Args:
            gap_pct: Strike difference as percentage (e.g., 0.1375 for BTC)
            time_remaining_minutes: Minutes until settlement (None = use 15)
            underlying: Asset name for volatility lookup (BTC/ETH/SOL)
            safety_buffer_pct: Additional margin above theoretical (default 3%)
        
        Returns:
            Required ROI percentage (e.g., 35.0 means need 35% ROI)
        """
        import math
        
        # Default to full window if time not provided
        if time_remaining_minutes is None or time_remaining_minutes <= 0:
            time_remaining_minutes = 15.0
        
        # Clamp to reasonable range
        time_remaining_minutes = max(0.5, min(15.0, time_remaining_minutes))
        
        # Annualized volatility — uses trailing realized vol when available
        vol_annual = self._get_volatility(underlying)
        
        # Convert annual vol to per-minute vol
        # vol_minute = vol_annual / sqrt(525600)  # minutes in a year
        vol_minute = vol_annual / math.sqrt(525600)
        
        # Expected price movement in remaining time (standard deviations)
        # sigma_t = vol_minute * sqrt(time_remaining_minutes)
        sigma_t = vol_minute * math.sqrt(time_remaining_minutes)
        
        # Probability of NOT crossing the gap (price stays within gap)
        # Using normal distribution CDF approximation
        # z = gap_pct / sigma_t / 100 (convert gap to decimal)
        if sigma_t > 0:
            z = (gap_pct / 100.0) / sigma_t
            # P(cross) ≈ 2 * (1 - Φ(z)) where Φ is normal CDF
            # Approximate normal CDF using error function
            def norm_cdf(x):
                return 0.5 * (1 + math.erf(x / math.sqrt(2)))
            
            p_cross = 2 * (1 - norm_cdf(z))  # Two-tailed (can cross either direction)
        else:
            p_cross = 0.5  # No time left = coin flip
        
        # Probability of loss (price lands in gap = both legs lose)
        p_loss = 1 - p_cross
        
        # To break even, we need: ROI * p_win >= cost * p_loss
        # Rearranging: ROI >= p_loss / p_win = p_loss / p_cross
        # But we lose 100% when we lose, so: ROI >= p_loss / p_cross * 100
        
        if p_cross > 0.01:  # Avoid division by near-zero
            theoretical_roi = (p_loss / p_cross) * 100.0
        else:
            theoretical_roi = 100.0  # Very unlikely to win, need huge ROI
        
        # Add safety buffer
        required_roi = theoretical_roi + safety_buffer_pct
        
        # Add settlement divergence buffer:
        # PM settles on Chainlink spot, Kalshi on 60-second CF Benchmarks TWAP.
        # When price lands near the strike at expiry, these can disagree.
        # Model as ADDITIONAL loss probability (not gap extension), because
        # divergence is probabilistic — it helps you ~half the time.
        ref_price = avg_strike or current_price
        divergence_pct = self._estimate_settlement_divergence_pct(underlying, ref_price)
        
        if divergence_pct > 0 and sigma_t > 0:
            # P(price lands within divergence zone of the gap boundary at expiry)
            z_gap = (gap_pct / 100.0) / sigma_t
            z_div = (divergence_pct / 100.0) / sigma_t
            
            # Probability of landing in the "danger zone" near the gap boundary
            p_near_boundary = 2 * abs(norm_cdf(z_gap + z_div) - norm_cdf(max(0, z_gap - z_div)))
            
            # In the danger zone, ~50% chance divergence flips the outcome against you
            p_divergence_loss = p_near_boundary * 0.5
            
            # Adjusted loss probability
            p_loss_adj = min(0.99, p_loss + p_divergence_loss)
            p_win_adj = max(0.01, 1 - p_loss_adj)
            
            divergence_adjusted_roi = (p_loss_adj / p_win_adj) * 100.0
            divergence_buffer = max(0, divergence_adjusted_roi - theoretical_roi)
            required_roi += divergence_buffer
        
        # Clamp to reasonable range (min 3%, max 80%)
        # The probability model + divergence buffer handles risk-based scaling,
        # so we only need a minimal floor as a sanity check.
        required_roi = max(3.0, min(80.0, required_roi))
        
        return required_roi
    
    def _get_required_roi_for_strike_diff(self, strike_diff_pct: float, time_elapsed_pct: float = 0.0, 
                                          strike_favorable: bool = False, underlying: str = None,
                                          time_remaining_minutes: float = None,
                                          current_price: float = None, avg_strike: float = None) -> float:
        """
        Calculate required ROI based on strike difference, time, AND favorability.
        
        FAVORABLE direction (strike gap creates win zone - both legs pay between strikes):
        - Flat low requirements that DECREASE with larger gaps (gap helps you):
          0.01-0.05%: 5.5% | 0.05-0.15%: 5.0% | 0.15-0.30%: 4.5% | 0.30%+: 4.0%
        
        UNFAVORABLE direction (strike gap creates loss zone):
        - DYNAMIC threshold based on probability theory:
          - Gap size (larger = more risk)
          - Time remaining (less time = price less likely to cross gap)
          - Asset volatility (BTC=70%, ETH=80%, SOL=90% annual)
          - Safety buffer (3% default)
        
        NEUTRAL (equal strikes or tiny diff < 0.01%):
        - Standard tiered ROI applies
        
        Time-based late penalty applies on top of all directions.
        """
        # Clamp time_elapsed_pct to just below late cutoff to prevent dashboard showing BLK
        late_cutoff = CONFIG["strategy"].get("late_market_cutoff_pct", 85) / 100.0
        time_elapsed_pct = max(0.0, min(time_elapsed_pct, late_cutoff - 0.01))
        
        min_diff = CONFIG["markets"].get("min_strike_diff_pct", 0.02)
        max_diff = CONFIG["markets"].get("max_strike_diff_pct", 1.0)
        safe_roi = CONFIG["strategy"].get("min_edge_pct", 5.0)  # ROI for safe zone
        min_roi = CONFIG["markets"].get("min_strike_roi_pct", 8.0)  # ROI at start of risky zone
        max_roi = CONFIG["markets"].get("max_strike_roi_pct", 14.0)  # ROI at max of risky zone (0.15%)
        
        # Late window settings
        late_start = CONFIG["strategy"].get("late_window_start_pct", 85) / 100.0
        late_end = CONFIG["strategy"].get("late_market_cutoff_pct", 85) / 100.0
        late_penalty = CONFIG["strategy"].get("late_window_roi_penalty", 0.0)
        
        # Calculate base ROI based on favorability
        # ANY non-favorable gap = unfavorable (risk of total loss)
        is_unfavorable = not strike_favorable and strike_diff_pct > 0
        
        if strike_favorable and strike_diff_pct > 0.01:
            # FAVORABLE: Three-outcome model (normal win / jackpot / divergence loss)
            # The gap between strikes creates a JACKPOT ZONE where both legs pay $2.
            # Expected payout > $1, so required standard ROI is lower.
            base_roi = self._calculate_favorable_roi(
                gap_pct=strike_diff_pct,
                time_remaining_minutes=time_remaining_minutes,
                underlying=underlying,
                safety_buffer_pct=3.0,
                current_price=current_price,
                avg_strike=avg_strike
            )
        elif is_unfavorable:
            # UNFAVORABLE: DYNAMIC threshold based on probability theory
            # Uses gap size, time remaining, volatility, AND settlement divergence
            base_roi = self._calculate_dynamic_unfavorable_roi(
                gap_pct=strike_diff_pct,
                time_remaining_minutes=time_remaining_minutes,
                underlying=underlying,
                safety_buffer_pct=3.0,  # 3% safety buffer
                current_price=current_price,
                avg_strike=avg_strike
            )
            
            # Hard block above 0.5% diff regardless of calculation
            if strike_diff_pct > 0.5:
                return 999.0
        else:
            # NEUTRAL: when we can't determine favorability or diff is essentially zero
            # Since direction is unknown, we conservatively blend favorable and unfavorable models.
            # Uses divergence risk as the primary input for tiny gaps, and worst-case
            # (unfavorable) weighting for larger gaps where direction matters more.
            import math
            
            ref_price = avg_strike or current_price
            divergence_pct = self._estimate_settlement_divergence_pct(underlying, ref_price)
            
            if strike_diff_pct <= 0.01:
                # Essentially zero diff — pure settlement divergence risk
                # P(divergence flips outcome) scales with divergence relative to price movement
                vol_annual = self._get_volatility(underlying)
                vol_minute = vol_annual / math.sqrt(525600)
                t_min = max(0.5, time_remaining_minutes) if time_remaining_minutes else 15.0
                sigma_t = vol_minute * math.sqrt(t_min)
                
                if sigma_t > 0:
                    def norm_pdf(x):
                        return math.exp(-x * x / 2) / math.sqrt(2 * math.pi)
                    div_sigmas = (divergence_pct / 100.0) / sigma_t
                    p_div_flip = min(0.30, 2 * div_sigmas * norm_pdf(0) * 0.5)
                    # Required ROI = safety + divergence penalty
                    base_roi = max(4.0, 3.0 + p_div_flip * 100.0)
                else:
                    base_roi = max(4.0, 3.0 + divergence_pct * 50)
            else:
                # Larger gap but direction unknown — conservative blend:
                # Use 70% unfavorable weight, 30% favorable weight
                # This is safe because if we can't determine direction, we're more likely wrong
                fav_roi = self._calculate_favorable_roi(
                    gap_pct=strike_diff_pct,
                    time_remaining_minutes=time_remaining_minutes,
                    underlying=underlying,
                    safety_buffer_pct=3.0,
                    current_price=current_price,
                    avg_strike=avg_strike
                )
                unfav_roi = self._calculate_dynamic_unfavorable_roi(
                    gap_pct=strike_diff_pct,
                    time_remaining_minutes=time_remaining_minutes,
                    underlying=underlying,
                    safety_buffer_pct=3.0,
                    current_price=current_price,
                    avg_strike=avg_strike
                )
                base_roi = 0.70 * unfav_roi + 0.30 * fav_roi
                
                # Hard block above 0.5% diff when direction is unknown
                if strike_diff_pct > 0.5:
                    return 999.0
        
        # Calculate time penalty for late window (applies to ALL directions)
        time_penalty = 0.0
        if time_elapsed_pct >= late_start:
            if time_elapsed_pct >= late_end:
                return 999.0  # No trading after cutoff
            t = (time_elapsed_pct - late_start) / (late_end - late_start)
            time_penalty = late_penalty * t
        
        final_roi = base_roi + time_penalty
        return final_roi
    
    # ==========================================================================
    # LATE WINDOW ASYMMETRIC ARBITRAGE
    # ==========================================================================
    
    def _get_late_asymmetric_roi_threshold(self, strike_diff_pct: float, 
                                            strike_favorable: bool,
                                            time_remaining_seconds: float = None,
                                            underlying: str = None) -> float:
        """
        Dynamically calculate required lottery ROI for late asymmetric trades.
        
        NEUTRAL: |diff| < 0.01% → flat 6% (oracles essentially agree)
        
        FAVORABLE: Exponential decay from 6% baseline → 0.5% floor
                   (larger gap = more protection = less ROI needed)
        
        UNFAVORABLE: Gentle linear increase from 6% → 8% cap
                     (at 91¢+ prices, market is near-certain, gap risk is low)
        """
        cfg = CONFIG.get("late_asymmetric", {})
        
        # Configurable parameters
        neutral_threshold = cfg.get("neutral_threshold_pct", 0.01)
        neutral_roi = cfg.get("neutral_roi", 6.0)
        
        favorable_floor = cfg.get("favorable_floor_roi", 0.5)
        favorable_decay = cfg.get("favorable_decay_rate", 25.0)
        
        unfavorable_max = cfg.get("unfavorable_max_roi", 8.0)
        unfavorable_slope = cfg.get("unfavorable_slope", 20.0)
        unfavorable_block = cfg.get("unfavorable_block_pct", 0.15)
        
        # NEUTRAL ZONE: |diff| < threshold
        if strike_diff_pct < neutral_threshold:
            return neutral_roi
        
        if strike_favorable:
            # FAVORABLE: Exponential decay from neutral_roi toward floor
            # roi = floor + (neutral - floor) * exp(-decay * diff)
            roi = favorable_floor + (neutral_roi - favorable_floor) * math.exp(-favorable_decay * strike_diff_pct)
            return max(favorable_floor, roi)
        
        else:
            # UNFAVORABLE: Gentle linear increase, capped
            # At 91¢+ prices, market is already near-certain of outcome
            if strike_diff_pct >= unfavorable_block:
                return 999.0
            
            roi = neutral_roi + unfavorable_slope * strike_diff_pct
            return min(unfavorable_max, roi)
    
    def calculate_asymmetric_sizing(self, pm_price: float, k_price: float) -> dict:
        """
        Calculate position sizes for late window arbitrage strategy.
        
        Simple logic: Buy 5 contracts on BOTH sides for equal $5 payouts.
        Profit = $5 payout - total cost (only positive when PM + K < $1)
        
        Returns dict with sizing details and expected outcomes.
        """
        # Fixed 5 contracts on each side (PM minimum, equal payouts)
        pm_contracts = 5
        k_contracts = 5
        
        # Costs
        pm_cost = pm_contracts * pm_price
        k_cost = k_contracts * k_price
        total_cost = pm_cost + k_cost
        
        # Fees - use same formulas as main bot
        # PM: fee_bps/10000 * price * (1-price), default 200bps = 2%
        pm_fee_bps = 200  # 2% for 15-min crypto markets
        pm_fee_rate = (pm_fee_bps / 10000) * pm_price * (1 - pm_price)
        pm_fee = pm_contracts * pm_fee_rate
        
        # K: 7% of price * (1-price), rounded up to nearest cent
        k_fee_rate = math.ceil(0.07 * k_price * (1 - k_price) * 100) / 100
        k_fee = k_contracts * k_fee_rate
        
        # Payout is always $5 (5 contracts × $1)
        payout = 5.0
        
        # Profit if PM wins: $5 payout - PM fee - total cost
        profit_if_pm_wins = payout - pm_fee - total_cost
        
        # Profit if K wins: $5 payout - K fee - total cost
        profit_if_k_wins = payout - k_fee - total_cost
        
        # Gross edge before fees (simple: $1 - combined price per contract × 5)
        gross_edge = (1.0 - pm_price - k_price) * 5
        
        # ROI based on profit / cost
        roi = (profit_if_pm_wins / total_cost) * 100 if total_cost > 0 else 0
        
        # Valid when there's positive expected profit (PM + K < $1)
        is_valid = (pm_price + k_price < 1.0 and 
                   pm_contracts >= 5 and 
                   pm_cost >= 1.0 and
                   profit_if_pm_wins > 0 and 
                   profit_if_k_wins > 0)
        
        return {
            'pm_contracts': pm_contracts,
            'pm_cost': pm_cost,
            'pm_price': pm_price,
            'k_contracts': k_contracts,
            'k_cost': k_cost,
            'k_price': k_price,
            'total_cost': total_cost,
            'gross_edge': gross_edge,
            'profit_if_favorite': profit_if_pm_wins,  # Keep old key names for compatibility
            'profit_if_longshot': profit_if_k_wins,
            'roi_if_longshot': roi,
            'is_valid': is_valid,
        }
    
    async def find_late_asymmetric_opportunities(self, pairs: list) -> list[ScoredOpportunity]:
        """
        Find late window asymmetric arbitrage opportunities.
        Only active when: 85% elapsed AND PM ≥ 91¢ AND K ≤ 9¢
        
        Returns opportunities with asymmetric sizing pre-calculated.
        """
        if not CONFIG.get("late_asymmetric", {}).get("enabled", False):
            return []
        
        opportunities = []
        cfg = CONFIG["late_asymmetric"]
        
        for pair in pairs:
            if not pair.is_valid or not pair.polymarket.token_id:
                continue
            
            # HARD REQUIREMENT: Kalshi strike must be confirmed (no assumed strikes in late window)
            if pair.kalshi.strike_price is None:
                continue
            
            # Check time window: 85% → 15s remaining
            time_to_close = pair.polymarket.time_to_close
            if time_to_close is None or time_to_close <= 0:
                continue
                
            window_minutes = 15  # Assuming 15-min markets
            window_seconds = window_minutes * 60
            pct_elapsed = 1 - (time_to_close / window_seconds)
            
            start_pct = cfg.get("window_start_pct", 85) / 100.0
            end_seconds = cfg.get("window_end_seconds", 15)
            
            if pct_elapsed < start_pct or time_to_close < end_seconds:
                continue
            
            underlying = pair.polymarket.underlying
            
            # Check trade limit for this underlying
            max_trades = cfg.get("max_trades_per_window", 1)
            current_trades = self._late_asymmetric_trade_counts.get(underlying, 0)
            if current_trades >= max_trades:
                continue
            
            # Check cumulative position cap (same as arb - prevents position pile-up)
            max_pos = CONFIG["markets"].get("max_position_per_underlying", 30)
            cum_pos = self._cumulative_positions.get(underlying, {}).get("size", 0)
            if cum_pos + 5 > max_pos:  # 5 = late_asym contract size
                log_print(f"  [ASYM] BLOCKED {underlying}: cumulative {cum_pos}+5 > {max_pos} cap")
                continue
            
            # Get orderbooks using same pattern as main bot
            try:
                pm_book, k_book = await asyncio.gather(
                    self.pm.get_orderbook(pair.polymarket.market_id),
                    self.kalshi.get_orderbook(pair.kalshi.market_id)
                )
                if not pm_book or not k_book:
                    continue
            except Exception:
                continue
            
            # Check both arb directions: PM one way + K opposite way
            # PM YES + K NO, or PM NO + K YES
            for pm_dir, k_dir in [(Direction.UP, Direction.DOWN), (Direction.DOWN, Direction.UP)]:
                # Get ask prices (what we'd pay to buy)
                if pm_dir == Direction.UP:
                    pm_ask = float(pm_book.best_yes_ask) if pm_book.best_yes_ask else 0
                else:
                    pm_ask = float(pm_book.best_no_ask) if pm_book.best_no_ask else 0
                
                if k_dir == Direction.UP:
                    k_ask = float(k_book.best_yes_ask) if k_book.best_yes_ask else 0
                else:
                    k_ask = float(k_book.best_no_ask) if k_book.best_no_ask else 0
                
                if pm_ask == 0 or k_ask == 0:
                    continue
                
                combined = pm_ask + k_ask
                min_pm = cfg.get("pm_min_price", 0.91)
                
                # Log near-misses to file only (for strategy review)
                dir_label = f"PM_{'YES' if pm_dir == Direction.UP else 'NO'}_K_{'YES' if k_dir == Direction.UP else 'NO'}"
                if pm_ask >= 0.85 and combined <= 1.05:  # Close to being valid
                    log_key = f"late_arb_near_{underlying}_{dir_label}"
                    if not hasattr(self, '_asym_near_log') or not isinstance(self._asym_near_log, dict):
                        self._asym_near_log = {}
                    last_log = self._asym_near_log.get(log_key, 0)
                    if time.time() - last_log > 10:  # Log every 10s max
                        edge = (1.0 - combined) * 100
                        pm_ok = "✓" if pm_ask >= min_pm else "✗"
                        arb_ok = "✓" if combined < 1.0 else "✗"
                        logger.log_text(f"[LATE-ARB] {underlying} {dir_label}: PM@{pm_ask:.2f}{pm_ok} K@{k_ask:.2f} = ${combined:.2f}{arb_ok} ({edge:+.1f}%)")
                        self._asym_near_log[log_key] = time.time()
                
                # STEP 1: Is there an arb? (combined < $1)
                if combined >= 1.0:
                    continue
                
                # STEP 2: Is PM the expensive side? (required for $5 minimum sizing)
                if pm_ask < min_pm:
                    continue
                
                # Calculate asymmetric sizing
                sizing = self.calculate_asymmetric_sizing(pm_ask, k_ask)
                
                if not sizing['is_valid']:
                    continue
                
                # Determine strike favorability for this direction
                # For PM UP: favorable if PM_strike < K_strike (price going up helps us)
                # For PM DOWN: favorable if PM_strike > K_strike (price going down helps us)
                pm_strike = pair.polymarket.strike_price
                k_strike = pair.kalshi.strike_price
                strike_diff_pct = abs(pm_strike - k_strike) / pm_strike * 100 if pm_strike else 0
                
                if pm_dir == Direction.UP:
                    strike_favorable = pm_strike < k_strike if pm_strike and k_strike else False
                else:
                    strike_favorable = pm_strike > k_strike if pm_strike and k_strike else False
                
                # Check ROI threshold
                required_roi = self._get_late_asymmetric_roi_threshold(
                    strike_diff_pct, strike_favorable, time_to_close, underlying
                )
                
                if sizing['roi_if_longshot'] < required_roi:
                    continue
                
                # Calculate basic score for sorting (higher ROI = higher score)
                score = 50 + sizing['roi_if_longshot'] * 2  # Base 50 + ROI bonus
                
                # Create opportunity with asymmetric flag
                opp = ScoredOpportunity(
                    market_pair=pair,
                    first_leg_venue=Venue.POLYMARKET,
                    first_leg_direction=pm_dir,
                    first_leg_price=Decimal(str(pm_ask)),
                    first_leg_market_id=pair.polymarket.market_id,
                    second_leg_venue=Venue.KALSHI,
                    second_leg_direction=k_dir,
                    second_leg_price=Decimal(str(k_ask)),
                    second_leg_market_id=pair.kalshi.market_id,
                    divergence_pct=Decimal(str(1 - pm_ask - k_ask)),  # Spread
                    minutes_since_open=(window_seconds - time_to_close) / 60,
                    price_extremity=Decimal(str(abs(pm_ask - 0.5))),
                    score=score,
                    pm_implied_prob=Decimal(str(pm_ask)) if pm_dir == Direction.UP else Decimal(str(1 - pm_ask)),
                    k_implied_prob=Decimal(str(1 - k_ask)) if k_dir == Direction.DOWN else Decimal(str(k_ask)),
                    lagging_venue=Venue.KALSHI,  # K is always the longshot in late asymmetric
                    raw_edge_pct=Decimal(str(sizing['roi_if_longshot'])),
                    net_edge_pct=Decimal(str(sizing['roi_if_longshot'])),
                    est_profit_per_contract=Decimal(str(sizing['profit_if_longshot'] / sizing['pm_contracts'])),
                    is_late_window_trade=True,
                    is_late_arb=True,
                    strike_favorable=strike_favorable,
                    strike_diff_pct=strike_diff_pct,
                    is_asymmetric=True,
                    asymmetric_sizing=sizing,
                )
                
                opportunities.append(opp)
                
                # Log discovery (throttled)
                if not hasattr(self, '_last_asym_log') or time.time() - self._last_asym_log > 10:
                    pm_dir_str = "YES" if pm_dir == Direction.UP else "NO"
                    k_dir_str = "YES" if k_dir == Direction.UP else "NO"
                    log_print(f"  [LATE-ARB] Found: {underlying} PM_{pm_dir_str}@{pm_ask:.2f} + K_{k_dir_str}@{k_ask:.2f} "
                              f"= ${pm_ask + k_ask:.2f} Edge:${sizing['gross_edge']:.2f} "
                              f"{'FAV' if strike_favorable else 'UNFAV'} {strike_diff_pct:.4f}%")
                    self._last_asym_log = time.time()
        
        # Sort by edge (highest first)
        opportunities.sort(key=lambda x: float(x.net_edge_pct), reverse=True)
        
        return opportunities
    
    async def execute_late_asymmetric(self, opp: ScoredOpportunity) -> bool:
        """
        Execute a late window arbitrage trade.
        
        Uses equal contract sizes per leg (5 each for $5 payout either way):
        - PM: 5 contracts at high price
        - K: 5 contracts at low price
        - Profit = $5 - total cost (when PM + K < $1)
        """
        underlying = opp.market_pair.polymarket.underlying
        sizing = opp.asymmetric_sizing
        
        # Check trade lock (shared with main bot)
        now = time.time()
        if hasattr(self, '_trade_lock_until') and now < self._trade_lock_until:
            remaining = self._trade_lock_until - now
            log_print(f"  [ASYM] BLOCKED: Trade lock active for {remaining:.1f}s")
            return False
        
        # Lock for 60 seconds (shorter than main bot - simpler trade)
        self._trade_lock_until = now + 60
        
        try:
            return await self._execute_late_asymmetric_inner(opp)
        finally:
            # Release lock with 5s cooldown
            self._trade_lock_until = time.time() + 5
    
    async def _execute_late_asymmetric_inner(self, opp: ScoredOpportunity) -> bool:
        """Inner execution for late asymmetric trade."""
        underlying = opp.market_pair.polymarket.underlying
        sizing = opp.asymmetric_sizing
        
        pm_size = sizing['pm_contracts']
        k_size = sizing['k_contracts']
        
        log_print(f"\n  🎰 LATE WINDOW ARB: {underlying}")
        log_print(f"      PM: {pm_size} contracts @ ${sizing['pm_price']:.2f} = ${sizing['pm_cost']:.2f}")
        log_print(f"      K:  {k_size} contracts @ ${sizing['k_price']:.2f} = ${sizing['k_cost']:.2f}")
        log_print(f"      Total: ${sizing['total_cost']:.2f} | Edge: ${sizing['gross_edge']:.2f}")
        log_print(f"      If PM wins: +${sizing['profit_if_favorite']:.2f} | If K wins: +${sizing['profit_if_longshot']:.2f}")
        
        # Capital check - need total_cost plus buffer
        threshold = CONFIG["capital_protection"]["min_balance_threshold"]
        try:
            pm_bal = await self.pm.get_balance()
            k_bal = await self.kalshi.get_balance()
            
            pm_needed = sizing['pm_cost'] + threshold
            k_needed = sizing['k_cost'] + threshold
            
            if float(pm_bal.cash) < pm_needed:
                log_print(f"  [ASYM] BLOCKED: PM balance ${pm_bal.cash:.2f} < needed ${pm_needed:.2f}")
                return False
            if float(k_bal.cash) < k_needed:
                log_print(f"  [ASYM] BLOCKED: K balance ${k_bal.cash:.2f} < needed ${k_needed:.2f}")
                return False
        except Exception as e:
            log_print(f"  [ASYM] Balance check failed: {e}")
            return False
        
        # Print trade info
        print(f"\n  🎰 LATE WINDOW ARB: {underlying}")
        print(f"     PM {opp.first_leg_direction.value.upper()} {pm_size}@${sizing['pm_price']:.2f} + "
              f"K {opp.second_leg_direction.value.upper()} {k_size}@${sizing['k_price']:.2f}")
        print(f"     Edge: ${sizing['gross_edge']:.2f} profit either way")
        
        self._last_trade_attempt = datetime.now(timezone.utc)
        window_end = opp.market_pair.polymarket.end_time
        
        # Verify PM market still exists in cache before firing
        pm_market = self.pm._cache.get(opp.first_leg_market_id)
        if not pm_market:
            log_print(f"  [ASYM] BLOCKED: PM market {opp.first_leg_market_id} not found in cache (expired?)")
            return False
        
        # Also check time_to_close is still positive
        ttc = opp.market_pair.polymarket.time_to_close
        if ttc is not None and ttc <= 5:
            log_print(f"  [ASYM] BLOCKED: PM market closing in {ttc:.0f}s - too risky")
            return False
        
        # Execute both legs in parallel with DIFFERENT sizes
        async def execute_pm_leg():
            try:
                order = await self.pm.place_market_order(
                    opp.first_leg_market_id, OrderSide.BUY, 
                    opp.first_leg_direction, pm_size
                )
                if order:
                    # Check for phantom fill (order went through but we don't have real order_id)
                    is_phantom = order.order_id.startswith("PHANTOM_")
                    if is_phantom:
                        log_print(f"      [PM] ⚠️ PHANTOM FILL - assuming filled based on balance change")
                        dir_str = "down" if opp.first_leg_direction == Direction.DOWN else "up"
                        self.pm.record_fill(opp.first_leg_market_id, pm_size, underlying, dir_str, window_end)
                        return order, pm_size, sizing['pm_price']
                    
                    # Wait for fill confirmation
                    await asyncio.sleep(1.0)
                    fill_info = await self.pm.get_order(order.order_id)
                    fill_size = float(fill_info.get("size_matched", 0) or fill_info.get("filled_count", 0) or 0)
                    
                    if fill_size > 0:
                        dir_str = "down" if opp.first_leg_direction == Direction.DOWN else "up"
                        self.pm.record_fill(opp.first_leg_market_id, int(fill_size), underlying, dir_str, window_end)
                        # Get actual PM fill price
                        pm_actual = sizing['pm_price']
                        try:
                            actual_fills = await self.pm.get_trade_fills(order.order_id)
                            if actual_fills:
                                tv = sum(f["price"] * f["size"] for f in actual_fills)
                                ts = sum(f["size"] for f in actual_fills)
                                if ts > 0:
                                    pm_actual = tv / ts
                        except Exception:
                            pass
                        log_print(f"      [PM] ✓ Filled {int(fill_size)}/{pm_size} @ ${pm_actual:.4f}")
                        return order, fill_size, pm_actual
                    else:
                        log_print(f"      [PM] ⚠️ No fill confirmed")
                        return order, 0, 0
                return None, 0, 0
            except Exception as e:
                log_print(f"      [PM] ❌ Error: {e}")
                return None, 0, 0
        
        async def execute_k_leg():
            try:
                # Cap at expected K price + 10¢ buffer to prevent catastrophic fills
                k_expected = sizing['k_price']
                k_max_price = min(k_expected + 0.10, 0.92)
                order = await self.kalshi.place_market_order(
                    opp.second_leg_market_id, OrderSide.BUY,
                    opp.second_leg_direction, k_size,
                    max_price=k_max_price
                )
                if order:
                    # Wait for fill confirmation
                    await asyncio.sleep(1.0)
                    fill_info = await self.kalshi.get_order(order.order_id)
                    fill_size = float(fill_info.get("filled_count", 0) or 0)
                    
                    if fill_size > 0:
                        dir_str = "yes" if opp.second_leg_direction == Direction.UP else "no"
                        self.kalshi.record_fill(opp.second_leg_market_id, int(fill_size), dir_str, window_end)
                        # Get actual K fill price
                        k_actual = sizing['k_price']
                        k_avg = fill_info.get("avg_fill_price")
                        if k_avg and isinstance(k_avg, (int, float)) and 0.01 <= float(k_avg) <= 0.99:
                            k_actual = float(k_avg)
                        log_print(f"      [K]  ✓ Filled {int(fill_size)}/{k_size} @ ${k_actual:.4f}")
                        return order, fill_size, k_actual
                    else:
                        log_print(f"      [K]  ⚠️ No fill confirmed")
                        return order, 0, 0
                return None, 0, 0
            except Exception as e:
                log_print(f"      [K]  ❌ Error: {e}")
                return None, 0, 0
        
        # Fire both legs simultaneously for speed
        log_print(f"      [ASYM] Firing both legs in parallel...")
        results = await asyncio.gather(execute_pm_leg(), execute_k_leg(), return_exceptions=True)
        
        pm_result = results[0] if not isinstance(results[0], Exception) else (None, 0, 0)
        k_result = results[1] if not isinstance(results[1], Exception) else (None, 0, 0)
        
        pm_order, pm_fill, pm_actual_price = pm_result
        k_order, k_fill, k_actual_price = k_result
        
        # Evaluate results
        pm_success = pm_fill > 0
        k_success = k_fill > 0
        
        if pm_success and k_success:
            # Both filled - success!
            print(f"     ✓ FILLED: PM {int(pm_fill)}, K {int(k_fill)}")
            log_print(f"      [ASYM] ✓ SUCCESS - Both legs filled")
            
            # ═══════════════════════════════════════════════════════════════
            # RE-VERIFY K FILL PRICE: The initial get_order() call (1s after
            # placement) often returns the CEILING/LIMIT price instead of the
            # actual execution price because Kalshi's fills endpoint hasn't
            # populated yet. Re-query with a longer delay to get the real price.
            # ═══════════════════════════════════════════════════════════════
            k_ceiling = min(sizing['k_price'] + 0.10, 0.92)
            if k_order and k_actual_price and abs(k_actual_price - k_ceiling) < 0.005:
                # k_actual_price matches the ceiling — almost certainly the limit price, not the fill
                log_print(f"      [ASYM] K price ${k_actual_price:.4f} matches ceiling ${k_ceiling:.4f} — re-verifying...")
                await asyncio.sleep(2.0)  # Give fills endpoint time to populate
                try:
                    fills = await self.kalshi.get_fills_for_order(k_order.order_id)
                    if fills:
                        order_side = "no" if opp.second_leg_direction == Direction.DOWN else "yes"
                        total_cost = 0
                        total_count = 0
                        for f in fills:
                            count = f.get("count", 0)
                            if order_side == "no":
                                price = f.get("no_price", 0) or f.get("price", 0)
                            else:
                                price = f.get("yes_price", 0) or f.get("price", 0)
                            if count > 0 and price > 0:
                                total_cost += price * count
                                total_count += count
                        if total_count > 0:
                            verified_price = (total_cost / total_count) / 100  # cents → dollars
                            if 0.001 <= verified_price <= 0.99 and verified_price < k_actual_price:
                                log_print(f"      [ASYM] K fill price corrected: ${k_actual_price:.4f} → ${verified_price:.4f}")
                                k_actual_price = verified_price
                            elif 0.001 <= verified_price <= 0.99:
                                log_print(f"      [ASYM] K fill price confirmed: ${verified_price:.4f}")
                                k_actual_price = verified_price
                        else:
                            log_print(f"      [ASYM] ⚠️ Fills endpoint returned no data — using sizing price ${sizing['k_price']:.4f}")
                            k_actual_price = sizing['k_price']
                    else:
                        log_print(f"      [ASYM] ⚠️ No fills returned — using sizing price ${sizing['k_price']:.4f}")
                        k_actual_price = sizing['k_price']
                except Exception as e:
                    log_print(f"      [ASYM] ⚠️ Fill re-verification failed: {e} — using sizing price ${sizing['k_price']:.4f}")
                    k_actual_price = sizing['k_price']
            
            log_print(f"      [ASYM] Actual: PM ${pm_actual_price:.4f} + K ${k_actual_price:.4f} = ${pm_actual_price + k_actual_price:.4f}")
            
            # Increment trade counter
            self._late_asymmetric_trade_counts[underlying] = self._late_asymmetric_trade_counts.get(underlying, 0) + 1
            
            # Update cumulative position tracker (MUST do this or positions pile up as orphans)
            new_pm_dir = opp.first_leg_direction
            new_direction = "bullish" if new_pm_dir == Direction.UP else "bearish"
            cum = self._cumulative_positions.get(underlying, {"size": 0, "direction": new_direction})
            cum["size"] = cum.get("size", 0) + int(min(pm_fill, k_fill))
            cum["direction"] = new_direction
            self._cumulative_positions[underlying] = cum
            max_pos = CONFIG["markets"].get("max_position_per_underlying", 30)
            log_print(f"      [ASYM] Cumulative position: {cum['size']}/{max_pos} {cum['direction']} for {underlying}")
            
            # Log trade for settlement tracking (use actual prices)
            self.record_trade_for_settlement(
                underlying=underlying,
                pm_strike=opp.market_pair.polymarket.strike_price,
                k_strike=opp.market_pair.kalshi.strike_price,
                pm_direction=opp.first_leg_direction.value,
                k_direction=opp.second_leg_direction.value,
                size=pm_fill,  # Use PM size as reference
                pm_price=pm_actual_price or sizing['pm_price'],
                k_price=k_actual_price or sizing['k_price'],
                expected_profit=sizing['profit_if_longshot']  # Optimistic case
            )
            
            # === TRADE JOURNAL ===
            pm_dir_str = "down" if opp.first_leg_direction == Direction.DOWN else "up"
            k_dir_str = "yes" if opp.second_leg_direction == Direction.UP else "no"
            profit_tracker.log_to_journal(
                underlying=underlying,
                strategy="late_asym",
                size=int(min(pm_fill, k_fill)),
                pm_dir=pm_dir_str,
                k_dir=k_dir_str,
                pm_expected=sizing['pm_price'],
                pm_actual=pm_actual_price or sizing['pm_price'],
                k_expected=sizing['k_price'],
                k_actual=k_actual_price or sizing['k_price'],
                strike_diff_pct=float(getattr(opp.market_pair, 'strike_diff_pct', 0) or 0),
                strike_favorable=getattr(opp, 'strike_favorable', True),
                k_ticker=opp.market_pair.kalshi.market_id,
                window_end=opp.market_pair.polymarket.end_time.isoformat() if opp.market_pair.polymarket.end_time else "",
                session_name=logger.session_name if hasattr(logger, 'session_name') else "",
                notes=f"pm_size={int(pm_fill)} k_size={int(k_fill)}",
            )
            
            return True
        
        elif pm_success and not k_success:
            # PM filled but K didn't - try to sell PM back to unwind
            print(f"     ⚠️ PARTIAL: PM filled {int(pm_fill)}, K FAILED")
            log_print(f"      [ASYM] ⚠️ PM filled but K failed - attempting PM sellback...")
            
            # Try to sell PM back with RETRY logic (3 attempts, declining prices)
            pm_market_id = opp.first_leg_market_id
            pm_direction = opp.first_leg_direction
            sellback_success = False
            total_sold = 0
            remaining_to_sell = int(pm_fill)
            
            for attempt in range(3):
                if remaining_to_sell <= 0:
                    sellback_success = True
                    break
                    
                try:
                    # Get fresh orderbook each attempt
                    pm_book = await self.pm.get_orderbook(opp.market_pair.polymarket.market_id)
                    if pm_direction == Direction.UP:
                        pm_bid = pm_book.best_yes_bid if pm_book else None
                    else:
                        pm_bid = pm_book.best_no_bid if pm_book else None
                    
                    if not pm_bid or float(pm_bid) <= 0:
                        log_print(f"      [ASYM] Sellback attempt {attempt+1}/3: No PM bid available")
                        await asyncio.sleep(1.0)
                        continue
                    
                    # Decline price each attempt: 90%, 80%, 70% of bid
                    discount = 0.90 - (attempt * 0.10)
                    sell_price = max(float(pm_bid) * discount, 0.01)
                    log_print(f"      [ASYM] Sellback attempt {attempt+1}/3: {remaining_to_sell} @ ${sell_price:.2f} ({discount*100:.0f}% of bid ${float(pm_bid):.2f})")
                    
                    sell_order = await self.pm.place_order(
                        pm_market_id, OrderSide.SELL, pm_direction,
                        Decimal(str(round(sell_price, 2))),
                        remaining_to_sell
                    )
                    
                    if sell_order:
                        await asyncio.sleep(2.0 + attempt)  # Longer wait each attempt
                        sell_check = await self.pm.get_order(sell_order.order_id)
                        sell_filled = float(sell_check.get("size_matched", 0) or sell_check.get("filled_count", 0) or 0)
                        total_sold += int(sell_filled)
                        remaining_to_sell -= int(sell_filled)
                        
                        if remaining_to_sell <= 0:
                            pm_cost = float(pm_fill) * sizing['pm_price']
                            pm_proceeds = float(total_sold) * sell_price
                            loss = pm_cost - pm_proceeds
                            print(f"     ✓ PM sold back: {total_sold} @ ~${sell_price:.2f} (loss: ~${loss:.2f})")
                            log_print(f"      [ASYM] ✓ PM sellback complete on attempt {attempt+1} - loss: ~${loss:.2f}")
                            sellback_success = True
                        else:
                            log_print(f"      [ASYM] Attempt {attempt+1}: sold {int(sell_filled)}, {remaining_to_sell} remaining")
                            # Cancel unfilled portion before retrying at lower price
                            try:
                                await self.pm.cancel_order(sell_order.order_id)
                            except Exception:
                                pass
                except Exception as e:
                    log_print(f"      [ASYM] Sellback attempt {attempt+1} error: {e}")
                
                await asyncio.sleep(0.5)
            
            if not sellback_success:
                # Couldn't fully unwind - trigger safety pause
                self.trigger_safety_pause(
                    f"[ASYM] PM filled {int(pm_fill)} but K FAILED - sellback incomplete",
                    unhedged={
                        "underlying": underlying,
                        "venue": "PM",
                        "size": int(pm_fill),
                        "direction": opp.first_leg_direction.value
                    }
                )
            return False
        
        elif not pm_success and k_success:
            # K filled but PM didn't - try to sell K back to unwind
            print(f"     ⚠️ PARTIAL: PM FAILED, K filled {int(k_fill)}")
            log_print(f"      [ASYM] ⚠️ K filled but PM failed - attempting K sellback...")
            
            # Try to sell K back with RETRY logic (3 attempts, declining prices)
            k_market_id = opp.second_leg_market_id
            k_direction = opp.second_leg_direction
            sellback_success = False
            total_sold = 0
            remaining_to_sell = int(k_fill)
            
            for attempt in range(3):
                if remaining_to_sell <= 0:
                    sellback_success = True
                    break
                    
                try:
                    # Get fresh orderbook each attempt
                    k_book = await self.kalshi.get_orderbook(k_market_id)
                    if k_direction == Direction.UP:
                        k_bid = k_book.best_yes_bid if k_book else None
                    else:
                        k_bid = k_book.best_no_bid if k_book else None
                    
                    if not k_bid or float(k_bid) <= 0:
                        log_print(f"      [ASYM] K sellback attempt {attempt+1}/3: No K bid available")
                        await asyncio.sleep(1.0)
                        continue
                    
                    # Decline price each attempt: 90%, 80%, 70% of bid
                    discount = 0.90 - (attempt * 0.10)
                    sell_price = max(float(k_bid) * discount, 0.01)
                    log_print(f"      [ASYM] K sellback attempt {attempt+1}/3: {remaining_to_sell} @ ${sell_price:.2f} ({discount*100:.0f}% of bid ${float(k_bid):.2f})")
                    
                    sell_order = await self.kalshi.place_order(
                        k_market_id, OrderSide.SELL, k_direction,
                        Decimal(str(round(sell_price, 2))),
                        Decimal(str(remaining_to_sell))
                    )
                    
                    if sell_order:
                        await asyncio.sleep(1.5 + attempt)
                        sell_check = await self.kalshi.get_order(sell_order.order_id)
                        sell_filled = int(sell_check.get("filled_count", 0) or 0)
                        total_sold += sell_filled
                        remaining_to_sell -= sell_filled
                        
                        if remaining_to_sell <= 0:
                            k_cost = float(k_fill) * sizing['k_price']
                            k_proceeds = float(total_sold) * sell_price
                            loss = k_cost - k_proceeds
                            print(f"     ✓ K sold back: {total_sold} @ ~${sell_price:.2f} (loss: ~${loss:.2f})")
                            log_print(f"      [ASYM] ✓ K sellback complete on attempt {attempt+1} - loss: ~${loss:.2f}")
                            sellback_success = True
                        else:
                            log_print(f"      [ASYM] Attempt {attempt+1}: sold {sell_filled}, {remaining_to_sell} remaining")
                            # Cancel unfilled portion before retrying
                            try:
                                await self.kalshi.cancel_order(sell_order.order_id)
                            except Exception:
                                pass
                except Exception as e:
                    log_print(f"      [ASYM] K sellback attempt {attempt+1} error: {e}")
                
                await asyncio.sleep(0.5)
            
            if not sellback_success:
                # Couldn't fully unwind - trigger safety pause
                self.trigger_safety_pause(
                    f"[ASYM] K filled {int(k_fill)} but PM FAILED - sellback incomplete",
                    unhedged={
                        "underlying": underlying,
                        "venue": "K",
                        "size": int(k_fill),
                        "direction": opp.second_leg_direction.value
                    }
                )
            return False
        
        else:
            # Neither filled
            print(f"     ❌ FAILED: Neither leg filled")
            log_print(f"      [ASYM] ❌ Both legs failed to fill")
            return False
    
    async def find_scored_opportunities(self, pairs: list) -> list[ScoredOpportunity]:
        """Find and score opportunities using momentum strategy"""
        opportunities = []

        # TIME-OF-DAY FILTER: Block trading during historically unprofitable hours
        time_cfg = CONFIG.get("time_filter", {})
        if time_cfg.get("enabled", False):
            current_hour_utc = datetime.now(timezone.utc).hour
            blocked_hours = time_cfg.get("blocked_hours_utc", [])
            if current_hour_utc in blocked_hours:
                if time_cfg.get("log_blocked", False):
                    if not hasattr(self, '_time_filter_logged_hour') or self._time_filter_logged_hour != current_hour_utc:
                        log_print(f"  [TIME FILTER] Blocking trades during UTC hour {current_hour_utc:02d}:00 (in blocked_hours_utc)")
                        self._time_filter_logged_hour = current_hour_utc
                return []

        # Filter pairs with valid tokens
        # Note: Pairs with assumed strikes (Kalshi TBD) are now allowed through pairing logic
        valid_pairs = [p for p in pairs if p.is_valid and p.polymarket.token_id]
        
        # Debug: Show pair counts
        if not hasattr(self, '_last_pair_debug') or time.time() - self._last_pair_debug > 30:
            if len(pairs) != len(valid_pairs):
                logger.log_text(f"[DEBUG] {len(pairs)} pairs input, {len(valid_pairs)} valid")
            self._last_pair_debug = time.time()
        
        if not valid_pairs:
            # Debug: why no valid pairs?
            if pairs:
                invalid_reasons = []
                for p in pairs:
                    if not p.is_valid:
                        if not p.polymarket.is_tradeable:
                            invalid_reasons.append(f"{p.polymarket.underlying}: PM not tradeable (ttc={p.polymarket.time_to_close})")
                        elif not p.kalshi.is_tradeable:
                            invalid_reasons.append(f"{p.polymarket.underlying}: K not tradeable")
                        elif p.confidence < 0.80:
                            invalid_reasons.append(f"{p.polymarket.underlying}: low confidence ({p.confidence})")
                    elif not p.polymarket.token_id:
                        invalid_reasons.append(f"{p.polymarket.underlying}: no token_id")
                if invalid_reasons:
                    logger.log_text(f"[DEBUG] {len(pairs)} pairs but none valid: {invalid_reasons[:3]}")
            return []
        
        # Fetch all orderbooks in parallel (both PM and K fetched concurrently per pair)
        async def fetch_pair_books(pair):
            pmb, kb = await asyncio.gather(
                self.pm.get_orderbook(pair.polymarket.market_id),
                self.kalshi.get_orderbook(pair.kalshi.market_id)
            )
            return (pair, pmb, kb)
        
        results = await asyncio.gather(*[fetch_pair_books(p) for p in valid_pairs])
        
        # Build edge list for dashboard
        self._last_edges = []
        
        # Debug: track why pairs are skipped
        skipped_no_pm_book = []
        skipped_no_k_book = []
        
        for pair, pmb, kb in results:
            try:
                # Need two-sided markets
                if not pmb.best_yes_ask or not pmb.best_yes_bid:
                    skipped_no_pm_book.append(pair.polymarket.underlying)
                    continue
                if not kb.best_yes_ask or not kb.best_yes_bid:
                    skipped_no_k_book.append(pair.polymarket.underlying)
                    continue
                
                # Calculate implied probabilities (YES = UP)
                # Use mid price for probability estimate
                pm_yes_mid = (pmb.best_yes_bid + pmb.best_yes_ask) / 2
                k_yes_mid = (kb.best_yes_bid + kb.best_yes_ask) / 2
                
                pm_implied_up = float(pm_yes_mid)
                k_implied_up = float(k_yes_mid)
                
                # Divergence = how much the venues disagree
                divergence_pct = (pm_implied_up - k_implied_up) * 100
                
                # Time since market opened
                market_duration = 15 * 60  # 15 minutes in seconds
                time_to_close = pair.polymarket.time_to_close
                time_since_open = (market_duration - time_to_close) / 60  # minutes
                
                # Determine which venue is lagging and what to buy
                # Evaluate BOTH directions so dashboard shows all possibilities
                # Direction A: PM UP + K DOWN (buy PM yes ask + K no ask)
                # Direction B: PM DOWN + K UP (buy PM no ask + K yes ask)
                
                pm_up_price = pmb.best_yes_ask
                pm_down_price = pmb.best_no_ask
                k_up_price = kb.best_yes_ask
                k_down_price = kb.best_no_ask
                
                # Skip if any price is missing
                if not pm_up_price or not pm_down_price or not k_up_price or not k_down_price:
                    continue
                
                directions = []
                
                # Direction A: PM UP + K DOWN
                cost_a = float(pm_up_price) + float(k_down_price)
                raw_edge_a = ((1 - cost_a) / cost_a) * 100 if cost_a > 0 else 0  # True ROI
                
                # Direction B: PM DOWN + K UP
                cost_b = float(pm_down_price) + float(k_up_price)
                raw_edge_b = ((1 - cost_b) / cost_b) * 100 if cost_b > 0 else 0  # True ROI
                
                # For each direction, determine first leg based on discount
                # Dir A: PM UP + K DOWN
                if divergence_pct <= 0:
                    # K > PM, PM UP is underpriced
                    pm_up_discount = k_implied_up - float(pm_up_price)
                    k_down_discount = (1 - k_implied_up) - float(k_down_price)
                    if pm_up_discount >= k_down_discount:
                        dir_a = (Venue.POLYMARKET, Direction.UP, pm_up_price, pair.polymarket.market_id,
                                 Venue.KALSHI, Direction.DOWN, k_down_price, pair.kalshi.market_id, float(pm_up_price))
                    else:
                        dir_a = (Venue.KALSHI, Direction.DOWN, k_down_price, pair.kalshi.market_id,
                                 Venue.POLYMARKET, Direction.UP, pm_up_price, pair.polymarket.market_id, float(k_down_price))
                else:
                    pm_up_discount = k_implied_up - float(pm_up_price)
                    k_down_discount = (1 - k_implied_up) - float(k_down_price)
                    if pm_up_discount >= k_down_discount:
                        dir_a = (Venue.POLYMARKET, Direction.UP, pm_up_price, pair.polymarket.market_id,
                                 Venue.KALSHI, Direction.DOWN, k_down_price, pair.kalshi.market_id, float(pm_up_price))
                    else:
                        dir_a = (Venue.KALSHI, Direction.DOWN, k_down_price, pair.kalshi.market_id,
                                 Venue.POLYMARKET, Direction.UP, pm_up_price, pair.polymarket.market_id, float(k_down_price))
                
                directions.append(('PU+KD', dir_a, cost_a, raw_edge_a, Direction.UP))  # PM direction = UP
                
                # Dir B: PM DOWN + K UP
                if divergence_pct > 0:
                    k_up_discount = pm_implied_up - float(k_up_price)
                    pm_down_discount = (1 - pm_implied_up) - float(pm_down_price)
                    if k_up_discount >= pm_down_discount:
                        dir_b = (Venue.KALSHI, Direction.UP, k_up_price, pair.kalshi.market_id,
                                 Venue.POLYMARKET, Direction.DOWN, pm_down_price, pair.polymarket.market_id, float(k_up_price))
                    else:
                        dir_b = (Venue.POLYMARKET, Direction.DOWN, pm_down_price, pair.polymarket.market_id,
                                 Venue.KALSHI, Direction.UP, k_up_price, pair.kalshi.market_id, float(pm_down_price))
                else:
                    k_up_discount = pm_implied_up - float(k_up_price)
                    pm_down_discount = (1 - pm_implied_up) - float(pm_down_price)
                    if k_up_discount >= pm_down_discount:
                        dir_b = (Venue.KALSHI, Direction.UP, k_up_price, pair.kalshi.market_id,
                                 Venue.POLYMARKET, Direction.DOWN, pm_down_price, pair.polymarket.market_id, float(k_up_price))
                    else:
                        dir_b = (Venue.POLYMARKET, Direction.DOWN, pm_down_price, pair.polymarket.market_id,
                                 Venue.KALSHI, Direction.UP, k_up_price, pair.kalshi.market_id, float(pm_down_price))
                
                directions.append(('PD+KU', dir_b, cost_b, raw_edge_b, Direction.DOWN))  # PM direction = DOWN
                
                # Process both directions
                best_opp = None
                best_net_edge = -999
                
                for dir_label, dir_info, total_cost, raw_edge_pct, pm_dir_for_fav in directions:
                    first_venue, first_dir, first_price, first_market_id, second_venue, second_dir, second_price, second_market_id, value_price = dir_info
                
                    # Calculate potential edge if arb completes
                    # Raw edge = $1 payout - cost of both legs
                    # (total_cost and raw_edge_pct already computed above)
                
                    # Periodic debug: log best edges every 30 seconds
                    if not hasattr(self, '_last_edge_debug') or time.time() - self._last_edge_debug > 30:
                        if raw_edge_pct > 0:
                            if not hasattr(self, '_edge_debug_buffer'):
                                self._edge_debug_buffer = []
                            self._edge_debug_buffer.append(f"{pair.polymarket.underlying}:{raw_edge_pct:.1f}%({dir_label})")
                
                    # Exact fee calculation using official platform formulas
                    # NO slippage estimate - prices come from live orderbook asks
                    
                    # Gross profit before fees
                    gross_profit = 1.0 - total_cost
                
                    # FEES:
                    # PM fee: feeRateBps/10000 * price * (1-price) per contract
                    # Use cached fee rate from actual orders if available, else default 200bps
                    pm_price = float(second_price) if second_venue == Venue.POLYMARKET else float(first_price)
                    pm_market_id = second_market_id if second_venue == Venue.POLYMARKET else first_market_id
                    pm_fee_bps = 200  # default
                    if hasattr(self, '_pm') and hasattr(self._pm, '_token_fee_rates'):
                        cached_bps = self._pm._token_fee_rates.get(pm_market_id)
                        if cached_bps is not None:
                            pm_fee_bps = cached_bps
                    pm_fee = (pm_fee_bps / 10000) * pm_price * (1.0 - pm_price)
                    
                    # Kalshi taker fee: roundup(0.07 * P * (1-P)) per contract
                    k_price = float(first_price) if second_venue == Venue.POLYMARKET else float(second_price)
                    k_fee = math.ceil(0.07 * k_price * (1.0 - k_price) * 100) / 100
                    
                    total_fees = pm_fee + k_fee
                
                    # Net profit per contract
                    net_profit_per_contract = gross_profit - total_fees
                    
                    # TRUE ROI = (profit / cost) * 100
                    if total_cost > 0:
                        net_edge_pct = (net_profit_per_contract / total_cost) * 100
                    else:
                        net_edge_pct = 0
                
                    # Calculate score
                    score, components = self._calculate_score(
                        divergence_pct, 
                        time_since_open,
                        value_price
                    )
                
                    # Time elapsed percentage
                    market_duration_minutes = 15
                    time_elapsed_pct = time_since_open / market_duration_minutes
                    time_elapsed_pct = max(0.0, min(time_elapsed_pct, (CONFIG["strategy"].get("late_market_cutoff_pct", 95) / 100.0) - 0.01))
                
                    # STRIKE FAVORABILITY CHECK
                    is_strike_favorable = False
                    is_strike_unfavorable = False
                    pm_strike_val = getattr(pair.polymarket, 'strike_price', None)
                    k_strike_val = getattr(pair.kalshi, 'strike_price', None)
                    strike_diff = getattr(pair, 'strike_diff_pct', 0.0)
                
                    # Always determine favorability based on strike direction, even for tiny diffs
                    # This ensures we don't give ROI bonuses to unfavorable trades
                    if pm_strike_val and k_strike_val and strike_diff:
                        if pm_strike_val < k_strike_val:
                            is_strike_favorable = (pm_dir_for_fav == Direction.UP)
                        elif pm_strike_val > k_strike_val:
                            is_strike_favorable = (pm_dir_for_fav == Direction.DOWN)
                        # If strikes are exactly equal, leave both False (truly neutral)
                    
                        is_strike_unfavorable = not is_strike_favorable and (pm_strike_val != k_strike_val)
                    
                        # Hard block unfavorable when diff > 0.5%
                        if is_strike_unfavorable and strike_diff > 0.5:
                            continue
                
                    # Store for dashboard - show BOTH directions
                    # Calculate time remaining for dynamic threshold
                    time_remaining_min = max(0.5, 15.0 - time_since_open)
                    
                    # Get current spot price from RTDS for settlement divergence calculation
                    _rtds_price = None
                    _avg_strike = None
                    try:
                        _sym = f"{pair.polymarket.underlying.lower()}/usd"
                        _rtds_price = self._rtds.get_current_price(_sym) if hasattr(self, '_rtds') and self._rtds else None
                    except:
                        pass
                    # Average of PM and K strikes as reference for divergence %
                    if pm_strike_val and k_strike_val:
                        _avg_strike = (pm_strike_val + k_strike_val) / 2.0
                    elif pm_strike_val:
                        _avg_strike = pm_strike_val
                    
                    required_roi = self._get_required_roi_for_strike_diff(
                        strike_diff, time_elapsed_pct, is_strike_favorable,
                        underlying=pair.polymarket.underlying,
                        time_remaining_minutes=time_remaining_min,
                        current_price=_rtds_price,
                        avg_strike=_avg_strike
                    )
                    
                    # Debug: log when 999 appears
                    if required_roi >= 999 and not hasattr(self, '_debug_999_logged'):
                        log_print(f"  [DEBUG 999] asset={pair.polymarket.underlying} diff={strike_diff:.6f} time={time_elapsed_pct:.4f} fav={is_strike_favorable} unfav={is_strike_unfavorable} pm_strike={pm_strike_val} k_strike={k_strike_val}")
                        self._debug_999_logged = True
                
                    self._last_edges.append({
                        "asset": pair.polymarket.underlying,
                        "divergence": divergence_pct,
                        "pm_implied_up": pm_implied_up,
                        "k_implied_up": k_implied_up,
                        "first_leg": f"{first_venue.value[:1].upper()}{first_dir.value[:1].upper()}",
                        "first_price": float(first_price),
                        "second_leg": f"{second_venue.value[:1].upper()}{second_dir.value[:1].upper()}",
                        "second_price": float(second_price),
                        "total_cost": total_cost,
                        "score": score,
                        "edge": net_edge_pct,
                        "raw_edge": raw_edge_pct,
                        "minutes": time_since_open,
                        "est_profit": net_profit_per_contract * 5,  # Flat 5 contracts
                        "strike_diff": strike_diff,
                        "req_roi": required_roi,
                        "time_elapsed_pct": time_elapsed_pct,
                        "strike_favorable": is_strike_favorable,
                        "dir_label": dir_label,
                    })
                    
                    # Track best direction for trading
                    if net_edge_pct > best_net_edge:
                        best_net_edge = net_edge_pct
                        best_opp = {
                            "first_venue": first_venue, "first_dir": first_dir, "first_price": first_price,
                            "first_market_id": first_market_id, "second_venue": second_venue, "second_dir": second_dir,
                            "second_price": second_price, "second_market_id": second_market_id, "value_price": value_price,
                            "total_cost": total_cost, "raw_edge_pct": raw_edge_pct, "net_edge_pct": net_edge_pct,
                            "score": score, "components": components, "net_profit_per_contract": net_profit_per_contract,
                            "is_strike_favorable": is_strike_favorable, "is_strike_unfavorable": is_strike_unfavorable,
                            "strike_diff": strike_diff, "time_elapsed_pct": time_elapsed_pct, "required_roi": required_roi,
                        }
                
                # After evaluating both directions, use the best one for trading logic
                if best_opp is None:
                    continue
                
                first_venue = best_opp["first_venue"]
                first_dir = best_opp["first_dir"]
                first_price = best_opp["first_price"]
                first_market_id = best_opp["first_market_id"]
                second_venue = best_opp["second_venue"]
                second_dir = best_opp["second_dir"]
                second_price = best_opp["second_price"]
                second_market_id = best_opp["second_market_id"]
                value_price = best_opp["value_price"]
                total_cost = best_opp["total_cost"]
                raw_edge_pct = best_opp["raw_edge_pct"]
                net_edge_pct = best_opp["net_edge_pct"]
                score = best_opp["score"]
                components = best_opp["components"]
                net_profit_per_contract = best_opp["net_profit_per_contract"]
                is_strike_favorable = best_opp["is_strike_favorable"]
                is_strike_unfavorable = best_opp["is_strike_unfavorable"]
                strike_diff = best_opp["strike_diff"]
                time_elapsed_pct = best_opp["time_elapsed_pct"]
                required_roi = best_opp["required_roi"]
                if raw_edge_pct >= 4.0:  # Log edges >= 4% raw for dashboard
                    # Determine why it wasn't picked (if it wasn't)
                    filter_reason = None
                    if score < CONFIG["strategy"]["min_score_to_trade"]:
                        filter_reason = f"score<{CONFIG['strategy']['min_score_to_trade']}"
                    elif net_edge_pct < required_roi:
                        filter_reason = f"edge<{required_roi:.0f}%"
                    elif time_elapsed_pct * 100 >= CONFIG["strategy"].get("late_market_cutoff_pct", 95):
                        filter_reason = "late_window"
                    else:
                        # Check can_trade conditions (cooldown, trade limits, orphans, etc)
                        can, can_reason = self.can_trade(pair, net_edge_pct, is_strike_favorable)
                        if not can:
                            filter_reason = can_reason
                        elif CONFIG["capital_protection"]["enabled"] and self._pm_balance and self._k_balance:
                            # Quick capital protection check for dashboard accuracy
                            pm_bal = float(self._pm_balance.cash)
                            k_bal = float(self._k_balance.cash)
                            threshold = CONFIG["capital_protection"]["min_balance_threshold"]
                            if pm_bal < threshold:
                                filter_reason = f"PM bal ${pm_bal:.0f}<${threshold}"
                            elif k_bal < threshold:
                                filter_reason = f"K bal ${k_bal:.0f}<${threshold}"
                    
                    self._recent_edges_history.append({
                        "time": datetime.now(timezone.utc),
                        "asset": pair.polymarket.underlying,
                        "raw_edge": raw_edge_pct,
                        "net_edge": net_edge_pct,
                        "score": score,
                        "first_leg": f"{first_venue.value[:1].upper()}{first_dir.value[:1].upper()}",
                        "filter_reason": filter_reason,  # None if tradeable
                    })
                    # Keep only last 10 (we display 3, but keep more for history)
                    self._recent_edges_history = self._recent_edges_history[-10:]
                
                # Create opportunity if score is meaningful
                if score >= 10:  # Log opportunities with decent scores
                    # Check if we're too late in the market window
                    market_duration_minutes = 15  # 15-minute markets
                    pct_elapsed = (time_since_open / market_duration_minutes) * 100
                    late_cutoff = CONFIG["strategy"].get("late_market_cutoff_pct", 95)
                    if pct_elapsed >= late_cutoff:
                        # Too late in market window - skip
                        if score >= 35:
                            self._log_filtered(pair.polymarket.underlying, score, net_edge_pct, 
                                f"late_market ({pct_elapsed:.0f}% >= {late_cutoff}%)")
                        continue
                    
                    # Check tiered ROI requirement based on strike difference AND time
                    strike_diff = getattr(pair, 'strike_diff_pct', 0.0)
                    is_assumed_strike = getattr(pair, 'assumed_strike', False)
                    
                    # Check if this is a late window trade (60-90%)
                    late_window_start = CONFIG["strategy"].get("late_window_start_pct", 60)
                    is_late_window_trade = pct_elapsed >= late_window_start
                    
                    # Get required ROI - higher for assumed strikes, and scales with time in late window
                    if is_assumed_strike:
                        # Use higher minimum when we don't have confirmed Kalshi strike
                        required_roi = CONFIG["markets"].get("assumed_strike_min_edge_pct", 7.0)
                        # Also add late window penalty if applicable
                        if is_late_window_trade:
                            late_end = late_cutoff / 100.0
                            late_start = late_window_start / 100.0
                            late_penalty = CONFIG["strategy"].get("late_window_roi_penalty", 18.0)
                            t = (pct_elapsed/100.0 - late_start) / (late_end - late_start)
                            required_roi += late_penalty * t
                    else:
                        time_remaining_min = max(0.5, 15.0 - time_since_open)
                        # Get price data for settlement divergence calculation
                        _rtds_price2 = None
                        _avg_strike2 = None
                        try:
                            _sym2 = f"{pair.polymarket.underlying.lower()}/usd"
                            _rtds_price2 = self._rtds.get_current_price(_sym2) if hasattr(self, '_rtds') and self._rtds else None
                        except:
                            pass
                        pm_s = getattr(pair.polymarket, 'strike_price', None)
                        k_s = getattr(pair.kalshi, 'strike_price', None)
                        if pm_s and k_s:
                            _avg_strike2 = (pm_s + k_s) / 2.0
                        elif pm_s:
                            _avg_strike2 = pm_s
                        required_roi = self._get_required_roi_for_strike_diff(
                            strike_diff, pct_elapsed / 100.0, is_strike_favorable,
                            underlying=pair.polymarket.underlying,
                            time_remaining_minutes=time_remaining_min,
                            current_price=_rtds_price2,
                            avg_strike=_avg_strike2
                        )
                    
                    # EXECUTION SLIPPAGE BUFFER: Reduce effective edge to account for
                    # real-world execution costs (orderbook movement, fill latency)
                    exec_cfg = CONFIG.get("execution_buffer", {})
                    adjusted_net_edge = net_edge_pct
                    if exec_cfg.get("enabled", False):
                        slippage_deduction = exec_cfg.get("kalshi_slippage_pct", 0) + exec_cfg.get("pm_slippage_pct", 0)
                        adjusted_net_edge = net_edge_pct - slippage_deduction

                    # MINIMUM STRIKE MARGIN: Ensure spot is far enough from strike
                    # to avoid pure coin-flip trades where model has no edge
                    margin_cfg = CONFIG.get("min_strike_margin", {})
                    if margin_cfg.get("enabled", False):
                        _spot_for_margin = _rtds_price2
                        _strike_for_margin = _avg_strike2
                        if _spot_for_margin and _strike_for_margin and _strike_for_margin > 0:
                            spot_strike_pct = abs(_spot_for_margin - _strike_for_margin) / _strike_for_margin * 100
                            min_margin = margin_cfg.get("min_pct", 0.5)
                            if spot_strike_pct < min_margin:
                                if margin_cfg.get("log_blocked", False) and score >= 35:
                                    self._log_filtered(pair.polymarket.underlying, score, net_edge_pct,
                                        f"strike_margin ({spot_strike_pct:.3f}% < {min_margin}% min)")
                                continue

                    if adjusted_net_edge < required_roi:
                        # ROI doesn't justify the risk
                        if score >= 35:
                            fav_tag = " [FAV]" if is_strike_favorable else (" [UNFAV]" if is_strike_unfavorable else "")
                            slippage_note = f" adj={adjusted_net_edge:.1f}%" if adjusted_net_edge != net_edge_pct else ""
                            if is_assumed_strike:
                                self._log_filtered(pair.polymarket.underlying, score, net_edge_pct,
                                    f"roi_too_low (need {required_roi:.1f}% for assumed strike{slippage_note})")
                            elif is_late_window_trade:
                                self._log_filtered(pair.polymarket.underlying, score, net_edge_pct,
                                    f"roi_too_low (need {required_roi:.1f}% for {strike_diff:.4f}% diff + {pct_elapsed:.0f}% elapsed{fav_tag}{slippage_note})")
                            else:
                                self._log_filtered(pair.polymarket.underlying, score, net_edge_pct,
                                    f"roi_too_low (need {required_roi:.1f}% for {strike_diff:.4f}% strike diff{fav_tag}{slippage_note})")
                        continue
                    
                    # Check if Kalshi leg would hit minimum order size ($1 minimum)
                    # Polymarket has NO minimum order size — only Kalshi enforces ~$1
                    trade_size = 5  # Flat 5 contracts
                    k_min_viable_price = 1.0 / trade_size  # $0.20 for 5 contracts
                    
                    # Only check the Kalshi leg for minimum order
                    if second_venue == Venue.KALSHI and float(second_price) < k_min_viable_price:
                        if score >= 35:
                            self._log_filtered(pair.polymarket.underlying, score, net_edge_pct,
                                f"min_order (K leg2 ${float(second_price)*trade_size:.2f}<$1)")
                        continue
                    
                    if first_venue == Venue.KALSHI and float(first_price) < k_min_viable_price:
                        if score >= 35:
                            self._log_filtered(pair.polymarket.underlying, score, net_edge_pct,
                                f"min_order (K leg1 ${float(first_price)*trade_size:.2f}<$1)")
                        continue
                    
                    # First leg is the "better value" side - execute it first
                    # Both venues can now be first leg since proxy enables PM chase/amend
                    # If first leg fills but second doesn't:
                    #   - K second: chase with amends (existing logic)
                    #   - PM second: chase with cancel+reorder at higher price
                    # Unwind: always sell on K (easy, no minimums)
                    
                    opp = ScoredOpportunity(
                        market_pair=pair,
                        first_leg_venue=first_venue,
                        first_leg_direction=first_dir,
                        first_leg_price=first_price,
                        first_leg_market_id=first_market_id,
                        second_leg_venue=second_venue,
                        second_leg_direction=second_dir,
                        second_leg_price=second_price,
                        second_leg_market_id=second_market_id,
                        divergence_pct=Decimal(str(divergence_pct)),
                        minutes_since_open=time_since_open,
                        price_extremity=Decimal(str(abs(value_price - 0.5))),
                        score=score,
                        pm_implied_prob=Decimal(str(pm_implied_up)),
                        k_implied_prob=Decimal(str(k_implied_up)),
                        lagging_venue=first_venue,  # First leg venue = the underpriced/lagging one
                        raw_edge_pct=Decimal(str(raw_edge_pct)),
                        net_edge_pct=Decimal(str(net_edge_pct)),
                        est_profit_per_contract=Decimal(str(net_profit_per_contract)),
                        is_late_window_trade=is_late_window_trade,
                        is_late_arb=(pct_elapsed >= 85),
                        strike_favorable=is_strike_favorable,
                        strike_diff_pct=strike_diff if strike_diff else 0.0,
                    )
                    opportunities.append(opp)
                    
            except Exception as e:
                # DON'T silently swallow errors - log them!
                if not hasattr(self, '_last_opp_error') or time.time() - self._last_opp_error > 5:
                    logger.log_text(f"[ERROR] Opportunity creation failed for {pair.polymarket.underlying if hasattr(pair, 'polymarket') else '?'}: {type(e).__name__}: {e}")
                    import traceback
                    logger.log_text(f"[ERROR] Traceback: {traceback.format_exc()[-200:]}")
                    self._last_opp_error = time.time()
                continue
        
        # Debug: Log if no edges found (throttled to avoid spam)
        if not self._last_edges and valid_pairs:
            if not hasattr(self, '_last_debug_print') or time.time() - self._last_debug_print > 10:
                if skipped_no_pm_book or skipped_no_k_book:
                    logger.log_text(f"[DEBUG] No edges: {len(valid_pairs)} pairs, no PM book: {skipped_no_pm_book}, no K book: {skipped_no_k_book}")
                else:
                    logger.log_text(f"[DEBUG] No edges from {len(valid_pairs)} valid pairs (orderbooks OK)")
                self._last_debug_print = time.time()
        
        # Sort by score descending
        opportunities.sort(key=lambda x: x.score, reverse=True)
        self._last_edges.sort(key=lambda x: x["score"], reverse=True)
        
        # Log all edges to history file for analysis (throttled to every 30s)
        logger.log_edges(self._last_edges)
        
        # Print edge debug buffer if any (only to log, not console - breaks dashboard)
        if hasattr(self, '_edge_debug_buffer') and self._edge_debug_buffer:
            if not hasattr(self, '_last_edge_debug') or time.time() - self._last_edge_debug > 30:
                self._last_edge_debug = time.time()
                logger.log_text(f"[Edges] Raw: {', '.join(self._edge_debug_buffer[:5])}")
                self._edge_debug_buffer = []
        
        return opportunities
    
    def has_orphan_positions(self, underlying: str) -> tuple:
        """
        Quick check for orphaned positions. Returns (has_orphans, orphan_count).
        Used in pre-filter to prevent trade spam when orphans exist.
        
        IMPORTANT: Only checks positions from the CURRENT window.
        Previous windows' positions are settled/settling and should not block new trades.
        """
        try:
            k_positions = self.kalshi.get_internal_positions()
            pm_positions = self.pm.get_internal_positions_with_metadata()
            
            # Get current window's K ticker for this underlying
            current_k_ticker = None
            if hasattr(self, '_market_pairs'):
                for pair in self._market_pairs:
                    if pair.polymarket.underlying == underlying and pair.kalshi.market_id:
                        current_k_ticker = pair.kalshi.market_id
                        break
            
            # Get K positions for this underlying - ONLY current window ticker
            if current_k_ticker:
                k_yes = sum(p.get("size", 0) for t, p in k_positions.items() 
                           if t == current_k_ticker and p.get("direction") == "yes")
                k_no = sum(p.get("size", 0) for t, p in k_positions.items() 
                          if t == current_k_ticker and p.get("direction") == "no")
            else:
                # Fallback: use all positions for this underlying (old behavior)
                k_yes = sum(p.get("size", 0) for t, p in k_positions.items() 
                           if underlying in t and p.get("direction") == "yes")
                k_no = sum(p.get("size", 0) for t, p in k_positions.items() 
                          if underlying in t and p.get("direction") == "no")
            
            # Get PM positions for this underlying - only current window
            now = datetime.now(timezone.utc)
            pm_up = 0
            pm_down = 0
            for t, p in pm_positions.items():
                if p.get("underlying") != underlying:
                    continue
                # Skip positions from expired windows
                window_end = p.get("window_end")
                if window_end and isinstance(window_end, datetime) and window_end < now:
                    continue
                if p.get("direction") == "up":
                    pm_up += p.get("size", 0)
                elif p.get("direction") == "down":
                    pm_down += p.get("size", 0)
            
            # Check hedged pairs: K YES + PM DOWN, K NO + PM UP
            hedged_yes_down = min(k_yes, pm_down)
            hedged_no_up = min(k_no, pm_up)
            
            # Unhedged amounts
            orphan_k_yes = k_yes - hedged_yes_down
            orphan_k_no = k_no - hedged_no_up
            orphan_pm_up = pm_up - hedged_no_up
            orphan_pm_down = pm_down - hedged_yes_down
            
            total_orphans = orphan_k_yes + orphan_k_no + orphan_pm_up + orphan_pm_down
            return (total_orphans > 0, total_orphans)
        except Exception:
            return (False, 0)  # Allow trade if check fails
    
    def can_trade(self, pair: MarketPair, roi_pct: float = 0.0, strike_favorable: bool = False) -> Tuple[bool, str]:
        """Check if we can trade this pair"""
        if self._active_position:
            return False, "Already have active position"
        
        # Trading must be explicitly enabled after startup
        if not getattr(self, '_trading_enabled', False):
            return False, "Trading not yet enabled"
        
        # Startup cooldown - no trades for first 8 seconds after bot starts
        # (gives time for strikes and orderbooks to populate)
        if hasattr(self, '_startup_time') and self._startup_time:
            startup_elapsed = (datetime.now(timezone.utc) - self._startup_time).total_seconds()
            if startup_elapsed < 8:
                return False, f"Startup cooldown ({max(0, int(8 - startup_elapsed))}s left)"
        
        # Check if PM is Cloudflare blocked
        if hasattr(self, 'pm') and self.pm.is_cloudflare_blocked():
            return False, "🚨 PM CLOUDFLARE BLOCKED - change VPN or wait"
        
        # Check for orphaned OR imbalanced positions that need attention
        if hasattr(self, '_has_orphaned_positions') and self._has_orphaned_positions:
            return False, "🚨 ORPHANED/IMBALANCED POSITIONS - wait for reconciliation"
        
        # Check if reconciliation is in progress (prevents trading during cooldown/reconcile period)
        if hasattr(self, '_reconciliation_in_progress') and self._reconciliation_in_progress:
            return False, "Reconciliation in progress"
        
        # Check time remaining - don't trade after late_market_cutoff_pct of window elapsed
        time_to_close = pair.polymarket.time_to_close  # seconds
        market_duration = pair.polymarket.interval_minutes * 60  # seconds
        time_remaining_pct = time_to_close / market_duration
        late_cutoff_pct = CONFIG["strategy"].get("late_market_cutoff_pct", 95) / 100.0
        min_remaining = 1.0 - late_cutoff_pct  # 0.15 when cutoff is 85%
        
        if time_remaining_pct < min_remaining:
            return False, f"Too late ({time_to_close:.0f}s left, {(1-time_remaining_pct)*100:.0f}% elapsed >= {late_cutoff_pct*100:.0f}% cutoff)"
        
        # Check for recent trade attempt (prevents rapid fire if first leg fails)
        if hasattr(self, '_last_trade_attempt') and self._last_trade_attempt:
            attempt_elapsed = (datetime.now(timezone.utc) - self._last_trade_attempt).total_seconds()
            if attempt_elapsed < 3:  # 3 second cooldown after orders are sent (market orders are fast)
                return False, f"Recent trade attempt ({int(3 - attempt_elapsed)}s cooldown)"
        
        if CONFIG["test_mode"]["enabled"]:
            if self._last_trade:
                elapsed = (datetime.now(timezone.utc) - self._last_trade).total_seconds()
                if elapsed < CONFIG["test_mode"]["min_trade_interval_seconds"]:
                    return False, f"Cooldown ({int(CONFIG['test_mode']['min_trade_interval_seconds'] - elapsed)}s left)"
            
            if CONFIG["test_mode"]["one_trade_per_segment"]:
                key = f"{pair.polymarket.underlying}_{pair.polymarket.end_time.isoformat()}"
                if key in self._traded_segments:
                    return False, "Already traded this segment"
        
        # Check per-underlying trade limit for this window
        underlying = pair.polymarket.underlying
        current_trades = self._window_trade_counts.get(underlying, 0)
        
        # Assumed strike: max 2 trades (higher risk)
        is_assumed = getattr(pair, 'assumed_strike', False) or pair.kalshi.strike_price is None
        if is_assumed:
            max_trades_tbd = CONFIG["markets"].get("max_trades_no_kalshi_strike", 2)
            if current_trades >= max_trades_tbd:
                return False, f"TBD strike limit ({current_trades}/{max_trades_tbd})"
        else:
            # Favorable: unlimited (balance-limited only)
            # Unfavorable: max 3 per window
            max_trades = self._get_max_trades_for_window(strike_favorable)
            if current_trades >= max_trades:
                return False, f"Trade limit ({current_trades}/{max_trades} {'fav' if strike_favorable else 'unfav'})"
        
        # AGGREGATE CONTRACT CAP: prevent correlated risk from multiple trades in same window
        # A single TWAP surprise can wipe all positions on the same underlying simultaneously
        max_contracts = CONFIG["markets"].get("max_contracts_per_window_per_asset", 50)
        current_contracts = self._window_contract_counts.get(underlying, 0)
        trade_size = 5  # Flat 5 contracts per trade
        if current_contracts + trade_size > max_contracts:
            return False, f"Aggregate cap ({current_contracts}/{max_contracts} contracts this window)"
        
        return True, ""
    
    def check_capital_protection(self, opp: ScoredOpportunity) -> Tuple[bool, str]:
        """Check if trade passes capital protection rules. Returns (can_trade, reason)"""
        if not CONFIG["capital_protection"]["enabled"]:
            return True, ""
        
        if not self._pm_balance or not self._k_balance:
            return True, ""  # No balance info yet, allow trade
        
        # Use flat 5 contracts for capital protection check
        size_val = 5
        size = Decimal(str(size_val))
        buffer_pct = Decimal(str(CONFIG["aggressive_fill"]["first_leg_buffer_pct"])) / 100
        price = min(opp.first_leg_price * (1 + buffer_pct), Decimal("0.99"))
        
        threshold = CONFIG["capital_protection"]["min_balance_threshold"]
        first_cost = float(price) * float(size)
        second_cost = float(opp.second_leg_price) * float(size) * 1.05
        
        pm_balance = float(self._pm_balance.cash)
        k_balance = float(self._k_balance.cash)
        
        # HARD RULE: Can't enter ANY trade if either account is already below threshold
        if pm_balance < threshold:
            return False, f"PM balance ${pm_balance:.2f} below ${threshold} minimum"
        if k_balance < threshold:
            return False, f"K balance ${k_balance:.2f} below ${threshold} minimum"
        
        # ACTUAL COST CHECK: verify each venue can afford its leg
        # Determine which venue gets which leg
        pm_is_first = (opp.first_leg_venue == Venue.POLYMARKET)
        pm_cost = first_cost if pm_is_first else second_cost
        k_cost = second_cost if pm_is_first else first_cost
        
        if pm_balance - pm_cost < 0:
            return False, f"PM can't afford ${pm_cost:.2f} (balance: ${pm_balance:.2f})"
        if k_balance - k_cost < 0:
            return False, f"K can't afford ${k_cost:.2f} (balance: ${k_balance:.2f})"
        
        # Trade allowed
        return True, ""
    
    async def execute_arb_parallel(self, opp: ScoredOpportunity) -> bool:
        """
        Execute both legs of the arb in parallel - don't wait for first leg to confirm.
        This is more aggressive but handles PM lag better.
        """
        underlying = opp.market_pair.polymarket.underlying
        fav_tag = "FAV" if getattr(opp, 'strike_favorable', False) else ("UNFAV" if getattr(opp, 'strike_diff_pct', 0) > 0.01 else "NEUTRAL")
        diff_val = getattr(opp, 'strike_diff_pct', 0)
        
        # Robust trade lock - use timestamp instead of boolean flag
        # This prevents double trades even if two scan loops somehow both enter here
        now = time.time()
        if hasattr(self, '_trade_lock_until') and now < self._trade_lock_until:
            remaining = self._trade_lock_until - now
            # Only log trade lock once per lock period to avoid spam
            if not hasattr(self, '_trade_lock_logged') or not self._trade_lock_logged:
                log_print(f"  [execute_arb_parallel] BLOCKED: Trade lock active for {remaining:.1f}s more")
                self._trade_lock_logged = True
            return False
        
        # Log ENTRY only after passing trade lock (prevents spam during lock periods)
        log_print(f"  [execute_arb_parallel] ENTRY for {underlying} [{fav_tag} {diff_val:.4f}%]")
        
        # Lock for 120 seconds (covers full trade + reconciliation cycle)
        # Will be extended or released in finally block
        self._trade_lock_until = now + 120
        self._trade_lock_logged = False
        
        try:
            result = await self._execute_arb_parallel_inner(opp)
            return result
        finally:
            # Release lock but keep a 5-second cooldown to prevent immediate re-entry
            self._trade_lock_until = time.time() + 5
    
    async def _execute_arb_parallel_inner(self, opp: ScoredOpportunity) -> bool:
        underlying = opp.market_pair.polymarket.underlying
        
        current_roi = float(opp.net_edge_pct) if hasattr(opp, 'net_edge_pct') else 0.0
        can, reason = self.can_trade(opp.market_pair, current_roi, getattr(opp, "strike_favorable", False))
        if not can:
            print(f"  ⚠️ Skipped: {reason}")
            log_print(f"  [execute_arb_parallel] BLOCKED: {reason}")
            return False
        
        # Check trade count limit based on strike diff + ROI
        strike_diff = getattr(opp.market_pair, 'strike_diff_pct', 0.0)
        is_fav = getattr(opp, 'strike_favorable', False)
        max_trades = self._get_max_trades_for_window(is_fav)
        current_trades = self._window_trade_counts.get(underlying, 0)
        
        if current_trades >= max_trades:
            # Only log once per underlying per window to avoid spam
            if not hasattr(self, '_logged_trade_limits'):
                self._logged_trade_limits = set()
            limit_key = f"{underlying}_{self._current_window_end}"
            if limit_key not in self._logged_trade_limits:
                print(f"  ⚠️ {underlying} at trade limit ({current_trades}/{max_trades}) for {strike_diff:.3f}% diff, {current_roi:.1f}% ROI")
                log_print(f"  [execute_arb_parallel] BLOCKED: Trade limit {current_trades}/{max_trades} for {underlying} (ROI {current_roi:.1f}%)")
                self._logged_trade_limits.add(limit_key)
            return False
        
        # Additional limit when Kalshi strike is TBD (assumed strike period)
        # Strike can still change, so limit to 1 trade per underlying
        kalshi_strike = opp.market_pair.kalshi.strike_price if hasattr(opp.market_pair, 'kalshi') else None
        is_assumed_strike = getattr(opp.market_pair, 'assumed_strike', False) or kalshi_strike is None
        
        if is_assumed_strike:
            max_trades_tbd = CONFIG["markets"].get("max_trades_no_kalshi_strike", 1)
            if current_trades >= max_trades_tbd:
                print(f"  ⚠️ Skipped: {underlying} at TBD strike limit ({current_trades}/{max_trades_tbd}) - Kalshi strike not confirmed")
                log_print(f"  [execute_arb_parallel] BLOCKED: TBD strike limit {current_trades}/{max_trades_tbd} for {underlying}")
                return False
        
        # Check if we already have unhedged positions on this underlying
        # This prevents stacking multiple trades before previous ones are reconciled
        # IMPORTANT: Only check positions from the CURRENT window - previous windows are settled/settling
        try:
            k_positions = self.kalshi.get_internal_positions()
            pm_positions = self.pm.get_internal_positions_with_metadata()
            
            # Get current window's K ticker for this underlying
            current_k_ticker = opp.second_leg_market_id if opp.second_leg_venue == Venue.KALSHI else opp.first_leg_market_id
            
            # Get K positions - ONLY current window ticker
            k_yes = sum(p.get("size", 0) for t, p in k_positions.items() 
                       if t == current_k_ticker and p.get("direction") == "yes")
            k_no = sum(p.get("size", 0) for t, p in k_positions.items() 
                      if t == current_k_ticker and p.get("direction") == "no")
            
            # Get PM positions - only current window (filter out expired)
            now = datetime.now(timezone.utc)
            pm_up = 0
            pm_down = 0
            for t, p in pm_positions.items():
                if p.get("underlying") != underlying:
                    continue
                window_end = p.get("window_end")
                if window_end and isinstance(window_end, datetime) and window_end < now:
                    continue
                if p.get("direction") == "up":
                    pm_up += p.get("size", 0)
                elif p.get("direction") == "down":
                    pm_down += p.get("size", 0)
            
            total_position = k_yes + k_no + pm_up + pm_down
            
            # If we have ANY position on this underlying, only allow SAME direction trades
            if total_position > 0:
                # Determine our current direction
                # K YES + PM DOWN = bearish, K NO + PM UP = bullish
                current_bullish = (k_no + pm_up) > 0
                current_bearish = (k_yes + pm_down) > 0
                
                # Determine the new trade's direction
                # PM UP + K NO = bullish, PM DOWN + K YES = bearish
                new_pm_dir = opp.first_leg_direction if opp.first_leg_venue == Venue.POLYMARKET else opp.second_leg_direction
                new_is_bullish = (new_pm_dir == Direction.UP)
                
                # Block if trying to trade opposite direction
                if current_bullish and not new_is_bullish:
                    print(f"  ⚠️ Skipped: Already have BULLISH {underlying} position (K NO/PM UP) - can't go bearish")
                    print(f"     K YES:{k_yes} NO:{k_no} | PM UP:{pm_up} DOWN:{pm_down}")
                    log_print(f"  [execute_arb_parallel] BLOCKED: Opposite direction {underlying}")
                    return False
                elif current_bearish and new_is_bullish:
                    print(f"  ⚠️ Skipped: Already have BEARISH {underlying} position (K YES/PM DOWN) - can't go bullish")
                    print(f"     K YES:{k_yes} NO:{k_no} | PM UP:{pm_up} DOWN:{pm_down}")
                    log_print(f"  [execute_arb_parallel] BLOCKED: Opposite direction {underlying}")
                    return False
            
            # Check hedged pairs: K YES + PM DOWN, K NO + PM UP
            hedged_yes_down = min(k_yes, pm_down)
            hedged_no_up = min(k_no, pm_up)
            
            # Unhedged amounts
            orphan_k_yes = k_yes - hedged_yes_down
            orphan_k_no = k_no - hedged_no_up
            orphan_pm_up = pm_up - hedged_no_up
            orphan_pm_down = pm_down - hedged_yes_down
            
            total_orphans = orphan_k_yes + orphan_k_no + orphan_pm_up + orphan_pm_down
            
            if total_orphans > 0:
                print(f"  ⚠️ Skipped: Existing unhedged {underlying} position - wait for reconciliation")
                print(f"     K YES:{k_yes} NO:{k_no} | PM UP:{pm_up} DOWN:{pm_down} | Orphans:{total_orphans}")
                log_print(f"  [execute_arb_parallel] BLOCKED: Unhedged {underlying} orphans={total_orphans}")
                return False
            
            # HARD CAP: Check cumulative position across ALL unsettled windows
            # Even favorable trades need a cap — K leg can fail, leaving unhedged exposure.
            # Favorable gets 2× the normal limit since the downside is better.
            is_favorable = getattr(opp, 'strike_favorable', False)
            max_pos = CONFIG["markets"].get("max_position_per_underlying", 30)
            if is_favorable:
                max_pos = max_pos * 2  # Favorable: 60 vs normal 30
            cum_pos = self._cumulative_positions.get(underlying, {}).get("size", 0)
            remaining_room = max_pos - cum_pos
            if remaining_room <= 0:
                log_print(f"  [execute_arb_parallel] BLOCKED: Position cap {cum_pos}/{max_pos} for {underlying} ({'fav' if is_favorable else 'unfav'} - no room)")
                return False
            # Store remaining room so sizing logic can use it
            self._position_room = remaining_room
        except Exception as e:
            # If we can't check, LOG the error and BLOCK trading to be safe
            print(f"  ⚠️ Skipped: Could not verify position balance ({e})")
            log_print(f"  [execute_arb_parallel] BLOCKED: Position check failed: {e}")
            return False
        
        # FLAT 5 CONTRACTS — no bonuses, no penalties, no scaling
        # Risk is managed via trade COUNT (favorable=more trades, unfavorable=fewer)
        size = Decimal("5")
        
        # Check minimum order value ($1.00) for KALSHI leg only
        # With 5 contracts, K price must be ≥ $0.20. If not, skip the trade.
        for leg_name, leg_price, leg_venue in [
            ("first", opp.first_leg_price, opp.first_leg_venue),
            ("second", opp.second_leg_price, opp.second_leg_venue)
        ]:
            if leg_venue != Venue.KALSHI:
                continue  # Only Kalshi has $1 minimum
            order_value = float(leg_price) * float(size)
            if order_value < 1.0:
                log_print(f"  [Size] {leg_name} leg ${order_value:.2f} < $1 min (K@${float(leg_price):.2f} x5) - SKIPPING")
                return False
        
        # Clamp size to remaining position room
        if hasattr(self, '_position_room') and self._position_room is not None:
            room = self._position_room
            if int(size) > room:
                clamped = max(5, room)  # PM minimum is 5
                if clamped < 5:
                    log_print(f"  [Size] Only {room} room left (PM min is 5) - skipping")
                    return False
                log_print(f"  [Size] Position cap: {int(size)} → {clamped} contracts (room={room})")
                size = Decimal(str(clamped))
        
        # Add buffer to ensure fill
        buffer_pct = Decimal(str(CONFIG["aggressive_fill"]["first_leg_buffer_pct"])) / 100
        first_price = min(opp.first_leg_price * (1 + buffer_pct), Decimal("0.99"))
        second_price = min(opp.second_leg_price * (1 + buffer_pct), Decimal("0.99"))
        
        # NOTE: Depth check removed for speed - we fire both legs immediately
        # If Kalshi doesn't have enough depth, reconciliation will handle it
        
        # Capital protection check
        # Skip for hedge orders - hedging orphaned positions is critical
        is_hedge_order = getattr(self, '_is_hedge_order', False)
        
        if CONFIG["capital_protection"]["enabled"] and self._pm_balance and self._k_balance and not is_hedge_order:
            threshold = CONFIG["capital_protection"]["min_balance_threshold"]
            first_cost = float(first_price) * float(size)
            second_cost = float(second_price) * float(size)
            
            pm_after = float(self._pm_balance.cash)
            k_after = float(self._k_balance.cash)
            
            if opp.first_leg_venue == Venue.POLYMARKET:
                pm_after -= first_cost
                k_after -= second_cost
            else:
                k_after -= first_cost
                pm_after -= second_cost
            
            # HARD RULE: NEVER go negative — no underdog override for this
            if pm_after < 0:
                log_print(f"  ❌ BLOCKED: PM can't afford order (need ${abs(pm_after):.2f} more, balance: ${float(self._pm_balance.cash):.2f}, cost: ${first_cost if opp.first_leg_venue == Venue.POLYMARKET else second_cost:.2f})")
                return False
            if k_after < 0:
                log_print(f"  ❌ BLOCKED: K can't afford order (need ${abs(k_after):.2f} more, balance: ${float(self._k_balance.cash):.2f}, cost: ${first_cost if opp.first_leg_venue == Venue.KALSHI else second_cost:.2f})")
                return False
            
            # Track skips for underdog bets - after 3 skips, allow anyway if still above threshold
            if not hasattr(self, '_underdog_skips'):
                self._underdog_skips = 0
            
            if pm_after < threshold:
                pm_leg_price = float(opp.first_leg_price) if opp.first_leg_venue == Venue.POLYMARKET else float(opp.second_leg_price)
                if pm_leg_price < 0.50:
                    if self._underdog_skips >= 3:
                        log_print(f"  ⚠️ PM underdog - allowing after 3 skips (${pm_after:.2f} remaining)")
                        self._underdog_skips = 0
                    else:
                        self._underdog_skips += 1
                        print(f"  ⚠️ Skipped: PM balance would drop to ${pm_after:.2f} on underdog bet ({self._underdog_skips}/3)")
                        return False
            
            if k_after < threshold:
                k_leg_price = float(opp.first_leg_price) if opp.first_leg_venue == Venue.KALSHI else float(opp.second_leg_price)
                if k_leg_price < 0.50:
                    if self._underdog_skips >= 3:
                        log_print(f"  ⚠️ K underdog - allowing after 3 skips (${k_after:.2f} remaining)")
                        self._underdog_skips = 0
                    else:
                        self._underdog_skips += 1
                        log_print(f"  ⚠️ Skipped: Kalshi balance would drop to ${k_after:.2f} on underdog bet ({self._underdog_skips}/3)")
                        return False
            
            # Reset skip counter if we pass the checks
            self._underdog_skips = 0
        
        # Check PM signing latency history - if slow, add extra buffer
        pm_signing_slow = False
        if hasattr(self.pm, '_latency_log') and self.pm._latency_log:
            recent_sign_times = [l.get("sign_ms", 0) for l in self.pm._latency_log[-5:]]
            avg_sign_ms = sum(recent_sign_times) / len(recent_sign_times) if recent_sign_times else 0
            expected_delay_ms = CONFIG["latency"].get("pm_signing_delay_expected_ms", 12000)
            
            if avg_sign_ms > 5000:  # If average signing > 5 seconds, we have slow signing
                pm_signing_slow = True
                log_print(f"  ⚠️ PM signing slow (avg {avg_sign_ms:.0f}ms) - adding extra buffer")
                
                # Skip signing delay ROI check for favorable trades
                is_favorable = getattr(opp, 'strike_favorable', False)
                if not is_favorable:
                    # Check if we have enough edge for slow signing (non-favorable only)
                    min_edge_for_delay = CONFIG["latency"].get("pm_signing_delay_min_edge_pct", 10.0)
                    current_edge = float(opp.raw_edge_pct)
                    if current_edge < min_edge_for_delay:
                        log_print(f"  ⚠️ Skipped: Edge {current_edge:.1f}% < {min_edge_for_delay:.1f}% required for slow signing")
                        return False
        
        # Check Kalshi latency - just log it, don't add buffers
        # Markets have plenty of opportunity, no need to overpay
        if hasattr(self, 'kalshi') and self.kalshi.is_slow():
            kalshi_avg = self.kalshi.get_avg_latency()
            avg_fill_ms = kalshi_avg['fill_ms']
            log_print(f"  [Info] Kalshi avg fill: {avg_fill_ms:.0f}ms")
        
        # Log PM fill latency for diagnostics
        pm_avg_fill_ms = self.pm.get_avg_fill_latency()
        if pm_avg_fill_ms > 0:
            log_print(f"  [Info] PM avg fill: {pm_avg_fill_ms:.0f}ms")
        
        # No buffers - use calculated prices directly
        # Prices already have first_leg_buffer_pct from config (3%)
        
        # FRESH balance check right before placing orders (Fix: prevent "not enough balance" errors)
        try:
            pm_cost = float(first_price) * float(size) if opp.first_leg_venue == Venue.POLYMARKET else float(second_price) * float(size)
            pm_bal_now = await self.pm.get_balance()
            min_balance_needed = pm_cost + CONFIG["capital_protection"]["min_balance_threshold"]
            if float(pm_bal_now.cash) < min_balance_needed:
                log_print(f"  [execute_arb_parallel] BLOCKED: PM balance ${pm_bal_now.cash:.2f} < needed ${min_balance_needed:.2f} (cost ${pm_cost:.2f} + buffer)")
                return False
        except Exception as e:
            log_print(f"  [execute_arb_parallel] Fresh balance check failed: {e}")
            # Continue anyway - the order will fail if balance is truly insufficient
        
        # NOW print trade info - after ALL checks passed
        trade_size = int(size)
        est_profit = (1.0 - float(first_price) - float(second_price)) * trade_size
        print(f"\n  🎯 EXECUTING: {opp.market_pair.polymarket.underlying} (Score {opp.score:.0f})")
        print(f"     ROI: {opp.net_edge_pct:.1f}% | Est: ${est_profit:.2f} ({trade_size} contracts)")
        print(f"     {opp.first_leg_venue.value} {opp.first_leg_direction.value.upper()} @ ${first_price:.2f} + {opp.second_leg_venue.value} {opp.second_leg_direction.value.upper()} @ ${second_price:.2f}")
        
        log_print(f"\n  🚀 PARALLEL EXECUTION: {opp.market_pair.polymarket.underlying}")
        log_print(f"      Leg 1: {opp.first_leg_venue.value} {opp.first_leg_direction.value.upper()} @ ${first_price:.2f}")
        log_print(f"      Leg 2: {opp.second_leg_venue.value} {opp.second_leg_direction.value.upper()} @ ${second_price:.2f}")
        
        # Mark that we're actually placing orders NOW (after all checks passed)
        self._last_trade_attempt = datetime.now(timezone.utc)
        trade_start_time = time.time()  # Track for duration calculation
        
        # Track timing for diagnostics
        timings = {}
        underlying = opp.market_pair.polymarket.underlying  # Capture for inner function
        window_end = opp.market_pair.polymarket.end_time  # Capture window for position tagging
        
        async def execute_leg(venue: Venue, market_id: str, direction: Direction, price: Decimal, leg_name: str):
            """Execute a single leg using market orders - fills immediately at best available price."""
            start_time = time.time()
            try:
                order_start = time.time()
                
                # Capture the target price before placing order (from orderbook)
                # This is our best estimate of actual fill price since neither API reports it reliably
                estimated_fill_price = float(price)  # price param is from orderbook
                
                if venue == Venue.POLYMARKET:
                    # PM: place limit order at 95¢ ceiling (effectively a market order)
                    order = await self.pm.place_market_order(market_id, OrderSide.BUY, direction, int(size))
                else:
                    # Kalshi: cap at expected price + 10¢ buffer (NOT $0.95!)
                    # This prevents catastrophic fills in thin/empty orderbooks
                    # while allowing reasonable slippage for fast fills
                    expected_price = float(price)
                    max_k_price = min(expected_price + 0.10, 0.92)
                    order = await self.kalshi.place_market_order(market_id, OrderSide.BUY, direction, int(size), max_price=max_k_price)
                
                order_time = time.time() - order_start
                timings[f"{leg_name}_order"] = order_time
                
                if not order:
                    log_print(f"      [{leg_name}] ❌ Order placement failed ({order_time:.2f}s)")
                    return None, 0, 0
                
                log_print(f"      [{leg_name}] Order placed in {order_time*1000:.0f}ms: {order.order_id[:20]}...")
                
                # Check for phantom fill (order went through but we don't have real order_id)
                is_phantom = order.order_id.startswith("PHANTOM_")
                if is_phantom and venue == Venue.POLYMARKET:
                    log_print(f"      [{leg_name}] ⚠️ PHANTOM FILL - assuming filled based on balance change")
                    # We detected the order went through via balance change, assume full fill
                    fill_size = float(size)
                    fill_price = estimated_fill_price
                    
                    # Record fill internally
                    dir_str = "down" if direction == Direction.DOWN else "up"
                    self.pm.record_fill(market_id, int(fill_size), underlying, dir_str, window_end)
                    
                    return order, fill_size, fill_price
                
                # Wait for fill confirmation
                # Kalshi needs longer timeout - API is slow to report fills
                # which causes phantom fills when failsafe fires too early
                fill_size = 0
                fill_price = 0.0
                max_wait = 10.0 if venue == Venue.KALSHI else 5.0
                check_interval = 0.5
                checks = int(max_wait / check_interval)
                
                for i in range(checks):
                    await asyncio.sleep(check_interval)
                    
                    try:
                        if venue == Venue.POLYMARKET:
                            fill_info = await self.pm.get_order(order.order_id)
                            fill_size = float(fill_info.get("size_matched", 0) or fill_info.get("filled_count", 0) or 0)
                        else:
                            fill_info = await self.kalshi.get_order(order.order_id)
                            fill_size = float(fill_info.get("filled_count", 0) or 0)
                        
                        # Use orderbook price as estimate - neither API returns actual fill price reliably
                        fill_price = estimated_fill_price
                        
                        if fill_size > 0:
                            break
                    except Exception as e:
                        log_print(f"      [{leg_name}] Fill check error: {e}")
                
                total_time = time.time() - start_time
                timings[f"{leg_name}_fill"] = total_time
                
                if fill_size > 0:
                    # Get actual fill price from exchange (not orderbook estimate)
                    actual_fill_price = None
                    if venue == Venue.POLYMARKET:
                        try:
                            actual_fills = await self.pm.get_trade_fills(order.order_id)
                            if actual_fills:
                                total_value = sum(f["price"] * f["size"] for f in actual_fills)
                                total_size = sum(f["size"] for f in actual_fills)
                                if total_size > 0:
                                    actual_fill_price = total_value / total_size
                        except Exception:
                            pass
                    else:
                        # Kalshi: avg_fill_price is already in get_order response
                        try:
                            k_avg = fill_info.get("avg_fill_price")
                            if k_avg and isinstance(k_avg, (int, float)) and 0.01 <= float(k_avg) <= 0.99:
                                actual_fill_price = float(k_avg)
                        except Exception:
                            pass
                    
                    if actual_fill_price is not None:
                        slippage = actual_fill_price - estimated_fill_price
                        if abs(slippage) > 0.003:  # Log if > 0.3c slippage
                            log_print(f"      [{leg_name}] Actual: ${actual_fill_price:.4f} (est: ${estimated_fill_price:.3f}, slip: ${slippage:+.4f})")
                        fill_price = actual_fill_price
                    
                    price_str = f"${fill_price:.4f}" if actual_fill_price else f"~${fill_price:.2f}"
                    log_print(f"      [{leg_name}] ✓ Filled {int(fill_size)} @ {price_str} ({total_time*1000:.0f}ms)")
                    
                    # Record fill internally for position tracking
                    if venue == Venue.POLYMARKET:
                        dir_str = "down" if direction == Direction.DOWN else "up"
                        self.pm.record_fill(market_id, int(fill_size), underlying, dir_str, window_end)
                        self.pm.record_fill_latency(total_time * 1000)
                    else:
                        dir_str = "yes" if direction == Direction.UP else "no"
                        self.kalshi.record_fill(market_id, int(fill_size), dir_str, window_end)
                        self.kalshi.record_latency(order_ms=order_time*1000, fill_ms=total_time*1000)
                    
                    return order, fill_size, fill_price
                else:
                    log_print(f"      [{leg_name}] ⚠️ No fill confirmed after {total_time:.1f}s")
                    return order, 0, 0
                    
            except Exception as e:
                log_print(f"      [{leg_name}] ❌ Error: {e}")
                import traceback
                traceback.print_exc()
                return None, 0, 0
        
        # Execute legs - TRUE PARALLEL via asyncio.gather
        # Both legs fire simultaneously for best price capture
        # K-first sequential was costing too much in price drift/slippage
        # If one leg fills and other doesn't, reconciliation handles the orphan
        log_print(f"      [Parallel] Firing both legs simultaneously...")
        
        leg1_task = execute_leg(
            opp.first_leg_venue, opp.first_leg_market_id, 
            opp.first_leg_direction, first_price, "Leg1"
        )
        leg2_task = execute_leg(
            opp.second_leg_venue, opp.second_leg_market_id,
            opp.second_leg_direction, second_price, "Leg2"
        )
        
        results = await asyncio.gather(leg1_task, leg2_task, return_exceptions=True)
        
        # Unpack results (handle exceptions from either leg)
        if isinstance(results[0], Exception):
            log_print(f"      [Parallel] Leg1 exception: {results[0]}")
            leg1_order, leg1_size, leg1_price = None, 0, 0
        else:
            leg1_order, leg1_size, leg1_price = results[0]
        
        if isinstance(results[1], Exception):
            log_print(f"      [Parallel] Leg2 exception: {results[1]}")
            leg2_order, leg2_size, leg2_price = None, 0, 0
        else:
            leg2_order, leg2_size, leg2_price = results[1]
        
        # If K filled but PM didn't, try to sell K back for clean exit
        pm_is_leg1 = opp.first_leg_venue == Venue.POLYMARKET
        k_filled = (leg2_size if pm_is_leg1 else leg1_size) > 0
        pm_filled = (leg1_size if pm_is_leg1 else leg2_size) > 0
        
        if k_filled and not pm_filled:
            # BEFORE selling K back, check if PM had a phantom fill
            # PM often returns "Request exception" but actually fills
            pm_market_id = opp.first_leg_market_id if pm_is_leg1 else opp.second_leg_market_id
            pm_direction = opp.first_leg_direction if pm_is_leg1 else opp.second_leg_direction
            pm_phantom_detected = False
            
            try:
                # Check if PM balance dropped (indicating phantom fill)
                current_pm_balance = await self.pm.get_balance()
                if hasattr(self.pm, '_pre_order_balance') and self.pm._pre_order_balance:
                    expected_pm_price = float(opp.first_leg_price if pm_is_leg1 else opp.second_leg_price)
                    expected_cost = expected_pm_price * size
                    balance_drop = float(self.pm._pre_order_balance.cash) - float(current_pm_balance.cash)
                    
                    # If balance dropped by roughly the expected amount, PM filled
                    # Lowered from 50% to 30% — partial fills and slippage can make
                    # the actual cost much less than expected, causing false negatives
                    # that lead to unnecessary K sellbacks
                    if balance_drop > expected_cost * 0.3:  # At least 30% of expected
                        log_print(f"      [Parallel] ⚠️ PM PHANTOM FILL DETECTED: Balance dropped ${balance_drop:.2f} (expected ~${expected_cost:.2f})")
                        pm_phantom_detected = True
                        pm_filled = True  # Override - PM actually filled
                        if pm_is_leg1:
                            leg1_size = size
                        else:
                            leg2_size = size
            except Exception as e:
                log_print(f"      [Parallel] Could not check PM phantom: {e}")
            
            # Only sell K back if PM truly didn't fill
            if not pm_phantom_detected:
                k_market_id = opp.second_leg_market_id if pm_is_leg1 else opp.first_leg_market_id
                k_direction = opp.second_leg_direction if pm_is_leg1 else opp.first_leg_direction
                k_sz = leg2_size if pm_is_leg1 else leg1_size
                k_fp = leg2_price if pm_is_leg1 else leg1_price
                log_print(f"      [Parallel] ⚠️ K filled {int(k_sz)} but PM didn't - selling K back...")
                try:
                    k_book = await self.kalshi.get_orderbook(k_market_id)
                    if k_direction == Direction.UP:
                        k_bid = k_book.best_yes_bid
                    else:
                        k_bid = k_book.best_no_bid
                    
                    if k_bid:
                        sell_price = max(float(k_bid) * 0.95, 0.01)
                        sell_order = await self.kalshi.place_order(
                            k_market_id, OrderSide.SELL, k_direction,
                            Decimal(str(round(sell_price, 2))),
                            Decimal(str(int(k_sz)))
                        )
                        if sell_order:
                            await asyncio.sleep(2)
                            sell_check = await self.kalshi.get_order(sell_order.order_id)
                            sell_filled = sell_check.get("filled_count", 0) or 0
                            k_paid = float(k_fp) if k_fp else 0
                            loss = (k_paid - sell_price) * sell_filled
                            log_print(f"      [Parallel] Sold {int(sell_filled)} K back @ ${sell_price:.2f} (bought @ ${k_paid:.2f}, loss: ${loss:.2f})")
                            
                            if sell_filled > 0:
                                profit_tracker.record_trade(
                                    underlying=opp.market_pair.polymarket.underlying,
                                    pm_price=1.0 - sell_price,
                                    k_price=k_paid,
                                    size=int(sell_filled),
                                    source="unwind",
                                    notes=f"K sell-back: PM didn't fill in parallel"
                                )
                            
                            # Check if fully sold or partial
                            if sell_filled < int(k_sz):
                                remaining = int(k_sz) - sell_filled
                                self.trigger_safety_pause(
                                    f"[Parallel] K sell-back PARTIAL: sold {sell_filled}/{int(k_sz)}, {remaining} unhedged",
                                    unhedged={"underlying": opp.market_pair.polymarket.underlying, "venue": "K", "size": remaining, "direction": k_direction.value}
                                )
                            
                            # Zero out K so it's not treated as orphan
                            if pm_is_leg1:
                                leg2_size = 0
                            else:
                                leg1_size = 0
                        else:
                            log_print(f"      [Parallel] K sell order failed - leaving for reconciliation")
                            self.trigger_safety_pause(
                                f"[Parallel] K sell-back FAILED: {int(k_sz)} K unhedged (PM didn't fill)",
                                unhedged={"underlying": opp.market_pair.polymarket.underlying, "venue": "K", "size": int(k_sz), "direction": k_direction.value}
                            )
                    else:
                        log_print(f"      [Parallel] No K bid available - leaving for reconciliation")
                        self.trigger_safety_pause(
                            f"[Parallel] K sell-back IMPOSSIBLE: no bid for {int(k_sz)} K (PM didn't fill)",
                            unhedged={"underlying": opp.market_pair.polymarket.underlying, "venue": "K", "size": int(k_sz), "direction": k_direction.value}
                        )
                except Exception as e:
                    log_print(f"      [Parallel] K sell-back error: {e} - leaving for reconciliation")
                    self.trigger_safety_pause(
                        f"[Parallel] K sell-back ERROR: {e} - {int(k_sz)} K unhedged",
                        unhedged={"underlying": opp.market_pair.polymarket.underlying, "venue": "K", "size": int(k_sz), "direction": k_direction.value}
                    )
        
        # ═══════════════════════════════════════════════════════════════════
        # PM FILL VERIFICATION: Safety net to catch any PM fill discrepancies
        # With PM-First sequential mode, PM first-leg fills are always confirmed
        # before K is placed. This section now primarily protects PM second-leg
        # edge cases where fill reporting might be inconsistent.
        # ═══════════════════════════════════════════════════════════════════
        trade_size = int(size)
        pm_leg = "Leg1" if opp.first_leg_venue == Venue.POLYMARKET else "Leg2"
        k_leg = "Leg2" if opp.first_leg_venue == Venue.POLYMARKET else "Leg1"
        pm_size = leg1_size if pm_leg == "Leg1" else leg2_size
        k_size = leg1_size if k_leg == "Leg1" else leg2_size
        pm_order = leg1_order if pm_leg == "Leg1" else leg2_order
        
        # If both "filled" but PM was assumed, verify PM actually filled
        if pm_size > 0 and k_size > 0 and pm_order:
            try:
                pm_check = await self.pm.get_order(pm_order.order_id)
                actual_pm_fill = float(pm_check.get("filled_count", 0) or pm_check.get("size_matched", 0) or 0)
                
                if actual_pm_fill == 0:
                    # PM didn't actually fill! We assumed it did but it didn't
                    log_print(f"\n      ⚠️ PM VERIFY: Order has 0 fills (was assumed {int(pm_size)})")
                    log_print(f"      Waiting 10s for PM to fill before bailing...")
                    
                    # Give PM 10 more seconds
                    pm_actually_filled = False
                    for check_i in range(10):
                        await asyncio.sleep(1.0)
                        try:
                            recheck = await self.pm.get_order(pm_order.order_id)
                            recheck_fill = float(recheck.get("filled_count", 0) or recheck.get("size_matched", 0) or 0)
                            if recheck_fill > 0:
                                actual_pm_fill = recheck_fill
                                pm_actually_filled = True
                                log_print(f"      ✓ PM filled {int(recheck_fill)} after {check_i+1}s wait")
                                break
                        except:
                            pass
                        if check_i % 3 == 2:
                            log_print(f"      Still waiting... ({check_i+1}s)")
                    
                    if not pm_actually_filled:
                        # PM truly didn't fill - cancel PM order and sell K
                        log_print(f"      ❌ PM still unfilled after 10s - cancelling PM and selling K")
                        
                        # Cancel PM order
                        try:
                            cancelled = await self.pm.cancel_order(pm_order.order_id)
                            if cancelled:
                                log_print(f"      ✓ PM order cancelled")
                            else:
                                log_print(f"      ⚠️ PM cancel returned false - checking if it filled")
                                # One more check in case it filled during cancel
                                final_check = await self.pm.get_order(pm_order.order_id)
                                final_fill = float(final_check.get("filled_count", 0) or final_check.get("size_matched", 0) or 0)
                                if final_fill > 0:
                                    actual_pm_fill = final_fill
                                    pm_actually_filled = True
                                    log_print(f"      ✓ PM actually filled {int(final_fill)} during cancel!")
                        except Exception as e:
                            log_print(f"      PM cancel error: {e}")
                        
                        if not pm_actually_filled:
                            # Market sell K position
                            k_direction_val = opp.first_leg_direction if opp.first_leg_venue == Venue.KALSHI else opp.second_leg_direction
                            k_market_id = opp.first_leg_market_id if opp.first_leg_venue == Venue.KALSHI else opp.second_leg_market_id
                            
                            try:
                                kb = await self.kalshi.get_orderbook(k_market_id)
                                if k_direction_val == Direction.UP:
                                    best_bid = kb.best_yes_bid
                                else:
                                    best_bid = kb.best_no_bid
                                
                                if best_bid:
                                    sell_price = max(float(best_bid) * 0.95, 0.01)
                                    sell_order = await self.kalshi.place_order(
                                        k_market_id,
                                        OrderSide.SELL,
                                        k_direction_val,
                                        Decimal(str(round(sell_price, 2))),
                                        Decimal(str(int(k_size)))
                                    )
                                    
                                    if sell_order:
                                        await asyncio.sleep(2)
                                        fill = await self.kalshi.get_order(sell_order.order_id)
                                        filled = fill.get("filled_count", 0) or 0
                                        log_print(f"      ✓ Sold {filled}/{int(k_size)} K @ ~${sell_price:.2f}")
                                        if filled < int(k_size):
                                            # Partial sell - still have exposure
                                            remaining = int(k_size) - filled
                                            self.trigger_safety_pause(
                                                f"K sell-back PARTIAL: only sold {filled}/{int(k_size)}, {remaining} unhedged",
                                                unhedged={"underlying": underlying, "venue": "K", "size": remaining, "direction": k_direction_val.value}
                                            )
                                    else:
                                        log_print(f"      ❌ K sell order failed")
                                        self._has_orphaned_positions = True
                                        self.trigger_safety_pause(
                                            f"K sell-back FAILED: {int(k_size)} K unhedged (PM didn't fill)",
                                            unhedged={"underlying": underlying, "venue": "K", "size": int(k_size), "direction": k_direction_val.value}
                                        )
                                else:
                                    log_print(f"      ❌ No K bid - can't sell")
                                    self._has_orphaned_positions = True
                                    self.trigger_safety_pause(
                                        f"K sell-back IMPOSSIBLE: no bid for {int(k_size)} K (PM didn't fill)",
                                        unhedged={"underlying": underlying, "venue": "K", "size": int(k_size), "direction": k_direction_val.value}
                                    )
                            except Exception as e:
                                log_print(f"      ❌ K sell error: {e}")
                                self._has_orphaned_positions = True
                                self.trigger_safety_pause(
                                    f"K sell-back ERROR: {e} - {int(k_size)} K unhedged",
                                    unhedged={"underlying": underlying, "venue": "K", "size": int(k_size), "direction": k_direction_val.value}
                                )
                            
                            # Clear internal positions since trade is unwound
                            self.pm._internal_positions = {}
                            if hasattr(self.pm, '_position_metadata'):
                                self.pm._position_metadata = {}
                            self.kalshi._internal_positions = {}
                            
                            log_print(f"      Trade unwound - no PM fill, K sold")
                            logger.log_text(f"  TRADE UNWOUND: PM unfilled, K sold for {underlying}")
                            return False
                    
                    # Update pm_size with actual fills
                    if pm_leg == "Leg1":
                        leg1_size = actual_pm_fill
                    else:
                        leg2_size = actual_pm_fill
                    pm_size = actual_pm_fill
                    
                elif actual_pm_fill < pm_size:
                    # PM partially filled (less than assumed)
                    log_print(f"      [PM VERIFY] Actual fill: {int(actual_pm_fill)} (assumed {int(pm_size)})")
                    if pm_leg == "Leg1":
                        leg1_size = actual_pm_fill
                    else:
                        leg2_size = actual_pm_fill
                    pm_size = actual_pm_fill
                    
            except Exception as e:
                log_print(f"      [PM VERIFY] Check failed: {e} - proceeding with assumed fills")
        
        # ═══════════════════════════════════════════════════════════════════
        # PM FILL WAITING: If K is full but PM is partial, wait for PM to fill more
        # The PM order might still be resting in the book
        # ═══════════════════════════════════════════════════════════════════
        
        if k_size >= trade_size and 0 < pm_size < trade_size and pm_order:
            # K is full but PM is partial - wait for PM to fill more
            log_print(f"      [{pm_leg}] K full ({int(k_size)}), PM partial ({int(pm_size)}) - waiting for more PM fills...")
            
            still_need = trade_size - int(pm_size)
            max_pm_wait = 15  # Wait up to 15 more seconds for PM
            
            for i in range(max_pm_wait):
                await asyncio.sleep(1.0)
                try:
                    fill = await self.pm.get_order(pm_order.order_id)
                    current_fill = fill.get("size_matched", 0) or fill.get("filled_count", 0) or 0
                    
                    if float(current_fill) > pm_size:
                        pm_size = float(current_fill)
                        new_price = float(fill.get("avg_fill_price") or leg1_price if pm_leg == "Leg1" else leg2_price)
                        
                        # Update the leg results
                        if pm_leg == "Leg1":
                            leg1_size = pm_size
                            leg1_price = new_price
                        else:
                            leg2_size = pm_size
                            leg2_price = new_price
                        
                        log_print(f"      [{pm_leg}] +{int(current_fill - pm_size)} more filled, now {int(pm_size)}/{trade_size}")
                        
                        if pm_size >= trade_size:
                            log_print(f"      [{pm_leg}] ✓ PM now fully filled!")
                            break
                    
                    if i % 5 == 4:
                        log_print(f"      [{pm_leg}] Still waiting... ({i+1}s, have {int(pm_size)}/{trade_size})")
                except Exception as e:
                    pass
            
            # Update internal tracking with final PM size
            if pm_size > (leg1_size if pm_leg == "Leg1" else leg2_size):
                dir_str = "down" if (opp.first_leg_direction if pm_leg == "Leg1" else opp.second_leg_direction) == Direction.DOWN else "up"
                pm_market_id = opp.first_leg_market_id if pm_leg == "Leg1" else opp.second_leg_market_id
                pm_window_end = opp.market_pair.polymarket.end_time if hasattr(opp.market_pair.polymarket, 'end_time') else None
                self.pm.record_fill(pm_market_id, int(pm_size), underlying, dir_str, pm_window_end)
        
        # ═══════════════════════════════════════════════════════════════════
        
        # Print timing summary with lag warnings
        log_print(f"\n      ⏱️ Timing Summary:")
        total_time = 0
        lag_warnings = []
        
        for key, val in timings.items():
            total_time += val
            warning = ""
            
            # Check against latency thresholds
            val_ms = val * 1000
            if "Leg1" in key and opp.first_leg_venue == Venue.KALSHI:
                if "order" in key and val_ms > CONFIG["latency"]["kalshi_order_warn_ms"]:
                    warning = " ⚠️ SLOW"
                    lag_warnings.append(f"Kalshi order: {val_ms:.0f}ms")
                elif "fill" in key and val_ms > CONFIG["latency"]["kalshi_fill_warn_ms"]:
                    warning = " ⚠️ SLOW"
                    lag_warnings.append(f"Kalshi fill: {val_ms:.0f}ms")
            elif "Leg1" in key and opp.first_leg_venue == Venue.POLYMARKET:
                if "order" in key and val_ms > CONFIG["latency"]["pm_order_warn_ms"]:
                    warning = " ⚠️ SLOW"
                    lag_warnings.append(f"PM order: {val_ms:.0f}ms")
                elif "fill" in key and val_ms > CONFIG["latency"]["pm_fill_warn_ms"]:
                    warning = " ⚠️ SLOW"
                    lag_warnings.append(f"PM fill: {val_ms:.0f}ms")
            elif "Leg2" in key and opp.second_leg_venue == Venue.KALSHI:
                if "order" in key and val_ms > CONFIG["latency"]["kalshi_order_warn_ms"]:
                    warning = " ⚠️ SLOW"
                    lag_warnings.append(f"Kalshi order: {val_ms:.0f}ms")
                elif "fill" in key and val_ms > CONFIG["latency"]["kalshi_fill_warn_ms"]:
                    warning = " ⚠️ SLOW"
                    lag_warnings.append(f"Kalshi fill: {val_ms:.0f}ms")
            elif "Leg2" in key and opp.second_leg_venue == Venue.POLYMARKET:
                if "order" in key and val_ms > CONFIG["latency"]["pm_order_warn_ms"]:
                    warning = " ⚠️ SLOW"
                    lag_warnings.append(f"PM order: {val_ms:.0f}ms")
                elif "fill" in key and val_ms > CONFIG["latency"]["pm_fill_warn_ms"]:
                    warning = " ⚠️ SLOW"
                    lag_warnings.append(f"PM fill: {val_ms:.0f}ms")
            
            log_print(f"         {key}: {val:.2f}s{warning}")
        
        # Check for catastrophic lag
        if total_time * 1000 > CONFIG["latency"]["max_total_lag_ms"]:
            log_print(f"      🚨 TOTAL LAG: {total_time:.1f}s - something is very wrong!")
        elif lag_warnings:
            log_print(f"      ⚠️ Lag detected: {', '.join(lag_warnings)}")
        
        # Determine outcome
        if leg1_size > 0 and leg2_size > 0:
            # Both filled - calculate profit using profit_tracker
            min_size = min(int(leg1_size), int(leg2_size))
            
            # Get prices for each venue
            pm_price = leg1_price if opp.first_leg_venue == Venue.POLYMARKET else leg2_price
            k_price = leg1_price if opp.first_leg_venue == Venue.KALSHI else leg2_price
            
            # Get directions
            pm_direction = opp.first_leg_direction.value if opp.first_leg_venue == Venue.POLYMARKET else opp.second_leg_direction.value
            k_direction = opp.first_leg_direction.value if opp.first_leg_venue == Venue.KALSHI else opp.second_leg_direction.value
            # Convert PM direction to match format
            if pm_direction == "yes":
                pm_direction = "up"
            elif pm_direction == "no":
                pm_direction = "down"
            
            # Record trade and calculate profit
            net_profit = profit_tracker.record_trade(
                underlying=opp.market_pair.polymarket.underlying,
                pm_price=float(pm_price),
                k_price=float(k_price),
                size=min_size,
                source="arb",
                notes=f"Parallel execution"
            )
            
            # Also record for settlement verification (catches strike diff losses)
            pm_strike = opp.market_pair.polymarket.strike_price
            k_strike = opp.market_pair.kalshi.strike_price
            k_ticker = opp.market_pair.kalshi.market_id
            pm_token_id = opp.market_pair.polymarket.token_id
            
            profit_tracker.record_pending_settlement(
                underlying=opp.market_pair.polymarket.underlying,
                pm_price=float(pm_price),
                k_price=float(k_price),
                size=min_size,
                pm_direction=pm_direction,
                k_direction=k_direction,
                k_ticker=k_ticker,
                pm_token_id=pm_token_id,
                pm_strike=pm_strike if pm_strike else 0,
                k_strike=k_strike if k_strike else 0,
                expected_profit=float(net_profit),
                notes=f"Strike diff: {getattr(opp.market_pair, 'strike_diff_pct', 0):.4f}%"
            )
            
            # Track for new settlement system (uses next window's strikes)
            if pm_strike and k_strike:
                self.record_trade_for_settlement(
                    underlying=opp.market_pair.polymarket.underlying,
                    pm_strike=pm_strike,
                    k_strike=k_strike,
                    pm_direction=pm_direction,
                    k_direction=k_direction,
                    size=min_size,
                    pm_price=float(pm_price),
                    k_price=float(k_price),
                    expected_profit=float(net_profit)
                )
            
            actual_cost = leg1_price + leg2_price
            
            # SANITY CHECK: If combined cost > $1.00, this trade loses money guaranteed
            if actual_cost > 1.00:
                log_print(f"\n  🚨 GUARANTEED LOSS: Combined cost ${actual_cost:.4f} > $1.00!")
                log_print(f"      PM ${float(pm_price):.4f} + K ${float(k_price):.4f} = ${actual_cost:.4f}")
                log_print(f"      This should not happen - K fill price may be incorrect")
                logger.log_text(f"  🚨 GUARANTEED LOSS DETECTED: cost=${actual_cost:.4f}")
            
            log_print(f"\n  ✅ ARB COMPLETE!")
            log_print(f"      Cost: PM ${float(pm_price):.4f} + K ${float(k_price):.4f} = ${actual_cost:.4f}")
            log_print(f"      Net profit: ${net_profit:.2f} ({min_size} contracts)")
            
            # === TRADE JOURNAL ===
            pm_is_leg1 = opp.first_leg_venue == Venue.POLYMARKET
            pm_expected = float(opp.first_leg_price if opp.first_leg_venue == Venue.POLYMARKET else opp.second_leg_price)
            k_expected = float(opp.second_leg_price if opp.first_leg_venue == Venue.POLYMARKET else opp.first_leg_price)
            pm_fill_ms = timings.get("Leg1_fill", 0) * 1000 if pm_is_leg1 else timings.get("Leg2_fill", 0) * 1000
            k_fill_ms = timings.get("Leg2_fill", 0) * 1000 if pm_is_leg1 else timings.get("Leg1_fill", 0) * 1000
            profit_tracker.log_to_journal(
                underlying=underlying,
                strategy="arb",
                size=min_size,
                pm_dir=pm_direction,
                k_dir=k_direction,
                pm_expected=pm_expected,
                pm_actual=float(pm_price),
                k_expected=k_expected,
                k_actual=float(k_price),
                strike_diff_pct=float(getattr(opp.market_pair, 'strike_diff_pct', 0) or 0),
                strike_favorable=getattr(opp, 'strike_favorable', True),
                k_ticker=opp.market_pair.kalshi.market_id,
                window_end=opp.market_pair.polymarket.end_time.isoformat() if opp.market_pair.polymarket.end_time else "",
                pm_fill_ms=pm_fill_ms,
                k_fill_ms=k_fill_ms,
                session_name=logger.session_name if hasattr(logger, 'session_name') else "",
            )
            
            # Increment trade count for this underlying in this window
            self._window_trade_counts[underlying] = self._window_trade_counts.get(underlying, 0) + 1
            self._window_contract_counts[underlying] = self._window_contract_counts.get(underlying, 0) + min_size
            trade_count = self._window_trade_counts[underlying]
            contract_count = self._window_contract_counts[underlying]
            is_fav = getattr(opp, 'strike_favorable', False)
            max_trades = self._get_max_trades_for_window(is_fav)
            max_contracts = CONFIG["markets"].get("max_contracts_per_window_per_asset", 50)
            log_print(f"      Trade count: {trade_count}/{max_trades} ({'fav' if is_fav else 'unfav'}) for {underlying}")
            log_print(f"      Contract count: {contract_count}/{max_contracts} for {underlying}")
            
            # Update cumulative position tracker (persists across windows)
            new_pm_dir = opp.first_leg_direction if opp.first_leg_venue == Venue.POLYMARKET else opp.second_leg_direction
            new_direction = "bullish" if new_pm_dir == Direction.UP else "bearish"
            cum = self._cumulative_positions.get(underlying, {"size": 0, "direction": new_direction})
            cum["size"] = cum.get("size", 0) + min_size
            cum["direction"] = new_direction
            self._cumulative_positions[underlying] = cum
            max_pos = CONFIG["markets"].get("max_position_per_underlying", 30)
            log_print(f"      Cumulative position: {cum['size']}/{max_pos} {cum['direction']} for {underlying}")
            
            # Update legacy session tracker for backward compat
            session.trades.append(ConfirmedTrade(
                timestamp=datetime.now(timezone.utc),
                underlying=opp.market_pair.polymarket.underlying,
                direction=f"{opp.first_leg_venue.value}_{opp.first_leg_direction.value}",
                size=min_size,
                pm_fill_price=Decimal(str(pm_price)),
                k_fill_price=Decimal(str(k_price)),
                total_cost=Decimal(str(actual_cost)) * min_size,
                guaranteed_payout=Decimal(str(min_size)),
                expected_profit=net_profit,
            ))
            
            self._last_trade = datetime.now(timezone.utc)
            
            # Record trade duration for reconciliation window calculation
            self._last_trade_duration = time.time() - trade_start_time
            
            # Clear pending unconfirmed orders - trade succeeded, don't cancel them!
            if hasattr(self, '_pending_unconfirmed_pm_orders'):
                self._pending_unconfirmed_pm_orders = []
            
            # Handle size mismatch - need to get hedged
            if leg1_size != leg2_size:
                diff = abs(int(leg1_size) - int(leg2_size))
                log_print(f"      ⚠️ Size mismatch: {int(leg1_size)} vs {int(leg2_size)} - {diff} contracts unhedged")
                
                # Determine which venue has excess
                pm_size = leg1_size if opp.first_leg_venue == Venue.POLYMARKET else leg2_size
                k_size = leg2_size if opp.first_leg_venue == Venue.POLYMARKET else leg1_size
                
                # Fresh PM check - that last contract may have filled since we last checked
                if k_size > pm_size and pm_order:
                    try:
                        fresh_pm = await self.pm.get_order(pm_order.order_id)
                        fresh_fill = float(fresh_pm.get("filled_count", 0) or fresh_pm.get("size_matched", 0) or 0)
                        if fresh_fill > pm_size:
                            log_print(f"      ✓ PM caught up: {int(fresh_fill)} filled (was {int(pm_size)})")
                            pm_size = fresh_fill
                            if pm_leg == "Leg1":
                                leg1_size = fresh_fill
                            else:
                                leg2_size = fresh_fill
                    except:
                        pass
                
                if k_size > pm_size:
                    # K has more than PM - market sell excess K to get hedged
                    excess = round(k_size - pm_size)
                    if excess <= 0:
                        log_print(f"      [Mismatch] K:{k_size} PM:{pm_size} rounds to 0 excess - skipping sell")
                    else:
                        k_direction = opp.second_leg_direction if opp.first_leg_venue == Venue.POLYMARKET else opp.first_leg_direction
                        k_market_id = opp.second_leg_market_id if opp.first_leg_venue == Venue.POLYMARKET else opp.first_leg_market_id
                        
                        log_print(f"      🔄 PM partial fill - market selling {excess} K to hedge")
                        
                        try:
                            # Get best bid and sell slightly below to ensure fill
                            kb = await self.kalshi.get_orderbook(k_market_id)
                            if k_direction == Direction.UP:
                                best_bid = kb.best_yes_bid
                            else:
                                best_bid = kb.best_no_bid
                            
                            if best_bid:
                                # Sell at 95% of best bid for quick fill
                                sell_price = max(float(best_bid) * 0.95, 0.01)
                                sell_order = await self.kalshi.place_order(
                                    k_market_id,
                                    OrderSide.SELL,
                                    k_direction,
                                    Decimal(str(round(sell_price, 2))),
                                    Decimal(str(excess))
                                )
                                
                                if sell_order:
                                    # Wait briefly for fill
                                    await asyncio.sleep(2)
                                    fill = await self.kalshi.get_order(sell_order.order_id)
                                    filled = fill.get("filled_count", 0) or 0
                                    
                                    if filled >= excess:
                                        log_print(f"      ✓ Sold {excess} K @ ~${sell_price:.2f} - now hedged at {int(pm_size)}/{int(pm_size)}")
                                    else:
                                        log_print(f"      ⚠️ Only sold {filled}/{excess} K - still {excess - filled} unhedged")
                                        self._has_orphaned_positions = True
                                else:
                                    log_print(f"      ❌ K sell order failed")
                                    self._has_orphaned_positions = True
                            else:
                                log_print(f"      ❌ No bid for K - can't sell excess")
                                self._has_orphaned_positions = True
                        except Exception as e:
                            log_print(f"      ❌ K sell error: {e}")
                            self._has_orphaned_positions = True
                else:
                    # PM has more than K - can't reliably sell PM, mark for tracking
                    # The K buy incremental logic should have already tried to fill
                    log_print(f"      ⚠️ K partial fill - accepting {int(k_size)}/{int(pm_size)} hedge")
                    self._has_orphaned_positions = True
            
            logger.log_text(f"  PARALLEL ARB COMPLETE: ${net_profit:.2f} profit")
            return True
            
        elif leg1_size > 0 or leg2_size > 0:
            # Only one leg filled - orphaned position
            filled_leg = "Leg1" if leg1_size > 0 else "Leg2"
            filled_size = leg1_size if leg1_size > 0 else leg2_size
            filled_venue = opp.first_leg_venue if leg1_size > 0 else opp.second_leg_venue
            unfilled_venue = opp.second_leg_venue if leg1_size > 0 else opp.first_leg_venue
            
            # Record trade duration for reconciliation window calculation
            self._last_trade_duration = time.time() - trade_start_time
            
            # ═══════════════════════════════════════════════════════════════════
            # FAILSAFE: If PM filled but K didn't, try aggressively to complete K
            # Steps up price incrementally toward max $0.92 to ensure fill
            # ═══════════════════════════════════════════════════════════════════
            time_to_close = opp.market_pair.polymarket.time_to_close if hasattr(opp.market_pair, 'polymarket') else 0
            
            # Failsafe conditions:
            # 1. PM filled, K didn't (most common case)
            # 2. At least 60 seconds remaining before settlement
            # 3. Not already past 95% of window (safety margin for late starts)
            pct_elapsed = 1 - (time_to_close / 900) if time_to_close else 1
            
            if (filled_venue == Venue.POLYMARKET and 
                unfilled_venue == Venue.KALSHI and 
                time_to_close >= 60 and 
                pct_elapsed < 0.95):
                
                log_print(f"\n  🔄 FAILSAFE: Stepping up K price to complete arb ({time_to_close:.0f}s remaining)...")
                
                # ═══════════════════════════════════════════════════════════════
                # CRITICAL: Cancel original K order and check for phantom fill
                # The original K order may have ALREADY filled but our polling
                # missed it. If we don't cancel first, failsafe will place NEW
                # orders creating 2x exposure on Kalshi.
                # ═══════════════════════════════════════════════════════════════
                k_order = leg2_order if opp.first_leg_venue == Venue.POLYMARKET else leg1_order
                
                if k_order and k_order.order_id and not k_order.order_id.startswith("PHANTOM_"):
                    log_print(f"      [Phantom Check] Cancelling original K order {k_order.order_id[:16]}...")
                    
                    # First, try to cancel the original order
                    cancel_succeeded = False
                    try:
                        cancel_succeeded = await self.kalshi.cancel_order(k_order.order_id)
                    except Exception as e:
                        log_print(f"      [Phantom Check] Cancel failed (may already be filled): {e}")
                    
                    # Whether cancel succeeded or failed, check the actual order status
                    await asyncio.sleep(0.5)  # Brief pause for Kalshi API to update
                    
                    try:
                        orig_status = await self.kalshi.get_order(k_order.order_id)
                        orig_filled = int(orig_status.get("filled_count", 0) or 0)
                        
                        if orig_filled > 0:
                            # PHANTOM FILL DETECTED - original order DID fill!
                            orig_fill_price = float(orig_status.get("avg_fill_price") or 0)
                            log_print(f"      🚨 PHANTOM FILL DETECTED: Original K order filled {orig_filled} @ ${orig_fill_price:.4f}")
                            log_print(f"      Original K order was filled but polling missed it - skipping failsafe")
                            
                            # Record the phantom fill properly
                            k_market_id_pf = opp.second_leg_market_id if opp.first_leg_venue == Venue.POLYMARKET else opp.first_leg_market_id
                            k_direction_pf = opp.second_leg_direction if opp.first_leg_venue == Venue.POLYMARKET else opp.first_leg_direction
                            dir_str_pf = "yes" if k_direction_pf == Direction.UP else "no"
                            k_window_end_pf = opp.market_pair.kalshi.end_time if hasattr(opp.market_pair.kalshi, 'end_time') else None
                            self.kalshi.record_fill(k_market_id_pf, orig_filled, dir_str_pf, k_window_end_pf)
                            
                            # Calculate and record profit
                            pm_price_pf = leg1_price if opp.first_leg_venue == Venue.POLYMARKET else leg2_price
                            pm_price_float_pf = float(pm_price_pf)
                            actual_cost = pm_price_float_pf + orig_fill_price
                            net_profit = profit_tracker.record_trade(
                                underlying=opp.market_pair.polymarket.underlying,
                                pm_price=pm_price_float_pf,
                                k_price=orig_fill_price,
                                size=orig_filled,
                                source="phantom_fill_recovered",
                                notes=f"Original K order filled but polling timed out"
                            )
                            
                            log_print(f"      Cost: ${pm_price_float_pf:.2f} + ${orig_fill_price:.4f} = ${actual_cost:.4f}")
                            log_print(f"      Net profit: ${net_profit:.2f}")
                            logger.log_text(f"  PHANTOM FILL RECOVERED: ${net_profit:.2f} profit ({orig_filled} contracts)")
                            
                            # Update cumulative position tracking
                            new_pm_dir = opp.first_leg_direction if opp.first_leg_venue == Venue.POLYMARKET else opp.second_leg_direction
                            new_direction = "bullish" if new_pm_dir == Direction.UP else "bearish"
                            cum = self._cumulative_positions.get(underlying, {"size": 0, "direction": new_direction})
                            if cum["direction"] == new_direction or cum["size"] == 0:
                                cum["size"] = cum.get("size", 0) + orig_filled
                                cum["direction"] = new_direction
                            else:
                                old_size = cum.get("size", 0)
                                if orig_filled >= old_size:
                                    cum["size"] = orig_filled - old_size
                                    cum["direction"] = new_direction
                                else:
                                    cum["size"] = old_size - orig_filled
                            self._cumulative_positions[underlying] = cum
                            
                            return True  # Skip failsafe entirely - original order filled
                        else:
                            log_print(f"      [Phantom Check] ✓ Original K order confirmed NOT filled - proceeding with failsafe")
                    except Exception as e:
                        log_print(f"      [Phantom Check] ⚠️ Could not verify original order status: {e}")
                        log_print(f"      [Phantom Check] Proceeding with failsafe cautiously")
                
                # Calculate max price based on PM leg cost
                pm_price = leg1_price if opp.first_leg_venue == Venue.POLYMARKET else leg2_price
                pm_price_float = float(pm_price)  # Ensure float for arithmetic
                # Profit floor: unfavorable trades need higher floor (8%)
                is_unfav = hasattr(opp, 'strike_favorable') and not opp.strike_favorable
                profit_floor_pct = 0.08 if is_unfav else 0.0075
                profit_floor_cap = round(1.0 / (1.0 + profit_floor_pct) - pm_price_float, 2)
                breakeven_price = round(1.00 - pm_price_float, 2)
                max_k_price = min(profit_floor_cap, breakeven_price)
                
                k_market_id = opp.second_leg_market_id if opp.first_leg_venue == Venue.POLYMARKET else opp.first_leg_market_id
                k_direction = opp.second_leg_direction if opp.first_leg_venue == Venue.POLYMARKET else opp.first_leg_direction
                original_k_price = float(opp.second_leg_price if opp.first_leg_venue == Venue.POLYMARKET else opp.first_leg_price)
                
                log_print(f"      PM filled @ ${pm_price_float:.2f}")
                log_print(f"      Breakeven K price: ${breakeven_price:.2f}")
                log_print(f"      Max acceptable (0.75% floor): ${max_k_price:.2f}")
                log_print(f"      Original K target: ${original_k_price:.2f}")
                
                # Step up prices: start from original, increase by $0.04 each attempt
                # Fewer, bigger steps reduce zombie risk from cancel-race conditions
                price_steps = []
                current_step = original_k_price
                while current_step <= max_k_price:
                    price_steps.append(round(current_step, 2))
                    current_step += 0.04
                
                # Always include max price as final step
                if not price_steps or price_steps[-1] < max_k_price:
                    price_steps.append(round(max_k_price, 2))
                
                log_print(f"      Price ladder: {[f'${p:.2f}' for p in price_steps[:5]]}{'...' if len(price_steps) > 5 else ''}")
                
                failsafe_filled = False
                failsafe_order_ids = []  # Track ALL failsafe orders for cleanup
                for step_idx, step_price in enumerate(price_steps):
                    if failsafe_filled:
                        break
                    
                    # Get fresh orderbook to check current ask
                    try:
                        fresh_book = await self.kalshi.get_orderbook(k_market_id)
                        if k_direction == Direction.UP:
                            current_ask = float(fresh_book.best_yes_ask) if fresh_book.best_yes_ask else None
                        else:
                            current_ask = float(fresh_book.best_no_ask) if fresh_book.best_no_ask else None
                        
                        # Use the LOWER of our step price or current ask (don't overpay)
                        if current_ask and current_ask <= step_price:
                            order_price = current_ask
                        else:
                            order_price = step_price
                        
                        # Skip if order price exceeds max
                        if order_price > max_k_price:
                            log_print(f"      [{step_idx+1}/{len(price_steps)}] Skip ${order_price:.2f} > max ${max_k_price:.2f}")
                            continue
                        
                        profit_if_fill = 1.0 - pm_price_float - order_price
                        log_print(f"      [{step_idx+1}/{len(price_steps)}] Trying ${order_price:.2f} (profit: ${profit_if_fill:.2f}/contract)")
                        
                        # Place order at this price
                        failsafe_order = await self.kalshi.place_order(
                            k_market_id,
                            OrderSide.BUY,
                            k_direction,
                            Decimal(str(order_price)),
                            Decimal(str(int(filled_size)))
                        )
                        
                        if failsafe_order:
                            failsafe_order_ids.append(failsafe_order.order_id)
                            
                            # Wait for fill (5 seconds per step — give K proper time to fill)
                            for wait_i in range(5):
                                await asyncio.sleep(1)
                                fill = await self.kalshi.get_order(failsafe_order.order_id)
                                fill_size = fill.get("filled_count", 0) or 0
                                
                                if fill_size > 0:
                                    fill_price = float(fill.get("avg_fill_price") or order_price)
                                    log_print(f"      ✅ FAILSAFE SUCCESS: K filled {int(fill_size)} @ ${fill_price:.2f}")
                                    
                                    # Record the fill with window info
                                    dir_str = "yes" if k_direction == Direction.UP else "no"
                                    k_window_end = opp.market_pair.kalshi.end_time if hasattr(opp.market_pair.kalshi, 'end_time') else None
                                    self.kalshi.record_fill(k_market_id, int(fill_size), dir_str, k_window_end)
                                    
                                    # Calculate and record profit
                                    actual_cost = pm_price_float + fill_price
                                    net_profit = profit_tracker.record_trade(
                                        underlying=opp.market_pair.polymarket.underlying,
                                        pm_price=pm_price_float,
                                        k_price=fill_price,
                                        size=int(fill_size),
                                        source="failsafe",
                                        notes=f"Failsafe step {step_idx+1}/{len(price_steps)}"
                                    )
                                    
                                    log_print(f"      Cost: ${pm_price_float:.2f} + ${fill_price:.2f} = ${actual_cost:.2f}")
                                    log_print(f"      Net profit: ${net_profit:.2f}")
                                    
                                    logger.log_text(f"  FAILSAFE SUCCESS: ${net_profit:.2f} profit")
                                    failsafe_filled = True
                                    break
                            
                            # Didn't fill at this price - cancel with verification
                            if not failsafe_filled:
                                try:
                                    await self.kalshi.cancel_order(failsafe_order.order_id)
                                    # VERIFY the cancel actually worked — give Kalshi time to process
                                    await asyncio.sleep(2.0)
                                    verify = await self.kalshi.get_order(failsafe_order.order_id)
                                    verify_filled = int(verify.get("filled_count", 0) or 0)
                                    verify_status = verify.get("status", "unknown")
                                    
                                    if verify_filled > 0:
                                        # Order filled DURING our cancel attempt!
                                        fill_price = float(verify.get("avg_fill_price") or order_price)
                                        log_print(f"      🚨 CANCEL-RACE: Order filled {verify_filled} during cancel @ ${fill_price:.2f}")
                                        
                                        # Record the fill
                                        dir_str = "yes" if k_direction == Direction.UP else "no"
                                        k_window_end = opp.market_pair.kalshi.end_time if hasattr(opp.market_pair.kalshi, 'end_time') else None
                                        self.kalshi.record_fill(k_market_id, verify_filled, dir_str, k_window_end)
                                        
                                        actual_cost = pm_price_float + fill_price
                                        net_profit = profit_tracker.record_trade(
                                            underlying=opp.market_pair.polymarket.underlying,
                                            pm_price=pm_price_float,
                                            k_price=fill_price,
                                            size=verify_filled,
                                            source="failsafe_cancel_race",
                                            notes=f"Filled during cancel at step {step_idx+1}"
                                        )
                                        log_print(f"      FAILSAFE SUCCESS (cancel-race): ${net_profit:.2f} profit")
                                        logger.log_text(f"  FAILSAFE CANCEL-RACE: ${net_profit:.2f} profit")
                                        failsafe_filled = True
                                    elif verify_status in ("cancelled", "canceled"):
                                        log_print(f"      No fill at ${order_price:.2f}, stepping up...")
                                    else:
                                        log_print(f"      ⚠️ Cancel status unclear ({verify_status}) at ${order_price:.2f}, stepping up...")
                                except Exception as e:
                                    log_print(f"      ⚠️ Cancel failed for order {failsafe_order.order_id[:16]}: {e}")
                                    # CRITICAL: Try again - zombie orders cause double fills
                                    try:
                                        await self.kalshi.cancel_order(failsafe_order.order_id)
                                    except:
                                        log_print(f"      🚨 CANCEL RETRY FAILED - zombie order may persist!")
                                    
                    except Exception as e:
                        log_print(f"      Step {step_idx+1} error: {e}")
                        continue
                
                # ═══════════════════════════════════════════════════════════
                # CLEANUP: Cancel ALL failsafe orders and check for zombie fills
                # Zombie fills ARE real fills — if detected, treat as success
                # ═══════════════════════════════════════════════════════════
                zombie_fills_total = 0
                zombie_fill_price = 0.0
                zombie_fill_order_id = None
                
                for fso_id in failsafe_order_ids:
                    try:
                        # Cancel regardless (idempotent - already cancelled is fine)
                        try:
                            await self.kalshi.cancel_order(fso_id)
                        except:
                            pass
                        
                        # Wait for Kalshi to process cancel before checking status
                        await asyncio.sleep(1.0)
                        
                        # Check for zombie fill
                        fso_status = await self.kalshi.get_order(fso_id)
                        fso_filled = int(fso_status.get("filled_count", 0) or 0)
                        if fso_filled > 0 and not failsafe_filled:
                            # Zombie fill detected — this IS our K fill
                            zfp = float(fso_status.get("avg_fill_price") or 0)
                            log_print(f"      🧟 ZOMBIE FILL: Failsafe order {fso_id[:16]} filled {fso_filled} @ ${zfp:.2f}")
                            zombie_fills_total += fso_filled
                            if zfp > 0:
                                zombie_fill_price = zfp
                            zombie_fill_order_id = fso_id
                    except:
                        pass
                
                # ═══════════════════════════════════════════════════════════
                # ZOMBIE FILLS = SUCCESS: Record them as real K fills
                # This prevents the PM close from firing and creating both
                # sides on PM + unhedged K contracts
                # ═══════════════════════════════════════════════════════════
                if zombie_fills_total > 0 and not failsafe_filled:
                    log_print(f"      ✅ ZOMBIE RECOVERY: {zombie_fills_total} contract(s) filled @ ${zombie_fill_price:.2f} — treating as successful hedge")
                    
                    # Record the K fill
                    dir_str = "yes" if k_direction == Direction.UP else "no"
                    k_window_end = opp.market_pair.kalshi.end_time if hasattr(opp.market_pair.kalshi, 'end_time') else None
                    self.kalshi.record_fill(k_market_id, zombie_fills_total, dir_str, k_window_end)
                    
                    # Record profit (may be negative — that's fine, position is hedged)
                    actual_cost = pm_price_float + zombie_fill_price
                    net_profit = profit_tracker.record_trade(
                        underlying=opp.market_pair.polymarket.underlying,
                        pm_price=pm_price_float,
                        k_price=zombie_fill_price,
                        size=zombie_fills_total,
                        source="zombie_recovered",
                        notes=f"Zombie fill recovered — order {zombie_fill_order_id[:16] if zombie_fill_order_id else '?'}"
                    )
                    
                    log_print(f"      Cost: ${pm_price_float:.2f} + ${zombie_fill_price:.2f} = ${actual_cost:.2f}")
                    log_print(f"      Net profit: ${net_profit:.2f}")
                    logger.log_text(f"  ZOMBIE RECOVERY: ${net_profit:.2f} profit ({zombie_fills_total} contracts)")
                    
                    # Update cumulative position tracking
                    new_pm_dir = opp.first_leg_direction if opp.first_leg_venue == Venue.POLYMARKET else opp.second_leg_direction
                    new_direction = "bullish" if new_pm_dir == Direction.UP else "bearish"
                    cum = self._cumulative_positions.get(underlying, {"size": 0, "direction": new_direction})
                    if cum["direction"] == new_direction or cum["size"] == 0:
                        cum["size"] = cum.get("size", 0) + zombie_fills_total
                        cum["direction"] = new_direction
                    else:
                        old_size = cum.get("size", 0)
                        if zombie_fills_total >= old_size:
                            cum["size"] = zombie_fills_total - old_size
                            cum["direction"] = new_direction
                        else:
                            cum["size"] = old_size - zombie_fills_total
                    self._cumulative_positions[underlying] = cum
                    
                    failsafe_filled = True
                
                if failsafe_filled:
                    return True
                else:
                    log_print(f"      ⚠️ FAILSAFE EXHAUSTED: Could not fill K at profit-floor price up to ${max_k_price:.2f}")
                    
                    pm_direction_val = opp.first_leg_direction if opp.first_leg_venue == Venue.POLYMARKET else opp.second_leg_direction
                    pm_market_id = opp.first_leg_market_id if opp.first_leg_venue == Venue.POLYMARKET else opp.second_leg_market_id
                    
                    # ═══════════════════════════════════════════════════════════
                    # EMERGENCY K HEDGE: Always try K first regardless of fill size
                    # Rationale: Balanced positions (even at a loss) are ALWAYS better
                    # than unhedged exposure. Winners make up for hedged losers,
                    # but completely failed trades are unrecoverable risk.
                    # Go up to breakeven — we'd rather make $0 than be unhedged.
                    # ═══════════════════════════════════════════════════════════
                    hard_cap = min(breakeven_price, 0.95)  # Go up to breakeven, hard cap $0.95
                    log_print(f"      🚨 EMERGENCY K HEDGE ({int(filled_size)} contracts): Trying up to ${hard_cap:.2f} (breakeven: ${breakeven_price:.2f})")
                    
                    emergency_price = max_k_price + 0.03
                    emergency_filled = False
                    emergency_order_ids = []
                    while emergency_price <= hard_cap:
                        try:
                            order_price = round(emergency_price, 2)
                            profit_if_fill = 1.0 - pm_price_float - order_price
                            log_print(f"      [EMERGENCY K] Trying ${order_price:.2f} (P&L/c: ${profit_if_fill:.3f})")
                            
                            emergency_order = await self.kalshi.place_order(
                                k_market_id, OrderSide.BUY, k_direction,
                                Decimal(str(order_price)),
                                Decimal(str(int(filled_size)))
                            )
                            
                            if emergency_order:
                                emergency_order_ids.append(emergency_order.order_id)
                                for w in range(5):
                                    await asyncio.sleep(1)
                                    fill = await self.kalshi.get_order(emergency_order.order_id)
                                    fill_count = fill.get("filled_count", 0) or 0
                                    if fill_count > 0:
                                        fill_price = float(fill.get("avg_fill_price") or order_price)
                                        actual_cost = pm_price_float + fill_price
                                        log_print(f"      ✅ EMERGENCY K FILLED: {int(fill_count)} @ ${fill_price:.2f} (total: ${actual_cost:.2f})")
                                        
                                        dir_str = "yes" if k_direction == Direction.UP else "no"
                                        k_window_end = opp.market_pair.kalshi.end_time if hasattr(opp.market_pair.kalshi, 'end_time') else None
                                        self.kalshi.record_fill(k_market_id, int(fill_count), dir_str, k_window_end)
                                        
                                        net_profit = profit_tracker.record_trade(
                                            underlying=opp.market_pair.polymarket.underlying,
                                            pm_price=pm_price_float,
                                            k_price=fill_price,
                                            size=int(fill_count),
                                            source="emergency_hedge",
                                            notes=f"Emergency K hedge at ${fill_price:.2f}"
                                        )
                                        log_print(f"      Net P&L: ${net_profit:.2f}")
                                        logger.log_text(f"  EMERGENCY HEDGE: ${net_profit:.2f} profit ({int(fill_count)} contracts)")
                                        emergency_filled = True
                                        break
                                
                                if emergency_filled:
                                    break
                                    
                                # Cancel with proper wait before next attempt
                                try:
                                    await self.kalshi.cancel_order(emergency_order.order_id)
                                    await asyncio.sleep(2.0)  # Let Kalshi process cancel
                                except:
                                    pass
                        except Exception as e:
                            log_print(f"      [EMERGENCY K] Error at ${order_price:.2f}: {e}")
                        
                        emergency_price += 0.04
                    
                    # Clean up any emergency orders that might still be live
                    for eid in emergency_order_ids:
                        try:
                            await self.kalshi.cancel_order(eid)
                        except:
                            pass
                    
                    # Check for zombie fills from emergency orders too
                    if not emergency_filled:
                        await asyncio.sleep(1.5)
                        for eid in emergency_order_ids:
                            try:
                                es = await self.kalshi.get_order(eid)
                                ef = int(es.get("filled_count", 0) or 0)
                                if ef > 0:
                                    efp = float(es.get("avg_fill_price") or 0)
                                    log_print(f"      🧟 EMERGENCY ZOMBIE: Order {eid[:16]} filled {ef} @ ${efp:.2f}")
                                    
                                    dir_str = "yes" if k_direction == Direction.UP else "no"
                                    k_window_end = opp.market_pair.kalshi.end_time if hasattr(opp.market_pair.kalshi, 'end_time') else None
                                    self.kalshi.record_fill(k_market_id, ef, dir_str, k_window_end)
                                    
                                    net_profit = profit_tracker.record_trade(
                                        underlying=opp.market_pair.polymarket.underlying,
                                        pm_price=pm_price_float,
                                        k_price=efp,
                                        size=ef,
                                        source="emergency_zombie",
                                        notes=f"Emergency zombie fill recovered"
                                    )
                                    log_print(f"      ✅ EMERGENCY ZOMBIE RECOVERED: ${net_profit:.2f}")
                                    logger.log_text(f"  EMERGENCY ZOMBIE RECOVERED: ${net_profit:.2f}")
                                    emergency_filled = True
                                    break
                            except:
                                pass
                    
                    if emergency_filled:
                        return True
                    
                    # ═══════════════════════════════════════════════════════════
                    # LAST RESORT: Close PM by buying opposite side
                    # Only reach here if K is completely unfillable after trying
                    # all the way to breakeven. This is the worst case — we're
                    # locking in a small loss but at least closing the exposure.
                    # ═══════════════════════════════════════════════════════════
                    log_print(f"      ❌ EMERGENCY K FAILED up to ${hard_cap:.2f} — falling back to PM close")
                    
                    opposite_direction = Direction.UP if pm_direction_val == Direction.DOWN else Direction.DOWN
                    log_print(f"      🔄 CLOSING PM: Buying {int(filled_size)} {opposite_direction.value} to close {pm_direction_val.value} position")
                    
                    # Get orderbook for opposite side to find ask price
                    try:
                        pm_book = await self.pm.get_orderbook(pm_market_id)
                        # Determine the correct ask price based on what we're buying
                        if opposite_direction == Direction.UP:
                            close_ask = pm_book.best_yes_ask if pm_book else None
                        else:
                            close_ask = pm_book.best_no_ask if pm_book else None
                        
                        if close_ask:
                            close_price = float(close_ask)
                        else:
                            # No book - estimate: opposite side ≈ 1.0 - our_price
                            close_price = round(1.0 - pm_price_float + 0.02, 2)  # +2¢ buffer
                        
                        # Cap at reasonable price - we're eating a loss but not unlimited
                        # Total cost to close = pm_price + close_price, should be close to $1.00
                        max_close_price = 0.92
                        close_price = min(close_price, max_close_price)
                        
                        total_close_cost = pm_price_float + close_price
                        close_pnl = 1.0 - total_close_cost  # Should be near 0 or slightly negative
                        log_print(f"      Close price: ${close_price:.2f} (total cost: ${total_close_cost:.2f}, P&L/c: ${close_pnl:.3f})")
                        
                        close_order = await self.pm.place_order(
                            pm_market_id, OrderSide.BUY, opposite_direction,
                            Decimal(str(round(close_price, 2))),
                            Decimal(str(int(filled_size)))
                        )
                        
                        if close_order:
                            # Wait for fill
                            close_filled = False
                            for w in range(10):
                                await asyncio.sleep(1)
                                try:
                                    status = await self.pm.get_order(close_order.order_id)
                                    fill_count = float(status.get("size_matched", 0) or status.get("filled_count", 0) or 0)
                                    if fill_count > 0:
                                        fill_price = float(status.get("avg_fill_price") or close_price)
                                        actual_total = pm_price_float + fill_price
                                        actual_pnl = (1.0 - actual_total) * fill_count
                                        log_print(f"      ✅ PM CLOSED: {int(fill_count)} {opposite_direction.value} @ ${fill_price:.2f}")
                                        log_print(f"      Position locked: ${pm_price_float:.2f} {pm_direction_val.value} + ${fill_price:.2f} {opposite_direction.value} = ${actual_total:.2f}/contract")
                                        log_print(f"      Settlement P&L: ${actual_pnl:.2f} ({int(fill_count)} contracts)")
                                        
                                        # Record the close - track as a hedge cost
                                        profit_tracker.record_hedge_cost(
                                            underlying=opp.market_pair.polymarket.underlying,
                                            venue="polymarket",
                                            price=fill_price,
                                            size=int(fill_count),
                                            notes=f"PM close: bought {opposite_direction.value} @ ${fill_price:.2f}"
                                        )
                                        close_filled = True
                                        break
                                except:
                                    pass
                            
                            if not close_filled:
                                log_print(f"      ⚠️ PM close didn't fill in 10s - trying higher price")
                                try:
                                    await self.pm.cancel_order(close_order.order_id)
                                except:
                                    pass
                                
                                # Retry at more aggressive price
                                aggressive_price = min(close_price + 0.05, max_close_price)
                                log_print(f"      Retrying PM close at ${aggressive_price:.2f}")
                                retry_order = await self.pm.place_order(
                                    pm_market_id, OrderSide.BUY, opposite_direction,
                                    Decimal(str(round(aggressive_price, 2))),
                                    Decimal(str(int(filled_size)))
                                )
                                if retry_order:
                                    for w in range(10):
                                        await asyncio.sleep(1)
                                        try:
                                            status = await self.pm.get_order(retry_order.order_id)
                                            fill_count = float(status.get("size_matched", 0) or status.get("filled_count", 0) or 0)
                                            if fill_count > 0:
                                                log_print(f"      ✅ PM CLOSED on retry: {int(fill_count)} @ ${aggressive_price:.2f}")
                                                close_filled = True
                                                break
                                        except:
                                            pass
                                    if not close_filled:
                                        log_print(f"      ❌ PM close retry failed - position remains open")
                                        try:
                                            await self.pm.cancel_order(retry_order.order_id)
                                        except:
                                            pass
                        else:
                            log_print(f"      ❌ PM close order failed to place")
                    except Exception as e:
                        log_print(f"      ❌ PM close error: {e}")
            
            # ═══════════════════════════════════════════════════════════════════
            # END FAILSAFE - continue with normal partial fill handling
            # ═══════════════════════════════════════════════════════════════════
            
            # ═══════════════════════════════════════════════════════════════════
            # REVERSE FAILSAFE: K filled but PM didn't → sell K to unwind
            # This happens when K is first leg and PM chase timed out
            # ═══════════════════════════════════════════════════════════════════
            if (filled_venue == Venue.KALSHI and 
                unfilled_venue == Venue.POLYMARKET):
                
                k_direction_val = opp.first_leg_direction if opp.first_leg_venue == Venue.KALSHI else opp.second_leg_direction
                k_market_id = opp.first_leg_market_id if opp.first_leg_venue == Venue.KALSHI else opp.second_leg_market_id
                
                # Fresh PM check - order may have filled after we returned 0
                pm_late_fill = 0
                if pm_order:
                    try:
                        await asyncio.sleep(1.0)  # Give PM a moment to settle
                        fresh_pm = await self.pm.get_order(pm_order.order_id)
                        pm_late_fill = float(fresh_pm.get("size_matched", 0) or fresh_pm.get("filled_count", 0) or 0)
                        if pm_late_fill > 0:
                            log_print(f"\n  🔍 LATE PM FILL: {int(pm_late_fill)} contracts filled after stepping loop ended!")
                    except:
                        pass
                
                if pm_late_fill >= int(filled_size):
                    # PM actually filled everything! Record it and we're good
                    log_print(f"  ✅ PM fully filled late ({int(pm_late_fill)}) - arb is complete, no K unwind needed")
                    pm_fill_price = float(fresh_pm.get("avg_fill_price") or 0)
                    
                    # Record PM fill
                    pm_dir = opp.first_leg_direction if opp.first_leg_venue == Venue.POLYMARKET else opp.second_leg_direction
                    pm_market = opp.first_leg_market_id if opp.first_leg_venue == Venue.POLYMARKET else opp.second_leg_market_id
                    dir_str = "down" if pm_dir == Direction.DOWN else "up"
                    pm_window = opp.market_pair.polymarket.end_time if hasattr(opp.market_pair.polymarket, 'end_time') else None
                    self.pm.record_fill(pm_market, int(pm_late_fill), underlying, dir_str, pm_window)
                    
                    # Record profit
                    k_fill_price = float(leg1_price if opp.first_leg_venue == Venue.KALSHI else leg2_price)
                    net_profit = profit_tracker.record_trade(
                        underlying=underlying,
                        pm_price=pm_fill_price,
                        k_price=k_fill_price,
                        size=int(pm_late_fill),
                        source="late_fill",
                        notes="PM filled after stepping loop"
                    )
                    log_print(f"  Late fill profit: ${net_profit:.2f}")
                    return True
                
                elif pm_late_fill > 0:
                    # PM partially filled late - sell K excess only
                    excess_k = int(filled_size) - int(pm_late_fill)
                    log_print(f"\n  🔄 REVERSE FAILSAFE: K has {int(filled_size)}, PM late-filled {int(pm_late_fill)} - selling {excess_k} K excess...")
                    filled_size = excess_k  # Adjust what we need to sell
                    
                    # Record PM partial fill
                    pm_dir = opp.first_leg_direction if opp.first_leg_venue == Venue.POLYMARKET else opp.second_leg_direction
                    pm_market = opp.first_leg_market_id if opp.first_leg_venue == Venue.POLYMARKET else opp.second_leg_market_id
                    dir_str = "down" if pm_dir == Direction.DOWN else "up"
                    pm_window = opp.market_pair.polymarket.end_time if hasattr(opp.market_pair.polymarket, 'end_time') else None
                    self.pm.record_fill(pm_market, int(pm_late_fill), underlying, dir_str, pm_window)
                else:
                    log_print(f"\n  🔄 REVERSE FAILSAFE: K filled {int(filled_size)} but PM didn't - selling K to unwind...")
                
                try:
                    kb = await self.kalshi.get_orderbook(k_market_id)
                    if k_direction_val == Direction.UP:
                        best_bid = kb.best_yes_bid
                    else:
                        best_bid = kb.best_no_bid
                    
                    if best_bid:
                        sell_price = max(float(best_bid) * 0.95, 0.01)
                        sell_order = await self.kalshi.place_order(
                            k_market_id,
                            OrderSide.SELL,
                            k_direction_val,
                            Decimal(str(round(sell_price, 2))),
                            Decimal(str(int(filled_size)))
                        )
                        
                        if sell_order:
                            await asyncio.sleep(2)
                            fill = await self.kalshi.get_order(sell_order.order_id)
                            filled_count = fill.get("filled_count", 0) or 0
                            
                            if filled_count >= int(filled_size):
                                log_print(f"      ✓ Sold {filled_count}/{int(filled_size)} K @ ~${sell_price:.2f} - position unwound")
                                # Clear internal positions
                                self.kalshi._internal_positions = {}
                                logger.log_text(f"  REVERSE FAILSAFE: K sold, trade unwound for {underlying}")
                                return False
                            else:
                                log_print(f"      ⚠️ Only sold {filled_count}/{int(filled_size)} K - {int(filled_size) - filled_count} orphaned")
                                self._has_orphaned_positions = True
                        else:
                            log_print(f"      ❌ K sell order failed")
                            self._has_orphaned_positions = True
                    else:
                        log_print(f"      ❌ No K bid - can't sell")
                        self._has_orphaned_positions = True
                except Exception as e:
                    log_print(f"      ❌ K sell error: {e}")
                    self._has_orphaned_positions = True
            
            # Track the unfilled order for later cancellation
            unfilled_order = leg2_order if leg1_size > 0 else leg1_order
            if unfilled_order:
                self._last_unfilled_order = {
                    "order_id": unfilled_order.order_id,
                    "venue": unfilled_venue,
                    "time": time.time()
                }
            
            # If PM filled but with < expected contracts, track it for later hedging
            # Use the actual trade size (which includes bonus contracts), not config base
            actual_trade_size = int(size)
            if leg1_size > 0 and leg1_size < actual_trade_size and opp.first_leg_venue == Venue.POLYMARKET and leg1_order:
                dir_str = "down" if opp.first_leg_direction == Direction.DOWN else "up"
                self.add_pending_pm_fill(
                    order_id=leg1_order.order_id,
                    market_id=opp.first_leg_market_id,
                    size=int(leg1_size),
                    underlying=opp.market_pair.polymarket.underlying,
                    direction=dir_str,
                    k_market_id=opp.second_leg_market_id,
                    k_direction=opp.second_leg_direction,
                    k_price=float(second_price)
                )
                log_print(f"      [Tracking] Added PM partial fill for hedging ({int(leg1_size)} contracts)")
            
            log_print(f"\n  ⚠️ PARTIAL FILL: Only {filled_leg} filled ({int(filled_size)} on {filled_venue.value})")
            log_print(f"      Will attempt to hedge via reconciliation...")
            
            # 🛑 SAFETY: Trigger pause on partial fills - unhedged exposure is dangerous
            unfilled_venue = "K" if filled_venue == Venue.POLYMARKET else "PM"
            self.trigger_safety_pause(
                f"PARTIAL FILL: {filled_leg} filled {int(filled_size)} on {filled_venue.value}, "
                f"{unfilled_venue} FAILED - unhedged exposure",
                unhedged={
                    "underlying": underlying,
                    "venue": filled_venue.value,
                    "size": int(filled_size),
                    "direction": opp.first_leg_direction.value if filled_leg == "Leg1" else opp.second_leg_direction.value
                }
            )
            
            # Still count partial fills toward the trade limit (they consume capital and add risk)
            self._window_trade_counts[underlying] = self._window_trade_counts.get(underlying, 0) + 1
            self._window_contract_counts[underlying] = self._window_contract_counts.get(underlying, 0) + int(filled_size)
            trade_count = self._window_trade_counts[underlying]
            is_fav = getattr(opp, 'strike_favorable', False)
            max_trades = self._get_max_trades_for_window(is_fav)
            log_print(f"      Trade count: {trade_count}/{max_trades} ({'fav' if is_fav else 'unfav'}) for {underlying}")
            
            # Update cumulative position tracker for partial fills too
            new_pm_dir = opp.first_leg_direction if opp.first_leg_venue == Venue.POLYMARKET else opp.second_leg_direction
            new_direction = "bullish" if new_pm_dir == Direction.UP else "bearish"
            cum = self._cumulative_positions.get(underlying, {"size": 0, "direction": new_direction})
            cum["size"] = cum.get("size", 0) + int(filled_size)
            cum["direction"] = new_direction
            self._cumulative_positions[underlying] = cum
            max_pos = CONFIG["markets"].get("max_position_per_underlying", 30)
            log_print(f"      Cumulative position: {cum['size']}/{max_pos} {cum['direction']} for {underlying}")
            
            logger.log_text(f"  PARTIAL FILL: {filled_leg} = {int(filled_size)} on {filled_venue.value}")
            return False
        else:
            # Neither filled
            log_print(f"\n  ❌ NO FILLS - both legs failed")
            return False

    # DEPRECATED: These functions are not used - execute_arb_parallel is the main function
    # Keeping as stubs to avoid breaking any potential references
    async def execute_first_leg(self, opp: ScoredOpportunity) -> Optional[ActivePosition]:
        """DEPRECATED - Do not use. Use execute_arb_parallel instead."""
        print("  ⚠️ WARNING: execute_first_leg is deprecated and disabled!")
        return None
    
    async def execute_parallel_arb(self, opp: ScoredOpportunity) -> Optional[ActivePosition]:
        """DEPRECATED - Do not use. Use execute_arb_parallel instead."""
        print("  ⚠️ WARNING: execute_parallel_arb is deprecated and disabled!")
        return None
    
    # NOTE: The old execute_parallel_arb code has been removed to prevent accidental order placement.
    # The main execution function is execute_arb_parallel (note the different name order).
    async def execute_second_leg(self, position: ActivePosition) -> bool:
        """Execute the second leg with time-based patience - work the orderbook"""
        opp = position.opportunity
        size = Decimal(str(position.first_leg_fill_size))
        
        # POSITION VERIFICATION: Check what we already have on the second leg venue
        # This prevents overfilling if a previous order filled after we thought it was cancelled
        try:
            if opp.second_leg_venue == Venue.POLYMARKET:
                existing_pos = await self.pm.get_position_for_token(opp.second_leg_market_id)
            else:
                existing_size, existing_dir = await self.kalshi.get_position_for_ticker(opp.second_leg_market_id)
                # Check if direction matches what we need
                needed_dir = "yes" if opp.second_leg_direction == Direction.UP else "no"
                existing_pos = existing_size if existing_dir == needed_dir else 0
            
            if existing_pos > 0:
                print(f"      ⚠️ Already have {existing_pos} contracts on second leg venue")
                # Reduce size needed
                adjusted_size = float(size) - existing_pos
                if adjusted_size <= 0:
                    print(f"      ✓ Second leg already complete (have {existing_pos}, need {size})")
                    # Mark as complete - calculate profit based on existing position
                    # Note: We don't know the exact price, so this is an estimate
                    fill_size = float(size)
                    fill_price = float(opp.second_leg_price)  # Use expected price as estimate
                    
                    actual_cost = float(position.first_leg_fill_price) + fill_price
                    gross_profit = 1.0 - actual_cost
                    # PM fee: feeRateBps/10000 * price * (1-price)
                    pm_p = fill_price if opp.second_leg_venue == Venue.POLYMARKET else float(position.first_leg_fill_price)
                    pm_fee = (200 / 10000) * pm_p * (1.0 - pm_p)  # 200bps default
                    # Kalshi taker fee
                    k_p = float(position.first_leg_fill_price) if opp.second_leg_venue == Venue.POLYMARKET else fill_price
                    k_fee = math.ceil(0.07 * k_p * (1.0 - k_p) * 100) / 100
                    net_profit = gross_profit - pm_fee - k_fee
                    total_net_profit = net_profit * int(fill_size)
                    
                    print(f"  <<< ARB COMPLETE (pre-filled)!")
                    print(f"      Est profit: ${total_net_profit:.2f}")
                    
                    self._active_position = None
                    return True
                else:
                    size = Decimal(str(int(adjusted_size)))
                    print(f"      Adjusted order size to {size}")
        except Exception as e:
            # Position check failed - continue with original size
            pass
        
        # Calculate time remaining in market window
        time_to_close = opp.market_pair.polymarket.time_to_close  # seconds until market closes
        market_duration = 15 * 60  # 15 minutes
        time_elapsed = market_duration - time_to_close  # seconds since market opened
        minutes_elapsed = time_elapsed / 60
        
        # Determine how much time we have for second leg based on when we entered
        # Shortened timers - we want to be more aggressive
        if minutes_elapsed < 2:
            total_time_for_fill = 30  # Was 60
        elif minutes_elapsed < 4:
            total_time_for_fill = 20  # Was 45
        elif minutes_elapsed < 6:
            total_time_for_fill = 15  # Was 30
        elif minutes_elapsed < 8:
            total_time_for_fill = 10  # Was 20
        else:
            total_time_for_fill = 8   # Was 15 - very urgent
        
        # If this was a partial fill, be much more aggressive (cut losses mode)
        if position.is_partial_fill:
            total_time_for_fill = min(total_time_for_fill, 10)  # Max 10 seconds for partial fills
            print(f"\n  >>> SECOND LEG (PARTIAL FILL - URGENT): {opp.second_leg_venue.value} {opp.second_leg_direction.value.upper()}")
        else:
            print(f"\n  >>> SECOND LEG: {opp.second_leg_venue.value} {opp.second_leg_direction.value.upper()}")
        
        # But don't exceed time remaining (leave 20s buffer before close)
        max_time = max(int(time_to_close) - 20, 5)
        total_time_for_fill = min(total_time_for_fill, max_time)
        
        print(f"      Time budget: {total_time_for_fill}s (entered at {minutes_elapsed:.1f} min)")
        
        # Get current orderbook
        async def get_current_book():
            if opp.second_leg_venue == Venue.POLYMARKET:
                return await self.pm.get_orderbook(opp.second_leg_market_id)
            else:
                return await self.kalshi.get_orderbook(opp.second_leg_market_id)
        
        def get_prices(book):
            """Get bid, mid, ask prices for the direction we need"""
            if opp.second_leg_direction == Direction.UP:
                bid = book.best_yes_bid or Decimal("0.01")
                ask = book.best_yes_ask or Decimal("0.99")
            else:
                bid = book.best_no_bid or Decimal("0.01")
                ask = book.best_no_ask or Decimal("0.99")
            mid = (bid + ask) / 2
            return bid, mid, ask
        
        book = await get_current_book()
        bid, mid, ask = get_prices(book)
        
        # Calculate breakeven price (what we need to pay to not lose money)
        first_leg_cost = float(position.first_leg_fill_price)
        breakeven_price = 1.0 - first_leg_cost
        
        # Maximum we're willing to pay - stay 1% under breakeven to guarantee profit
        # (after fees we need some margin)
        max_acceptable_price = breakeven_price - 0.01  # At least 1 cent profit per contract
        
        # Log the price levels
        print(f"      Breakeven: ${breakeven_price:.2f} | Bid: ${bid:.2f} | Mid: ${mid:.2f} | Ask: ${ask:.2f}")
        print(f"      Max acceptable: ${max_acceptable_price:.2f}")
        
        # Check if ANY price can meet minimum order size
        min_order_value = 1.0
        min_viable_price = min_order_value / float(size)
        buffer_pct = Decimal(str(CONFIG["aggressive_fill"]["second_leg_buffer_pct"])) / 100
        max_price = min(ask * (1 + buffer_pct), Decimal("0.99"))
        
        if float(max_price) < min_viable_price:
            print(f"      ⚠️ All prices below $1 minimum (need ${min_viable_price:.2f}, max is ${max_price:.2f})")
            print(f"      Position remains open - price too low for {size} contracts")
            return False
        
        # FAST SECOND LEG EXECUTION:
        # - Start at bid price, rapidly escalate to ask
        # - No waiting between price adjustments - spam orders
        # - Cancel and reorder immediately if no fill
        
        target_size = int(size)  # This is what first leg filled
        total_filled = 0
        weighted_price_sum = 0
        current_order = None
        current_order_price = Decimal("0")
        counted_order_fills = {}  # Track fills already counted per order_id
        
        # Price escalation: start at bid, move toward max_acceptable rapidly
        current_price = bid
        
        elapsed = 0
        iteration = 0
        
        print(f"      Target: {target_size} contracts (must match first leg)")
        
        while elapsed < total_time_for_fill and total_filled < target_size:
            iteration += 1
            
            # Refresh orderbook every few iterations
            if iteration % 5 == 1:
                book = await get_current_book()
                bid, mid, ask = get_prices(book)
            
            # Calculate remaining size needed
            remaining = target_size - total_filled
            
            # Determine price for this iteration
            # Escalate from bid toward max_acceptable over time
            progress = min(elapsed / total_time_for_fill, 1.0)
            # Use sqrt escalation - starts faster, slows down
            target_price = float(bid) + (max_acceptable_price - float(bid)) * (progress ** 0.5)
            current_price = Decimal(str(round(min(target_price, max_acceptable_price), 2)))
            
            # Ensure order meets minimum value
            order_value = float(current_price) * remaining
            if order_value < 1.0:
                # Price too low for order size - jump to minimum viable
                current_price = Decimal(str(round(max(1.0 / remaining + 0.01, float(current_price)), 2)))
            
            # Cap at max acceptable
            if current_price > Decimal(str(max_acceptable_price)):
                current_price = Decimal(str(round(max_acceptable_price, 2)))
            
            # Log every iteration now since we're slower
            should_log = iteration == 1 or iteration % 2 == 0
            if should_log:
                tier_name = "bid" if float(current_price) <= float(bid) else "mid" if float(current_price) <= float(mid) else "ask" if float(current_price) <= float(ask) else "ask+buf"
                profit_per = 1.0 - float(position.first_leg_fill_price) - float(current_price)
                print(f"      [{int(elapsed)}s] {tier_name}: ${current_price:.2f} -> ${profit_per:+.2f}/contract")
            
            # Cancel existing order and place new one at current price
            if current_order:
                try:
                    if opp.second_leg_venue == Venue.POLYMARKET:
                        await self.pm.cancel_order(current_order.order_id)
                    else:
                        await self.kalshi.cancel_order(current_order.order_id)
                    
                    # Quick check for fills on cancelled order
                    if opp.second_leg_venue == Venue.POLYMARKET:
                        check = await self.pm.get_order(current_order.order_id)
                    else:
                        check = await self.kalshi.get_order(current_order.order_id)
                    
                    order_filled = check.get("filled_count", 0)
                    # Only count NEW fills we haven't counted before
                    already_counted = counted_order_fills.get(current_order.order_id, 0)
                    new_fills = order_filled - already_counted
                    if new_fills > 0:
                        order_price = check.get("avg_fill_price") or float(current_order_price)
                        if isinstance(order_price, str):
                            order_price = float(order_price)
                        weighted_price_sum += new_fills * float(order_price)
                        total_filled += int(new_fills)
                        counted_order_fills[current_order.order_id] = order_filled
                        remaining = target_size - total_filled
                        if remaining <= 0:
                            print(f"      ✓ FILLED {int(new_fills)} @ ${float(order_price):.2f}!")
                            break
                except:
                    pass
                current_order = None
            
            # Place new order if we still need contracts
            if remaining > 0:
                try:
                    if opp.second_leg_venue == Venue.POLYMARKET:
                        current_order = await self.pm.place_order(
                            opp.second_leg_market_id, OrderSide.BUY,
                            opp.second_leg_direction, current_price, Decimal(str(remaining))
                        )
                    else:
                        current_order = await self.kalshi.place_order(
                            opp.second_leg_market_id, OrderSide.BUY,
                            opp.second_leg_direction, current_price, Decimal(str(remaining))
                        )
                    current_order_price = current_price
                    
                    # Immediately check for fill (limit orders can fill instantly)
                    if opp.second_leg_venue == Venue.POLYMARKET:
                        fill = await self.pm.get_order(current_order.order_id)
                    else:
                        fill = await self.kalshi.get_order(current_order.order_id)
                    
                    order_filled = fill.get("filled_count", 0)
                    # Track and only count new fills
                    already_counted = counted_order_fills.get(current_order.order_id, 0)
                    new_fills = order_filled - already_counted
                    if new_fills >= remaining:
                        order_price = fill.get("avg_fill_price") or float(current_order_price)
                        if isinstance(order_price, str):
                            order_price = float(order_price)
                        weighted_price_sum += new_fills * float(order_price)
                        total_filled += int(new_fills)
                        counted_order_fills[current_order.order_id] = order_filled
                        print(f"      ✓ FILLED {int(new_fills)} @ ${float(order_price):.2f}!")
                        current_order = None
                        break
                except:
                    current_order = None
            
            # Delay between price adjustments - give orders time to fill
            # 2.5 seconds gives market makers time to see and match
            await asyncio.sleep(2.5)
            elapsed += 2.5
        
        # Clean up any remaining order
        if current_order:
            try:
                if opp.second_leg_venue == Venue.POLYMARKET:
                    await self.pm.cancel_order(current_order.order_id)
                else:
                    await self.kalshi.cancel_order(current_order.order_id)
                
                # Final check for fills
                await asyncio.sleep(0.3)
                if opp.second_leg_venue == Venue.POLYMARKET:
                    check = await self.pm.get_order(current_order.order_id)
                else:
                    check = await self.kalshi.get_order(current_order.order_id)
                
                order_filled = check.get("filled_count", 0)
                # Only count NEW fills we haven't counted before
                already_counted = counted_order_fills.get(current_order.order_id, 0)
                new_fills = order_filled - already_counted
                if new_fills > 0:
                    order_price = check.get("avg_fill_price") or float(current_order_price)
                    if isinstance(order_price, str):
                        order_price = float(order_price)
                    weighted_price_sum += new_fills * float(order_price)
                    total_filled += int(new_fills)
                    counted_order_fills[current_order.order_id] = order_filled
                    print(f"      Final fills: {int(new_fills)}")
            except:
                pass
        
        # Calculate final fill info
        filled = total_filled >= target_size
        fill_size = total_filled
        fill_price = weighted_price_sum / total_filled if total_filled > 0 else 0
        
        if not filled or fill_size == 0:
            # Increment attempt counter
            position.fill_attempts += 1
            max_attempts = CONFIG["aggressive_fill"].get("cut_loss_after_attempts", 2)
            
            if position.fill_attempts >= max_attempts:
                # We've tried multiple times - now accept a loss to exit
                max_loss_pct = CONFIG["aggressive_fill"].get("max_loss_to_cut_pct", 10.0) / 100
                max_loss_price = breakeven_price * (1 + max_loss_pct)  # Allow paying more than breakeven
                
                # Calculate remaining contracts needed
                remaining_for_cutloss = target_size - total_filled
                if remaining_for_cutloss <= 0:
                    # Actually we're done - fills came through
                    filled = True
                    fill_size = total_filled
                    fill_price = weighted_price_sum / total_filled if total_filled > 0 else 0
                else:
                    print(f"      ⚠️ Attempt {position.fill_attempts}/{max_attempts} failed - trying CUT LOSS mode")
                    print(f"      Remaining: {remaining_for_cutloss} contracts | Max loss price: ${max_loss_price:.2f}")
                    
                    # Try one more time at the loss-accepting price
                    book = await get_current_book()
                    bid, mid, ask = get_prices(book)
                    
                    # Use ask price but cap at max loss
                    cut_price = min(float(ask), max_loss_price)
                    
                    if cut_price >= 0.99:
                        print(f"      ✗ Price too high even for cut-loss (${cut_price:.2f}) - position stays OPEN")
                        return False
                    
                    try:
                        if opp.second_leg_venue == Venue.POLYMARKET:
                            order = await self.pm.place_order(
                                opp.second_leg_market_id, OrderSide.BUY,
                                opp.second_leg_direction, Decimal(str(cut_price)), Decimal(str(remaining_for_cutloss))
                            )
                        else:
                            order = await self.kalshi.place_order(
                                opp.second_leg_market_id, OrderSide.BUY,
                                opp.second_leg_direction, Decimal(str(cut_price)), Decimal(str(remaining_for_cutloss))
                            )
                        
                        # Wait for fill
                        for _ in range(5):
                            await asyncio.sleep(1)
                            if opp.second_leg_venue == Venue.POLYMARKET:
                                fill_info = await self.pm.get_order(order.order_id)
                            else:
                                fill_info = await self.kalshi.get_order(order.order_id)
                            
                            cut_fill_size = fill_info.get("filled_count", 0)
                            if cut_fill_size > 0:
                                cut_fill_price = float(fill_info.get("avg_fill_price") or cut_price)
                                weighted_price_sum += cut_fill_size * cut_fill_price
                                total_filled += int(cut_fill_size)
                                filled = total_filled >= target_size
                                fill_size = total_filled
                                fill_price = weighted_price_sum / total_filled
                                print(f"      ✓ CUT LOSS FILLED {int(cut_fill_size)} @ ${cut_fill_price:.2f}")
                                break
                        
                        if not filled:
                            # Cancel and give up
                            try:
                                if opp.second_leg_venue == Venue.POLYMARKET:
                                    await self.pm.cancel_order(order.order_id)
                                else:
                                    await self.kalshi.cancel_order(order.order_id)
                            except:
                                pass
                            
                            # MARKET ORDER FALLBACK - must close position at any price
                            # Recalculate remaining in case cut-loss partially filled
                            remaining_for_market = target_size - total_filled
                            if remaining_for_market <= 0:
                                filled = True
                                fill_size = total_filled
                                fill_price = weighted_price_sum / total_filled if total_filled > 0 else 0
                            else:
                                print(f"      ⚠️ Cut loss failed - forcing MARKET ORDER at $0.99 for {remaining_for_market}")
                                try:
                                    market_price = Decimal("0.99")
                                    if opp.second_leg_venue == Venue.POLYMARKET:
                                        market_order = await self.pm.place_order(
                                            opp.second_leg_market_id, OrderSide.BUY,
                                            opp.second_leg_direction, market_price, Decimal(str(remaining_for_market))
                                        )
                                    else:
                                        market_order = await self.kalshi.place_order(
                                            opp.second_leg_market_id, OrderSide.BUY,
                                            opp.second_leg_direction, market_price, Decimal(str(remaining_for_market))
                                        )
                                    
                                    # Wait for market order fill
                                    for wait_sec in range(10):
                                        await asyncio.sleep(1)
                                        if opp.second_leg_venue == Venue.POLYMARKET:
                                            fill_info = await self.pm.get_order(market_order.order_id)
                                        else:
                                            fill_info = await self.kalshi.get_order(market_order.order_id)
                                        
                                        market_filled = fill_info.get("filled_count", 0)
                                        if market_filled > 0:
                                            # Get actual fill price if available
                                            market_fill_price = float(fill_info.get("avg_fill_price") or fill_info.get("avg_price") or market_price)
                                            weighted_price_sum += market_filled * market_fill_price
                                            total_filled += int(market_filled)
                                            filled = total_filled >= target_size
                                            fill_size = total_filled
                                            fill_price = weighted_price_sum / total_filled
                                            print(f"      ✓ MARKET ORDER FILLED {int(market_filled)} @ ${market_fill_price:.2f}")
                                            break
                                    
                                    if not filled:
                                        print(f"      ✗ MARKET ORDER FAILED - position stays OPEN (manual intervention needed)")
                                        logger.log_text(f"  SECOND LEG MARKET ORDER FAILED - position OPEN")
                                        return False
                                        
                                except Exception as e:
                                    print(f"      ✗ Market order error: {e} - position stays OPEN")
                                    return False
                    
                    except Exception as e:
                        print(f"      ✗ Cut loss order failed: {e}")
                        return False
            else:
                print(f"      ✗ No fill after {elapsed}s (attempt {position.fill_attempts}/{max_attempts}) - will retry")
                logger.log_text(f"  SECOND LEG ATTEMPT {position.fill_attempts}/{max_attempts} FAILED - will retry")
                return False
        
        if not filled:
            return False
        
        # Success! Get prices for profit calculation
        pm_price = fill_price if opp.second_leg_venue == Venue.POLYMARKET else float(position.first_leg_fill_price)
        k_price = fill_price if opp.second_leg_venue == Venue.KALSHI else float(position.first_leg_fill_price)
        
        # Record trade and calculate profit using profit_tracker
        net_profit = profit_tracker.record_trade(
            underlying=opp.market_pair.polymarket.underlying,
            pm_price=pm_price,
            k_price=k_price,
            size=int(fill_size),
            source="second_leg",
            notes=f"After {position.seconds_held:.0f}s hold"
        )
        
        actual_cost = float(position.first_leg_fill_price) + fill_price
        print(f"  <<< ARB COMPLETE!")
        print(f"      Cost: ${position.first_leg_fill_price:.2f} + ${fill_price:.2f} = ${actual_cost:.2f}")
        print(f"      Net profit: ${net_profit:.2f} ({int(fill_size)} contracts)")
        
        # FINAL RECONCILIATION: Check for late fills on first leg (PM only)
        # If PM filled more contracts after we moved to second leg, hedge them now
        if opp.first_leg_venue == Venue.POLYMARKET:
            await asyncio.sleep(0.5)  # Brief wait for any late fills to settle
            try:
                final_first_leg = await self.pm.get_order(position.first_leg_order.order_id)
                final_first_leg_fills = final_first_leg.get("filled_count", 0)
                
                if final_first_leg_fills > position.first_leg_fill_size:
                    late_fills = int(final_first_leg_fills - position.first_leg_fill_size)
                    print(f"\n      ⚠️ LATE FILL DETECTED: +{late_fills} on first leg!")
                    print(f"      Hedging with market order on Kalshi...")
                    
                    # Place market order on Kalshi to hedge the late fills
                    try:
                        hedge_order = await self.kalshi.place_order(
                            opp.second_leg_market_id, OrderSide.BUY,
                            opp.second_leg_direction, Decimal("0.99"), Decimal(str(late_fills))
                        )
                        
                        # Wait for hedge fill
                        for _ in range(5):
                            await asyncio.sleep(0.5)
                            hedge_fill = await self.kalshi.get_order(hedge_order.order_id)
                            hedge_filled = hedge_fill.get("filled_count", 0)
                            if hedge_filled > 0:
                                hedge_price = float(hedge_fill.get("avg_fill_price") or 0.99)
                                print(f"      ✓ HEDGE FILLED: +{int(hedge_filled)} K {opp.second_leg_direction.value} @ ${hedge_price:.2f}")
                                
                                # Calculate and record additional profit
                                late_fill_price = float(final_first_leg.get("avg_fill_price") or position.first_leg_fill_price)
                                hedge_profit = profit_tracker.record_trade(
                                    underlying=opp.market_pair.polymarket.underlying,
                                    pm_price=late_fill_price,
                                    k_price=hedge_price,
                                    size=int(hedge_filled),
                                    source="late_fill_hedge",
                                    notes="Late PM fill hedged on Kalshi"
                                )
                                print(f"      Additional profit: ${hedge_profit:.2f}")
                                break
                        else:
                            print(f"      ⚠️ Hedge order not filled - may need manual intervention")
                            logger.log_text(f"  LATE FILL HEDGE FAILED: {late_fills} contracts")
                    except Exception as e:
                        print(f"      ✗ Hedge order failed: {e}")
                        logger.log_text(f"  LATE FILL HEDGE ERROR: {e}")
            except Exception as e:
                # Final check failed - not critical, just log it
                pass
        
        # Log to bot_log.txt as well
        logger.log_text(f"  SECOND LEG SUCCESS: {opp.second_leg_venue.value} {opp.second_leg_direction.value} x{int(fill_size)} @ ${fill_price:.2f}")
        logger.log_text(f"  ARB COMPLETE: ${net_profit:.2f} profit")
        
        # Record PM fill internally for position tracking
        if opp.second_leg_venue == Venue.POLYMARKET:
            pm_window_end = opp.market_pair.polymarket.end_time if hasattr(opp.market_pair.polymarket, 'end_time') else None
            dir_str = "down" if opp.second_leg_direction == Direction.DOWN else "up"
            self.pm.record_fill(opp.second_leg_market_id, int(fill_size), opp.market_pair.polymarket.underlying, dir_str, pm_window_end)
        
        # Track if this was an assumed-strike trade that needs monitoring
        if getattr(opp.market_pair, 'assumed_strike', False):
            if not hasattr(self, '_assumed_strike_trades'):
                self._assumed_strike_trades = []
            self._assumed_strike_trades.append({
                'underlying': opp.market_pair.polymarket.underlying,
                'pm_market_id': opp.market_pair.polymarket.market_id,
                'k_market_id': opp.market_pair.kalshi.market_id,
                'pm_direction': opp.first_leg_direction if opp.first_leg_venue == Venue.POLYMARKET else opp.second_leg_direction,
                'k_direction': opp.first_leg_direction if opp.first_leg_venue == Venue.KALSHI else opp.second_leg_direction,
                'size': int(fill_size),
                'pm_fill_price': float(position.first_leg_fill_price) if opp.first_leg_venue == Venue.POLYMARKET else fill_price,
                'k_fill_price': float(position.first_leg_fill_price) if opp.first_leg_venue == Venue.KALSHI else fill_price,
                'timestamp': datetime.now(timezone.utc),
                'window_end': opp.market_pair.polymarket.end_time,
            })
            print(f"      [!] Assumed-strike trade - monitoring for strike divergence...")
        
        # Confirm trade in session - use NET profit
        session.trades.append(ConfirmedTrade(
            timestamp=datetime.now(timezone.utc),
            underlying=opp.market_pair.polymarket.underlying,
            direction=f"{opp.first_leg_venue.value}_{opp.first_leg_direction.value}",
            size=int(fill_size),
            pm_fill_price=position.first_leg_fill_price if opp.first_leg_venue == Venue.POLYMARKET else Decimal(str(fill_price)),
            k_fill_price=position.first_leg_fill_price if opp.first_leg_venue == Venue.KALSHI else Decimal(str(fill_price)),
            total_cost=Decimal(str(actual_cost)) * int(fill_size),
            guaranteed_payout=Decimal(str(fill_size)),
            expected_profit=Decimal(str(total_net_profit)),  # Use NET profit
        ))
        
        self._last_trade = datetime.now(timezone.utc)
        self._active_position = None
        
        logger.log_trade_v2(opp, position, fill_price, int(fill_size), success=True, profit=net_profit, total_profit=total_net_profit)
        
        return True
    
    async def reconcile_positions(self, pairs: list, delay_seconds: float = None) -> bool:
        """
        Check if positions are balanced across venues after a trade.
        If we find orphaned positions (one side without the hedge), try to hedge them.
        Returns True if positions are balanced or successfully hedged.
        
        delay_seconds: Optional delay before checking. If None, uses scaled delay based on market timing.
        """
        # CRITICAL: Check if window has transitioned - if so, old positions will settle automatically
        # Don't try to reconcile/sell positions from previous windows
        if hasattr(self, '_current_window_end'):
            # Get the current window end from pairs
            current_window = None
            for pair in pairs:
                if hasattr(pair, 'polymarket') and pair.polymarket.end_time:
                    current_window = pair.polymarket.end_time
                    break
            
            if current_window and current_window != self._current_window_end:
                print(f"\n  ⏭️ Window changed - skipping reconciliation (old positions will settle)")
                log_print(f"  [Reconcile] Skipped - window transitioned from {self._current_window_end} to {current_window}")
                # Clear internal positions since window changed
                self.pm._internal_positions = {}
                self.kalshi._internal_positions = {}
                self._has_orphaned_positions = False
                return True  # Positions will settle automatically
        
        # Set lock to prevent new trades during reconciliation
        # NOTE: With market orders, fills are near-instant so this should rarely matter
        self._reconciliation_in_progress = True
        
        try:
            # No delay needed with market orders - fills are instant
            # Just check positions immediately
            log_print(f"\n  🔍 Checking positions...")
            
            # Get positions from both venues
            k_positions = await self.kalshi.get_positions()
            pm_positions = await self.pm.get_positions()
            
            # ── Filter out vol bot positions before reconciliation ──
            # Without this, arb bot sees vol trades as "phantom fills"
            # or "orphans" and tries to sell/hedge them
            if _HAS_COORDINATOR:
                k_before = len(k_positions)
                k_positions = _coord_filter_positions(k_positions, platform="kalshi", my_strategy="arb")
                if len(k_positions) != k_before:
                    log_print(f"    [Coordinator] Filtered {k_before - len(k_positions)} vol bot position(s) from Kalshi reconciliation")
            
            log_print(f"\n  🔍 Reconciling positions...")
            
            # Get current window end for filtering (used throughout reconciliation)
            current_window_end = None
            for pair in pairs:
                if hasattr(pair, 'polymarket') and pair.polymarket.end_time:
                    current_window_end = pair.polymarket.end_time
                    break
            
            # Log positions in clean format (tag stale positions from previous windows)
            if k_positions:
                for ticker, pos in k_positions.items():
                    asset = "BTC" if "BTC" in ticker else "ETH" if "ETH" in ticker else "SOL" if "SOL" in ticker else ticker[:6]
                    direction = pos.get('direction', '?').upper()
                    size = pos.get('size', 0)
                    is_current = is_current_window_ticker(ticker, current_window_end)
                    stale_tag = "" if is_current else " [STALE/PREV WINDOW]"
                    log_print(f"       K: {asset} {direction} x{size}{stale_tag}")
            if pm_positions:
                # Only show current window positions (internal tracking) to avoid spam
                # The Data API returns ALL historical positions which is too noisy
                internal_pm = self.pm.get_internal_positions_with_metadata() if hasattr(self.pm, 'get_internal_positions_with_metadata') else {}
                if internal_pm:
                    for token_id, pos_data in internal_pm.items():
                        size = self.pm._internal_positions.get(token_id, 0)
                        underlying = pos_data.get('underlying', '?')
                        direction = pos_data.get('direction', '?').upper()
                        log_print(f"      PM: {underlying} {direction} x{int(size)}")
                else:
                    # Fallback: show count only
                    log_print(f"      PM: {len(pm_positions)} total positions (use Data API for details)")
            
            if not k_positions and not pm_positions:
                log_print(f"    ✓ No open positions found")
                return True  # Nothing to reconcile
            
            # ═══════════════════════════════════════════════════════════════
            # PHANTOM FILL DETECTION: Compare Kalshi API positions against
            # internal tracking. If API shows MORE than internal, we have
            # undetected fills that need to be synced.
            #
            # CRITICAL: Only check positions from the CURRENT window.
            # Kalshi API returns unsettled positions from previous windows
            # which would otherwise be re-ingested as phantom fills,
            # corrupting the internal tracker and causing position pileup.
            # ═══════════════════════════════════════════════════════════════
            kalshi_internal = self.kalshi._internal_positions if hasattr(self.kalshi, '_internal_positions') else {}
            if k_positions and kalshi_internal is not None:
                for ticker, api_pos in k_positions.items():
                    # CRITICAL FILTER: Skip positions from previous windows
                    if not is_current_window_ticker(ticker, current_window_end):
                        asset = "BTC" if "BTC" in ticker else "ETH" if "ETH" in ticker else "SOL" if "SOL" in ticker else ticker[:6]
                        api_size = api_pos.get('size', 0)
                        log_print(f"    ⏭️ Skipping stale K API position: {asset} {api_pos.get('direction', '?').upper()} x{api_size} ({ticker} = previous window)")
                        continue
                    
                    api_size = api_pos.get('size', 0)
                    api_dir = api_pos.get('direction', '')
                    
                    # Find matching internal position for this ticker
                    internal_size = 0
                    for key, int_pos in kalshi_internal.items():
                        if int_pos.get('ticker') == ticker and int_pos.get('direction') == api_dir:
                            internal_size += int_pos.get('size', 0)
                    
                    if api_size > internal_size:
                        excess = api_size - internal_size
                        asset = "BTC" if "BTC" in ticker else "ETH" if "ETH" in ticker else "SOL" if "SOL" in ticker else ticker[:6]
                        log_print(f"    🚨 PHANTOM FILL: K API has {asset} {api_dir.upper()} x{api_size} but internal only tracks {internal_size} (excess: {excess})")
                        
                        # Sync internal tracking to match API reality
                        # Find or create the internal position entry
                        found_key = None
                        for key, int_pos in kalshi_internal.items():
                            if int_pos.get('ticker') == ticker and int_pos.get('direction') == api_dir:
                                found_key = key
                                break
                        
                        if found_key:
                            self.kalshi._internal_positions[found_key]['size'] = api_size
                            log_print(f"    🔧 Synced internal K tracking: {asset} {api_dir.upper()} {internal_size} → {api_size}")
                        else:
                            # Create new internal entry to match API
                            new_key = f"phantom_{ticker}_{api_dir}"
                            current_window_end_sync = None
                            for pair in pairs:
                                if hasattr(pair, 'kalshi') and pair.kalshi.market_id == ticker:
                                    current_window_end_sync = pair.kalshi.end_time if hasattr(pair.kalshi, 'end_time') else None
                                    break
                            self.kalshi._internal_positions[new_key] = {
                                'ticker': ticker,
                                'size': api_size,
                                'direction': api_dir,
                                'window_end': current_window_end_sync
                            }
                            log_print(f"    🔧 Created internal K tracking: {asset} {api_dir.upper()} x{api_size}")
            
            # Build position map by underlying
            # We match positions by underlying - if K has ETH and PM has ETH, they're paired
            # This is simpler and more reliable than trying to match by window tokens
            position_map = {}  # underlying -> {"kalshi": [...], "pm": [...]}
            
            # Map Kalshi positions from internal tracking (to get window_end)
            # Skip positions from different windows
            kalshi_internal = self.kalshi._internal_positions if hasattr(self.kalshi, '_internal_positions') else {}
            for key, pos in kalshi_internal.items():
                pos_window_end = pos.get("window_end")
                
                # Skip if position is from a different window
                if pos_window_end and current_window_end and pos_window_end != current_window_end:
                    log_print(f"    ⏭️ Skipping K position {key} - from previous window (will settle)")
                    continue
                
                ticker = pos.get("ticker", "")
                if ticker.startswith("KXBTC"):
                    underlying = "BTC"
                elif ticker.startswith("KXETH"):
                    underlying = "ETH"
                elif ticker.startswith("KXSOL"):
                    underlying = "SOL"
                else:
                    continue
                
                if underlying not in position_map:
                    position_map[underlying] = {"kalshi": [], "pm": []}
                
                position_map[underlying]["kalshi"].append({
                    "ticker": ticker,
                    "size": pos["size"],
                    "direction": pos["direction"],
                    "window_end": pos_window_end
                })
            
            # Map PM positions from internal tracking (has metadata including window_end)
            internal_positions = self.pm.get_internal_positions_with_metadata()
            for token_id, pos_data in internal_positions.items():
                underlying = pos_data.get("underlying")
                direction = pos_data.get("direction")
                pos_window_end = pos_data.get("window_end")
                
                if not underlying or not direction:
                    continue
                
                # Skip if position is from a different window
                if pos_window_end and current_window_end and pos_window_end != current_window_end:
                    log_print(f"    ⏭️ Skipping PM position {token_id[:16]}... - from previous window (will settle)")
                    continue
                
                if underlying not in position_map:
                    position_map[underlying] = {"kalshi": [], "pm": []}
                
                position_map[underlying]["pm"].append({
                    "token_id": token_id,
                    "size": pos_data["size"],
                    "direction": direction,
                    "window_end": pos_window_end
                })
            
            # Also add PM positions from API that weren't in internal tracking
            for token_id, pos in pm_positions.items():
                already_tracked = any(
                    p["token_id"] == token_id 
                    for positions in position_map.values() 
                    for p in positions.get("pm", [])
                )
                if already_tracked:
                    continue
                
                # Try to identify underlying from pairs
                for pair in pairs:
                    direction = None
                    if pair.polymarket.token_id == token_id:
                        direction = "up"
                    elif pair.polymarket.no_token_id == token_id:
                        direction = "down"
                    
                    if direction:
                        underlying = pair.polymarket.underlying
                        if underlying not in position_map:
                            position_map[underlying] = {"kalshi": [], "pm": []}
                        position_map[underlying]["pm"].append({
                            "token_id": token_id,
                            "size": pos["size"],
                            "direction": direction
                        })
                        break
            
            # Check each underlying for proper hedging
            has_orphans = False
            for underlying, positions in position_map.items():
                k_positions_list = positions["kalshi"]
                pm_positions_list = positions["pm"]
                
                # Sum up sizes by direction
                k_yes_size = sum(p["size"] for p in k_positions_list if p["direction"] == "yes")
                k_no_size = sum(p["size"] for p in k_positions_list if p["direction"] == "no")
                pm_up_size = sum(p["size"] for p in pm_positions_list if p["direction"] == "up")
                pm_down_size = sum(p["size"] for p in pm_positions_list if p["direction"] == "down")
                
                # Find matching pair for hedging
                matching_pair = None
                for pair in pairs:
                    if pair.polymarket.underlying == underlying:
                        matching_pair = pair
                        break
                
                # Check hedging: K YES + PM DOWN, or K NO + PM UP
                hedged_yes_down = min(k_yes_size, pm_down_size)
                hedged_no_up = min(k_no_size, pm_up_size)
                
                # Unhedged amounts
                orphan_k_yes = k_yes_size - hedged_yes_down
                orphan_k_no = k_no_size - hedged_no_up
                orphan_pm_up = pm_up_size - hedged_no_up
                orphan_pm_down = pm_down_size - hedged_yes_down
                
                # Report status
                if hedged_yes_down > 0:
                    log_print(f"    ✓ {underlying}: Hedged {hedged_yes_down}x (K YES + PM DOWN)")
                if hedged_no_up > 0:
                    log_print(f"    ✓ {underlying}: Hedged {hedged_no_up}x (K NO + PM UP)")
                
                # Handle orphaned PM positions - hedge on Kalshi
                if orphan_pm_up > 0 and matching_pair:
                    print(f"    ⚠️ {underlying}: Orphaned PM UP x{orphan_pm_up} - hedging with K NO...")
                    
                    # Retry up to 2 times with short delays if no liquidity
                    hedged = False
                    for attempt in range(2):
                        kb = await self.kalshi.get_orderbook(matching_pair.kalshi.market_id)
                        if kb.best_no_ask:
                            max_price = min(kb.best_no_ask * Decimal("1.03"), Decimal("0.95"))
                            success = await self._execute_hedge_order(
                                matching_pair, Venue.KALSHI, Direction.DOWN,
                                int(orphan_pm_up), float(max_price)
                            )
                            if success:
                                print(f"    ✓ Hedged {underlying} PM UP with K NO")
                                hedged = True
                                break
                            else:
                                print(f"    ❌ Failed to hedge {underlying}")
                        else:
                            if attempt < 1:
                                print(f"    ⏳ No Kalshi NO asks for {underlying}, waiting 3s...")
                                await asyncio.sleep(3)
                            else:
                                print(f"    ❌ No Kalshi NO asks available for {underlying}")
                    
                    if not hedged:
                        has_orphans = True
                
                if orphan_pm_down > 0 and matching_pair:
                    print(f"    ⚠️ {underlying}: Orphaned PM DOWN x{orphan_pm_down} - hedging with K YES...")
                    
                    # Retry up to 2 times with short delays if no liquidity
                    hedged = False
                    for attempt in range(2):
                        kb = await self.kalshi.get_orderbook(matching_pair.kalshi.market_id)
                        if kb.best_yes_ask:
                            max_price = min(kb.best_yes_ask * Decimal("1.03"), Decimal("0.95"))
                            success = await self._execute_hedge_order(
                                matching_pair, Venue.KALSHI, Direction.UP,
                                int(orphan_pm_down), float(max_price)
                            )
                            if success:
                                print(f"    ✓ Hedged {underlying} PM DOWN with K YES")
                                hedged = True
                                break
                            else:
                                print(f"    ❌ Failed to hedge {underlying}")
                        else:
                            if attempt < 1:
                                print(f"    ⏳ No Kalshi YES asks for {underlying}, waiting 3s...")
                                await asyncio.sleep(3)
                            else:
                                print(f"    ❌ No Kalshi YES asks available for {underlying}")
                    
                    if not hedged:
                        has_orphans = True
                
                # Orphaned Kalshi positions - SELL them at market to exit cleanly
                # This is simpler and more reliable than buying the opposite side
                # NOTE: The window check at the start of reconcile_positions() prevents
                # this from running on positions from previous windows
                if orphan_k_yes > 0 and matching_pair:
                    print(f"    ⚠️ {underlying}: Orphaned K YES x{orphan_k_yes} - selling to exit...")
                    kb = await self.kalshi.get_orderbook(matching_pair.kalshi.market_id)
                    if kb.best_yes_bid:
                        # Sell at slightly below best bid to ensure fill
                        sell_price = max(kb.best_yes_bid * Decimal("0.90"), Decimal("0.01"))
                        try:
                            order = await self.kalshi.place_order(
                                matching_pair.kalshi.market_id,
                                OrderSide.SELL,
                                Direction.UP,  # Selling YES
                                sell_price,
                                Decimal(str(orphan_k_yes))
                            )
                            if order:
                                # Wait for fill
                                for _ in range(10):
                                    await asyncio.sleep(0.5)
                                    status = await self.kalshi.get_order(order.order_id)
                                    if status and status.get("filled_count", 0) >= orphan_k_yes:
                                        print(f"    ✓ Sold {underlying} K YES x{orphan_k_yes} @ ~${float(sell_price):.2f}")
                                        # Clear from internal tracking
                                        self.kalshi._internal_positions = {
                                            k: v for k, v in self.kalshi._internal_positions.items()
                                            if not (v.get("ticker") == matching_pair.kalshi.market_id and v.get("direction") == "yes")
                                        }
                                        break
                                else:
                                    print(f"    ⚠️ Sell order placed but not confirmed filled")
                        except Exception as e:
                            print(f"    ❌ Failed to sell K YES: {e}")
                            has_orphans = True
                    else:
                        print(f"    ❌ No bids for {underlying} K YES - letting settle")
                        has_orphans = True
                
                if orphan_k_no > 0 and matching_pair:
                    print(f"    ⚠️ {underlying}: Orphaned K NO x{orphan_k_no} - selling to exit...")
                    kb = await self.kalshi.get_orderbook(matching_pair.kalshi.market_id)
                    if kb.best_no_bid:
                        # Sell at slightly below best bid to ensure fill
                        sell_price = max(kb.best_no_bid * Decimal("0.90"), Decimal("0.01"))
                        try:
                            order = await self.kalshi.place_order(
                                matching_pair.kalshi.market_id,
                                OrderSide.SELL,
                                Direction.DOWN,  # Selling NO
                                sell_price,
                                Decimal(str(orphan_k_no))
                            )
                            if order:
                                # Wait for fill
                                for _ in range(10):
                                    await asyncio.sleep(0.5)
                                    status = await self.kalshi.get_order(order.order_id)
                                    if status and status.get("filled_count", 0) >= orphan_k_no:
                                        print(f"    ✓ Sold {underlying} K NO x{orphan_k_no} @ ~${float(sell_price):.2f}")
                                        # Clear from internal tracking
                                        self.kalshi._internal_positions = {
                                            k: v for k, v in self.kalshi._internal_positions.items()
                                            if not (v.get("ticker") == matching_pair.kalshi.market_id and v.get("direction") == "no")
                                        }
                                        break
                                else:
                                    print(f"    ⚠️ Sell order placed but not confirmed filled")
                        except Exception as e:
                            print(f"    ❌ Failed to sell K NO: {e}")
                            has_orphans = True
                    else:
                        print(f"    ❌ No bids for {underlying} K NO - letting settle")
                        has_orphans = True
            
            if not has_orphans:
                log_print(f"    ✓ Positions balanced - ready to trade")
            
            self._has_orphaned_positions = has_orphans
            return not has_orphans
            
        except Exception as e:
            print(f"    ❌ Reconciliation error: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            # Always clear the reconciliation lock
            self._reconciliation_in_progress = False
    
    async def _execute_hedge_order(self, pair: MarketPair, venue: Venue, direction: Direction, 
                                    size: int, max_price: float) -> bool:
        """Execute a hedge order with smart fill logic and lag handling"""
        # Safety check - don't hedge if trading is disabled
        if not getattr(self, '_trading_enabled', False):
            print(f"    [Hedge] ⚠️ Skipped - trading not enabled")
            return False
        
        try:
            print(f"    [Hedge] {venue.value} {direction.value} x{size} @ ${max_price:.2f}")
            
            if venue == Venue.POLYMARKET:
                # Track timing to detect lag
                start_time = time.time()
                
                # Use aggressive fill to ensure we get filled
                order = await self.pm.place_order(
                    market_id=pair.polymarket.market_id,
                    side=OrderSide.BUY,
                    direction=direction,
                    price=Decimal(str(max_price)),
                    size=Decimal(str(size))
                )
                
                order_time = time.time() - start_time
                if order_time > 3.0:
                    print(f"    [Hedge] ⚠️ PM order took {order_time:.1f}s (lag detected)")
                
                if not order:
                    print(f"    [Hedge] PM order placement returned None")
                    return False
                
                print(f"    [Hedge] PM order placed: {order.order_id[:20]}...")
                
                # Wait for fill with timeout - longer timeout if lag detected
                timeout_loops = 30 if order_time > 3.0 else 20  # 15s vs 10s
                for i in range(timeout_loops):
                    await asyncio.sleep(0.5)
                    status = await self.pm.get_order(order.order_id)
                    if status:
                        filled = float(status.get("size_matched", 0) or status.get("filled", 0) or status.get("filled_count", 0) or 0)
                        if filled >= size:
                            print(f"    [Hedge] PM filled {filled}")
                            # Record fill internally with window info
                            pm_window_end = pair.polymarket.end_time if hasattr(pair.polymarket, 'end_time') else None
                            dir_str = "down" if direction == Direction.DOWN else "up"
                            self.pm.record_fill(pair.polymarket.market_id, int(filled), pair.polymarket.underlying, dir_str, pm_window_end)
                            return True
                
                print(f"    [Hedge] PM order timed out, cancelling")
                # Try to cancel if not filled
                try:
                    self.pm._client.cancel(order.order_id)
                except:
                    pass
                
                # If lag was detected and we still didn't fill, try a more aggressive price
                if order_time > 3.0 and max_price < 0.92:
                    print(f"    [Hedge] Retrying with more aggressive price due to lag...")
                    return await self._execute_hedge_order(pair, venue, direction, size, min(max_price + 0.05, 0.92))
                
                return False
                
            else:  # Kalshi
                start_time = time.time()
                
                order = await self.kalshi.place_order(
                    market_id=pair.kalshi.market_id,
                    side=OrderSide.BUY,
                    direction=direction,
                    price=Decimal(str(max_price)),
                    size=Decimal(str(size))
                )
                
                order_time = time.time() - start_time
                if order_time > 1.0:
                    print(f"    [Hedge] ⚠️ Kalshi order took {order_time:.1f}s (lag detected)")
                
                if not order:
                    return False
                
                # Wait for fill - Kalshi is usually fast
                timeout_loops = 30 if order_time > 1.0 else 20
                for _ in range(timeout_loops):
                    await asyncio.sleep(0.5)
                    status = await self.kalshi.get_order(order.order_id)
                    if status:
                        filled = status.get("filled_count", 0) or status.get("size_matched", 0) or 0
                        remaining = status.get("remaining_count", size)
                        if filled >= size or remaining == 0:
                            # Record internally with window info
                            dir_str = "yes" if direction == Direction.UP else "no"
                            k_window_end = pair.kalshi.end_time if hasattr(pair.kalshi, 'end_time') else None
                            self.kalshi.record_fill(pair.kalshi.market_id, int(filled), dir_str, k_window_end)
                            return True
                
                # If lag and didn't fill, try more aggressive
                if order_time > 1.0 and max_price < 0.92:
                    print(f"    [Hedge] Retrying with more aggressive price due to lag...")
                    return await self._execute_hedge_order(pair, venue, direction, size, min(max_price + 0.05, 0.92))
                
                return False
                
        except Exception as e:
            print(f"    Hedge order error: {e}")
            return False
    
    async def check_and_unwind_diverged_trades(self, pairs: list) -> None:
        """
        Check if any completed assumed-strike trades need to be unwound
        due to strike divergence >= 0.03%.
        """
        if not hasattr(self, '_assumed_strike_trades') or not self._assumed_strike_trades:
            return
        
        unwind_threshold = 0.03  # 0.03% strike diff triggers unwind
        now = datetime.now(timezone.utc)
        
        # Clean up expired trades (window has closed)
        self._assumed_strike_trades = [
            t for t in self._assumed_strike_trades 
            if t['window_end'] > now
        ]
        
        for trade in self._assumed_strike_trades[:]:  # Copy list since we modify it
            underlying = trade['underlying']
            
            # Find current pair for this underlying
            current_pair = None
            for pair in pairs:
                if pair.polymarket.underlying == underlying:
                    current_pair = pair
                    break
            
            if not current_pair:
                continue
            
            # Check if Kalshi now has actual strike
            k_strike = current_pair.kalshi.strike_price
            pm_strike = current_pair.polymarket.strike_price
            
            if not k_strike or not pm_strike:
                continue  # Still no Kalshi strike
            
            # Calculate strike difference
            strike_diff_pct = abs(pm_strike - k_strike) / k_strike * 100
            
            if strike_diff_pct < unwind_threshold:
                # Strikes OK - remove from monitoring
                print(f"  [✓] {underlying} strike confirmed: diff {strike_diff_pct:.4f}% < {unwind_threshold}%")
                self._assumed_strike_trades.remove(trade)
                continue
            
            # STRIKE DIVERGED - UNWIND THE TRADE
            print(f"\n  🚨 STRIKE DIVERGENCE - UNWINDING {underlying}")
            print(f"     PM: ${pm_strike:,.2f} | K: ${k_strike:,.2f} | Diff: {strike_diff_pct:.4f}%")
            print(f"     Selling both legs to exit position...")
            
            await self._unwind_trade(trade)
            self._assumed_strike_trades.remove(trade)
    
    async def _unwind_trade(self, trade: dict) -> None:
        """Sell both legs of a completed trade to unwind the position."""
        underlying = trade['underlying']
        size = trade['size']
        
        # Sell PM leg (opposite of what we bought)
        pm_sell_dir = Direction.DOWN if trade['pm_direction'] == Direction.UP else Direction.UP
        
        # Sell K leg (opposite of what we bought)  
        k_sell_dir = Direction.DOWN if trade['k_direction'] == Direction.UP else Direction.UP
        
        print(f"     Selling PM {pm_sell_dir.value} and K {k_sell_dir.value}...")
        
        total_received = 0
        
        # Sell PM position - use rapid price descent like second leg
        try:
            pm_book = await self.pm.get_orderbook(trade['pm_market_id'])
            if pm_sell_dir == Direction.UP:
                bid = float(pm_book.best_yes_bid or Decimal("0.01"))
            else:
                bid = float(pm_book.best_no_bid or Decimal("0.01"))
            
            # Start at bid, go down to 0.01 if needed
            pm_filled = 0
            pm_received = 0
            for attempt in range(20):  # Try for ~2 seconds
                sell_price = max(bid * (1 - attempt * 0.02), 0.01)  # Drop 2% each attempt
                try:
                    order = await self.pm.place_order(
                        trade['pm_market_id'], OrderSide.SELL,
                        pm_sell_dir, Decimal(str(round(sell_price, 2))), Decimal(str(size - pm_filled))
                    )
                    await asyncio.sleep(0.1)
                    fill = await self.pm.get_order(order.order_id)
                    filled = fill.get("filled_count", 0)
                    if filled > 0:
                        price = fill.get("avg_fill_price") or sell_price
                        pm_filled += int(filled)
                        pm_received += filled * float(price)
                        if pm_filled >= size:
                            break
                    await self.pm.cancel_order(order.order_id)
                except:
                    pass
                await asyncio.sleep(0.1)
            
            if pm_filled > 0:
                print(f"     ✓ PM SOLD: {pm_filled} @ ${pm_received/pm_filled:.2f} = ${pm_received:.2f}")
                total_received += pm_received
            else:
                print(f"     ✗ PM SELL FAILED")
        except Exception as e:
            print(f"     ✗ PM SELL ERROR: {e}")
        
        # Sell K position
        try:
            k_book = await self.kalshi.get_orderbook(trade['k_market_id'])
            if k_sell_dir == Direction.UP:
                bid = float(k_book.best_yes_bid or Decimal("0.01"))
            else:
                bid = float(k_book.best_no_bid or Decimal("0.01"))
            
            k_filled = 0
            k_received = 0
            for attempt in range(20):
                sell_price = max(bid * (1 - attempt * 0.02), 0.01)
                try:
                    order = await self.kalshi.place_order(
                        trade['k_market_id'], OrderSide.SELL,
                        k_sell_dir, Decimal(str(round(sell_price, 2))), Decimal(str(size - k_filled))
                    )
                    await asyncio.sleep(0.1)
                    fill = await self.kalshi.get_order(order.order_id)
                    filled = fill.get("filled_count", 0)
                    if filled > 0:
                        price = fill.get("avg_fill_price") or sell_price
                        k_filled += int(filled)
                        k_received += filled * float(price)
                        if k_filled >= size:
                            break
                    await self.kalshi.cancel_order(order.order_id)
                except:
                    pass
                await asyncio.sleep(0.1)
            
            if k_filled > 0:
                print(f"     ✓ K SOLD: {k_filled} @ ${k_received/k_filled:.2f} = ${k_received:.2f}")
                total_received += k_received
            else:
                print(f"     ✗ K SELL FAILED")
        except Exception as e:
            print(f"     ✗ K SELL ERROR: {e}")
        
        # Calculate P&L from unwind
        original_cost = trade['pm_fill_price'] * size + trade['k_fill_price'] * size
        unwind_pnl = total_received - original_cost
        print(f"     UNWIND COMPLETE: Received ${total_received:.2f}, Cost was ${original_cost:.2f}")
        print(f"     Unwind P&L: ${unwind_pnl:+.2f}")
        
        logger.log_text(f"  UNWIND: {underlying} - received ${total_received:.2f}, P&L: ${unwind_pnl:+.2f}")
        """
        Check if an assumed-strike position needs to be exited.
        Called when we have a position that was entered with assumed Kalshi strike.
        Returns True if we should exit the position, False to continue normally.
        """
        position = self._active_position
        if not position:
            return False
        
        # Check if this position was entered with assumed strike
        if not getattr(position.opportunity.market_pair, 'assumed_strike', False):
            return False
        
        # Find the current pair for this underlying
        underlying = position.opportunity.market_pair.polymarket.underlying
        current_pair = None
        for pair in pairs:
            if pair.polymarket.underlying == underlying:
                current_pair = pair
                break
        
        if not current_pair:
            return False
        
        # Check if Kalshi now has an actual strike price
        k_strike = current_pair.kalshi.strike_price
        pm_strike = current_pair.polymarket.strike_price
        
        if not k_strike or not pm_strike:
            return False  # Still no Kalshi strike, continue with assumption
        
        # Calculate actual strike difference
        strike_diff_pct = abs(pm_strike - k_strike) / k_strike * 100
        exit_threshold = CONFIG["markets"].get("exit_strike_diff_pct", 0.02)
        
        if strike_diff_pct <= exit_threshold:
            # Strikes are close enough - update the pair to no longer be assumed
            position.opportunity.market_pair.assumed_strike = False
            print(f"  [✓] {underlying} strike confirmed: diff {strike_diff_pct:.4f}% <= {exit_threshold}%")
            return False
        
        # Strikes diverged too much - need to exit!
        print(f"\n  ⚠️ STRIKE DIVERGENCE: {underlying}")
        print(f"     PM: ${pm_strike:,.2f} | K: ${k_strike:,.2f} | Diff: {strike_diff_pct:.4f}%")
        print(f"     Exceeds exit threshold of {exit_threshold}%")
        print(f"     Exiting position to lock in available profit...")
        
        return True

# =============================================================================
# MAIN
# =============================================================================

# =============================================================================
# ALL-MARKET DISCOVERY AND MATCHING
# =============================================================================

def normalize_title(title: str) -> str:
    """Normalize market title for matching."""
    import re
    t = title.lower()
    # Remove common prefixes/suffixes
    t = re.sub(r'^(will|does|is|are|can|has|have)\s+', '', t)
    # Remove punctuation
    t = re.sub(r'[^\w\s]', '', t)
    # Normalize whitespace
    t = ' '.join(t.split())
    return t

def title_similarity(t1: str, t2: str) -> float:
    """Calculate similarity between two titles (0-1)."""
    n1 = normalize_title(t1)
    n2 = normalize_title(t2)
    
    # Exact match after normalization
    if n1 == n2:
        return 1.0
    
    # Word overlap (Jaccard similarity)
    words1 = set(n1.split())
    words2 = set(n2.split())
    
    if not words1 or not words2:
        return 0.0
    
    intersection = words1 & words2
    union = words1 | words2
    
    return len(intersection) / len(union)

@dataclass
class UniversalMarketPair:
    """A matched pair of markets across Polymarket and Kalshi."""
    pm_market: dict
    k_market: dict
    similarity: float
    
    # Pricing
    pm_yes: Decimal
    pm_no: Decimal
    k_yes: Decimal
    k_no: Decimal
    
    # Timing
    duration_minutes: int
    time_remaining_minutes: float
    pct_elapsed: float
    
    @property
    def arb_edge_buy_pm_yes_k_no(self) -> Decimal:
        """ROI if we buy PM YES + K NO. Positive = profit opportunity."""
        cost = self.pm_yes + self.k_no
        if cost <= 0:
            return Decimal("0")
        profit = Decimal("1") - cost
        return profit / cost  # True ROI
    
    @property
    def arb_edge_buy_pm_no_k_yes(self) -> Decimal:
        """ROI if we buy PM NO + K YES. Positive = profit opportunity."""
        cost = self.pm_no + self.k_yes
        if cost <= 0:
            return Decimal("0")
        profit = Decimal("1") - cost
        return profit / cost  # True ROI
    
    @property
    def best_edge(self) -> Decimal:
        """Best available ROI (before fees)."""
        return max(self.arb_edge_buy_pm_yes_k_no, self.arb_edge_buy_pm_no_k_yes)
    
    @property
    def best_direction(self) -> str:
        """Which direction is better: 'pm_yes_k_no' or 'pm_no_k_yes'."""
        if self.arb_edge_buy_pm_yes_k_no >= self.arb_edge_buy_pm_no_k_yes:
            return "pm_yes_k_no"
        return "pm_no_k_yes"


class AllMarketScanner:
    """Scanner for ALL markets across both venues."""
    
    # Fee estimates (conservative)
    PM_TAKER_FEE = Decimal("0.02")  # ~2% worst case
    K_TAKER_FEE = Decimal("0.01")   # ~1%
    SLIPPAGE = Decimal("0.005")    # ~0.5% slippage buffer (market orders fill near ask)
    
    def __init__(self, pm: 'PolymarketConnector', kalshi: 'KalshiConnector'):
        self.pm = pm
        self.kalshi = kalshi
        self._matched_pairs_cache = []
        self._last_discovery = 0
    
    async def discover_pairs(self, max_minutes: int = 60, min_similarity: float = 0.6) -> list:
        """Discover all matching market pairs across venues."""
        print(f"\n  📊 Discovering markets (closing within {max_minutes} min)...")
        
        # Fetch all markets from both venues
        pm_markets = await self.pm.get_all_markets(max_minutes)
        k_markets = await self.kalshi.get_all_markets(max_minutes)
        
        print(f"     PM: {len(pm_markets)} markets | K: {len(k_markets)} markets")
        
        if not pm_markets or not k_markets:
            return []
        
        now = datetime.now(timezone.utc)
        pairs = []
        
        # Try to match markets by title similarity
        for pm in pm_markets:
            pm_title = pm.get("title", "")
            pm_event = pm.get("event_title", "")
            
            for k in k_markets:
                k_title = k.get("title", "")
                
                # Calculate similarity (try both market title and event title)
                sim1 = title_similarity(pm_title, k_title)
                sim2 = title_similarity(pm_event, k_title)
                sim = max(sim1, sim2)
                
                if sim < min_similarity:
                    continue
                
                # Check end times are close (within 2 hours)
                pm_end = pm.get("end_time")
                k_end = k.get("end_time")
                if abs((pm_end - k_end).total_seconds()) > 7200:
                    continue
                
                # Calculate timing
                pm_start = pm.get("start_time")
                duration = (pm_end - pm_start).total_seconds() / 60
                remaining = (pm_end - now).total_seconds() / 60
                elapsed_pct = 1 - (remaining / duration) if duration > 0 else 1
                
                pair = UniversalMarketPair(
                    pm_market=pm,
                    k_market=k,
                    similarity=sim,
                    pm_yes=pm.get("yes_price", Decimal("0.5")),
                    pm_no=pm.get("no_price", Decimal("0.5")),
                    k_yes=k.get("yes_price", Decimal("0.5")),
                    k_no=k.get("no_price", Decimal("0.5")),
                    duration_minutes=int(duration),
                    time_remaining_minutes=remaining,
                    pct_elapsed=elapsed_pct
                )
                
                pairs.append(pair)
        
        # Sort by time remaining (soonest first)
        pairs.sort(key=lambda p: p.time_remaining_minutes)
        
        self._matched_pairs_cache = pairs
        self._last_discovery = time.time()
        
        return pairs
    
    def filter_arb_opportunities(self, pairs: list, min_edge_pct: float = 4.0) -> list:
        """Filter pairs to only those with real arb opportunities after fees."""
        total_fees = self.PM_TAKER_FEE + self.K_TAKER_FEE + self.SLIPPAGE
        min_edge = Decimal(str(min_edge_pct / 100))
        
        opportunities = []
        for pair in pairs:
            # Check if edge covers fees
            net_edge = pair.best_edge - total_fees
            if net_edge >= min_edge:
                opportunities.append((pair, net_edge))
        
        # Sort by net edge (best first)
        opportunities.sort(key=lambda x: x[1], reverse=True)
        
        return opportunities
    
    def print_discovery_report(self, pairs: list):
        """Print a report of discovered market pairs."""
        print(f"\n  {'='*70}")
        print(f"  MARKET PAIR DISCOVERY REPORT")
        print(f"  {'='*70}")
        
        if not pairs:
            print("  No matching markets found.")
            return
        
        # Group by similarity
        high_conf = [p for p in pairs if p.similarity >= 0.9]
        med_conf = [p for p in pairs if 0.7 <= p.similarity < 0.9]
        low_conf = [p for p in pairs if p.similarity < 0.7]
        
        print(f"\n  Found {len(pairs)} potential pairs:")
        print(f"    High confidence (≥90%): {len(high_conf)}")
        print(f"    Medium (70-89%): {len(med_conf)}")
        print(f"    Low (<70%): {len(low_conf)}")
        
        # Show best opportunities
        opps = self.filter_arb_opportunities(pairs, min_edge_pct=0)
        
        print(f"\n  {'─'*70}")
        print(f"  TOP OPPORTUNITIES (sorted by edge)")
        print(f"  {'─'*70}")
        
        # Table header
        print(f"  {'Title':<35} {'PM':<6} {'K':<6} {'Edge':<7} {'Time':<8} {'Match'}")
        print(f"  {'-'*35} {'-'*6} {'-'*6} {'-'*7} {'-'*8} {'-'*5}")
        
        for pair, net_edge in opps[:15]:
            title = pair.pm_market.get("title", "")[:33]
            
            if pair.best_direction == "pm_yes_k_no":
                pm_price = f"Y{float(pair.pm_yes):.2f}"
                k_price = f"N{float(pair.k_no):.2f}"
            else:
                pm_price = f"N{float(pair.pm_no):.2f}"
                k_price = f"Y{float(pair.k_yes):.2f}"
            
            edge_str = f"{float(net_edge)*100:+.1f}%"
            time_str = f"{pair.time_remaining_minutes:.0f}m"
            match_str = f"{pair.similarity*100:.0f}%"
            
            # Color code by edge
            if net_edge > Decimal("0.04"):
                edge_str = f"🟢{edge_str}"
            elif net_edge > 0:
                edge_str = f"🟡{edge_str}"
            else:
                edge_str = f"🔴{edge_str}"
            
            print(f"  {title:<35} {pm_price:<6} {k_price:<6} {edge_str:<7} {time_str:<8} {match_str}")
        
        print(f"\n  Note: Edge shown is AFTER estimated fees (~5%)")
        print(f"  {'='*70}\n")


async def run_discovery_mode():
    """Run in discovery mode - just scan and report, no trading."""
    print("\n" + "="*60)
    print("  ALL-MARKET DISCOVERY MODE")
    print("  Scanning Polymarket & Kalshi for matching markets")
    print("="*60)
    
    keys = load_keys()
    
    # Connect to exchanges
    kalshi = KalshiConnector(keys["kalshi_api_key_id"], keys["kalshi_private_key_pem"], CONFIG["kalshi_api_url"])
    
    try:
        print(f"\n  Connecting...")
        await kalshi.connect()
        
        # Proxy was set at module load (before imports) - just log it
        if _PROXY_CONFIG.get("enabled"):
            print(f"  [OK] Residential proxy: {_PROXY_CONFIG['host']}:{_PROXY_CONFIG['port']} (set at startup)")
        
        pm = PolymarketConnector(
            keys["polymarket_private_key"], 
            keys["polymarket_funder_address"],
            CONFIG["polymarket_clob_url"], 
            CONFIG["polymarket_gamma_url"], 
            CONFIG["polygon_rpc_url"]
        )
        await pm.connect()
        
        scanner = AllMarketScanner(pm, kalshi)
        
        # Search broader: markets ending within 24 hours
        # We'll prioritize short-term ones in the display
        max_minutes = 60 * 24  # 24 hours
        print(f"\n  Searching for markets ending within {max_minutes//60} hours...")
        
        pairs = await scanner.discover_pairs(max_minutes=max_minutes, min_similarity=0.5)
        
        # Print report
        scanner.print_discovery_report(pairs)
        
        # Print report
        scanner.print_discovery_report(pairs)
        
        # Also save to file for analysis
        with open("market_pairs.log", "w") as f:
            f.write(f"Discovery run: {datetime.now(timezone.utc).isoformat()}\n")
            f.write(f"Total pairs found: {len(pairs)}\n\n")
            
            for pair in pairs:
                f.write(f"{'='*60}\n")
                f.write(f"PM: {pair.pm_market.get('title', '')}\n")
                f.write(f"K:  {pair.k_market.get('title', '')}\n")
                f.write(f"Similarity: {pair.similarity*100:.1f}%\n")
                f.write(f"PM YES: {pair.pm_yes} | K YES: {pair.k_yes}\n")
                f.write(f"PM NO: {pair.pm_no} | K NO: {pair.k_no}\n")
                f.write(f"Best edge: {float(pair.best_edge)*100:.2f}% ({pair.best_direction})\n")
                f.write(f"Time remaining: {pair.time_remaining_minutes:.1f} min\n")
                f.write(f"Duration: {pair.duration_minutes} min ({pair.pct_elapsed*100:.1f}% elapsed)\n\n")
        
        print(f"  Full report saved to: market_pairs.log")
        
    finally:
        await pm.disconnect()
        await kalshi.disconnect()


async def main():
    # ============================================================
    # SINGLE INSTANCE CHECK - Prevent running multiple bots
    # ============================================================
    import tempfile
    import sys
    
    lock_file = Path(tempfile.gettempdir()) / "arb_scanner.lock"
    
    # Check if another instance is running
    if lock_file.exists():
        try:
            with open(lock_file, 'r') as f:
                old_pid = int(f.read().strip())
            
            # Check if that process is still running
            process_alive = False
            try:
                import psutil
                if psutil.pid_exists(old_pid):
                    try:
                        proc = psutil.Process(old_pid)
                        cmdline = ' '.join(proc.cmdline()).lower()
                        if 'python' in proc.name().lower() and 'arb_scanner' in cmdline:
                            process_alive = True
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
            except ImportError:
                # No psutil - use OS-level check
                try:
                    os.kill(old_pid, 0)  # Signal 0 = just check if process exists
                    # Process exists - assume it's us (conservative)
                    process_alive = True
                except (OSError, ProcessLookupError):
                    process_alive = False
            
            if process_alive:
                print("\n" + "!"*60)
                print("  ⛔ ANOTHER BOT INSTANCE IS ALREADY RUNNING!")
                print(f"     PID: {old_pid}")
                print("     Close the other instance first, or delete:")
                print(f"     {lock_file}")
                print("!"*60)
                input("\n  Press Enter to exit...")
                sys.exit(1)
            else:
                # Stale lock file - clean it up automatically
                print(f"  [Info] Cleaned up stale lock file (PID {old_pid} not running)")
                lock_file.unlink()
        except (ValueError, FileNotFoundError):
            pass  # Invalid lock file, safe to continue
    
    # Write our PID to lock file
    current_pid = os.getpid()
    with open(lock_file, 'w') as f:
        f.write(str(current_pid))
    
    # Make sure to clean up lock file on exit
    import atexit
    import signal
    
    def cleanup_lock():
        try:
            if lock_file.exists():
                with open(lock_file, 'r') as f:
                    if int(f.read().strip()) == current_pid:
                        lock_file.unlink()
        except:
            pass
    
    atexit.register(cleanup_lock)
    
    # Also handle signals for when window is closed with X
    def signal_handler(sig, frame):
        cleanup_lock()
        sys.exit(0)
    
    # Register signal handlers (SIGINT = Ctrl+C, SIGTERM = kill, SIGBREAK = Windows close)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    if hasattr(signal, 'SIGBREAK'):  # Windows only
        signal.signal(signal.SIGBREAK, signal_handler)
    
    print("\n" + "="*60)
    print("  CRYPTO ARBITRAGE SCANNER")
    print("  15-min BTC/ETH/SOL Markets")
    print("  Polymarket <-> Kalshi")
    print("="*60)
    
    # Show speed optimizations
    scan_rate = 1000 / CONFIG["speed"]["scan_interval_ms"]
    print(f"\n  ⚡ Speed: {scan_rate:.0f} scans/sec | uvloop: {'✓' if UVLOOP_ENABLED else '✗'} | HTTP/2: ✓")
    
    # Wait for strike collector to have good strikes
    print(f"\n  Checking for strike prices from collector...")
    strikes_file = Path.home() / "strikes.json"
    print(f"  (Looking for: {strikes_file})")
    required_cryptos = CONFIG["markets"]["cryptos"]  # ["BTC", "ETH", "SOL"]
    
    no_file_warned = False
    missing_strikes_warned = False
    manual_offered = False
    
    while True:
        strikes = load_strikes_from_file()
        
        # Check if we have HIGH or MED confidence strikes for the CURRENT window
        now = datetime.now(timezone.utc)
        current_minute = (now.minute // 15) * 15
        current_window = now.replace(minute=current_minute, second=0, microsecond=0)
        window_suffix = current_window.strftime('%H:%M')
        
        good_strikes = {}
        for crypto in required_cryptos:
            key = f"{crypto}_{window_suffix}"
            if key in strikes:
                conf = strikes[key].get("confidence", "LOW")
                if conf in ["HIGH", "MED"]:
                    good_strikes[crypto] = strikes[key]
        
        if len(good_strikes) >= len(required_cryptos):
            # Check if the window has ENDED (strikes are stale)
            window_end = current_window + timedelta(minutes=15)
            if now >= window_end:
                # Window has ended, these strikes are stale
                if not manual_offered:
                    manual_offered = True
                    mins_past = int((now - window_end).total_seconds() // 60)
                    print(f"\n  ⚠️ Strikes are from ENDED window {window_suffix} ({mins_past}m ago)")
                    print(f"  Options:")
                    print(f"    [W] Wait for collector to update (press Enter)")
                    print(f"    [M] Enter strikes manually")
                    
                    try:
                        choice = input(f"\n  Choice: ").strip().upper()
                        if choice == 'M':
                            # Jump to manual entry
                            good_strikes = {}  # Clear so we hit the manual entry below
                    except KeyboardInterrupt:
                        print("\n")
                
                if good_strikes:  # Still have strikes (user chose wait)
                    print(f"\r  Waiting for collector...  ", end="", flush=True)
                    time.sleep(5)
                    continue
            else:
                if no_file_warned or missing_strikes_warned:
                    print()  # Newline after dots
                print(f"  [OK] Have {len(good_strikes)} good strikes for window {window_suffix}:")
                for crypto, data in good_strikes.items():
                    print(f"       {crypto}: ${data['price']:,.2f} ({data['confidence']} @ {data['seconds']:.1f}s)")
                break
        
        # Offer manual input once
        if not manual_offered:
            manual_offered = True
            missing = [c for c in required_cryptos if c not in good_strikes]
            
            seconds_into_window = (now - current_window).total_seconds()
            seconds_to_next = (15 * 60) - seconds_into_window
            mins_to_next = int(seconds_to_next // 60)
            secs_to_next = int(seconds_to_next % 60)
            
            print(f"\n  Missing strikes for window {window_suffix}: {', '.join(missing)}")
            print(f"  Next boundary in {mins_to_next}m {secs_to_next}s")
            print(f"\n  Options:")
            print(f"    [M] Enter strikes manually (check PM website for 'Price to Beat')")
            print(f"    [W] Wait for collector (press Enter)")
            
            try:
                choice = input(f"\n  Choice: ").strip().upper()
                if choice == 'M':
                    # Manual entry
                    print(f"\n  Enter 'Price to Beat' from Polymarket for window {window_suffix}:")
                    print(f"  (Enter price or press Enter to skip)\n")
                    
                    entered = {}
                    for crypto in missing:
                        try:
                            user_input = input(f"  {crypto} strike: $").strip()
                            if user_input:
                                price = float(user_input.replace(',', '').replace('$', ''))
                                entered[crypto] = price
                                print(f"       ✓ {crypto}: ${price:,.2f}")
                        except ValueError:
                            print(f"       ✗ Invalid, skipping {crypto}")
                        except KeyboardInterrupt:
                            print("\n       Cancelled")
                            break
                    
                    if entered:
                        # Save to strikes file
                        import json
                        try:
                            if strikes_file.exists():
                                with open(strikes_file, "r") as f:
                                    data = json.load(f)
                            else:
                                data = {"strikes": {}}
                            
                            for crypto, price in entered.items():
                                key = f"{crypto}_{window_suffix}"
                                data["strikes"][key] = {
                                    "price": price,
                                    "time": current_window.isoformat(),
                                    "chainlink_ts": int(current_window.timestamp() * 1000),
                                    "seconds": 0.0,
                                    "confidence": "HIGH"
                                }
                            
                            data["updated"] = datetime.now(timezone.utc).isoformat()
                            
                            with open(strikes_file, "w") as f:
                                json.dump(data, f, indent=2)
                            
                            print(f"\n  Saved {len(entered)} strikes to {strikes_file}")
                        except Exception as e:
                            print(f"\n  Error saving: {e}")
                    
                    continue  # Re-check strikes
            except KeyboardInterrupt:
                print("\n")
            
            print(f"\n  Waiting for collector", end="", flush=True)
        
        # Waiting mode - just show dots
        if not strikes:
            if not no_file_warned:
                print(f"\n  [WAIT] No strikes.json found - is strike_collector.py running?")
                no_file_warned = True
        
        print(f".", end="", flush=True)
        await asyncio.sleep(10)
    
    # Don't need RTDS fallback anymore - we require collector
    print(f"\n  Strike prices ready!")
    
    keys = load_keys()
    
    # Connect to exchanges
    # NOTE: Kalshi connects FIRST, before we set proxy env vars
    # This ensures Kalshi uses VPN directly, not the residential proxy
    kalshi = KalshiConnector(keys["kalshi_api_key_id"], keys["kalshi_private_key_pem"], CONFIG["kalshi_api_url"])
    
    try:
        print(f"\n  Connecting...")
        await kalshi.connect()
        
        # Proxy was set at module load (before imports) - just log it
        if _PROXY_CONFIG.get("enabled"):
            print(f"  [OK] Residential proxy: {_PROXY_CONFIG['host']}:{_PROXY_CONFIG['port']} (set at startup)")
        
        pm_clob_url = CONFIG["polymarket_clob_url"]
        pm_gamma_url = CONFIG["polymarket_gamma_url"]
        
        pm = PolymarketConnector(
            keys["polymarket_private_key"], 
            keys["polymarket_funder_address"],
            pm_clob_url, 
            pm_gamma_url, 
            CONFIG["polygon_rpc_url"]
        )
        
        await pm.connect()
        
        # Check for strike collector data - REQUIRED for PM strikes
        strikes_file = Path.home() / "strikes.json"
        if strikes_file.exists():
            try:
                with open(strikes_file, "r") as f:
                    strikes_data = json.load(f)
                strike_count = len(strikes_data.get("strikes", {}))
                last_update = strikes_data.get("last_update", "unknown")
                print(f"  [OK] Strike data found: {strike_count} entries (updated: {last_update})")
            except:
                print(f"  [WARN] strikes.json exists but couldn't be read")
        else:
            print(f"  [ERROR] strikes.json not found!")
            print(f"          PM strike prices are REQUIRED - run strike_collector.py first!")
            print(f"          Without PM strikes, NO trades can be executed.")
            print(f"          Start strike_collector.py in another terminal, then restart this bot.")
        
        # Check for and cancel any open orders from previous runs
        # This is safe - doesn't affect filled positions, only clears the orderbook
        print(f"\n  Clearing stale orders...")
        pm_cancelled = await pm.check_and_cancel_open_orders()
        k_cancelled = await kalshi.cancel_all_orders()
        if pm_cancelled + k_cancelled > 0:
            print(f"  [Startup] Cleared {pm_cancelled} PM + {k_cancelled} K stale order(s)")
        
        pm_bal = await pm.get_balance()
        k_bal = await kalshi.get_balance()
        
        # Initialize balance-based P&L tracking
        total_balance = float(pm_bal.cash) + float(k_bal.cash)
        profit_tracker.initialize_balance_baseline(total_balance)
        profit_tracker.update_session_start_balance(total_balance)
        
        scanner = Scanner(pm, kalshi)
        
        # Start WebSockets for real-time orderbooks
        if CONFIG["speed"]["use_websocket"]:
            pm_markets = await pm.get_markets()
            k_markets = await kalshi.get_markets()
            
            # Polymarket WebSocket (works well)
            pm.start_websocket(pm_markets)
            
            # Kalshi WebSocket - disabled for now, needs more testing
            # k_tickers = [m.market_id for m in k_markets]
            # if k_tickers:
            #     kalshi.start_websocket(k_tickers)
            
            await asyncio.sleep(1)  # Let WebSockets connect
        
        # Check for orphaned positions from previous runs
        # NOTE: We only LOG positions at startup, we don't try to hedge them
        # because we don't have context about what market window they belong to.
        # The user should manually manage positions from previous sessions.
        print(f"\n  Checking for orphaned positions...")
        initial_pairs = await scanner.find_pairs(force_refresh=True)
        if initial_pairs:
            # Just check and report, don't try to hedge
            k_positions = await kalshi.get_positions()
            pm_positions = await pm.get_positions()
            if k_positions or pm_positions:
                print(f"  ⚠️ Found positions from previous session:")
                
                # Track K positions per asset for trade count reconstruction
                # Separate current-window (for trade count) from all (for cumulative position)
                k_current_window = {}  # {"BTC": 10, ...} - only current window
                k_all_sizes = {}  # {"BTC": 14, ...} - all unsettled windows
                k_asset_directions = {}  # {"BTC": "bearish", ...}
                
                # Determine current window time for filtering
                now_utc = datetime.now(timezone.utc)
                # Current 15-min window: floor to nearest 15 minutes
                current_minute = (now_utc.minute // 15) * 15
                window_start_utc = now_utc.replace(minute=current_minute, second=0, microsecond=0)
                window_end_utc = window_start_utc + timedelta(minutes=15)
                
                if k_positions:
                    for ticker, pos in k_positions.items():
                        # Parse ticker: KXBTC15M-26JAN310415-15 -> BTC
                        asset = "BTC" if "BTC" in ticker else "ETH" if "ETH" in ticker else "SOL" if "SOL" in ticker else ticker[:6]
                        direction = pos.get('direction', '?').upper()
                        size = pos.get('size', 0)
                        
                        # Check if this ticker belongs to the current window
                        is_current = is_current_window_ticker(ticker, window_end_utc)
                        window_tag = " [CURRENT]" if is_current else " [PREV]"
                        print(f"       K: {asset} {direction} x{size}{window_tag}")
                        
                        # All positions count toward cumulative cap
                        k_all_sizes[asset] = k_all_sizes.get(asset, 0) + size
                        
                        # Only current window positions count toward trade limit
                        if is_current:
                            k_current_window[asset] = k_current_window.get(asset, 0) + size
                        
                        # K YES + PM DOWN = bearish, K NO + PM UP = bullish
                        if direction == "YES":
                            k_asset_directions[asset] = "bearish"
                        elif direction == "NO":
                            k_asset_directions[asset] = "bullish"
                if pm_positions:
                    # Only show current window positions to avoid spam
                    internal_pm = pm.get_internal_positions_with_metadata() if hasattr(pm, 'get_internal_positions_with_metadata') else {}
                    if internal_pm:
                        for token_id, pos_data in internal_pm.items():
                            size = pm._internal_positions.get(token_id, 0)
                            underlying = pos_data.get('underlying', '?')
                            direction = pos_data.get('direction', '?').upper()
                            print(f"      PM: {underlying} {direction} x{int(size)}")
                    else:
                        print(f"      PM: {len(pm_positions)} historical positions (current window: 0)")
                print(f"  (Will settle automatically - no action needed)")
                
                # CRITICAL: Reconstruct trade counts and cumulative positions from existing positions
                # This prevents the bot from exceeding limits after a restart mid-window
                # Trade count: only from CURRENT window (prevents old windows inflating count)
                # Cumulative position: ALL unsettled windows (real exposure)
                
                for asset, k_size in k_all_sizes.items():
                    # Reconstruct cumulative position from ALL unsettled positions
                    direction = k_asset_directions.get(asset, "bullish")
                    scanner._cumulative_positions[asset] = {"size": k_size, "direction": direction}
                
                for asset, current_size in k_current_window.items():
                    # Estimate trade count from CURRENT WINDOW only
                    estimated_trades = max(1, current_size // 5)
                    scanner._window_trade_counts[asset] = estimated_trades
                    
                    total_size = k_all_sizes.get(asset, current_size)
                    direction = k_asset_directions.get(asset, "bullish")
                    print(f"  [Restart] {asset}: trades={estimated_trades}/3 (this window: {current_size}), position={total_size}/30 {direction}")
                
                # Log assets with only old-window positions (no current trade count)
                for asset in k_all_sizes:
                    if asset not in k_current_window:
                        total_size = k_all_sizes[asset]
                        direction = k_asset_directions.get(asset, "bullish")
                        print(f"  [Restart] {asset}: trades=0/3 (no current window), position={total_size}/30 {direction} [prev window]")
                
                if k_all_sizes:
                    log_print(f"  [Restart] Reconstructed limits: {len(k_current_window)} current-window, {len(k_all_sizes)} total positions")
            else:
                print(f"  ✓ No orphaned positions found")
        else:
            print(f"  (No active markets to check)")
        
        # Enable trading after all safety checks pass
        print(f"\n  ✅ Safety checks complete - enabling trading in 2 seconds...")
        await asyncio.sleep(2)
        scanner._trading_enabled = True
        log_print(f"  ✅ Trading ENABLED")
        
        n = 0
        dashboard_lines = 0
        last_balance_check = 0
        paused_low_balance = False
        last_scan_time = time.time()
        last_dashboard_edges = []  # Track if edges changed
        
        def clear_lines(count):
            """Clear N lines above cursor."""
            if count > 0:
                print(f"\033[{count}A", end="")
                for _ in range(count):
                    print("\033[K")  # Clear line
                print(f"\033[{count}A", end="")
        
        def print_dashboard(scan_num: int, edges: list, pm_bal, k_bal, scan_rate: float, 
                           position: Optional[ActivePosition] = None, paused=False, force_full=False):
            """Print momentum strategy dashboard"""
            nonlocal dashboard_lines, last_dashboard_edges
            
            # Check if edges changed significantly (different assets or big score change)
            edges_changed = False
            if edges and last_dashboard_edges:
                if len(edges) != len(last_dashboard_edges):
                    edges_changed = True
                else:
                    for e1, e2 in zip(edges, last_dashboard_edges):
                        if e1.get('underlying') != e2.get('underlying'):
                            edges_changed = True
                            break
            elif edges != last_dashboard_edges:
                edges_changed = True
            
            last_dashboard_edges = edges.copy() if edges else []
            
            lines = []
            
            # Trade-based P&L
            session_pnl = profit_tracker.session_profit
            all_time_pnl = profit_tracker.all_time_profit
            pnl_str = f"+${session_pnl:.2f}" if session_pnl >= 0 else f"-${abs(session_pnl):.2f}"
            total_str = f"+${all_time_pnl:.2f}" if all_time_pnl >= 0 else f"-${abs(all_time_pnl):.2f}"
            session_trades = len([t for t in profit_tracker.session_trades if t.get("source") != "hedge"])
            
            # Minimal display if no edges
            if not edges and not position and not paused:
                lines.append(f"  P&L: {pnl_str} session | {total_str} total | Trades: {session_trades} | Waiting for markets...")
                for line in lines:
                    print(line)
                dashboard_lines = len(lines)
                return
            
            # Full dashboard
            lines.append(f"  P&L: {pnl_str} session | {total_str} total | Trades: {session_trades} | Scans: {scan_num} ({scan_rate:.1f}/s)")
            
            # Balance line
            pm_ok = "✓" if pm_bal.cash >= CONFIG["min_cash_balance"] else "✗"
            k_ok = "✓" if k_bal.cash >= CONFIG["min_cash_balance"] else "✗"
            total_value = pm_bal.cash + k_bal.cash
            lines.append(f"  PM: {pm_ok}${pm_bal.cash:.2f} | K: {k_ok}${k_bal.cash:.2f} | 💰 Total: ${total_value:.2f}")
            
            # Position status
            if position:
                held = position.seconds_held
                opp = position.opportunity
                lines.append(f"  📍 POSITION: {opp.first_leg_venue.value} {opp.first_leg_direction.value.upper()} x{position.first_leg_fill_size} @ ${position.first_leg_fill_price:.2f} ({held:.0f}s)")
            elif paused:
                lines.append(f"  ⏸ PAUSED - Need ${CONFIG['min_cash_balance']:.0f}+ cash")
            
            # Cumulative positions (cross-window)
            if scanner._cumulative_positions:
                max_pos = CONFIG["markets"].get("max_position_per_underlying", 30)
                cum_parts = []
                for asset, data in sorted(scanner._cumulative_positions.items()):
                    sz = data.get("size", 0)
                    dr = data.get("direction", "?")[0].upper()  # B or b
                    cum_parts.append(f"{asset}:{sz}/{max_pos}{dr}")
                lines.append(f"  📊 Positions: {' | '.join(cum_parts)}")
            
            # Market table header
            lines.append(f"  {'':─<86}")
            lines.append(f"  {'Asset':<5} {'Dir':>5} {'PM$':>5} {'K$':>5} {'Score':>5} {'Net%':>5} {'Est$':>6} {'Diff':>6} {'ROI':>4} {'Fav':>4} {'1st':>4}")
            lines.append(f"  {'':─<86}")
            
            # Show ALL directions for all assets
            shown_assets = set()
            all_display = []
            
            for edge in edges:
                est_profit = edge.get('est_profit', 0)
                net_edge = edge.get('edge', 0)
                strike_diff = edge.get('strike_diff', 0)
                # If strike_diff is 0 and we know strikes are assumed, show assumed ROI
                if edge.get('strike_diff', 0) == 0:
                    req_roi = CONFIG["markets"].get("assumed_strike_min_edge_pct", 8.0)
                else:
                    req_roi = edge.get('req_roi', CONFIG["strategy"]["min_edge_pct"])
                tradeable = edge['score'] >= CONFIG["strategy"]["min_score_to_trade"] and net_edge >= req_roi and net_edge >= CONFIG["strategy"]["min_edge_pct"]
                indicator = "→" if tradeable else " "
                est_str = f"${est_profit:+.2f}" if est_profit != 0 else "$0.00"
                
                # Direction label
                dir_lbl = edge.get('dir_label', edge.get('first_leg', '??'))
                
                # Strike diff display
                if strike_diff > 0:
                    diff_str = f".{strike_diff*1000:03.0f}%"
                else:
                    diff_str = "0"
                
                # Favorability - clear column
                is_fav = edge.get('strike_favorable', False)
                if strike_diff > 0.01:
                    fav_str = " ✓F" if is_fav else " ✗U"
                else:
                    fav_str = "  -"
                
                # Round ROI to nearest 0.5% to reduce visual noise from dynamic calculation
                req_roi_rounded = round(req_roi * 2) / 2
                if req_roi >= 999:
                    roi_str = "BLK"
                elif req_roi_rounded % 1 == 0.5:
                    roi_str = f"{min(req_roi_rounded, 99):.1f}%"
                else:
                    roi_str = f"{int(min(req_roi_rounded, 99))}%"
                
                # Show ACTUAL ask prices (what we'd pay), not implied mid prices
                first_leg_code = edge.get('first_leg', '??')  # e.g. "PU", "KD"
                second_leg_code = edge.get('second_leg', '??')
                first_price = edge.get('first_price', 0)
                second_price = edge.get('second_price', 0)
                
                # Map to PM and K costs based on which venue each leg is
                if first_leg_code.startswith('P'):
                    pm_cost = first_price
                    k_cost = second_price
                else:
                    pm_cost = second_price
                    k_cost = first_price
                
                first_leg = edge.get('first_leg', '??')
                all_display.append(f"  {indicator}{edge['asset']:<4} {dir_lbl:>5} {pm_cost:>5.2f} {k_cost:>5.2f} {edge['score']:>5.0f} {net_edge:>+5.1f}% {est_str:>6} {diff_str:>6} {roi_str:>4} {fav_str:>4} {first_leg:>4}")
                shown_assets.add(edge['asset'])
            
            # Add placeholder rows for any missing assets
            for asset in ["BTC", "ETH", "SOL"]:
                if asset not in shown_assets:
                    all_display.append(f"   {asset:<4} {'---':>5} {'---':>5} {'---':>5} {'---':>5} {'---':>5} {'---':>6} {'---':>6} {'---':>4} {'---':>4} {'---':>4}")
            
            for line in all_display:
                lines.append(line)
            
            lines.append(f"  {'':─<74}")
            lines.append(f"  Min score: {CONFIG['strategy']['min_score_to_trade']} | Min edge: {CONFIG['strategy']['min_edge_pct']}% | Delay: {CONFIG['strategy']['second_leg_delay_seconds']}s")
            
            # Show recent significant edges (last 3) - scrolling history
            recent_edges = scanner._recent_edges_history[-3:] if hasattr(scanner, '_recent_edges_history') and scanner._recent_edges_history else []
            if len(recent_edges) > 0:
                lines.append(f"  {'':─<74}")
                lines.append(f"  Recent edges (≥6%):")
                for edge in reversed(recent_edges):  # Most recent first
                    age_secs = (datetime.now(timezone.utc) - edge['time']).total_seconds()
                    if age_secs < 60:
                        age_str = f"{age_secs:.0f}s"
                    else:
                        age_str = f"{age_secs/60:.0f}m"
                    
                    # Show indicator and reason
                    filter_reason = edge.get('filter_reason')
                    if filter_reason:
                        indicator = " "
                        # Truncate long reasons for display
                        short = filter_reason[:18]
                        reason_str = f"[{short}]"
                    else:
                        indicator = "→"
                        reason_str = "[TRADE]"
                    
                    lines.append(f"  {indicator}{edge['asset']:<4} Raw:{edge['raw_edge']:>+5.1f}% Net:{edge['net_edge']:>+5.1f}% S:{edge['score']:>2.0f} {edge['first_leg']} {reason_str:<15} {age_str:>3}")
            
            for line in lines:
                print(line)
            
            dashboard_lines = len(lines)
        
        while True:
            n += 1
            scan_start = time.time()
            try:
                # Periodic logging every 1000 scans (about every 100 seconds)
                if n % 1000 == 0:
                    logger.log_text(f"Scan #{n} - P&L: ${profit_tracker.session_profit:.2f} | Trades: {len([t for t in profit_tracker.session_trades if t.get('source') != 'hedge'])}")
                
                # Refresh balances periodically
                if scan_start - last_balance_check >= CONFIG["balance_refresh_seconds"]:
                    last_balance_check = scan_start
                    pm_bal = await pm.get_balance()
                    k_bal = await kalshi.get_balance()
                    
                    # Update scanner with balances for capital protection
                    scanner.update_balances(pm_bal, k_bal)
                    
                    min_bal = CONFIG["min_cash_balance"]
                    has_enough_cash = pm_bal.cash >= min_bal and k_bal.cash >= min_bal
                    
                    if not has_enough_cash and not paused_low_balance:
                        paused_low_balance = True
                    elif has_enough_cash and paused_low_balance:
                        paused_low_balance = False
                    
                    # Check for settled trades (every balance refresh cycle)
                    if profit_tracker.pending_settlements:
                        try:
                            settled = await profit_tracker.check_settlements(kalshi)
                            for trade in settled:
                                if trade.get("actual_profit") != trade.get("expected_profit"):
                                    diff = trade.get("actual_profit", 0) - trade.get("expected_profit", 0)
                                    logger.log_text(f"[Settlement] {trade['underlying']} adjusted: ${diff:+.2f}")
                        except Exception as e:
                            pass  # Don't let settlement check errors break the bot
                
                # Scan crypto pairs
                pairs = await scanner.find_pairs()
                scanner._market_pairs = pairs or []
                
                if not pairs:
                    # Overwrite same line instead of spamming
                    print(f"\r  Waiting for markets...    ", end="", flush=True)
                    await asyncio.sleep(10)
                    continue
                else:
                    # Clear the waiting message when we have pairs
                    print(f"\r{' ' * 40}\r", end="", flush=True)
                
                # Check for window transition - clear stale orders from previous window
                await scanner.check_window_transition(pairs)
                
                # Check pending PM fills - this is URGENT, do it before other checks
                # If PM filled < trade_size, we need to hedge on Kalshi immediately
                await scanner.check_pending_pm_fills(pairs)
                
                # Check if any completed assumed-strike trades need unwinding
                await scanner.check_and_unwind_diverged_trades(pairs)
                
                # Check if we have an active position that needs second leg
                position = scanner.get_active_position()
                if position:
                    held_time = position.seconds_held
                    delay = CONFIG["strategy"]["second_leg_delay_seconds"]
                    max_hold = CONFIG["strategy"]["max_position_hold_seconds"]
                    
                    # Check if this was an assumed-strike trade that needs to exit
                    should_exit = await scanner.check_assumed_strike_position(pairs)
                    
                    if should_exit or held_time >= delay:
                        # Time to complete the arb (or forced exit due to strike divergence)
                        if should_exit:
                            print(f"  🚨 Emergency exit due to strike divergence!")
                        else:
                            print(f"\n  ⏰ Completing arb after {held_time:.0f}s hold...")
                        dashboard_lines = 0
                        success = await scanner.execute_second_leg(position)
                        
                        # Skip reconciliation for late window trades - accept the loss if it fails
                        skip_reconcile = CONFIG["strategy"].get("late_window_skip_reconciliation", True)
                        is_late_trade = getattr(position.opportunity, 'is_late_window_trade', False)
                        
                        if is_late_trade and skip_reconcile:
                            print(f"  [Late window trade - skipping reconciliation]")
                        else:
                            # ALWAYS reconcile positions after a trade to catch any mismatches
                            await scanner.reconcile_positions(pairs)
                        
                        # ALWAYS refresh balances after a trade attempt
                        pm_bal = await pm.get_balance()
                        k_bal = await kalshi.get_balance()
                        scanner.update_balances(pm_bal, k_bal)
                        last_balance_check = time.time()
                        
                        # Update pause status
                        min_bal = CONFIG["min_cash_balance"]
                        has_enough_cash = pm_bal.cash >= min_bal and k_bal.cash >= min_bal
                        if not has_enough_cash:
                            paused_low_balance = True
                        
                        if not success and held_time >= max_hold:
                            # Don't give up - the market order should have filled
                            # If we're here, something went wrong (network, etc.)
                            # Keep the position open and retry next iteration
                            print(f"  ⚠️ Second leg failed after {held_time:.0f}s - retrying...")
                            # Don't clear position - will retry on next loop iteration
                    await asyncio.sleep(CONFIG["speed"]["scan_interval_ms"] / 1000)
                    continue
                
                # Find scored opportunities
                opportunities = await scanner.find_scored_opportunities(pairs)
                
                # Find late window asymmetric opportunities (separate strategy)
                late_asym_opportunities = await scanner.find_late_asymmetric_opportunities(pairs)
                
                # Calculate scan rate
                scan_end = time.time()
                scan_duration = scan_end - last_scan_time
                scan_rate = 1.0 / scan_duration if scan_duration > 0 else 0
                last_scan_time = scan_end
                
                # Periodic garbage collection (every 500 scans)
                if n % 500 == 0:
                    import gc
                    gc.collect()
                
                # Update dashboard
                # Update every 10 scans when active, every 50 when idle
                update_freq = 10 if scanner._last_edges else 50
                if n % update_freq == 1 or n == 1:
                    if n > 1:
                        clear_lines(dashboard_lines)
                    print_dashboard(n, scanner._last_edges, pm_bal, k_bal, scan_rate, position, paused_low_balance)
                
                # Execute if we have a good opportunity and no active position
                traded = False
                # DIAGNOSTIC: Log why trades don't execute (throttled to every 5s)
                if not hasattr(scanner, '_diag_last') or time.time() - scanner._diag_last > 5:
                    if opportunities:
                        _top = opportunities[0]
                        _s = _top.score
                        _e = float(_top.net_edge_pct)
                        _ms = CONFIG["strategy"]["min_score_to_trade"]
                        _me = CONFIG["strategy"]["min_edge_pct"]
                        if _s >= _ms and _e >= _me:
                            if paused_low_balance:
                                logger.log_text(f"[DIAG] {_top.market_pair.polymarket.underlying} s={_s:.0f} e={_e:.1f}% BLOCKED: paused_low_balance")
                                scanner._diag_last = time.time()
                            elif position:
                                logger.log_text(f"[DIAG] {_top.market_pair.polymarket.underlying} s={_s:.0f} e={_e:.1f}% BLOCKED: active_position")
                                scanner._diag_last = time.time()
                            else:
                                # Check for orphans before declaring "passes"
                                _has_orphans, _orphan_count = scanner.has_orphan_positions(_top.market_pair.polymarket.underlying)
                                if _has_orphans:
                                    logger.log_text(f"[DIAG] {_top.market_pair.polymarket.underlying} s={_s:.0f} e={_e:.1f}% BLOCKED: orphan_positions={_orphan_count}")
                                else:
                                    logger.log_text(f"[DIAG] {_top.market_pair.polymarket.underlying} s={_s:.0f} e={_e:.1f}% PASSES all pre-checks, attempting trade")
                                scanner._diag_last = time.time()
                        elif _s >= 35 and _e >= 5.0:
                            logger.log_text(f"[DIAG] {_top.market_pair.polymarket.underlying} s={_s:.0f} e={_e:.1f}% BLOCKED: score<{_ms}={_s<_ms} edge<{_me}={_e<_me}")
                            scanner._diag_last = time.time()
                    elif scanner._last_edges:
                        # Edges exist on dashboard but no opportunities in list
                        best = max(scanner._last_edges, key=lambda x: x.get('edge', 0))
                        if best.get('edge', 0) >= 5.0:
                            # Throttle DIAG to every 30s to reduce log spam
                            if not hasattr(scanner, '_diag_empty_last') or time.time() - scanner._diag_empty_last > 30:
                                logger.log_text(f"[DIAG] Dashboard shows {best['asset']} edge={best['edge']:.1f}% but opportunities list is EMPTY")
                                scanner._diag_empty_last = time.time()
                            scanner._diag_last = time.time()
                
                # 🛑 SAFETY PAUSE CHECK - Block ALL trading if paused
                is_paused, pause_reason = scanner.is_safety_paused()
                if is_paused:
                    # Log periodically (every 30s) so user knows bot is paused
                    if not hasattr(scanner, '_last_pause_log') or time.time() - scanner._last_pause_log > 30:
                        print(f"\n  🛑 TRADING PAUSED: {pause_reason}")
                        print(f"     Will auto-resume on next window transition")
                        logger.log_text(f"[SAFETY] Still paused: {pause_reason}")
                        scanner._last_pause_log = time.time()
                    # Skip all trading when paused
                    await asyncio.sleep(CONFIG["speed"]["scan_interval_ms"] / 1000)
                    continue
                
                # LATE ASYMMETRIC: Check and execute before main arb (priority in late window)
                if late_asym_opportunities and not paused_low_balance and not position:
                    asym_top = late_asym_opportunities[0]
                    asym_underlying = asym_top.market_pair.polymarket.underlying
                    
                    # Log opportunity (throttled)
                    if not hasattr(scanner, '_last_asym_diag') or time.time() - scanner._last_asym_diag > 5:
                        logger.log_text(f"[ASYM] {asym_underlying} ROI:{float(asym_top.net_edge_pct):.1f}% "
                                       f"PM@{float(asym_top.first_leg_price):.2f} K@{float(asym_top.second_leg_price):.2f}")
                        scanner._last_asym_diag = time.time()
                    
                    # Check trade lock BEFORE logging to prevent spam
                    # (execute_late_asymmetric checks this too, but logging 222 blocked attempts pollutes logs)
                    _asym_lock_active = hasattr(scanner, '_trade_lock_until') and time.time() < scanner._trade_lock_until
                    if not _asym_lock_active:
                        # Execute late asymmetric trade
                        logger.log_text(f"[ASYM] TRADE ATTEMPT: {asym_underlying} ROI:{float(asym_top.net_edge_pct):.1f}%")
                        asym_result = await scanner.execute_late_asymmetric(asym_top)
                    
                        if asym_result:
                            traded = True
                            logger.log_text(f"[ASYM] ✓ Trade completed for {asym_underlying}")
                
                # Regular arb execution (skip if we just did an asymmetric trade)
                if opportunities and not paused_low_balance and not position and not traded:
                    top = opportunities[0]
                    min_score = CONFIG["strategy"]["min_score_to_trade"]
                    min_edge = CONFIG["strategy"]["min_edge_pct"]
                    
                    if top.score >= min_score and float(top.net_edge_pct) >= min_edge:
                        # Check ALL conditions BEFORE printing anything
                        can_trade, trade_reason = scanner.can_trade(top.market_pair, float(top.net_edge_pct), getattr(top, "strike_favorable", False))
                        can_capital, capital_reason = scanner.check_capital_protection(top)
                        
                        if not can_trade or not can_capital:
                            # LOG BLOCKED TRADES - don't silently drop qualified opportunities
                            block_reason = trade_reason if not can_trade else capital_reason
                            # Throttle logging to once per 10s per asset to avoid spam
                            _block_key = f"{top.market_pair.polymarket.underlying}_{block_reason}"
                            _now = time.time()
                            if not hasattr(scanner, '_last_block_log'):
                                scanner._last_block_log = {}
                            if _block_key not in scanner._last_block_log or _now - scanner._last_block_log[_block_key] > 10:
                                scanner._last_block_log[_block_key] = _now
                                logger.log_text(f"[BLOCKED] {top.market_pair.polymarket.underlying}: "
                                    f"score={top.score:.0f} edge={float(top.net_edge_pct):.1f}% - {block_reason}")
                        
                        if can_trade and can_capital:
                            # Log to file only - don't print until trade actually executes
                            # execute_arb_parallel has additional checks that may still block
                            logger.log_text(f"TRADE ATTEMPT: {top.market_pair.polymarket.underlying} Score:{top.score:.0f} Edge:{top.net_edge_pct:.1f}%")
                            
                            # Execute BOTH legs in parallel (don't wait for first leg to confirm)
                            result = await scanner.execute_arb_parallel(top)
                            
                            # Only reconcile if we actually placed orders (not if skipped due to depth/capital)
                            # Check if any orders were placed by looking at internal tracking
                            # Dynamic window: base 30s + extra time if we know the trade was slow
                            base_window = 30
                            if hasattr(scanner, '_last_trade_duration') and scanner._last_trade_duration:
                                # Add the actual trade duration + 10s buffer
                                recon_window = scanner._last_trade_duration + 10
                            else:
                                recon_window = base_window
                            
                            orders_placed = hasattr(scanner, '_last_trade_attempt') and scanner._last_trade_attempt and \
                                (datetime.now(timezone.utc) - scanner._last_trade_attempt).total_seconds() < recon_window
                            
                            if orders_placed:
                                # Skip reconciliation for late window trades - accept the loss if it fails
                                skip_reconcile = CONFIG["strategy"].get("late_window_skip_reconciliation", True)
                                is_late_trade = getattr(top, 'is_late_window_trade', False)
                                
                                if is_late_trade and skip_reconcile:
                                    print(f"  [Late window trade - skipping reconciliation]")
                                else:
                                    # Reconcile positions after trade attempt
                                    await scanner.reconcile_positions(pairs)
                                
                                # Refresh balances
                                pm_bal = await pm.get_balance()
                                k_bal = await kalshi.get_balance()
                                scanner.update_balances(pm_bal, k_bal)
                                last_balance_check = time.time()
                            else:
                                # Debug: why no reconciliation?
                                if not result:
                                    elapsed = (datetime.now(timezone.utc) - scanner._last_trade_attempt).total_seconds() if hasattr(scanner, '_last_trade_attempt') and scanner._last_trade_attempt else 999
                                    logger.log_text(f"  [DEBUG] No reconciliation: orders_placed={orders_placed}, elapsed={elapsed:.0f}s")
                            
                            if result:
                                logger.log_text(f"  PARALLEL ARB SUCCESS")
                            else:
                                logger.log_text(f"  PARALLEL ARB FAILED/PARTIAL")
                                if orders_placed:
                                    await asyncio.sleep(2)
                                else:
                                    # No orders placed (blocked by orphan check, balance, etc.)
                                    # Cooldown to prevent tight spam loop — 5s is enough to 
                                    # let conditions change without hammering the same check
                                    await asyncio.sleep(5)
                
                await asyncio.sleep(CONFIG["speed"]["scan_interval_ms"] / 1000)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                import traceback
                print(f"\n  Error: {type(e).__name__}: {e}")
                traceback.print_exc()
                dashboard_lines = 0
                await asyncio.sleep(5)
    
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"\n  [ERROR] {e}")
        import traceback
        traceback.print_exc()
    finally:
        print(f"\n\n  " + "="*60)
        print(f"  SESSION SUMMARY")
        print(f"  " + "="*60)
        print(f"  {session.summary()}")
        if session.trades:
            print(f"\n  Completed Arbs:")
            for i, t in enumerate(session.trades, 1):
                print(f"    {i}. {t.underlying} x{t.size} = ${t.total_cost:.2f} → +${t.expected_profit:.2f}")
        
        # Warn about open position (only if scanner was created)
        try:
            if scanner.has_active_position():
                pos = scanner.get_active_position()
                print(f"\n  ⚠️ OPEN POSITION:")
                print(f"     {pos.opportunity.first_leg_venue.value} {pos.opportunity.first_leg_direction.value} x{pos.first_leg_fill_size}")
                print(f"     Entry: ${pos.first_leg_fill_price:.2f}")
        except NameError:
            pass  # Scanner wasn't created yet
        
        print(f"  " + "="*60)
        print(f"\n  Disconnecting...")
        
        # IMPORTANT: Cancel all open orders before disconnecting
        try:
            print(f"  Cancelling any open orders...")
            pm_cancelled = await pm.cancel_all_orders()
            if pm_cancelled > 0:
                print(f"  Cancelled {pm_cancelled} PM order(s)")
        except Exception as e:
            print(f"  Could not cancel PM orders: {e}")
        
        try:
            await pm.disconnect()
            await kalshi.disconnect()
        except NameError:
            pass  # Connectors weren't created yet
        print("  Done.")

def set_high_priority():
    """Set process to high priority for better performance"""
    try:
        import sys
        if sys.platform == 'win32':
            import ctypes
            # Set process priority to HIGH_PRIORITY_CLASS
            handle = ctypes.windll.kernel32.GetCurrentProcess()
            ctypes.windll.kernel32.SetPriorityClass(handle, 0x00000080)  # HIGH_PRIORITY_CLASS
            print("  [Perf] Process priority set to HIGH")
        else:
            import os
            os.nice(-10)  # Lower nice = higher priority (needs root on Linux)
            print("  [Perf] Process nice level reduced")
    except Exception as e:
        print(f"  [Perf] Could not set high priority: {e}")

def setup_performance():
    """Configure system for high performance"""
    import gc
    
    # Disable garbage collection during hot paths (we'll manually trigger it)
    gc.disable()
    print("  [Perf] GC disabled (manual collection enabled)")
    
    # Set high process priority
    set_high_priority()
    
    # Try to use uvloop on Unix systems
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        print("  [Perf] uvloop enabled")
    except ImportError:
        pass  # uvloop not available on Windows

if __name__ == "__main__":
    # Check for --reset flag to reset profit tracker
    if "--reset" in sys.argv or "-r" in sys.argv:
        print("\n  🔄 RESETTING PROFIT TRACKER...")
        profit_tracker.reset_all()
        print("  Profit tracker has been reset to $0.00")
        print("  Remove --reset flag and restart to begin trading.\n")
        input("Press Enter to exit...")
        sys.exit(0)
    
    if CONFIG["speed"].get("high_performance_mode"):
        print("\n  ⚡ HIGH PERFORMANCE MODE ⚡")
        setup_performance()
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    input("\nPress Enter to close...")
