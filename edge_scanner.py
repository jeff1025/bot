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
from logging.handlers import RotatingFileHandler
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
SCAN_INTERVAL_SECONDS = 3       # Seconds between full market scans (must be < window width)
RECONCILE_INTERVAL_SECONDS = 120  # Seconds between position reconciliation checks
SETTLEMENT_CHECK_INTERVAL = 30  # Seconds between settlement checks
STATS_PRINT_INTERVAL = 60       # Seconds between dashboard prints
VOL_WARMUP_SECONDS = 30         # Seconds of Chainlink data before trading
VOL_WARMUP_MIN_TICKS = 2       # Minimum price ticks per asset before trading

# ── Fee model (Kalshi taker) ────────────────────────────────────────────────
# Taker fee: roundup(0.07 * C * P * (1 - P)), max $0.02/contract
KALSHI_TAKER_FEE_RATE = 0.07
KALSHI_MAX_FEE_PER_CONTRACT = 0.02
# Maker fee (resting limit that gets filled): roundup(0.0175 * C * P * (1-P))
KALSHI_MAKER_FEE_RATE = 0.0175

# ── Global risk limits ──────────────────────────────────────────────────────
MAX_OPEN_POSITIONS_GLOBAL = 9      # Circuit breaker: stop trading above this (9 categories)
MAX_POSITIONS_PER_MARKET = 1       # Max concurrent positions in a single market
MAX_TRADES_PER_HOUR = 18           # Rate limit on order placement (~2/hr per category)

# ── Bankroll management (anti-martingale with trailing stop) ───────────────
BANKROLL_STOP_LOSS = 20.0             # Base daily stop loss in dollars
BANKROLL_PROFIT_LOCK_PCT = 0.60       # Lock in 60% of peak daily profit
BANKROLL_SCALE_TIERS = [              # (min_daily_pnl, contracts_per_trade)
    (0,   1),                         # Base: 1 contract (prove the edge)
    (8,   2),                         # Up $8+:  scale to 2 contracts
    (20,  3),                         # Up $20+: scale to 3 contracts
    (40,  4),                         # Up $40+: scale to 4 contracts
]
BANKROLL_MAX_CONTRACTS = 4            # Hard cap on contracts per trade
BANKROLL_DRAWDOWN_STEP_DOWN = 5.0     # Drop back 1 tier if P&L is $5+ below HWM
BANKROLL_MAX_RISK_PCT = 0.10          # Never risk more than 10% of balance on 1 trade

# ── Orderbook batching ────────────────────────────────────────────────────
MAX_ORDERBOOK_CANDIDATES = 15      # Only fetch orderbook for top N candidates by edge potential
FAIR_VALUE_EXTREME_LOW = 0.03      # Skip markets with fair value below this (near-certain no-settle)
FAIR_VALUE_EXTREME_HIGH = 0.97     # Skip markets with fair value above this (near-certain settle)

# ── Vol model parameters ────────────────────────────────────────────────────
MINUTES_PER_YEAR = 525_960
SECONDS_PER_YEAR = MINUTES_PER_YEAR * 60
EWMA_LAMBDA_FAST = 0.90    # Half-life ~7 observations
EWMA_LAMBDA_SLOW = 0.97    # Half-life ~23 observations
VOL_DAMPENER = 0.90         # Conservative 10% haircut on vol (fewer but higher-quality trades)
VOL_RANGE_BOOST = 1.15      # Vol BOOST for ranges (lower vol inflates range FV, so counteract)

MIN_VOL_ABSOLUTE = {        # Safety-net floor (only for data gaps / warmup)
    "BTC": 0.15,
    "ETH": 0.20,
    "SOL": 0.25,
    "XRP": 0.25,
    "DOGE": 0.30,
}
VOL_FLOOR_EWMA_MULT = 0.60  # Dynamic floor = 60% of slow EWMA vol

# ── Chainlink price feed ────────────────────────────────────────────────────
CHAINLINK_WS_URL = "wss://ws-live-data.polymarket.com"
CHAINLINK_SYMBOLS = {
    "BTC": "btc/usd",
    "ETH": "eth/usd",
    "SOL": "sol/usd",
    "XRP": "xrp/usd",
    "DOGE": "doge/usd",
}

# ── Binance REST fallback (for assets not on Polymarket RTDS Chainlink) ────
# DOGE is not available via Polymarket's Chainlink WebSocket feed.
# Poll Binance REST API as a fallback price source.
BINANCE_REST_URL = "https://api.binance.com/api/v3/ticker/price"
BINANCE_FALLBACK_SYMBOLS = {
    "DOGE": "DOGEUSDT",
}
BINANCE_POLL_INTERVAL = 10  # seconds between REST polls

# ── Rate limiting ───────────────────────────────────────────────────────────
API_READS_PER_SECOND = 15         # Below ~20 tier limit; headroom for orderbook bursts
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
    max_positions_per_event: int      # Max positions per event (correlated risk limit)
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
        min_edge_pct=25.0,
        max_edge_pct=60.0,
        max_price=0.75,
        min_price=0.02,
        min_seconds_to_expiry=50,
        max_seconds_to_expiry=60,
        max_positions_per_category=2,
        max_positions_per_event=2,
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
        min_edge_pct=25.0,
        max_edge_pct=60.0,
        max_price=0.75,
        min_price=0.02,
        min_seconds_to_expiry=50,
        max_seconds_to_expiry=60,
        max_positions_per_category=2,
        max_positions_per_event=2,
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
        min_edge_pct=25.0,
        max_edge_pct=60.0,
        max_price=0.75,
        min_price=0.02,
        min_seconds_to_expiry=50,
        max_seconds_to_expiry=60,
        max_positions_per_category=2,
        max_positions_per_event=2,
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
        min_edge_pct=25.0,
        max_edge_pct=60.0,
        max_price=0.75,
        min_price=0.02,
        min_seconds_to_expiry=50,
        max_seconds_to_expiry=60,
        max_positions_per_category=1,
        max_positions_per_event=1,
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
        min_edge_pct=25.0,
        max_edge_pct=60.0,
        max_price=0.75,
        min_price=0.02,
        min_seconds_to_expiry=50,
        max_seconds_to_expiry=60,
        max_positions_per_category=1,
        max_positions_per_event=1,
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
        min_edge_pct=25.0,
        max_edge_pct=60.0,
        max_price=0.75,
        min_price=0.02,
        min_seconds_to_expiry=50,
        max_seconds_to_expiry=60,
        max_positions_per_category=1,
        max_positions_per_event=1,
        contracts_per_trade=1,
        loss_threshold_dollars=-25.0,
        min_trades_for_breaker=20,
        min_win_rate=0.30,
    ),
    # ── XRP markets ────────────────────────────────────────────────────────
    "crypto_15min_xrp": CategoryConfig(
        name="Crypto 15min XRP",
        enabled=True,
        series_tickers=["KXXRPMINY"],
        timeframe="15min",
        fair_value_model="vol_bs",
        min_edge_pct=25.0,
        max_edge_pct=60.0,
        max_price=0.75,
        min_price=0.02,
        min_seconds_to_expiry=50,
        max_seconds_to_expiry=60,
        max_positions_per_category=1,
        max_positions_per_event=1,
        contracts_per_trade=1,
        loss_threshold_dollars=-25.0,
        min_trades_for_breaker=20,
        min_win_rate=0.30,
    ),
    "crypto_hourly_xrp": CategoryConfig(
        name="Crypto Hourly XRP",
        enabled=True,
        series_tickers=["KXXRP"],
        timeframe="hourly",
        fair_value_model="vol_bs_range",
        min_edge_pct=25.0,
        max_edge_pct=60.0,
        max_price=0.75,
        min_price=0.02,
        min_seconds_to_expiry=50,
        max_seconds_to_expiry=60,
        max_positions_per_category=2,
        max_positions_per_event=2,
        contracts_per_trade=1,
        loss_threshold_dollars=-25.0,
        min_trades_for_breaker=20,
        min_win_rate=0.30,
    ),
    # ── DOGE hourly range markets ──────────────────────────────────────────
    "crypto_hourly_doge": CategoryConfig(
        name="Crypto Hourly DOGE",
        enabled=True,
        series_tickers=["KXDOGE"],
        timeframe="hourly",
        fair_value_model="vol_bs_range",
        min_edge_pct=25.0,
        max_edge_pct=60.0,
        max_price=0.75,
        min_price=0.02,
        min_seconds_to_expiry=50,
        max_seconds_to_expiry=60,
        max_positions_per_category=2,
        max_positions_per_event=2,
        contracts_per_trade=1,
        loss_threshold_dollars=-25.0,
        min_trades_for_breaker=20,
        min_win_rate=0.30,
    ),
}


def _apply_dashboard_overrides():
    """
    Load config overrides from the dashboard JSON file and apply to CATEGORY_CONFIGS.
    The dashboard writes per-category min_edge_pct and global profit_margin values.
    Called at the start of each scan cycle so changes take effect within ~15s.
    """
    global CALIBRATION_PROFIT_MARGIN
    if not os.path.exists(OVERRIDES_PATH):
        return
    try:
        with open(OVERRIDES_PATH, "r") as f:
            overrides = json.load(f)
        for cat_key, vals in overrides.items():
            if cat_key == "_global":
                if "profit_margin" in vals:
                    CALIBRATION_PROFIT_MARGIN = max(0.0, min(30.0, float(vals["profit_margin"])))
                continue
            if cat_key in CATEGORY_CONFIGS and "min_edge_pct" in vals:
                new_val = max(5.0, min(50.0, float(vals["min_edge_pct"])))
                CATEGORY_CONFIGS[cat_key].min_edge_pct = new_val
    except (json.JSONDecodeError, IOError, TypeError):
        pass


# =============================================================================
# LOGGING SETUP
# =============================================================================

LOG_DIR = os.path.join(os.path.expanduser("~"), ".edge_scanner")
os.makedirs(LOG_DIR, exist_ok=True)

DB_PATH = os.path.join(LOG_DIR, "trades.db")
OVERRIDES_PATH = os.path.join(LOG_DIR, "dashboard_overrides.json")

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
_fh = RotatingFileHandler(
    os.path.join(LOG_DIR, "edge_scanner.log"),
    maxBytes=10_000_000,   # 10 MB per file
    backupCount=3,          # Keep 3 old files (~40 MB total cap)
    encoding="utf-8",
)
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
            close_time TEXT,
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
        CREATE TABLE IF NOT EXISTS shadow_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            category TEXT NOT NULL,
            asset TEXT NOT NULL,
            ticker TEXT NOT NULL,
            event_ticker TEXT,
            direction TEXT NOT NULL,
            strike REAL NOT NULL,
            spot REAL NOT NULL,
            seconds_left REAL NOT NULL,
            close_time TEXT NOT NULL,
            model_vol REAL NOT NULL,
            model_fair REAL NOT NULL,
            ask_price REAL NOT NULL,
            gross_edge_pct REAL NOT NULL,
            net_edge_pct REAL NOT NULL,
            fee REAL NOT NULL,
            implied_vol REAL,
            -- Settlement (filled in later)
            settled INTEGER DEFAULT 0,
            settlement_result TEXT,
            won INTEGER,
            hypothetical_pnl REAL,
            settled_at TEXT
        )
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_shadow_settled ON shadow_trades(settled)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_shadow_ticker ON shadow_trades(ticker)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_shadow_close_time ON shadow_trades(close_time)
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

    # Migration: add close_time column to existing trades table
    try:
        conn.execute("ALTER TABLE trades ADD COLUMN close_time TEXT")
    except Exception:
        pass  # Column already exists

    # Migration: add side column (yes/no) to trades and shadow_trades
    try:
        conn.execute("ALTER TABLE trades ADD COLUMN side TEXT")
    except Exception:
        pass  # Column already exists
    try:
        conn.execute("ALTER TABLE shadow_trades ADD COLUMN side TEXT")
    except Exception:
        pass  # Column already exists

    # Migration: add event_ticker column to trades (for position limit tracking across restarts)
    try:
        conn.execute("ALTER TABLE trades ADD COLUMN event_ticker TEXT DEFAULT ''")
    except Exception:
        pass  # Column already exists

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_trades_unsettled ON trades(settled, close_time)
    """)

    # Calibration history snapshots (one row per ~10 shadow settlements)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS calibration_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            brier REAL NOT NULL,
            overconfidence REAL NOT NULL,
            predicted_wr REAL NOT NULL,
            actual_wr REAL NOT NULL,
            n INTEGER NOT NULL,
            asset_bias TEXT,
            lookback_days INTEGER
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_cal_snap_ts ON calibration_snapshots(timestamp)
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

    @staticmethod
    def range_fair_value(
        S: float, K_floor: float, K_cap: float, seconds_left: float, sigma: float
    ) -> float:
        """
        Price a range binary option: pays $1 if K_floor <= S < K_cap at expiry.
        Fair value = P(S > K_floor) - P(S > K_cap).
        """
        p_above_floor = BinaryPricer.fair_value(S, K_floor, seconds_left, sigma, "up")
        p_above_cap = BinaryPricer.fair_value(S, K_cap, seconds_left, sigma, "up")
        return max(0.0, p_above_floor - p_above_cap)


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


class BinanceFallbackFeed:
    """
    REST-based price poller for assets not available on Polymarket's Chainlink
    WebSocket feed (e.g. DOGE). Polls Binance public API every N seconds and
    feeds prices into the same callback pipeline as ChainlinkFeed.
    """

    def __init__(self):
        self._running = False
        self._callbacks: list = []
        self._last_prices: dict[str, float] = {}
        self._last_update: dict[str, float] = {}
        self._update_count = 0

    def on_price(self, callback):
        self._callbacks.append(callback)

    def start(self):
        if not BINANCE_FALLBACK_SYMBOLS:
            return
        if self._running:
            return
        self._running = True
        t = threading.Thread(target=self._run, daemon=True)
        t.start()

    def stop(self):
        self._running = False

    def _run(self):
        import urllib.request
        logger.info(
            f"Binance fallback feed starting for: "
            f"{', '.join(BINANCE_FALLBACK_SYMBOLS.keys())}"
        )
        while self._running:
            for asset, symbol in BINANCE_FALLBACK_SYMBOLS.items():
                try:
                    url = f"{BINANCE_REST_URL}?symbol={symbol}"
                    req = urllib.request.Request(url, headers={"User-Agent": "edge-scanner/1.0"})
                    with urllib.request.urlopen(req, timeout=5) as resp:
                        data = json.loads(resp.read().decode())
                    price = float(data.get("price", 0))
                    if price > 0:
                        self._last_prices[asset] = price
                        self._last_update[asset] = time.time()
                        self._update_count += 1
                        for cb in self._callbacks:
                            try:
                                cb(asset, price)
                            except Exception:
                                pass
                except Exception as e:
                    logger.debug(f"Binance fallback poll error for {asset}: {e}")
            time.sleep(BINANCE_POLL_INTERVAL)

    def get_price(self, asset: str) -> Optional[float]:
        return self._last_prices.get(asset.upper())

    def is_stale(self, asset: str, max_age: float = 30) -> bool:
        ts = self._last_update.get(asset.upper())
        if not ts:
            return True
        return (time.time() - ts) > max_age

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

        # Dynamic floor: adapts to current vol regime instead of fixed minimums
        abs_floor = MIN_VOL_ABSOLUTE.get(asset, 0.15)
        slow_vol = self.get_ewma_vol(asset, "slow")
        if slow_vol and slow_vol > 0:
            dynamic_floor = max(abs_floor, slow_vol * VOL_FLOOR_EWMA_MULT)
        else:
            dynamic_floor = abs_floor
        raw_vol = max(raw_vol, dynamic_floor)

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
        return self.tick_count.get(asset, 0) >= VOL_WARMUP_MIN_TICKS

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
    direction: str          # "up" (YES = above strike), "down" (NO = below strike), or "range"
    side: str               # "yes" or "no" — the side we're buying
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
                logger.warning(f"Scan error for {series}: {e}")

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

    @staticmethod
    def _reject(diag, reason, ticker):
        """Record a parse rejection in diagnostics."""
        if diag is not None:
            diag[reason] = diag.get(reason, 0) + 1
            samples = diag.setdefault("samples", {})
            sl = samples.setdefault(reason, [])
            if len(sl) < 3:
                sl.append(ticker)
        logger.debug(f"PARSE REJECT: {ticker} -> {reason}")
        return None

    def parse_market(
        self, raw: dict, now: datetime, diag: dict = None
    ) -> Optional[dict]:
        """
        Parse a raw Kalshi market dict into structured data.
        Returns dict with ticker, asset, strike, direction, close_time, seconds_left
        or None if unparseable. If diag dict is provided, records rejection reasons.
        """
        ticker = raw.get("ticker", "")
        status = raw.get("status", "")
        if status not in ("open", "active"):
            return self._reject(diag, "status_filtered", f"{ticker}[status={status}]")

        close_str = raw.get("close_time") or raw.get("expiration_time")
        if not close_str:
            return self._reject(diag, "no_close_time", ticker)

        try:
            close_time = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return self._reject(diag, "bad_datetime", ticker)

        seconds_left = (close_time - now).total_seconds()
        if seconds_left <= 0:
            return self._reject(diag, "expired", ticker)

        # Extract asset first (needed for range detection)
        series = raw.get("_series", "")
        asset = _extract_asset_from_series(series)
        if not asset:
            # Try from ticker directly
            for a in CHAINLINK_SYMBOLS:
                if a in ticker.upper():
                    asset = a
                    break
        if not asset:
            return self._reject(diag, "no_asset", ticker)

        # Check for range market (has both floor_strike and cap_strike)
        floor_strike = raw.get("floor_strike")
        cap_strike = raw.get("cap_strike")
        if floor_strike is not None and cap_strike is not None:
            try:
                floor_val = float(str(floor_strike).replace(',', '').replace('$', ''))
                cap_val = float(str(cap_strike).replace(',', '').replace('$', ''))
                if floor_val > 0 and cap_val > floor_val:
                    return {
                        "ticker": ticker,
                        "event_ticker": raw.get("event_ticker", ""),
                        "category": raw.get("_category", ""),
                        "asset": asset,
                        "direction": "range",
                        "strike": floor_val,
                        "cap_strike": cap_val,
                        "close_time": close_time,
                        "seconds_left": seconds_left,
                    }
            except (ValueError, TypeError):
                pass

        # Extract strike and direction from ticker
        parsed = _extract_strike_from_ticker(ticker)
        if parsed:
            strike, direction = parsed
        else:
            # Fallback: extract strike from subtitle/metadata (15-min markets)
            strike = _extract_strike_from_market_data(raw)
            if not strike:
                return self._reject(diag, "no_strike", ticker)
            direction = None  # Signal: both sides should be evaluated

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

    def has_yes_in_event(self, event_ticker: str) -> bool:
        """Check if we already hold a YES position in this event."""
        with self._lock:
            return any(
                p.event_ticker == event_ticker and p.side == "yes"
                for p in self._positions.values()
            )


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
# BANKROLL MANAGER — Anti-martingale with trailing stop + profit lock
# =============================================================================

class BankrollManager:
    """
    Dynamic bankroll management with trailing stop and profit-based upscaling.
    Resets daily at midnight UTC.

    Stop logic (anti-martingale trailing stop):
      stop_level = max(-STOP_LOSS, HWM * PROFIT_LOCK_PCT - STOP_LOSS)

      Examples (stop=$20, lock=60%):
        HWM=$0  → stop=-$20   (base stop)
        HWM=$10 → stop=-$14   (trailing tightens)
        HWM=$33 → stop=~$0    (breakeven secured)
        HWM=$50 → stop=$10    (profit locked)

    Sizing: tiered contracts based on current daily P&L, with drawdown
    step-down and balance guard.
    """

    def __init__(self, db: sqlite3.Connection):
        self.db = db
        self._balance: float = 0.0
        self._stopped: bool = False
        self._current_date: str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Initialize from today's settled P&L
        self._daily_pnl = self._query_daily_pnl()
        self._hwm = max(0.0, self._daily_pnl)

        logger.info(
            f"BankrollManager init: P&L=${self._daily_pnl:+.2f} HWM=${self._hwm:.2f} "
            f"stop=${self.get_stop_level():+.2f}"
        )

    def _query_daily_pnl(self) -> float:
        """Query today's settled P&L from the trades table."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        row = self.db.execute(
            "SELECT COALESCE(SUM(pnl), 0) FROM trades WHERE settled=1 AND settled_at LIKE ?",
            (f"{today}%",),
        ).fetchone()
        return row[0] if row else 0.0

    def update_pnl(self):
        """Refresh daily P&L from DB. Handles midnight reset."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self._current_date:
            # New day — reset everything
            logger.info(
                f"BankrollManager daily reset: {self._current_date} → {today} "
                f"(prev HWM=${self._hwm:.2f}, P&L=${self._daily_pnl:+.2f})"
            )
            self._current_date = today
            self._hwm = 0.0
            self._stopped = False

        self._daily_pnl = self._query_daily_pnl()
        self._hwm = max(self._hwm, self._daily_pnl)

    def update_balance(self, balance: float):
        """Store latest Kalshi account balance (in dollars)."""
        self._balance = balance

    def get_stop_level(self) -> float:
        """Compute dynamic stop level based on HWM and profit lock."""
        secured = self._hwm * BANKROLL_PROFIT_LOCK_PCT
        return max(-BANKROLL_STOP_LOSS, secured - BANKROLL_STOP_LOSS)

    def should_stop(self) -> tuple:
        """
        Check if trading should halt.
        Returns (stopped: bool, reason: str).
        Once triggered, latches for the rest of the day.
        """
        if self._stopped:
            return (True, f"Stopped earlier today (P&L=${self._daily_pnl:+.2f})")

        stop_level = self.get_stop_level()
        if self._daily_pnl < stop_level:
            self._stopped = True
            reason = (
                f"P&L ${self._daily_pnl:+.2f} hit stop ${stop_level:+.2f} "
                f"(HWM=${self._hwm:.2f}, lock={BANKROLL_PROFIT_LOCK_PCT:.0%})"
            )
            logger.warning(f"BANKROLL STOP: {reason}")
            self.write_state_file()
            return (True, reason)

        return (False, "")

    def get_tier(self) -> int:
        """Return current tier index (0-based) from BANKROLL_SCALE_TIERS."""
        tier = 0
        for i, (threshold, _contracts) in enumerate(BANKROLL_SCALE_TIERS):
            if self._daily_pnl >= threshold:
                tier = i
        return tier

    def get_contracts(self, ask_price: float) -> int:
        """
        Determine contract count for a trade.
        Uses tiered scaling with drawdown guard and balance guard.
        """
        if ask_price <= 0:
            return 1

        # Find tier based on current daily P&L
        tier = self.get_tier()
        contracts = BANKROLL_SCALE_TIERS[tier][1]

        # Drawdown guard: if P&L has dropped $5+ from HWM, step down 1 tier
        drawdown = self._hwm - self._daily_pnl
        if drawdown >= BANKROLL_DRAWDOWN_STEP_DOWN and tier > 0:
            tier -= 1
            contracts = BANKROLL_SCALE_TIERS[tier][1]
            logger.debug(
                f"Bankroll drawdown guard: HWM=${self._hwm:.2f} "
                f"P&L=${self._daily_pnl:+.2f} (dd=${drawdown:.2f}) → tier {tier} ({contracts}x)"
            )

        # Hard cap
        contracts = min(contracts, BANKROLL_MAX_CONTRACTS)

        # Balance guard: don't risk more than 10% of balance on one trade
        if self._balance > 0:
            max_by_balance = int(self._balance * BANKROLL_MAX_RISK_PCT / ask_price)
            if max_by_balance < contracts:
                logger.debug(
                    f"Bankroll balance guard: ${self._balance:.2f} × {BANKROLL_MAX_RISK_PCT:.0%} "
                    f"/ ${ask_price:.2f} = {max_by_balance} contracts (wanted {contracts})"
                )
                contracts = max_by_balance

        return max(contracts, 0)

    def get_state(self) -> dict:
        """Return snapshot of bankroll state for dashboard/logging."""
        tier = self.get_tier()
        return {
            "daily_pnl": round(self._daily_pnl, 2),
            "hwm": round(self._hwm, 2),
            "stop_level": round(self.get_stop_level(), 2),
            "tier": tier,
            "contracts": BANKROLL_SCALE_TIERS[tier][1],
            "balance": round(self._balance, 2),
            "stopped": self._stopped,
            "date": self._current_date,
        }

    def write_state_file(self):
        """Write bankroll state to JSON for the web dashboard to read."""
        try:
            state_path = os.path.join(LOG_DIR, "bankroll_state.json")
            with open(state_path, "w") as f:
                json.dump(self.get_state(), f, indent=2)
        except Exception:
            pass  # Best-effort; don't crash on file write failure


# =============================================================================
# CALIBRATION TRACKER — Measures model accuracy from shadow trades
# =============================================================================

CALIBRATION_MIN_SAMPLES = 20  # Don't report metrics until this many shadow settlements

# ── Adaptive edge thresholds (dynamic per-price-level edge requirements) ──
ADAPTIVE_EDGE_PRICE_BANDWIDTH = 0.03    # Gaussian kernel bandwidth in dollars ($0.03)
ADAPTIVE_EDGE_MIN_WEIGHT = 3.0          # Minimum effective sample weight to produce adjustment
ADAPTIVE_EDGE_FLOOR_PCT = 15.0          # Hard floor: never require less edge than this
ADAPTIVE_EDGE_MIN_SAMPLES = 30          # Don't activate until this many settled shadow trades

# Per-timeframe recency half-lives (hours): faster markets forget faster
ADAPTIVE_EDGE_HALF_LIFE_BY_TIMEFRAME = {
    "15min": 12.0,    # 15-min markets cycle 4x faster → 4x shorter memory
    "hourly": 48.0,   # Hourly markets: standard 2-day half-life
}
ADAPTIVE_EDGE_HALF_LIFE_DEFAULT = 48.0  # Fallback for unknown timeframes

# seconds_left kernel (normalized to [0,1] fraction of trading window)
ADAPTIVE_EDGE_TIME_LEFT_BANDWIDTH = 0.15  # Gaussian bandwidth in normalized units
ADAPTIVE_EDGE_TIME_BUCKETS = 5            # Cache discretization buckets
ADAPTIVE_EDGE_WINDOW_BOUNDS = {           # (min_sec, max_sec) per timeframe
    "15min": (90, 840),
    "hourly": (180, 3300),
}

# Calibration-driven edge: per-FV-bucket breakeven edge from actual win rates
# Same buckets as dashboard calibration chart
CALIBRATION_BUCKETS = [
    (0.05, 0.20), (0.20, 0.30), (0.30, 0.40), (0.40, 0.50),
    (0.50, 0.60), (0.60, 0.70), (0.70, 0.80), (0.80, 0.95),
]
CALIBRATION_BUCKET_MIN_N = 10    # Need ≥10 settled samples to trust a bucket
CALIBRATION_PROFIT_MARGIN = 10.0 # % margin on top of breakeven edge (fees + variance + profit)

class CalibrationTracker:
    """
    Measures how well the model's fair values predict actual outcomes.
    Uses settled shadow trades (which log model_fair + settlement_result
    for every priced market, not just ones we trade).

    Key metrics:
        Brier Score: mean((predicted - outcome)²). Perfect=0, coin flip=0.25.
        Overconfidence: mean(predicted) / actual_win_rate. >1 = model overconfident.
        Per-asset bias: predicted vs actual win rate per asset.
    """

    def __init__(self, db: sqlite3.Connection):
        self.db = db
        self._state: dict = {}
        self._settlements_since_snapshot: int = 0

    def compute(self, lookback_days: int = 7) -> dict:
        """Compute calibration metrics from recent shadow trades."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).isoformat()

        rows = self.db.execute(
            """
            SELECT model_fair, direction, settlement_result, won, asset
            FROM shadow_trades
            WHERE settled=1 AND settlement_result IN ('yes', 'no')
            AND settled_at > ?
            """,
            (cutoff,),
        ).fetchall()

        n = len(rows)
        if n < CALIBRATION_MIN_SAMPLES:
            self._state = {"n": n, "sufficient": False}
            return self._state

        # For each shadow trade, model_fair is the probability the contract pays out
        # in the direction we would buy. won=1 means it did pay out.
        brier_sum = 0.0
        pred_sum = 0.0
        wins = 0
        asset_data: dict = {}

        for model_fair, direction, settlement_result, won, asset in rows:
            outcome = 1.0 if won else 0.0
            brier_sum += (model_fair - outcome) ** 2
            pred_sum += model_fair
            wins += won or 0

            if asset not in asset_data:
                asset_data[asset] = {"pred_sum": 0.0, "wins": 0, "n": 0}
            asset_data[asset]["pred_sum"] += model_fair
            asset_data[asset]["wins"] += won or 0
            asset_data[asset]["n"] += 1

        brier = brier_sum / n
        actual_wr = wins / n if n > 0 else 0
        predicted_wr = pred_sum / n if n > 0 else 0
        overconfidence = predicted_wr / actual_wr if actual_wr > 0 else 0.0

        # Per-asset bias: (predicted_wr - actual_wr) as percentage points
        asset_bias = {}
        for asset, d in asset_data.items():
            a_n = d["n"]
            if a_n >= 5:
                a_pred = d["pred_sum"] / a_n
                a_actual = d["wins"] / a_n
                asset_bias[asset] = round((a_pred - a_actual) * 100, 1)

        self._state = {
            "n": n,
            "sufficient": True,
            "brier": round(brier, 4),
            "predicted_wr": round(predicted_wr * 100, 1),
            "actual_wr": round(actual_wr * 100, 1),
            "overconfidence": round(overconfidence, 3),
            "asset_bias": asset_bias,
            "lookback_days": lookback_days,
        }
        return self._state

    def should_warn(self) -> Optional[str]:
        """Return warning string if calibration is badly off."""
        if not self._state.get("sufficient"):
            return None
        brier = self._state.get("brier", 0)
        oc = self._state.get("overconfidence", 1.0)
        if brier > 0.30:
            return f"Brier score {brier:.3f} is high (>0.30) — model poorly calibrated"
        if oc > 1.3:
            return f"Overconfidence {oc:.2f}x — model predicting {self._state['predicted_wr']:.0f}% but actual {self._state['actual_wr']:.0f}%"
        return None

    def compute_bucket_calibration(self, lookback_days: int = 7):
        """Compute per-FV-bucket actual win rates from settled shadow trades.

        Populates _bucket_wr: dict mapping (lo, hi) → actual_wr for buckets
        with sufficient samples (n >= CALIBRATION_BUCKET_MIN_N).
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).isoformat()
        rows = self.db.execute(
            """
            SELECT model_fair, won
            FROM shadow_trades
            WHERE settled=1 AND settlement_result IN ('yes', 'no')
            AND settled_at > ?
            """,
            (cutoff,),
        ).fetchall()

        self._bucket_wr = {}
        for lo, hi in CALIBRATION_BUCKETS:
            bucket_rows = [(mf, w) for mf, w in rows if lo <= mf < hi]
            n = len(bucket_rows)
            if n >= CALIBRATION_BUCKET_MIN_N:
                wins = sum(1 for _, w in bucket_rows if w)
                self._bucket_wr[(lo, hi)] = wins / n

    def get_calibrated_min_edge(self, model_fair: float) -> Optional[float]:
        """Data-driven min edge from actual win rates per FV bucket.

        Returns breakeven edge + profit margin, or None if insufficient
        calibration data for this bucket (caller uses fallback).

        Formula: breakeven = (1 - actual_wr / model_fair) × 100
        A 40-50% bucket with 30% actual WR at model_fair=0.45:
          breakeven = (1 - 0.30/0.45) × 100 = 33.3%, + 10% margin = 43.3%
        """
        if not hasattr(self, '_bucket_wr') or not self._bucket_wr:
            return None

        # Find the bucket for this model_fair
        for lo, hi in CALIBRATION_BUCKETS:
            if lo <= model_fair < hi:
                actual_wr = self._bucket_wr.get((lo, hi))
                if actual_wr is None:
                    return None  # Insufficient data for this bucket
                breakeven = max(0.0, (1.0 - actual_wr / model_fair) * 100.0)
                return breakeven + CALIBRATION_PROFIT_MARGIN

        return None  # model_fair outside all buckets

    def get_state(self) -> dict:
        return dict(self._state)

    def write_state_file(self):
        """Write calibration state to JSON for the web dashboard."""
        try:
            state_path = os.path.join(LOG_DIR, "calibration_state.json")
            with open(state_path, "w") as f:
                json.dump(self._state, f, indent=2)
        except Exception:
            pass

    def seed_initial_snapshot(self, db: sqlite3.Connection):
        """Insert an initial snapshot if the table is empty and calibration data exists."""
        self.compute()
        self.compute_bucket_calibration()
        if not self._state.get("sufficient"):
            return
        try:
            count = db.execute("SELECT COUNT(*) FROM calibration_snapshots").fetchone()[0]
            if count > 0:
                return
            self._settlements_since_snapshot = 10  # force past threshold
            self.record_snapshot(db, 0)
        except Exception as e:
            logger.debug(f"Calibration seed snapshot error: {e}")

    def record_snapshot(self, db: sqlite3.Connection, settled_count: int):
        """Record a calibration snapshot every 10 shadow settlements for history chart."""
        self._settlements_since_snapshot += settled_count
        if self._settlements_since_snapshot < 10:
            return
        self._settlements_since_snapshot = 0

        if not self._state.get("sufficient"):
            return

        try:
            db.execute(
                """
                INSERT INTO calibration_snapshots
                (timestamp, brier, overconfidence, predicted_wr, actual_wr, n, asset_bias, lookback_days)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    datetime.now(timezone.utc).isoformat(),
                    self._state["brier"],
                    self._state["overconfidence"],
                    self._state["predicted_wr"],
                    self._state["actual_wr"],
                    self._state["n"],
                    json.dumps(self._state.get("asset_bias", {})),
                    self._state.get("lookback_days", 7),
                ),
            )
            db.commit()

            # Prune old snapshots (keep last 14 days)
            cutoff = (datetime.now(timezone.utc) - timedelta(days=14)).isoformat()
            db.execute("DELETE FROM calibration_snapshots WHERE timestamp < ?", (cutoff,))
            db.commit()
        except Exception as e:
            logger.debug(f"Calibration snapshot error: {e}")


# =============================================================================
# ADAPTIVE EDGE MANAGER — Per-price-level dynamic edge thresholds
# =============================================================================

class AdaptiveEdgeManager:
    """
    Computes per-price-level required edge thresholds using kernel-weighted
    shadow trade outcomes.

    Uses Gaussian kernels in both time (recency) and price space so that:
      - Each penny ($0.01) gets its own threshold (smooth interpolation)
      - Recent trades matter more than old ones (exponential decay)
      - Sparse price points borrow strength from neighbors

    The adjustment is a calibration ratio:
      ratio = predicted_wr / actual_wr  at that price level
    Applied as: adjusted_edge = base_min_edge * ratio
      - ratio > 1  → model overconfident at this price → require MORE edge
      - ratio < 1  → model underconfident → require LESS edge (within bounds)
    """

    def __init__(self, db: sqlite3.Connection, calibration: 'CalibrationTracker' = None):
        self.db = db
        self._calibration = calibration
        self._cache: dict = {}       # (asset, price_cents, time_bucket) -> calibration_ratio
        self._active: bool = False
        self._last_refresh: float = 0
        self._stats: dict = {}

    @staticmethod
    def _normalize_seconds_left(seconds_left: float, timeframe: str) -> float:
        """Normalize seconds_left to [0, 1] fraction of the trading window."""
        bounds = ADAPTIVE_EDGE_WINDOW_BOUNDS.get(timeframe, (180, 3300))
        window_range = bounds[1] - bounds[0]
        if window_range <= 0:
            return 0.5
        return max(0.0, min(1.0, (seconds_left - bounds[0]) / window_range))

    @staticmethod
    def _seconds_left_to_bucket(frac: float) -> int:
        """Convert normalized fraction to cache bucket index."""
        return min(ADAPTIVE_EDGE_TIME_BUCKETS - 1, int(frac * ADAPTIVE_EDGE_TIME_BUCKETS))

    def refresh(self):
        """Recompute the adaptive edge surface from shadow trade history.

        Three-dimensional kernel: recency (per-timeframe half-life) ×
        price (Gaussian) × time-left (Gaussian on normalized window fraction).
        """
        rows = self.db.execute(
            """
            SELECT asset, ask_price, model_fair, won, settled_at, category, seconds_left
            FROM shadow_trades
            WHERE settled=1 AND settlement_result IN ('yes', 'no')
            """
        ).fetchall()

        n = len(rows)
        if n < ADAPTIVE_EDGE_MIN_SAMPLES:
            self._active = False
            self._stats = {"n": n, "active": False}
            return

        now = datetime.now(timezone.utc)
        ln2 = math.log(2)

        # Build category -> timeframe lookup
        cat_to_tf: dict = {}
        for cat_key, cfg in CATEGORY_CONFIGS.items():
            cat_to_tf[cat_key] = cfg.timeframe

        # Parse rows with per-timeframe recency weight and normalized time fraction
        trades = []
        for asset, ask_price, model_fair, won, settled_at, category, secs_left in rows:
            try:
                settled_dt = datetime.fromisoformat(settled_at)
                if settled_dt.tzinfo is None:
                    settled_dt = settled_dt.replace(tzinfo=timezone.utc)
                age_hours = max(0.0, (now - settled_dt).total_seconds() / 3600)
            except Exception:
                age_hours = 168.0

            timeframe = cat_to_tf.get(category, "hourly")
            half_life = ADAPTIVE_EDGE_HALF_LIFE_BY_TIMEFRAME.get(
                timeframe, ADAPTIVE_EDGE_HALF_LIFE_DEFAULT
            )
            recency_w = math.exp(-age_hours / half_life * ln2)
            frac = self._normalize_seconds_left(secs_left, timeframe)
            trades.append((asset, ask_price, model_fair, won or 0, recency_w, frac))

        # Group by asset
        assets: dict = {}
        for asset, price, fair, won, rw, frac in trades:
            if asset not in assets:
                assets[asset] = []
            assets[asset].append((price, fair, won, rw, frac))

        price_bw_sq_2 = 2.0 * ADAPTIVE_EDGE_PRICE_BANDWIDTH ** 2
        tl_bw_sq_2 = 2.0 * ADAPTIVE_EDGE_TIME_LEFT_BANDWIDTH ** 2
        new_cache: dict = {}
        adjustments_log: dict = {}

        for asset, asset_trades in assets.items():
            adjustments_log[asset] = []

            for price_cents in range(2, 76):
                target_price = price_cents / 100.0

                for time_bucket in range(ADAPTIVE_EDGE_TIME_BUCKETS):
                    target_frac = (time_bucket + 0.5) / ADAPTIVE_EDGE_TIME_BUCKETS

                    weighted_wins = 0.0
                    weighted_total = 0.0
                    weighted_pred_sum = 0.0

                    for price, fair, won, rw, frac in asset_trades:
                        price_w = math.exp(-((price - target_price) ** 2) / price_bw_sq_2)
                        tl_w = math.exp(-((frac - target_frac) ** 2) / tl_bw_sq_2)
                        combined_w = rw * price_w * tl_w

                        weighted_wins += won * combined_w
                        weighted_total += combined_w
                        weighted_pred_sum += fair * combined_w

                    if weighted_total < ADAPTIVE_EDGE_MIN_WEIGHT:
                        continue

                    actual_wr = weighted_wins / weighted_total
                    predicted_wr = weighted_pred_sum / weighted_total

                    if actual_wr > 0.05:
                        ratio = predicted_wr / actual_wr
                    elif actual_wr > 0.01:
                        ratio = min(2.0, predicted_wr / actual_wr)
                    else:
                        ratio = 2.0

                    ratio = max(0.6, min(2.0, ratio))
                    new_cache[(asset, price_cents, time_bucket)] = ratio
                    adjustments_log[asset].append((price_cents, time_bucket, ratio))

        self._cache = new_cache
        self._active = True
        self._last_refresh = time.time()

        # Build summary stats for dashboard/logging
        asset_summaries = {}
        for asset, adjustments in adjustments_log.items():
            if not adjustments:
                continue
            ratios = [r for _, _, r in adjustments]
            asset_summaries[asset] = {
                "price_points": len(adjustments),
                "avg_ratio": round(sum(ratios) / len(ratios), 3),
                "min_ratio": round(min(ratios), 3),
                "max_ratio": round(max(ratios), 3),
            }
        self._stats = {
            "n": n,
            "active": True,
            "assets": asset_summaries,
            "total_price_points": len(new_cache),
        }

    def get_required_edge(
        self, asset: str, ask_price: float, base_min_edge: float,
        category: str = "", seconds_left: float = 0.0,
        model_fair: float = 0.5,
    ) -> float:
        """
        Data-driven edge threshold from calibration win rates.

        Priority:
          1. Calibrated breakeven edge (from actual WR per FV bucket) + profit margin
          2. Adaptive kernel ratio (per asset × price × time fine-tuning)
          3. Falls back to base_min_edge when calibration data insufficient

        No ceiling — if calibration says 80% edge needed, require 80%.
        The max_edge_pct filter handles blocking impossibly-demanding buckets.
        """
        # Step 1: calibration-based minimum edge from actual win rates
        effective_base = base_min_edge
        if self._calibration:
            cal_edge = self._calibration.get_calibrated_min_edge(model_fair)
            if cal_edge is not None:
                effective_base = cal_edge

        # Step 2: adaptive kernel ratio (asset/price/time learned adjustment)
        if self._active:
            price_cents = round(ask_price * 100)

            time_bucket = ADAPTIVE_EDGE_TIME_BUCKETS // 2
            if category:
                cfg = CATEGORY_CONFIGS.get(category)
                tf_key = cfg.timeframe if cfg else "hourly"
                frac = self._normalize_seconds_left(seconds_left, tf_key)
                time_bucket = self._seconds_left_to_bucket(frac)

            ratio = self._cache.get((asset, price_cents, time_bucket))
            if ratio is not None:
                effective_base *= ratio

        return max(ADAPTIVE_EDGE_FLOOR_PCT, effective_base)

    def get_state(self) -> dict:
        return dict(self._stats)

    def write_state_file(self):
        """Write adaptive edge state to JSON for the web dashboard."""
        try:
            state_path = os.path.join(LOG_DIR, "adaptive_edge_state.json")
            # Include thresholds grouped by asset, keyed as "$0.XX_tN"
            sample_thresholds = {}
            for (asset, pc, tb), ratio in sorted(self._cache.items()):
                if asset not in sample_thresholds:
                    sample_thresholds[asset] = {}
                sample_thresholds[asset][f"${pc/100:.2f}_t{tb}"] = round(ratio, 3)

            state = {**self._stats, "thresholds": sample_thresholds}
            with open(state_path, "w") as f:
                json.dump(state, f, indent=2)
        except Exception:
            pass


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
        self.binance_feed = BinanceFallbackFeed()
        self.vol_tracker = VolTracker()
        self.pricer = BinaryPricer()
        self.scanner = MarketScanner(api)
        self.positions = PositionTracker()
        self.cat_stats = CategoryStatsTracker(self.db)
        self.bankroll = BankrollManager(self.db)
        self.calibration = CalibrationTracker(self.db)
        self.calibration.seed_initial_snapshot(self.db)
        self.adaptive_edge = AdaptiveEdgeManager(self.db, calibration=self.calibration)

        # Connect price feeds to vol tracker
        self.chainlink.on_price(self.vol_tracker.feed_price)
        self.binance_feed.on_price(self.vol_tracker.feed_price)

        # Trade counting for hourly rate limit
        self._hourly_trades: deque = deque()

        # Shutdown flag
        self._running = False

        # Track disabled categories (from circuit breakers)
        self._disabled_categories: set = set()

        # Scan diagnostics (updated each scan, shown on dashboard)
        self._last_scan_diag: dict = {
            "raw_markets": 0,       # markets returned by API
            "markets_scanned": 0,   # after parsing (time/strike/direction)
            "evaluated": 0,         # got fair value + orderbook pricing
            "opportunities": 0,
            "best_gross_edge": 0.0,
            "best_ticker": "",
            "near_misses": 0,       # edge > 0 but below threshold
            "no_edge": 0,           # fair <= ask (no edge at all)
            "price_filtered": 0,    # ask outside price range
            "time_filtered": 0,     # expiry outside time range
            "pre_screened": 0,      # passed pre-evaluation (fair value computed)
            "fv_extreme_filtered": 0,  # fair value too close to 0 or 1
            "orderbook_fetched": 0, # actually fetched orderbook
        }

        # Parse diagnostics (updated each scan, shown on dashboard)
        self._last_parse_diag: dict = {
            "total": 0, "status_filtered": 0, "no_close_time": 0,
            "bad_datetime": 0, "expired": 0, "no_strike": 0,
            "no_asset": 0, "passed": 0, "samples": {},
        }

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
            spot = self.binance_feed.get_price(asset)
        if not spot:
            return None
        # Reject if both price feeds are stale (e.g. overnight disconnect)
        if self.chainlink.is_stale(asset, max_age=30) and self.binance_feed.is_stale(asset, max_age=30):
            self._last_scan_diag["stale_price"] += 1
            return None

        vol = self.vol_tracker.get_adaptive_vol(asset, seconds_left)
        if not vol:
            return None

        fair = self.pricer.fair_value(spot, strike, seconds_left, vol, direction)
        # Clamp to reasonable range
        fair = max(0.001, min(0.999, fair))

        return fair, vol, spot

    def _fair_value_vol_bs_range(
        self, asset: str, floor_strike: float, cap_strike: float, seconds_left: float
    ) -> Optional[tuple]:
        """
        Black-Scholes vol-based fair value for range binary options.
        Pays $1 if floor_strike <= S < cap_strike at expiry.
        Returns (fair_value, vol_used, spot) or None.
        """
        spot = self.chainlink.get_price(asset)
        if not spot:
            spot = self.binance_feed.get_price(asset)
        if not spot:
            return None
        # Reject if both price feeds are stale (e.g. overnight disconnect)
        if self.chainlink.is_stale(asset, max_age=30) and self.binance_feed.is_stale(asset, max_age=30):
            self._last_scan_diag["stale_price"] += 1
            return None

        vol = self.vol_tracker.get_adaptive_vol(asset, seconds_left)
        if not vol:
            return None
        # Undo dampener + apply range boost: ranges need HIGHER vol
        # (lower vol = tighter distribution = inflated range FV when spot is in-range)
        vol = vol / VOL_DAMPENER * VOL_RANGE_BOOST

        fair = self.pricer.range_fair_value(spot, floor_strike, cap_strike, seconds_left, vol)
        fair = max(0.001, min(0.999, fair))

        return fair, vol, spot

    def compute_fair_value(
        self, category: str, asset: str, strike: float, seconds_left: float,
        direction: str, cap_strike: float = None
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

        if config.fair_value_model == "vol_bs_range":
            if cap_strike is not None:
                return self._fair_value_vol_bs_range(asset, strike, cap_strike, seconds_left)
            # Non-range market in a range category — fall back to vol_bs
            return self._fair_value_vol_bs(asset, strike, seconds_left, direction)

        logger.warning(f"Unknown fair value model: {config.fair_value_model}")
        return None

    # ── Opportunity Evaluation ──────────────────────────────────────────────

    def pre_evaluate_market(
        self, parsed: dict, config: CategoryConfig
    ) -> Optional[dict]:
        """
        Pre-evaluate a market WITHOUT fetching orderbook.
        Computes fair value and theoretical edge potential.
        Returns enriched dict with fair, vol, spot, edge_potential added, or None.
        """
        ticker = parsed["ticker"]
        category = parsed["category"]
        asset = parsed["asset"]
        direction = parsed["direction"]
        strike = parsed["strike"]
        seconds_left = parsed["seconds_left"]

        diag = self._last_scan_diag

        # Time filter
        if seconds_left < config.min_seconds_to_expiry:
            diag["time_filtered"] += 1
            return None
        if seconds_left > config.max_seconds_to_expiry:
            diag["time_filtered"] += 1
            return None

        # Check position limits
        if self.positions.count_by_ticker(ticker) >= MAX_POSITIONS_PER_MARKET:
            return None
        if self.positions.count_by_category(category) >= config.max_positions_per_category:
            return None
        event_ticker = parsed.get("event_ticker", "")
        if event_ticker and self.positions.count_by_event(event_ticker) >= config.max_positions_per_event:
            return None

        # Compute fair value (NO orderbook needed)
        cap_strike = parsed.get("cap_strike")
        fv_result = self.compute_fair_value(
            category, asset, strike, seconds_left, direction, cap_strike=cap_strike
        )
        if not fv_result:
            diag["no_fair_value"] += 1
            return None
        fair, vol, spot = fv_result

        # Filter extreme fair values (near 0 or 1 = no edge opportunity)
        if fair < FAIR_VALUE_EXTREME_LOW or fair > FAIR_VALUE_EXTREME_HIGH:
            diag["fv_extreme_filtered"] += 1
            return None

        # Edge potential: markets near 0.50 have the most room for edge
        edge_potential = min(fair, 1.0 - fair)

        return {
            **parsed,
            "fair": fair,
            "vol": vol,
            "spot": spot,
            "edge_potential": edge_potential,
        }

    async def evaluate_market_with_orderbook(
        self, candidate: dict, config: CategoryConfig
    ) -> Optional[MarketOpportunity]:
        """
        Evaluate a pre-screened market candidate by fetching its orderbook.
        The candidate dict must contain fair, vol, spot from pre_evaluate_market().
        Returns MarketOpportunity if edge meets threshold, None otherwise.
        """
        ticker = candidate["ticker"]
        category = candidate["category"]
        asset = candidate["asset"]
        direction = candidate["direction"]
        strike = candidate["strike"]
        seconds_left = candidate["seconds_left"]
        close_time = candidate["close_time"]
        fair = candidate["fair"]
        vol = candidate["vol"]
        spot = candidate["spot"]

        diag = self._last_scan_diag

        # Get orderbook
        ob = await self.api.get_orderbook(ticker)

        # Determine side to trade
        if direction == "range":
            # Range markets: evaluate both YES and NO, pick better edge
            yes_ask = ob.get("yes_ask")
            no_ask = ob.get("no_ask")
            yes_fair = fair
            no_fair = 1.0 - fair

            yes_edge = ((yes_fair - yes_ask) / yes_fair * 100) if (yes_fair > 0 and yes_ask and yes_ask > 0) else -999
            no_edge = ((no_fair - no_ask) / no_fair * 100) if (no_fair > 0 and no_ask and no_ask > 0) else -999

            if no_edge > yes_edge:
                side = "no"
                ask_price = no_ask
                fair = no_fair  # Flip to NO perspective for all downstream calcs
            else:
                side = "yes"
                ask_price = yes_ask
        else:
            side = "yes" if direction == "up" else "no"
            ask_price = ob.get(f"{side}_ask")

        # Range markets: block duplicate YES (brackets are mutually exclusive)
        if direction == "range" and side == "yes":
            evt = candidate.get("event_ticker", "")
            if evt and self.positions.has_yes_in_event(evt):
                return None

        ask_size = ob.get(f"{side}_ask_size", 0)

        if not ask_price or ask_price <= 0:
            return None

        # Price filters
        if ask_price < config.min_price or ask_price > config.max_price:
            diag["price_filtered"] += 1
            return None

        diag["evaluated"] += 1

        # Gross edge
        gross_edge_pct = ((fair - ask_price) / fair) * 100 if fair > 0 else 0

        # Net edge (after fees)
        contracts = config.contracts_per_trade
        net_edge_pct, fee, net_profit = net_edge_after_fees(fair, ask_price, contracts)

        # Track best edge seen this scan (even if below threshold)
        if gross_edge_pct > diag["best_gross_edge"]:
            diag["best_gross_edge"] = gross_edge_pct
            diag["best_ticker"] = ticker

        # Log shadow trade for every priced market with positive edge
        if gross_edge_pct > 0:
            self._log_shadow_trade(
                category=category, asset=asset, ticker=ticker,
                event_ticker=candidate.get("event_ticker", ""),
                direction=direction, side=side, strike=strike, spot=spot,
                seconds_left=seconds_left, close_time=close_time,
                model_vol=vol, model_fair=fair, ask_price=ask_price,
                gross_edge_pct=gross_edge_pct, net_edge_pct=net_edge_pct,
                fee=fee, implied_vol=None,
            )

        # Edge filters — use adaptive threshold when available
        required_edge = self.adaptive_edge.get_required_edge(
            asset, ask_price, config.min_edge_pct,
            category=category, seconds_left=seconds_left, model_fair=fair,
        )
        if gross_edge_pct < required_edge:
            if gross_edge_pct > 0:
                diag["near_misses"] += 1
            else:
                diag["no_edge"] += 1
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
            event_ticker=candidate["event_ticker"],
            category=category,
            asset=asset,
            direction=direction,
            side=side,
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

        contracts = self.bankroll.get_contracts(opp.ask_price)
        if contracts <= 0:
            logger.warning(f"Bankroll: insufficient balance for {opp.ticker}")
            return None
        side = opp.side
        trade_id = f"ES-{int(time.time())}-{uuid.uuid4().hex[:6]}"

        # Pre-execution logging
        config = CATEGORY_CONFIGS.get(opp.category)
        req_edge = self.adaptive_edge.get_required_edge(
            opp.asset, opp.ask_price, config.min_edge_pct if config else 25.0,
            category=opp.category, seconds_left=opp.seconds_left,
            model_fair=opp.model_fair,
        )
        logger.info(
            f"{'[PAPER] ' if self.paper_mode else ''}"
            f"TRADE {opp.asset} {opp.direction.upper()} | "
            f"{opp.ticker} | strike=${opp.strike:,.0f} | "
            f"fair={opp.model_fair:.3f} ask={opp.ask_price:.3f} | "
            f"edge={opp.gross_edge_pct:.1f}% (net={opp.net_edge_pct:.1f}%) "
            f"req={req_edge:.1f}% | "
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

        # Rejected/unfilled orders have no real position — mark as settled immediately
        is_no_fill = order_status in ("rejected", "unfilled")

        try:
            self.db.execute(
                """
                INSERT OR REPLACE INTO trades (
                    trade_id, timestamp, category, asset, ticker, event_ticker,
                    direction, side,
                    strike, spot_entry, seconds_left, model_vol, model_fair,
                    ask_price, fill_price, edge_pct, implied_vol, vol_regime,
                    contracts, cost, fee, order_id, order_status, paper_trade,
                    close_time, settled, settlement_result, pnl, payout
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade_id,
                    datetime.now(timezone.utc).isoformat(),
                    opp.category,
                    opp.asset,
                    opp.ticker,
                    opp.event_ticker,
                    opp.direction,
                    opp.side,
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
                    opp.close_time.isoformat() if isinstance(opp.close_time, datetime) else str(opp.close_time),
                    1 if is_no_fill else 0,
                    "no_fill" if is_no_fill else None,
                    0.0,
                    0.0,
                ),
            )
            self.db.commit()
        except Exception as e:
            logger.error(f"DB write error: {e}")

    def _log_shadow_trade(
        self,
        category: str, asset: str, ticker: str, event_ticker: str,
        direction: str, side: str, strike: float, spot: float, seconds_left: float,
        close_time: datetime, model_vol: float, model_fair: float,
        ask_price: float, gross_edge_pct: float, net_edge_pct: float,
        fee: float, implied_vol: Optional[float],
    ):
        """Log a shadow trade — every priced market with positive edge, regardless of threshold."""
        try:
            # Deduplicate: only log once per ticker+direction+side per close_time window
            # (same market scanned every 15s, we only need one entry per market window)
            existing = self.db.execute(
                "SELECT id FROM shadow_trades WHERE ticker=? AND direction=? AND side=? AND settled=0 LIMIT 1",
                (ticker, direction, side),
            ).fetchone()
            if existing:
                return  # Already tracking this market

            self.db.execute(
                """
                INSERT INTO shadow_trades (
                    timestamp, category, asset, ticker, event_ticker, direction, side,
                    strike, spot, seconds_left, close_time,
                    model_vol, model_fair, ask_price,
                    gross_edge_pct, net_edge_pct, fee, implied_vol
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    datetime.now(timezone.utc).isoformat(),
                    category, asset, ticker, event_ticker, direction, side,
                    strike, spot, seconds_left,
                    close_time.isoformat() if isinstance(close_time, datetime) else str(close_time),
                    model_vol, model_fair, ask_price,
                    gross_edge_pct, net_edge_pct, fee, implied_vol,
                ),
            )
            self.db.commit()
        except Exception as e:
            logger.debug(f"Shadow trade log error: {e}")

    # ── Settlement Monitoring ───────────────────────────────────────────────

    async def check_settlements(self):
        """Check all open positions for settlement (in-memory + DB fallback)."""
        now = datetime.now(timezone.utc)
        settled_trade_ids = set()

        # ── Phase 1: Check in-memory positions (fast path) ──
        positions = self.positions.get_all()
        for pos in positions:
            if now < pos.close_time + timedelta(seconds=30):
                continue

            try:
                settlement = await self.api.get_market_settlement(pos.ticker)
                if not settlement.get("settled"):
                    age = (now - pos.close_time).total_seconds()
                    if age > 600:
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

                self._settle_trade_in_db(pos.trade_id, pos.category, result, won, payout, pnl, pos.cost, now)
                settled_trade_ids.add(pos.trade_id)

                logger.info(
                    f"SETTLE {pos.ticker} | {'WIN' if won else 'LOSS'} | "
                    f"result={result} | payout=${payout:.2f} cost=${pos.cost:.2f} "
                    f"fee=${pos.fee:.2f} | P&L=${pnl:+.2f}"
                )
                self.positions.remove(pos.trade_id)

            except Exception as e:
                logger.warning(f"Settlement check error for {pos.ticker}: {e}")

        # ── Phase 2: DB fallback — catch orphaned trades ──
        try:
            cutoff = (now - timedelta(seconds=30)).isoformat()
            rows = self.db.execute(
                """
                SELECT trade_id, ticker, category, direction, contracts, cost, fee, close_time, side
                FROM trades
                WHERE settled=0
                  AND order_status IN ('filled', 'paper_filled')
                  AND close_time IS NOT NULL
                  AND close_time < ?
                ORDER BY close_time ASC
                LIMIT 50
                """,
                (cutoff,),
            ).fetchall()
        except Exception as e:
            logger.warning(f"DB settlement query error: {e}")
            rows = []

        for row in rows:
            trade_id, ticker, category, direction, contracts, cost, fee, close_time_str, row_side = row
            if trade_id in settled_trade_ids:
                continue  # Already settled in phase 1

            try:
                settlement = await self.api.get_market_settlement(ticker)
                if not settlement.get("settled"):
                    # Check if it's been too long
                    try:
                        ct = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))
                        age = (now - ct).total_seconds()
                        if age > 600:
                            logger.warning(
                                f"DB trade {trade_id} ({ticker}) still unsettled {age:.0f}s after close"
                            )
                    except Exception:
                        pass
                    continue

                result = settlement.get("result", "")
                # Use stored side, fall back to direction-based for old trades
                side = row_side if row_side else ("yes" if direction in ("up", "range") else "no")
                won = (side == "yes" and result == "yes") or (side == "no" and result == "no")

                payout = contracts * 1.0 if won else 0.0
                pnl = payout - (cost or 0) - (fee or 0)

                self._settle_trade_in_db(trade_id, category, result, won, payout, pnl, cost or 0, now)

                logger.info(
                    f"SETTLE (DB) {ticker} | {'WIN' if won else 'LOSS'} | "
                    f"result={result} | payout=${payout:.2f} cost=${cost or 0:.2f} "
                    f"fee=${fee or 0:.2f} | P&L=${pnl:+.2f}"
                )

                # Remove from in-memory tracker if present
                self.positions.remove(trade_id)

            except Exception as e:
                logger.warning(f"DB settlement error for {ticker}: {e}")

    def _settle_trade_in_db(
        self, trade_id: str, category: str, result: str,
        won: bool, payout: float, pnl: float, cost: float, now: datetime
    ):
        """Settle a trade: update DB + category stats + circuit breaker."""
        self.db.execute(
            """
            UPDATE trades SET
                settled=1, settlement_result=?, payout=?, pnl=?, settled_at=?
            WHERE trade_id=?
            """,
            (result, payout, pnl, now.isoformat(), trade_id),
        )
        self.db.commit()

        self.cat_stats.record_settlement(category, won, pnl, cost, payout)
        self.bankroll.update_pnl()
        self.bankroll.write_state_file()

        disable_reason = self.cat_stats.check_circuit_breaker(category)
        if disable_reason and category not in self._disabled_categories:
            self._disabled_categories.add(category)
            CATEGORY_CONFIGS[category].enabled = False
            logger.warning(
                f"CIRCUIT BREAKER: Disabled category '{category}': {disable_reason}"
            )
            self.db.execute(
                "UPDATE category_stats SET disabled_at=?, disable_reason=? WHERE category=?",
                (now.isoformat(), disable_reason, category),
            )
            self.db.commit()

    # ── Startup Backfill ───────────────────────────────────────────────────

    async def backfill_unsettled_trades(self):
        """
        On startup, find any unsettled bot trades from previous sessions
        and resolve them. Fetches market data from Kalshi to backfill
        close_time and determine settlement outcome.

        Only touches trades created by this bot (trade_id LIKE 'ES-%').
        """
        rows = self.db.execute(
            """
            SELECT trade_id, ticker, category, direction, contracts, cost, fee,
                   close_time, order_status, side
            FROM trades
            WHERE settled=0
              AND trade_id LIKE 'ES-%'
              AND order_status IN ('filled', 'paper_filled')
            ORDER BY timestamp ASC
            """,
        ).fetchall()

        if not rows:
            logger.info("BACKFILL: No unsettled trades from previous sessions")
            return

        logger.info(f"BACKFILL: Found {len(rows)} unsettled trades from previous sessions")
        now = datetime.now(timezone.utc)
        settled_count = 0
        backfilled_count = 0

        # Cache market data to avoid duplicate API calls for same ticker
        market_cache = {}

        for row in rows:
            trade_id, ticker, category, direction, contracts, cost, fee, close_time_str, order_status, row_side = row

            try:
                # Fetch market data (cached per ticker)
                if ticker not in market_cache:
                    market_data = await self.api.get_market(ticker)
                    market_cache[ticker] = market_data

                market_data = market_cache[ticker]

                # Backfill close_time if missing
                if not close_time_str:
                    ct_str = market_data.get("close_time") or market_data.get("expiration_time")
                    if ct_str:
                        self.db.execute(
                            "UPDATE trades SET close_time=? WHERE trade_id=?",
                            (ct_str, trade_id),
                        )
                        self.db.commit()
                        backfilled_count += 1
                        logger.info(f"BACKFILL: Set close_time for {trade_id} ({ticker}) -> {ct_str}")

                # Check settlement
                status = market_data.get("status", "")
                result = market_data.get("result", "")

                if status == "settled" or result in ("yes", "no"):
                    # Use stored side, fall back to direction-based for old trades
                    side = row_side if row_side else ("yes" if direction in ("up", "range") else "no")
                    won = (side == "yes" and result == "yes") or (side == "no" and result == "no")

                    payout = (contracts or 0) * 1.0 if won else 0.0
                    pnl = payout - (cost or 0) - (fee or 0)

                    self._settle_trade_in_db(trade_id, category, result, won, payout, pnl, cost or 0, now)
                    settled_count += 1

                    logger.info(
                        f"BACKFILL SETTLE {ticker} | {'WIN' if won else 'LOSS'} | "
                        f"result={result} | payout=${payout:.2f} cost=${cost or 0:.2f} "
                        f"fee=${fee or 0:.2f} | P&L=${pnl:+.2f}"
                    )
                else:
                    logger.info(
                        f"BACKFILL: {trade_id} ({ticker}) still active (status={status}), will check later"
                    )

            except Exception as e:
                logger.warning(f"BACKFILL error for {trade_id} ({ticker}): {e}")

        logger.info(
            f"BACKFILL complete: {settled_count}/{len(rows)} settled, "
            f"{backfilled_count} close_times backfilled"
        )

    # ── Shadow Settlement ─────────────────────────────────────────────────

    async def check_shadow_settlements(self):
        """
        Resolve unsettled shadow trades. For each, check if the market
        has settled, then compute whether a hypothetical buy would have won.
        """
        now = datetime.now(timezone.utc)

        # Get unsettled shadow trades whose close_time has passed
        rows = self.db.execute(
            """
            SELECT id, ticker, direction, close_time, ask_price, fee, model_fair, side
            FROM shadow_trades
            WHERE settled=0 AND close_time < ?
            ORDER BY close_time ASC
            LIMIT 50
            """,
            ((now - timedelta(seconds=30)).isoformat(),),
        ).fetchall()

        if not rows:
            return

        # Batch by unique ticker to avoid duplicate API calls
        ticker_results = {}
        for row in rows:
            row_id, ticker, direction, close_time_str, ask_price, fee, model_fair, row_side = row
            if ticker not in ticker_results:
                try:
                    settlement = await self.api.get_market_settlement(ticker)
                    ticker_results[ticker] = settlement
                except Exception as e:
                    logger.debug(f"Shadow settlement check error for {ticker}: {e}")
                    ticker_results[ticker] = {"settled": False}

            settlement = ticker_results[ticker]
            if not settlement.get("settled"):
                # Check if it's very old (>10 min past close) — mark as expired
                try:
                    ct = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))
                    if (now - ct).total_seconds() > 600:
                        self.db.execute(
                            "UPDATE shadow_trades SET settled=1, settlement_result='expired', settled_at=? WHERE id=?",
                            (now.isoformat(), row_id),
                        )
                except Exception:
                    pass
                continue

            result = settlement.get("result", "")

            # Determine if hypothetical trade would have won
            # Use stored side, fall back to direction-based for old shadow trades
            side = row_side if row_side else ("yes" if direction in ("up", "range") else "no")
            won = (side == "yes" and result == "yes") or (side == "no" and result == "no")

            # Hypothetical P&L: buy 1 contract at ask_price
            payout = 1.0 if won else 0.0
            hypothetical_pnl = payout - ask_price - fee

            self.db.execute(
                """
                UPDATE shadow_trades SET
                    settled=1, settlement_result=?, won=?, hypothetical_pnl=?, settled_at=?
                WHERE id=?
                """,
                (result, 1 if won else 0, hypothetical_pnl, now.isoformat(), row_id),
            )

        self.db.commit()

        settled_count = sum(1 for t in ticker_results.values() if t.get("settled"))
        if settled_count > 0:
            logger.info(f"SHADOW: Settled {settled_count} shadow trades")
            # Update calibration metrics after new settlements
            self.calibration.compute()
            self.calibration.compute_bucket_calibration()
            self.calibration.write_state_file()
            self.calibration.record_snapshot(self.db, settled_count)
            warn = self.calibration.should_warn()
            if warn:
                logger.warning(f"CALIBRATION: {warn}")
            # Update adaptive edge thresholds from new settlement data
            self.adaptive_edge.refresh()
            self.adaptive_edge.write_state_file()
            ae_state = self.adaptive_edge.get_state()
            if ae_state.get("active"):
                logger.info(
                    f"ADAPTIVE EDGE: active | {ae_state['n']} shadow trades | "
                    f"{ae_state['total_price_points']} price points"
                )

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

        # Reset scan diagnostics
        self._last_scan_diag = {
            "raw_markets": 0, "markets_scanned": 0, "evaluated": 0, "opportunities": 0,
            "best_gross_edge": 0.0, "best_ticker": "",
            "near_misses": 0, "no_edge": 0, "price_filtered": 0, "time_filtered": 0,
            "pre_screened": 0, "fv_extreme_filtered": 0, "orderbook_fetched": 0,
            "no_config": 0, "disabled": 0, "no_vol": 0,
            "no_fair_value": 0, "stale_price": 0,
        }
        self._last_parse_diag = {
            "total": 0, "status_filtered": 0, "no_close_time": 0,
            "bad_datetime": 0, "expired": 0, "no_strike": 0,
            "no_asset": 0, "passed": 0, "samples": {},
        }

        # Apply dashboard config overrides (min_edge_pct sliders)
        _apply_dashboard_overrides()

        # Global checks
        if self.positions.count_total() >= MAX_OPEN_POSITIONS_GLOBAL:
            logger.debug("Global position limit reached, skipping scan")
            return {"markets_scanned": 0, "opportunities": 0, "trades": 0, "skipped": "position_limit"}

        # Bankroll stop check (trailing stop with profit lock)
        self.bankroll.update_pnl()
        stopped, reason = self.bankroll.should_stop()
        if stopped:
            logger.warning(f"Bankroll stop: {reason}")
            return {"markets_scanned": 0, "opportunities": 0, "trades": 0, "skipped": "bankroll_stop"}

        # Hourly trade rate check
        cutoff = time.time() - 3600
        while self._hourly_trades and self._hourly_trades[0] < cutoff:
            self._hourly_trades.popleft()
        if len(self._hourly_trades) >= MAX_TRADES_PER_HOUR:
            logger.debug("Hourly trade limit reached")
            return {"markets_scanned": 0, "opportunities": 0, "trades": 0, "skipped": "hourly_limit"}

        # 1. Discover markets
        raw_markets = await self.scanner.scan_all()
        self._last_scan_diag["raw_markets"] = len(raw_markets)
        if not raw_markets:
            logger.info("SCAN: 0 raw markets from API (check series tickers / API key / network)")

        # 2. Parse and filter
        parsed_markets = []
        pd = self._last_parse_diag
        for raw in raw_markets:
            pd["total"] += 1
            parsed = self.scanner.parse_market(raw, now, diag=pd)
            if not parsed:
                continue
            pd["passed"] += 1
            if parsed["direction"] is None:
                # 15-min market: evaluate both up (YES) and down (NO) sides
                for d in ("up", "down"):
                    entry = dict(parsed)
                    entry["direction"] = d
                    parsed_markets.append(entry)
            else:
                parsed_markets.append(parsed)

        # Log parse diagnostics
        logger.debug(
            f"PARSE: {pd['total']} raw -> {pd['passed']} parsed | "
            f"status={pd['status_filtered']} expired={pd['expired']} "
            f"no_strike={pd['no_strike']} no_asset={pd['no_asset']} "
            f"no_close={pd['no_close_time']} bad_dt={pd['bad_datetime']}"
        )
        if self.verbose:
            for reason, tickers in pd.get("samples", {}).items():
                logger.info(f"  PARSE REJECT [{reason}]: {', '.join(tickers)}")

        # 3. Pre-evaluate: compute fair values (NO orderbook calls)
        self._last_scan_diag["markets_scanned"] = len(parsed_markets)
        candidates = []
        for parsed in parsed_markets:
            config = CATEGORY_CONFIGS.get(parsed["category"])
            if not config or not config.enabled:
                self._last_scan_diag["no_config"] += 1
                continue
            if parsed["category"] in self._disabled_categories:
                self._last_scan_diag["disabled"] += 1
                continue

            # Vol warmup check
            if not self.vol_tracker.is_warmed_up(parsed["asset"]):
                self._last_scan_diag["no_vol"] += 1
                continue

            candidate = self.pre_evaluate_market(parsed, config)
            if candidate:
                candidates.append(candidate)

        # 4. Sort by edge potential (highest first) and take top N
        candidates.sort(key=lambda c: c["edge_potential"], reverse=True)
        top_candidates = candidates[:MAX_ORDERBOOK_CANDIDATES]

        self._last_scan_diag["pre_screened"] = len(candidates)
        self._last_scan_diag["orderbook_fetched"] = len(top_candidates)

        logger.debug(
            f"BATCH: {len(parsed_markets)} parsed -> {len(candidates)} pre-screened -> "
            f"top {len(top_candidates)} for orderbook"
        )

        # Periodic scan summary (every ~30s to avoid spam)
        if not hasattr(self, '_last_summary_time'):
            self._last_summary_time = 0
        d = self._last_scan_diag
        if time.time() - self._last_summary_time >= 30:
            self._last_summary_time = time.time()
            logger.info(
                f"STATUS: {len(parsed_markets)} mkts | "
                f"no_vol={d['no_vol']} time_out={d['time_filtered']} "
                f"stale={d['stale_price']} no_fv={d['no_fair_value']} "
                f"fv_extreme={d['fv_extreme_filtered']} | "
                f"{len(candidates)} passed"
            )
        # Always log immediately when candidates found (active window)
        if len(candidates) > 0:
            logger.info(
                f"ACTIVE: {len(candidates)} candidates | "
                f"top {len(top_candidates)} for orderbook | "
                f"stale={d['stale_price']} no_fv={d['no_fair_value']} "
                f"fv_extreme={d['fv_extreme_filtered']}"
            )

        # 5. Fetch orderbook ONLY for top candidates
        opportunities = []
        for candidate in top_candidates:
            config = CATEGORY_CONFIGS.get(candidate["category"])
            if not config:
                continue
            opp = await self.evaluate_market_with_orderbook(candidate, config)
            if opp:
                opportunities.append(opp)

        self._last_scan_diag["opportunities"] = len(opportunities)

        # 6. Rank by net edge (best first), with near-the-money preference as tiebreaker
        def _rank_key(opp):
            # Primary: net edge (higher is better)
            # Secondary: closeness to ATM (lower moneyness distance is better)
            if opp.spot > 0 and opp.strike > 0:
                moneyness_distance = abs(math.log(opp.spot / opp.strike))
            else:
                moneyness_distance = 999
            return (opp.net_edge_pct, -moneyness_distance)

        opportunities.sort(key=_rank_key, reverse=True)

        # 7. Execute (best first, within limits)
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
            # Range markets: block duplicate YES (brackets are mutually exclusive)
            if opp.direction == "range" and opp.side == "yes" and opp.event_ticker:
                if self.positions.has_yes_in_event(opp.event_ticker):
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

        scan_msg = (
            f"SCAN: {len(parsed_markets)} markets | "
            f"{len(opportunities)} opportunities | "
            f"{trades_executed} trades | "
            f"best edge={best_edge:.1f}% | "
            f"{scan_duration_ms:.0f}ms"
        )
        if opportunities:
            logger.info(scan_msg)
        else:
            logger.debug(scan_msg)

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

        # Parse diagnostics
        pd = self._last_parse_diag
        if pd["total"] > 0:
            lines.append(
                f"\n  Parse: {pd['total']} raw → {pd['passed']} ok | "
                f"status={pd['status_filtered']} expired={pd['expired']} "
                f"no_strike={pd['no_strike']} no_asset={pd['no_asset']}"
            )
            for reason in ("no_strike", "no_asset", "expired", "status_filtered"):
                samples = pd.get("samples", {}).get(reason, [])
                if samples:
                    lines.append(f"    [{reason}] e.g. {', '.join(samples[:2])}")

        # Scan diagnostics
        d = self._last_scan_diag
        best_edge_str = f"{d['best_gross_edge']:.1f}%"
        if d["best_ticker"]:
            best_edge_str += f" ({d['best_ticker']})"
        lines.append(
            f"  Last Scan: {d['raw_markets']} raw → {d['markets_scanned']} parsed → "
            f"{d.get('pre_screened', 0)} pre-screened → "
            f"{d.get('orderbook_fetched', 0)} OB fetched → "
            f"{d['evaluated']} priced | "
            f"{d['near_misses']} near-miss | "
            f"{d['opportunities']} opps | "
            f"best edge={best_edge_str}"
        )

        # Global stats + bankroll
        hourly_count = len(self._hourly_trades)
        bk = self.bankroll.get_state()
        if bk["stopped"]:
            lines.append(
                f"  Bankroll: STOPPED | P&L=${bk['daily_pnl']:+.2f} | "
                f"HWM=${bk['hwm']:.2f} | Stop=${bk['stop_level']:+.2f}"
            )
        else:
            lines.append(
                f"  Bankroll: P&L=${bk['daily_pnl']:+.2f} | "
                f"HWM=${bk['hwm']:.2f} | Stop=${bk['stop_level']:+.2f} | "
                f"Tier={bk['tier']} ({bk['contracts']}x) | "
                f"Bal=${bk['balance']:.2f}"
            )
        lines.append(f"  Trades/hr: {hourly_count}/{MAX_TRADES_PER_HOUR}")

        # Calibration
        cal = self.calibration.get_state()
        if cal.get("sufficient"):
            bias_parts = [f"{a}={b:+.0f}%" for a, b in cal.get("asset_bias", {}).items()]
            bias_str = " ".join(bias_parts) if bias_parts else "—"
            lines.append(
                f"  Calibration: Brier={cal['brier']:.3f} | "
                f"Bias={cal['overconfidence']:.2f}x | "
                f"{cal['n']} shadow | {bias_str}"
            )
        elif cal.get("n", 0) > 0:
            lines.append(f"  Calibration: awaiting data ({cal['n']}/{CALIBRATION_MIN_SAMPLES} shadow settlements)")

        # Adaptive edge
        ae = self.adaptive_edge.get_state()
        if ae.get("active"):
            ae_parts = []
            for asset, info in ae.get("assets", {}).items():
                avg = info.get("avg_ratio", 1.0)
                # Show as effective edge: base 25% * ratio
                eff = 25.0 * avg
                ae_parts.append(f"{asset}={eff:.0f}%avg")
            ae_str = " ".join(ae_parts) if ae_parts else "—"
            lines.append(
                f"  Adaptive Edge: ON | {ae['n']} trades | "
                f"{ae['total_price_points']} price pts | {ae_str}"
            )
        elif ae.get("n", 0) > 0:
            lines.append(f"  Adaptive Edge: awaiting data ({ae['n']}/{ADAPTIVE_EDGE_MIN_SAMPLES} settled shadows)")

        # Disabled categories
        if self._disabled_categories:
            lines.append(f"\n  DISABLED: {', '.join(self._disabled_categories)}")

        lines.append(f"{'='*70}")
        print("\n".join(lines))

    def _restore_state_from_db(self):
        """Reload open positions and recent trade count from database after restart."""
        # 1. Restore open positions (unsettled, non-rejected trades)
        now = datetime.now(timezone.utc)
        rows = self.db.execute(
            """
            SELECT trade_id, ticker, event_ticker, category, asset, direction, side,
                   contracts, fill_price, cost, fee, order_id, timestamp, close_time
            FROM trades
            WHERE settled = 0
              AND order_status NOT IN ('rejected', 'unfilled', 'no_fill')
            """
        ).fetchall()

        for row in rows:
            try:
                close_time = datetime.fromisoformat(row[13]) if row[13] else now
            except (ValueError, TypeError):
                continue
            # Skip if already expired
            if close_time < now:
                continue
            pos = OpenPosition(
                trade_id=row[0],
                ticker=row[1],
                event_ticker=row[2] or "",
                category=row[3],
                asset=row[4],
                direction=row[5],
                side=row[6] or "yes",
                contracts=row[7],
                entry_price=row[8] or 0,
                cost=row[9] or 0,
                fee=row[10] or 0,
                order_id=row[11] or "",
                opened_at=datetime.fromisoformat(row[12]) if row[12] else now,
                close_time=close_time,
            )
            self.positions.add(pos)

        if self.positions.count_total() > 0:
            logger.info(f"RESTORE: Loaded {self.positions.count_total()} open positions from previous session")

        # 2. Restore hourly trade count
        rows = self.db.execute(
            """
            SELECT timestamp FROM trades
            WHERE timestamp > datetime('now', '-1 hour')
              AND order_status NOT IN ('rejected', 'unfilled', 'no_fill')
            """
        ).fetchall()

        for row in rows:
            try:
                ts = datetime.fromisoformat(row[0])
                self._hourly_trades.append(ts.timestamp())
            except (ValueError, TypeError):
                pass

        if self._hourly_trades:
            logger.info(f"RESTORE: {len(self._hourly_trades)} trades in last hour toward hourly limit")

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
            self.bankroll.update_balance(balance)
        else:
            # Paper mode: still connect for market data
            try:
                balance = await self.api.connect()
                logger.info(f"Kalshi balance: ${balance:.2f} (paper mode — no real trades)")
                self.bankroll.update_balance(balance)
            except Exception as e:
                logger.warning(f"Kalshi connection failed (paper mode OK): {e}")

        self.bankroll.write_state_file()

        # Backfill unsettled trades from previous sessions
        await self.backfill_unsettled_trades()

        # Restore position tracker and hourly trade count from DB
        self._restore_state_from_db()

        # Initialize adaptive edge from existing shadow trade history
        self.adaptive_edge.refresh()
        self.adaptive_edge.write_state_file()
        ae = self.adaptive_edge.get_state()
        if ae.get("active"):
            logger.info(
                f"Adaptive edge initialized: {ae['n']} shadow trades, "
                f"{ae['total_price_points']} price points"
            )
        else:
            logger.info(f"Adaptive edge: awaiting data ({ae.get('n', 0)}/{ADAPTIVE_EDGE_MIN_SAMPLES} settled shadows)")

        # Start price feeds
        self.chainlink.start()
        self.binance_feed.start()
        logger.info("Price feeds starting (Chainlink + Binance fallback)...")

        # Wait for initial vol warmup (only require core assets, not fallback ones)
        core_assets = set(CHAINLINK_SYMBOLS.keys()) - set(BINANCE_FALLBACK_SYMBOLS.keys())
        logger.info(f"Waiting {VOL_WARMUP_SECONDS}s for vol warmup (core: {', '.join(sorted(core_assets))})...")
        warmup_start = time.time()
        while self._running:
            all_warmed = all(
                self.vol_tracker.is_warmed_up(a) for a in core_assets
            )
            if all_warmed:
                logger.info("Vol warmup complete for core assets")
                break
            elapsed = time.time() - warmup_start
            if elapsed > VOL_WARMUP_SECONDS * 3:  # 3x timeout
                logger.warning("Vol warmup timeout — proceeding with available data")
                break
            # Print progress
            warmed = sum(1 for a in CHAINLINK_SYMBOLS if self.vol_tracker.is_warmed_up(a))
            core_ready = sum(1 for a in core_assets if self.vol_tracker.is_warmed_up(a))
            if int(elapsed) % 15 == 0:
                logger.info(
                    f"Warmup: {warmed}/{len(CHAINLINK_SYMBOLS)} total, "
                    f"{core_ready}/{len(core_assets)} core ready | "
                    f"{self.vol_tracker.status_str()}"
                )
            await asyncio.sleep(3)

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

                # Settlement checks (real + shadow)
                if time.time() - last_settlement_check > SETTLEMENT_CHECK_INTERVAL:
                    await self.check_settlements()
                    await self.check_shadow_settlements()
                    last_settlement_check = time.time()
                    # Refresh balance for bankroll sizing
                    if not self.paper_mode:
                        try:
                            self.bankroll.update_balance(await self.api.get_balance())
                        except Exception:
                            pass  # Balance refresh is best-effort

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
