#!/usr/bin/env python3
"""
Strike Price Collector v4 - Persistent Storage

Captures Chainlink prices at exactly the 15-minute boundaries (:00, :15, :30, :45).
Stores data in a hidden folder (.arb_data) for persistence across restarts.

Features:
- Stores strikes in ~/.arb_data/strikes.json (hidden folder)
- Also writes to ~/strikes.json for bot compatibility
- Persists data across restarts - no need to wait for new boundaries
- Automatic cleanup of old data (>6 hours)
- Session tracking for debugging

Run this BEFORE starting the main bot:
    python strike_collector.py
"""

import json
import threading
import time
import sys
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Hidden data folder
DATA_DIR = os.path.join(os.path.expanduser("~"), ".arb_data")
STRIKES_FILE = os.path.join(DATA_DIR, "strikes.json")
SESSION_LOG = os.path.join(DATA_DIR, "collector_sessions.log")

# Also write to home directory for bot compatibility
BOT_STRIKES_FILE = os.path.join(os.path.expanduser("~"), "strikes.json")

# Polymarket RTDS WebSocket
RTDS_URL = "wss://ws-live-data.polymarket.com"

# Symbols to track (only ones on both PM and Kalshi)
SYMBOLS = ["btc/usd", "eth/usd", "sol/usd", "xrp/usd"]


class StrikeCollector:
    def __init__(self):
        self._lock = threading.Lock()
        self._strikes = {}  # "BTC_20:30" -> {"price": ..., "confidence": "HIGH"}
        self._connected = False
        self._current_prices = {}  # symbol -> {price, chainlink_ts}
        self._session_start = datetime.now(timezone.utc)
        self._captures_this_session = 0
        self._last_message_time = time.time()  # Staleness watchdog
        self._ws = None  # WebSocket reference for forced reconnect
        
        # Ensure data directory exists
        self._ensure_data_dir()
        
        # Load existing strikes
        self._load_strikes()
        
        # Log session start
        self._log_session("START")
    
    def _ensure_data_dir(self):
        """Create hidden data directory if it doesn't exist"""
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
            
            # On Windows, set hidden attribute
            if sys.platform == 'win32':
                try:
                    import ctypes
                    ctypes.windll.kernel32.SetFileAttributesW(DATA_DIR, 0x02)  # FILE_ATTRIBUTE_HIDDEN
                except:
                    pass
            
            print(f"  Data folder: {DATA_DIR}")
        except Exception as e:
            print(f"  Warning: Could not create data folder: {e}")
    
    def _log_session(self, event: str):
        """Log session events for debugging"""
        try:
            with open(SESSION_LOG, "a") as f:
                now = datetime.now(timezone.utc).isoformat()
                f.write(f"{now} | {event} | strikes={len(self._strikes)}\n")
        except:
            pass
    
    def _load_strikes(self):
        """Load existing strikes from file"""
        loaded = False
        
        # Try hidden folder first
        try:
            if Path(STRIKES_FILE).exists():
                with open(STRIKES_FILE, "r") as f:
                    data = json.load(f)
                    self._strikes = data.get("strikes", {})
                    if self._strikes:
                        print(f"  ✓ Loaded {len(self._strikes)} strikes from {STRIKES_FILE}")
                        loaded = True
        except Exception as e:
            print(f"  Could not load from hidden folder: {e}")
        
        # Fall back to bot file if needed
        if not loaded:
            try:
                if Path(BOT_STRIKES_FILE).exists():
                    with open(BOT_STRIKES_FILE, "r") as f:
                        data = json.load(f)
                        self._strikes = data.get("strikes", {})
                        if self._strikes:
                            print(f"  ✓ Loaded {len(self._strikes)} strikes from {BOT_STRIKES_FILE}")
                            loaded = True
            except Exception as e:
                print(f"  Could not load from bot file: {e}")
        
        # Cleanup old strikes
        self._cleanup_old_strikes()
        
        if loaded:
            # Show currently valid strikes
            self._show_valid_strikes()
    
    def _show_valid_strikes(self):
        """Show strikes that are still valid (within current/next window)"""
        now = datetime.now(timezone.utc)
        current_window = self._get_current_boundary()
        next_window = self._get_next_boundary()
        
        valid_strikes = []
        for key, data in self._strikes.items():
            try:
                strike_time = datetime.fromisoformat(data["time"])
                # Valid if within last 30 minutes (covers current and previous window)
                if strike_time >= current_window - timedelta(minutes=15):
                    sym = key.split("_")[0]
                    window = key.split("_")[1]
                    conf = data.get("confidence", "?")
                    price = data.get("price", 0)
                    valid_strikes.append(f"    {sym} @ {window}: ${price:,.2f} ({conf})")
            except:
                pass
        
        if valid_strikes:
            print(f"\n  Valid strikes for current/upcoming windows:")
            for s in valid_strikes:
                print(s)
    
    def _save_strikes(self):
        """Save strikes to both files"""
        data = {
            "updated": datetime.now(timezone.utc).isoformat(),
            "session_start": self._session_start.isoformat(),
            "captures_this_session": self._captures_this_session,
            "strikes": self._strikes
        }
        
        # Save to hidden folder (primary)
        try:
            with open(STRIKES_FILE, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"  Error saving to hidden folder: {e}")
        
        # Save to bot file (for compatibility)
        try:
            with open(BOT_STRIKES_FILE, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"  Error saving to bot file: {e}")
    
    def _get_window_key(self, dt: datetime, symbol: str) -> str:
        """Get key for a 15-minute window, e.g., 'BTC_20:30'"""
        minute = (dt.minute // 15) * 15
        window_time = dt.replace(minute=minute, second=0, microsecond=0)
        sym = symbol.upper().replace("/USD", "")
        return f"{sym}_{window_time.strftime('%H:%M')}"
    
    def _get_current_boundary(self) -> datetime:
        """Get the current 15-minute boundary (start of current window)"""
        now = datetime.now(timezone.utc)
        current_minute = (now.minute // 15) * 15
        return now.replace(minute=current_minute, second=0, microsecond=0)
    
    def _get_next_boundary(self) -> datetime:
        """Get the next 15-minute boundary"""
        return self._get_current_boundary() + timedelta(minutes=15)
    
    def _cleanup_old_strikes(self):
        """Remove strikes older than 6 hours"""
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=6)
        
        keys_to_remove = []
        for key, data in self._strikes.items():
            try:
                strike_time = datetime.fromisoformat(data["time"])
                if strike_time < cutoff:
                    keys_to_remove.append(key)
            except:
                pass
        
        if keys_to_remove:
            for key in keys_to_remove:
                del self._strikes[key]
            print(f"  Cleaned up {len(keys_to_remove)} old strikes")
    
    def _check_and_capture(self, symbol: str, price: float, chainlink_ts: int):
        """Check if this price should be captured as a strike"""
        # Convert chainlink timestamp to datetime
        price_time = datetime.fromtimestamp(chainlink_ts / 1000, tz=timezone.utc)
        
        # Get the window this price belongs to
        window_minute = (price_time.minute // 15) * 15
        window_start = price_time.replace(minute=window_minute, second=0, microsecond=0)
        window_key = self._get_window_key(window_start, symbol)
        
        # Calculate how many seconds after the boundary
        boundary_ms = int(window_start.timestamp() * 1000)
        seconds_after = (chainlink_ts - boundary_ms) / 1000.0
        
        # Only capture if:
        # 1. We haven't captured this window yet
        # 2. The price is within first 2 seconds of the boundary
        if window_key not in self._strikes and seconds_after >= 0 and seconds_after <= 2.0:
            # Determine confidence
            if seconds_after <= 0.5:
                confidence = "HIGH"
            elif seconds_after <= 1.0:
                confidence = "HIGH"
            else:
                confidence = "MED"
            
            self._strikes[window_key] = {
                "price": price,
                "time": window_start.isoformat(),
                "chainlink_ts": chainlink_ts,
                "seconds": round(seconds_after, 3),
                "confidence": confidence,
                "captured_at": datetime.now(timezone.utc).isoformat()
            }
            
            self._captures_this_session += 1
            self._save_strikes()
            self._cleanup_old_strikes()
            
            # Print capture
            sym = symbol.upper().replace("/USD", "")
            marker = "★" if confidence == "HIGH" else "●"
            print(f"\n  {marker} CAPTURED {window_key}: ${price:,.2f} (+{seconds_after:.3f}s - {confidence})")
            
            # Log the capture
            self._log_session(f"CAPTURE {window_key} @ ${price:,.2f}")
            
            return True
        
        return False
    
    def get_strike_for_window(self, symbol: str, window_time: datetime) -> dict:
        """Get strike for a specific window (for bot to query)"""
        key = self._get_window_key(window_time, symbol)
        return self._strikes.get(key)
    
    def run(self):
        """Main loop - stay connected and capture at boundaries"""
        import websocket
        
        print(f"\n  Mode: Continuous connection, capture at boundaries")
        print(f"  Symbols: {', '.join(s.upper().replace('/USD', '') for s in SYMBOLS)}")
        print("-" * 60)
        
        def on_message(ws, message):
            try:
                self._last_message_time = time.time()
                data = json.loads(message)
                topic = data.get("topic", "")
                
                if "crypto_prices_chainlink" in topic:
                    payload = data.get("payload", {})
                    symbol = payload.get("symbol", "").lower()
                    value = payload.get("value")
                    chainlink_ts = payload.get("timestamp")
                    
                    if symbol in SYMBOLS and value and chainlink_ts:
                        # Update current price
                        with self._lock:
                            self._current_prices[symbol] = {
                                "price": float(value),
                                "chainlink_ts": chainlink_ts
                            }
                        
                        # Check if this should be captured as a strike
                        self._check_and_capture(symbol, float(value), chainlink_ts)
                        
            except json.JSONDecodeError:
                pass
            except Exception as e:
                pass
        
        def on_open(ws):
            self._connected = True
            self._ws = ws
            self._last_message_time = time.time()
            print(f"\n  [WS] Connected to RTDS")
            ws.send(json.dumps({
                "action": "subscribe",
                "subscriptions": [{"topic": "crypto_prices_chainlink", "type": "*", "filters": ""}]
            }))
        
        def on_error(ws, error):
            err_str = str(error)
            if "Service restarting" in err_str:
                print(f"\n  [WS] Service restarting, will reconnect...")
            else:
                print(f"\n  [WS] Error: {error}")
        
        def on_close(ws, code, msg):
            self._connected = False
            self._ws = None
            if msg and "Service restarting" in str(msg):
                pass  # Suppress normal restart messages
            else:
                print(f"\n  [WS] Disconnected ({code})")
        
        # Countdown display thread with staleness watchdog
        def countdown_thread():
            last_print_time = 0
            stale_warned = False
            while True:
                try:
                    now = datetime.now(timezone.utc)
                    next_boundary = self._get_next_boundary()
                    seconds_until = (next_boundary - now).total_seconds()
                    mins = int(seconds_until // 60)
                    secs = int(seconds_until % 60)
                    
                    # ═══════════════════════════════════════════════════
                    # STALENESS WATCHDOG: Force reconnect if no data
                    # WS can appear "connected" but stop sending messages.
                    # If no messages for 30s, the connection is dead.
                    # Force-close so the reconnect loop picks it up.
                    # ═══════════════════════════════════════════════════
                    stale_seconds = time.time() - self._last_message_time
                    if self._connected and stale_seconds > 30:
                        if not stale_warned:
                            print(f"\n  [WATCHDOG] No data for {stale_seconds:.0f}s — forcing reconnect...")
                            self._log_session(f"WATCHDOG: stale {stale_seconds:.0f}s, forcing reconnect")
                            stale_warned = True
                        if self._ws:
                            try:
                                self._ws.close()
                            except:
                                pass
                            self._ws = None
                            self._connected = False
                    # Pre-boundary safety: if within 10s of capture and stale > 10s,
                    # reconnect NOW rather than risk missing the strike
                    elif self._connected and stale_seconds > 10 and seconds_until < 10:
                        print(f"\n  [WATCHDOG] Stale {stale_seconds:.0f}s with boundary in {seconds_until:.0f}s — emergency reconnect!")
                        self._log_session(f"WATCHDOG: pre-boundary emergency reconnect (stale {stale_seconds:.0f}s)")
                        stale_warned = True
                        if self._ws:
                            try:
                                self._ws.close()
                            except:
                                pass
                            self._ws = None
                            self._connected = False
                    elif stale_seconds < 10:
                        stale_warned = False
                    
                    # Get current prices for display
                    btc_price = self._current_prices.get("btc/usd", {}).get("price", 0)
                    eth_price = self._current_prices.get("eth/usd", {}).get("price", 0)
                    sol_price = self._current_prices.get("sol/usd", {}).get("price", 0)
                    
                    status = "●" if self._connected else "○"
                    stale_tag = f" ⚠️STALE {stale_seconds:.0f}s" if self._connected and stale_seconds > 15 else ""
                    
                    # Build price display
                    prices = []
                    if btc_price:
                        prices.append(f"BTC:${btc_price:,.0f}")
                    if eth_price:
                        prices.append(f"ETH:${eth_price:,.0f}")
                    if sol_price:
                        prices.append(f"SOL:${sol_price:.2f}")
                    
                    price_str = " | ".join(prices) if prices else "Waiting..."
                    
                    line = f"  {status} Next: {next_boundary.strftime('%H:%M')} (-{mins:02d}:{secs:02d}) | {price_str} | Strikes: {len(self._strikes)}{stale_tag}"
                    
                    sys.stdout.write(f"\r{line}          ")
                    sys.stdout.flush()
                    
                    time.sleep(1)
                except:
                    time.sleep(1)
        
        # Start countdown thread
        t = threading.Thread(target=countdown_thread, daemon=True)
        t.start()
        
        # Main WebSocket loop with reconnection
        while True:
            try:
                self._last_message_time = time.time()  # Reset on each connection attempt
                ws = websocket.WebSocketApp(
                    RTDS_URL,
                    on_message=on_message,
                    on_open=on_open,
                    on_error=on_error,
                    on_close=on_close
                )
                ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                print(f"\n  Connection error: {e}")
            
            # Fast reconnect (1s) — watchdog already validated the stale state
            print(f"\n  Reconnecting in 1s...")
            time.sleep(1)


def show_status():
    """Show current strikes status without running collector"""
    print("=" * 60)
    print("  STRIKE DATA STATUS")
    print("=" * 60)
    
    # Check both files
    for filepath, name in [(STRIKES_FILE, "Hidden folder"), (BOT_STRIKES_FILE, "Bot file")]:
        print(f"\n  {name}: {filepath}")
        try:
            if Path(filepath).exists():
                with open(filepath, "r") as f:
                    data = json.load(f)
                    strikes = data.get("strikes", {})
                    updated = data.get("updated", "?")
                    print(f"    Last updated: {updated}")
                    print(f"    Total strikes: {len(strikes)}")
                    
                    if strikes:
                        # Show recent strikes
                        now = datetime.now(timezone.utc)
                        print(f"    Recent strikes:")
                        for key, sdata in sorted(strikes.items(), key=lambda x: x[1].get("time", ""), reverse=True)[:6]:
                            price = sdata.get("price", 0)
                            conf = sdata.get("confidence", "?")
                            secs = sdata.get("seconds", 0)
                            print(f"      {key}: ${price:,.2f} (+{secs:.1f}s, {conf})")
            else:
                print(f"    File does not exist")
        except Exception as e:
            print(f"    Error reading: {e}")


def main():
    print("=" * 60)
    print("  STRIKE PRICE COLLECTOR v4")
    print("  Persistent Storage with Hidden Data Folder")
    print("=" * 60)
    
    # Check for --status flag
    if len(sys.argv) > 1 and sys.argv[1] == "--status":
        show_status()
        return
    
    print(f"\n  Data folder: {DATA_DIR}")
    print(f"  Bot file: {BOT_STRIKES_FILE}")
    print(f"  Started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    # Show time until next boundary
    now = datetime.now(timezone.utc)
    current_minute = (now.minute // 15) * 15
    current_boundary = now.replace(minute=current_minute, second=0, microsecond=0)
    next_boundary = current_boundary + timedelta(minutes=15)
    seconds_to_boundary = (next_boundary - now).total_seconds()
    
    print(f"\n  Next boundary: {next_boundary.strftime('%H:%M:%S')} UTC")
    print(f"  Time until: {int(seconds_to_boundary//60)}m {int(seconds_to_boundary%60)}s")
    
    print(f"\n  Tip: Run with --status to check saved strikes without starting collector")
    
    collector = StrikeCollector()
    collector.run()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  Stopped.")
