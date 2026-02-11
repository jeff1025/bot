#!/usr/bin/env python3
"""
Edge Scanner Control Dashboard — Live Auto-Updating
=====================================================
Reads from the edge_scanner SQLite database and provides:
  - Per-category P&L, win rate, trade count
  - Adjustable min_edge_pct sliders per category
  - Cumulative P&L chart, win/loss bar chart, edge distribution
  - Auto-updates every 5 seconds

Usage:
    python edge_dashboard.py
    python edge_dashboard.py --port 8060
    python edge_dashboard.py --refresh 10

No pip installs needed — uses only Python stdlib.
Config changes are written to ~/.edge_scanner/dashboard_overrides.json
and picked up by edge_scanner.py on its next scan cycle.
"""

import http.server
import json
import os
import sys
import sqlite3
import argparse
from pathlib import Path
from urllib.parse import urlparse

# ── Config ────────────────────────────────────────────────────────────────────
DATA_DIR = Path(os.path.expanduser("~")) / ".edge_scanner"
DB_PATH = DATA_DIR / "trades.db"
OVERRIDES_PATH = DATA_DIR / "dashboard_overrides.json"
DEFAULT_PORT = 8060
DEFAULT_REFRESH = 5  # seconds

# Category display order and defaults
CATEGORIES = [
    ("crypto_hourly_btc", "Hourly BTC", "#f7931a"),
    ("crypto_hourly_eth", "Hourly ETH", "#627eea"),
    ("crypto_hourly_sol", "Hourly SOL", "#9945ff"),
    ("crypto_15min_btc", "15min BTC", "#f7931a"),
    ("crypto_15min_eth", "15min ETH", "#627eea"),
    ("crypto_15min_sol", "15min SOL", "#9945ff"),
]


def get_db():
    """Open read-only connection to the trades database."""
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def load_overrides():
    """Load current config overrides."""
    if OVERRIDES_PATH.exists():
        try:
            with open(OVERRIDES_PATH, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {}


def save_overrides(overrides: dict):
    """Save config overrides to shared JSON file."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(OVERRIDES_PATH, "w") as f:
        json.dump(overrides, f, indent=2)


def get_dashboard_data() -> dict:
    """Load all data needed by the dashboard."""
    db = get_db()
    if not db:
        return {"error": "Database not found", "trades": [], "categories": {}, "scans": []}

    try:
        # All trades (settled and open)
        trades = []
        for row in db.execute(
            "SELECT * FROM trades ORDER BY timestamp DESC LIMIT 500"
        ):
            trades.append(dict(row))

        # Per-category stats
        cat_stats = {}
        for row in db.execute("SELECT * FROM category_stats"):
            d = dict(row)
            cat_stats[d["category"]] = d

        # Recent scans (last 100)
        scans = []
        for row in db.execute(
            "SELECT * FROM scan_log ORDER BY timestamp DESC LIMIT 100"
        ):
            scans.append(dict(row))

        # Daily P&L
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        row = db.execute(
            "SELECT COALESCE(SUM(pnl), 0) FROM trades WHERE settled=1 AND settled_at LIKE ?",
            (f"{today}%",),
        ).fetchone()
        daily_pnl = row[0] if row else 0.0

        # Total P&L (all time)
        row = db.execute(
            "SELECT COALESCE(SUM(pnl), 0) FROM trades WHERE settled=1"
        ).fetchone()
        total_pnl = row[0] if row else 0.0

        # Settled trades for charts (chronological)
        settled = []
        for row in db.execute(
            "SELECT trade_id, timestamp, settled_at, category, asset, ticker, "
            "direction, strike, edge_pct, fill_price, cost, fee, pnl, "
            "settlement_result, contracts, paper_trade "
            "FROM trades WHERE settled=1 ORDER BY settled_at ASC"
        ):
            settled.append(dict(row))

        # Load current overrides
        overrides = load_overrides()

        # Load bankroll state from JSON (written by edge_scanner)
        bankroll = {}
        bankroll_path = DATA_DIR / "bankroll_state.json"
        if bankroll_path.exists():
            try:
                with open(bankroll_path, "r") as f:
                    bankroll = json.load(f)
            except (json.JSONDecodeError, IOError):
                pass

        # Shadow trades — settled ones for calibration analysis
        shadow_settled = []
        shadow_pending = 0
        try:
            for row in db.execute(
                "SELECT id, timestamp, category, asset, ticker, direction, "
                "strike, spot, seconds_left, close_time, model_vol, model_fair, "
                "ask_price, gross_edge_pct, net_edge_pct, fee, "
                "settled, settlement_result, won, hypothetical_pnl, settled_at "
                "FROM shadow_trades WHERE settled=1 AND settlement_result IN ('yes','no') "
                "ORDER BY settled_at ASC"
            ):
                shadow_settled.append(dict(row))

            row = db.execute(
                "SELECT COUNT(*) FROM shadow_trades WHERE settled=0"
            ).fetchone()
            shadow_pending = row[0] if row else 0
        except Exception:
            pass  # Table may not exist yet

        db.close()

        return {
            "trades": trades,
            "settled": settled,
            "categories": cat_stats,
            "scans": scans,
            "daily_pnl": round(daily_pnl, 2),
            "total_pnl": round(total_pnl, 2),
            "overrides": overrides,
            "bankroll": bankroll,
            "shadow_settled": shadow_settled,
            "shadow_pending": shadow_pending,
        }
    except Exception as e:
        db.close()
        return {"error": str(e), "trades": [], "categories": {}, "scans": []}


# ── Dashboard HTML ────────────────────────────────────────────────────────────

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>Edge Scanner — Control Dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&display=swap');

  * { margin:0; padding:0; box-sizing:border-box; }
  body {
    background: #0f172a; color: #e2e8f0;
    font-family: 'JetBrains Mono', 'SF Mono', monospace;
    padding: 20px;
  }

  .header {
    max-width: 1100px; margin: 0 auto 20px;
    display: flex; align-items: baseline; justify-content: space-between; flex-wrap: wrap; gap: 8px;
  }
  .header h1 { font-size: 20px; font-weight: 700; color: #f8fafc; }
  .header .meta { font-size: 11px; color: #64748b; }
  .header .meta .live-dot {
    display: inline-block; width: 7px; height: 7px; border-radius: 50%;
    background: #22c55e; margin-right: 4px; animation: pulse 2s infinite;
  }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }

  /* Summary cards */
  .summary-row {
    max-width: 1100px; margin: 0 auto 16px;
    display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 10px;
  }
  .card {
    background: #1e293b; border-radius: 10px; border: 1px solid #334155;
    padding: 14px 16px; text-align: center;
  }
  .card .label { font-size: 10px; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 6px; }
  .card .value { font-size: 22px; font-weight: 700; }
  .card .value.green { color: #22c55e; }
  .card .value.red { color: #ef4444; }
  .card .value.neutral { color: #94a3b8; }
  .card .value.yellow { color: #eab308; }

  /* Bankroll panel */
  .bankroll-section {
    max-width: 1100px; margin: 0 auto 16px;
  }
  .bankroll-section h3 {
    font-size: 11px; color: #64748b; text-transform: uppercase;
    letter-spacing: 0.08em; margin: 0 0 8px 4px;
  }
  .bankroll-row {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 10px;
  }
  .bankroll-row .card { border-color: #3b4f6b; }
  .bankroll-row .card.stopped { border-color: #ef4444; background: #1c1520; }
  .stop-bar {
    margin-top: 6px; height: 4px; border-radius: 2px; background: #334155; overflow: hidden;
  }
  .stop-bar-fill {
    height: 100%; border-radius: 2px; transition: width 0.5s ease;
  }

  /* Category config table */
  .config-section {
    max-width: 1100px; margin: 0 auto 16px;
    background: #1e293b; border-radius: 12px; border: 1px solid #334155;
    padding: 16px; overflow-x: auto;
  }
  .config-section h2 { font-size: 13px; color: #94a3b8; margin-bottom: 12px; text-transform: uppercase; letter-spacing: 0.05em; }

  table { width: 100%; border-collapse: collapse; font-size: 12px; }
  th { text-align: left; color: #64748b; font-weight: 600; padding: 8px 10px; border-bottom: 1px solid #334155; font-size: 10px; text-transform: uppercase; letter-spacing: 0.05em; }
  td { padding: 8px 10px; border-bottom: 1px solid #1e293b; vertical-align: middle; }
  tr:hover td { background: #162032; }

  .cat-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }
  .pnl-pos { color: #22c55e; font-weight: 600; }
  .pnl-neg { color: #ef4444; font-weight: 600; }
  .pnl-zero { color: #64748b; }
  .wr-good { color: #22c55e; }
  .wr-bad { color: #ef4444; }

  /* Slider styling */
  .edge-control { display: flex; align-items: center; gap: 8px; min-width: 200px; }
  .edge-control input[type=range] {
    flex: 1; height: 4px; -webkit-appearance: none; appearance: none;
    background: #334155; border-radius: 2px; outline: none;
  }
  .edge-control input[type=range]::-webkit-slider-thumb {
    -webkit-appearance: none; appearance: none;
    width: 14px; height: 14px; border-radius: 50%;
    background: #6366f1; cursor: pointer; border: 2px solid #818cf8;
  }
  .edge-control input[type=range]::-moz-range-thumb {
    width: 14px; height: 14px; border-radius: 50%;
    background: #6366f1; cursor: pointer; border: 2px solid #818cf8;
  }
  .edge-val { font-size: 13px; font-weight: 700; color: #e2e8f0; min-width: 42px; text-align: right; }
  .edge-saved { font-size: 10px; color: #22c55e; opacity: 0; transition: opacity 0.3s; }
  .edge-saved.show { opacity: 1; }

  /* Charts */
  .charts-row {
    max-width: 1100px; margin: 0 auto 16px;
    display: grid; grid-template-columns: 1fr 1fr; gap: 12px;
  }
  @media (max-width: 800px) { .charts-row { grid-template-columns: 1fr; } }
  .chart-wrap {
    background: #1e293b; border-radius: 12px; border: 1px solid #334155;
    padding: 16px;
  }
  .chart-wrap .subtitle { font-size: 11px; color: #64748b; margin-bottom: 10px; }
  .chart-wrap.full { grid-column: 1 / -1; }
  canvas { width: 100% !important; }

  /* Trades table */
  .trades-section {
    max-width: 1100px; margin: 0 auto 16px;
    background: #1e293b; border-radius: 12px; border: 1px solid #334155;
    padding: 16px; overflow-x: auto;
  }
  .trades-section h2 { font-size: 13px; color: #94a3b8; margin-bottom: 12px; text-transform: uppercase; letter-spacing: 0.05em; }
  .badge { padding: 2px 8px; border-radius: 4px; font-size: 10px; font-weight: 600; }
  .badge.win { background: #064e3b; color: #22c55e; }
  .badge.loss { background: #450a0a; color: #ef4444; }
  .badge.open { background: #1e3a5f; color: #60a5fa; }
  .badge.rejected { background: #451a03; color: #f97316; }
  .badge.unfilled { background: #27272a; color: #a1a1aa; }
  .badge.paper { background: #3b3514; color: #eab308; font-size: 9px; margin-left: 4px; }

  .footer { max-width: 1100px; margin: 20px auto 0; text-align: center; font-size: 10px; color: #475569; }
</style>
</head>
<body>

<div class="header">
  <h1>Edge Scanner Dashboard</h1>
  <div class="meta"><span class="live-dot"></span>Auto-updating every __REFRESH__s</div>
</div>

<!-- Summary Cards -->
<div class="summary-row" id="summary-cards"></div>

<!-- Bankroll Management -->
<div class="bankroll-section" id="bankroll-section">
  <h3>Bankroll Management</h3>
  <div class="bankroll-row" id="bankroll-panel"></div>
</div>

<!-- Category Controls -->
<div class="config-section">
  <h2>Category Controls — Min Edge %</h2>
  <table>
    <thead>
      <tr>
        <th>Category</th>
        <th>Trades</th>
        <th>Win Rate</th>
        <th>P&L</th>
        <th>Avg Edge</th>
        <th style="min-width:240px">Min Edge Threshold</th>
      </tr>
    </thead>
    <tbody id="cat-table"></tbody>
  </table>
</div>

<!-- Charts -->
<div class="charts-row">
  <div class="chart-wrap full">
    <div class="subtitle">Cumulative P&L Over Time</div>
    <canvas id="pnlChart" height="70"></canvas>
  </div>
</div>
<div class="charts-row">
  <div class="chart-wrap">
    <div class="subtitle">P&L by Category</div>
    <canvas id="catPnlChart" height="110"></canvas>
  </div>
  <div class="chart-wrap">
    <div class="subtitle">Win / Loss by Category</div>
    <canvas id="winLossChart" height="110"></canvas>
  </div>
</div>
<div class="charts-row">
  <div class="chart-wrap">
    <div class="subtitle">Edge % Distribution (Settled Trades)</div>
    <canvas id="edgeDistChart" height="110"></canvas>
  </div>
  <div class="chart-wrap">
    <div class="subtitle">P&L by Hour of Day (UTC)</div>
    <canvas id="hourChart" height="110"></canvas>
  </div>
</div>

<!-- Shadow Trade Analysis -->
<div class="config-section">
  <h2>Shadow Trade Analysis — Model Calibration</h2>
  <div id="shadow-summary" style="margin-bottom:14px;font-size:12px;color:#94a3b8"></div>
  <div class="charts-row" style="max-width:100%;margin:0 0 16px 0">
    <div class="chart-wrap">
      <div class="subtitle">Model Calibration — Fair Value vs Actual Win Rate</div>
      <canvas id="calibrationChart" height="130"></canvas>
    </div>
    <div class="chart-wrap">
      <div class="subtitle">Missed Profit by Edge Bucket — If You Had Traded</div>
      <canvas id="missedProfitChart" height="130"></canvas>
    </div>
  </div>
  <div class="charts-row" style="max-width:100%;margin:0">
    <div class="chart-wrap full">
      <div class="subtitle">Cumulative Shadow P&L — What Every Positive-Edge Trade Would Have Earned</div>
      <canvas id="shadowPnlChart" height="70"></canvas>
    </div>
  </div>
</div>

<!-- Recent Trades -->
<div class="trades-section">
  <h2>Recent Trades</h2>
  <table>
    <thead>
      <tr>
        <th>Time</th>
        <th>Category</th>
        <th>Ticker</th>
        <th>Dir</th>
        <th>Edge%</th>
        <th>Price</th>
        <th>Result</th>
        <th>P&L</th>
      </tr>
    </thead>
    <tbody id="trades-table"></tbody>
  </table>
</div>

<div class="footer">Edge Scanner Dashboard — reads from ~/.edge_scanner/trades.db</div>

<script>
const REFRESH_MS = __REFRESH__ * 1000;
const CATS = [
  {key: "crypto_hourly_btc", label: "Hourly BTC", color: "#f7931a"},
  {key: "crypto_hourly_eth", label: "Hourly ETH", color: "#627eea"},
  {key: "crypto_hourly_sol", label: "Hourly SOL", color: "#9945ff"},
  {key: "crypto_15min_btc",  label: "15min BTC",  color: "#f7931a"},
  {key: "crypto_15min_eth",  label: "15min ETH",  color: "#627eea"},
  {key: "crypto_15min_sol",  label: "15min SOL",  color: "#9945ff"},
];

let charts = {};
let lastData = null;

// ── Edge slider logic ────────────────────────────────────────────────────────

function onEdgeChange(catKey, val) {
  document.getElementById('edge-val-' + catKey).textContent = val + '%';
}

function onEdgeCommit(catKey, val) {
  fetch('/api/config', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({category: catKey, min_edge_pct: parseFloat(val)})
  }).then(r => r.json()).then(d => {
    const el = document.getElementById('edge-saved-' + catKey);
    if (el) { el.classList.add('show'); setTimeout(() => el.classList.remove('show'), 1500); }
  });
}

// ── Rendering ────────────────────────────────────────────────────────────────

function pnlClass(v) { return v > 0.001 ? 'pnl-pos' : v < -0.001 ? 'pnl-neg' : 'pnl-zero'; }
function pnlStr(v) { return (v >= 0 ? '+' : '') + v.toFixed(2); }

function renderBankroll(data) {
  const bk = data.bankroll || {};
  const el = document.getElementById('bankroll-panel');
  const section = document.getElementById('bankroll-section');
  if (!el) return;
  if (!bk.date) { section.style.display = 'none'; return; }
  section.style.display = '';

  const pnl = bk.daily_pnl || 0;
  const hwm = bk.hwm || 0;
  const stop = bk.stop_level || -20;
  const tier = bk.tier || 0;
  const contracts = bk.contracts || 1;
  const balance = bk.balance || 0;
  const stopped = bk.stopped || false;

  const pnlCls = pnl >= 0 ? 'green' : 'red';
  const stopCls = stop >= 0 ? 'green' : 'red';
  const statusCls = stopped ? 'red' : 'green';
  const statusText = stopped ? 'STOPPED' : 'ACTIVE';
  const stoppedCard = stopped ? ' stopped' : '';

  // Stop distance bar: how far P&L is from stop (as % of range)
  const range = hwm - stop;
  const dist = pnl - stop;
  const pct = range > 0 ? Math.max(0, Math.min(100, (dist / range) * 100)) : 100;
  const barColor = pct > 50 ? '#22c55e' : pct > 20 ? '#eab308' : '#ef4444';

  const tierNames = ['Base', 'Scaled', 'Strong', 'Max'];
  const tierLabel = tierNames[tier] || 'Base';

  el.innerHTML = `
    <div class="card${stoppedCard}"><div class="label">Daily P&L</div><div class="value ${pnlCls}">$${pnlStr(pnl)}</div></div>
    <div class="card${stoppedCard}"><div class="label">High Water Mark</div><div class="value neutral">$${hwm.toFixed(2)}</div></div>
    <div class="card${stoppedCard}"><div class="label">Stop Level</div><div class="value ${stopCls}">$${pnlStr(stop)}</div>
      <div class="stop-bar"><div class="stop-bar-fill" style="width:${pct.toFixed(0)}%;background:${barColor}"></div></div>
    </div>
    <div class="card${stoppedCard}"><div class="label">Tier</div><div class="value neutral">${tierLabel} (${contracts}x)</div></div>
    <div class="card${stoppedCard}"><div class="label">Balance</div><div class="value ${balance < 5 ? 'yellow' : 'neutral'}">$${balance.toFixed(2)}</div></div>
    <div class="card${stoppedCard}"><div class="label">Status</div><div class="value ${statusCls}">${statusText}</div></div>
  `;
}

function renderSummary(data) {
  const settled = data.settled || [];
  const totalTrades = settled.length;
  const wins = settled.filter(t => t.pnl > 0).length;
  const wr = totalTrades > 0 ? (wins / totalTrades * 100).toFixed(0) : '—';
  const wrClass = totalTrades === 0 ? 'neutral' : (wins/totalTrades >= 0.5 ? 'green' : 'red');
  const tpClass = data.total_pnl >= 0 ? 'green' : 'red';
  const dpClass = data.daily_pnl >= 0 ? 'green' : 'red';

  document.getElementById('summary-cards').innerHTML = `
    <div class="card"><div class="label">Total P&L</div><div class="value ${tpClass}">$${pnlStr(data.total_pnl)}</div></div>
    <div class="card"><div class="label">Today P&L</div><div class="value ${dpClass}">$${pnlStr(data.daily_pnl)}</div></div>
    <div class="card"><div class="label">Win Rate</div><div class="value ${wrClass}">${wr}%</div></div>
    <div class="card"><div class="label">Total Trades</div><div class="value neutral">${totalTrades}</div></div>
    <div class="card"><div class="label">Wins / Losses</div><div class="value neutral">${wins} / ${totalTrades - wins}</div></div>
  `;
}

function renderCatTable(data) {
  const cats = data.categories || {};
  const overrides = data.overrides || {};
  const settled = data.settled || [];

  let html = '';
  for (const c of CATS) {
    const s = cats[c.key] || {};
    const total = s.total_trades || 0;
    const wins = s.wins || 0;
    const pnl = s.total_pnl || 0;
    const wr = total > 0 ? (wins / total * 100).toFixed(0) : '—';
    const wrClass = total === 0 ? '' : (wins/total >= 0.5 ? 'wr-good' : 'wr-bad');

    // Avg edge from settled trades
    const catTrades = settled.filter(t => t.category === c.key);
    const avgEdge = catTrades.length > 0 ? (catTrades.reduce((s,t) => s + (t.edge_pct||0), 0) / catTrades.length).toFixed(1) : '—';

    // Current min_edge (from overrides or default)
    const curEdge = overrides[c.key]?.min_edge_pct ?? 25.0;

    html += `<tr>
      <td><span class="cat-dot" style="background:${c.color}"></span>${c.label}</td>
      <td>${total}</td>
      <td class="${wrClass}">${wr}%</td>
      <td class="${pnlClass(pnl)}">${pnlStr(pnl)}</td>
      <td>${avgEdge}%</td>
      <td>
        <div class="edge-control">
          <input type="range" min="5" max="50" step="1" value="${curEdge}"
            oninput="onEdgeChange('${c.key}', this.value)"
            onchange="onEdgeCommit('${c.key}', this.value)" />
          <span class="edge-val" id="edge-val-${c.key}">${curEdge}%</span>
          <span class="edge-saved" id="edge-saved-${c.key}">saved</span>
        </div>
      </td>
    </tr>`;
  }
  document.getElementById('cat-table').innerHTML = html;
}

function renderTradesTable(data) {
  const trades = (data.trades || []).slice(0, 50);
  let html = '';
  for (const t of trades) {
    const isSettled = t.settled === 1;
    const won = t.pnl > 0;
    const st = (t.order_status || '').toLowerCase();
    const resultBadge = st === 'rejected' ? '<span class="badge rejected">REJECTED</span>' :
      st === 'unfilled' ? '<span class="badge unfilled">UNFILLED</span>' :
      !isSettled ? '<span class="badge open">OPEN</span>' :
      (won ? '<span class="badge win">WIN</span>' : '<span class="badge loss">LOSS</span>');
    const paperBadge = t.paper_trade ? '<span class="badge paper">PAPER</span>' : '';
    const ts = (t.timestamp || '').replace('T', ' ').slice(0, 19);
    const cat = CATS.find(c => c.key === t.category);
    const catLabel = cat ? cat.label : t.category;
    const catColor = cat ? cat.color : '#64748b';
    const pnl = isSettled ? `<span class="${pnlClass(t.pnl)}">$${pnlStr(t.pnl)}</span>` : '—';

    html += `<tr>
      <td style="font-size:11px;color:#94a3b8">${ts}</td>
      <td><span class="cat-dot" style="background:${catColor}"></span>${catLabel}</td>
      <td style="font-size:11px">${t.ticker || ''}</td>
      <td>${(t.direction||'').toUpperCase()}</td>
      <td>${(t.edge_pct||0).toFixed(1)}%</td>
      <td>$${(t.fill_price || t.ask_price || 0).toFixed(2)}</td>
      <td>${resultBadge}${paperBadge}</td>
      <td>${pnl}</td>
    </tr>`;
  }
  document.getElementById('trades-table').innerHTML = html || '<tr><td colspan="8" style="text-align:center;color:#475569;padding:20px">No trades yet</td></tr>';
}

// ── Charts ───────────────────────────────────────────────────────────────────

function initCharts() {
  const gridColor = '#1e293b';
  const tickColor = '#475569';
  const defaultOpts = {
    responsive: true,
    maintainAspectRatio: true,
    plugins: { legend: { labels: { color: tickColor, font: { size: 10, family: "'JetBrains Mono'" } } } },
    scales: {
      x: { ticks: { color: tickColor, font: { size: 9 } }, grid: { color: gridColor } },
      y: { ticks: { color: tickColor, font: { size: 9 } }, grid: { color: gridColor } }
    }
  };

  // Cumulative P&L
  charts.pnl = new Chart(document.getElementById('pnlChart'), {
    type: 'line',
    data: { labels: [], datasets: [] },
    options: {
      ...defaultOpts,
      plugins: { ...defaultOpts.plugins, legend: { display: true, labels: { color: tickColor, font: { size: 10, family: "'JetBrains Mono'" } } } },
      elements: { point: { radius: 2 }, line: { tension: 0.2, borderWidth: 2 } },
    }
  });

  // P&L by category (bar)
  charts.catPnl = new Chart(document.getElementById('catPnlChart'), {
    type: 'bar',
    data: { labels: [], datasets: [] },
    options: { ...defaultOpts, plugins: { legend: { display: false } }, indexAxis: 'y' }
  });

  // Win/Loss by category (stacked bar)
  charts.winLoss = new Chart(document.getElementById('winLossChart'), {
    type: 'bar',
    data: { labels: [], datasets: [] },
    options: {
      ...defaultOpts,
      plugins: { legend: { display: true, labels: { color: tickColor, font: { size: 10, family: "'JetBrains Mono'" } } } },
      scales: { ...defaultOpts.scales, x: { ...defaultOpts.scales.x, stacked: true }, y: { ...defaultOpts.scales.y, stacked: true } }
    }
  });

  // Edge distribution (histogram)
  charts.edgeDist = new Chart(document.getElementById('edgeDistChart'), {
    type: 'bar',
    data: { labels: [], datasets: [] },
    options: { ...defaultOpts, plugins: { legend: { display: false } } }
  });

  // P&L by hour
  charts.hour = new Chart(document.getElementById('hourChart'), {
    type: 'bar',
    data: { labels: [], datasets: [] },
    options: { ...defaultOpts, plugins: { legend: { display: false } } }
  });

  // Shadow: Calibration scatter
  charts.calibration = new Chart(document.getElementById('calibrationChart'), {
    type: 'bar',
    data: { labels: [], datasets: [] },
    options: {
      ...defaultOpts,
      plugins: {
        legend: { display: true, labels: { color: '#475569', font: { size: 10, family: "'JetBrains Mono'" } } },
        tooltip: { callbacks: { label: ctx => ctx.dataset.label + ': ' + ctx.parsed.y.toFixed(0) + '%' } }
      },
      scales: {
        ...defaultOpts.scales,
        y: { ...defaultOpts.scales.y, min: 0, max: 100, title: { display: true, text: 'Win %', color: '#475569', font: { size: 10 } } }
      }
    }
  });

  // Shadow: Missed profit by edge bucket
  charts.missedProfit = new Chart(document.getElementById('missedProfitChart'), {
    type: 'bar',
    data: { labels: [], datasets: [] },
    options: {
      ...defaultOpts,
      plugins: { legend: { display: false } },
      scales: {
        ...defaultOpts.scales,
        y: { ...defaultOpts.scales.y, title: { display: true, text: 'P&L $', color: '#475569', font: { size: 10 } } }
      }
    }
  });

  // Shadow: Cumulative shadow P&L
  charts.shadowPnl = new Chart(document.getElementById('shadowPnlChart'), {
    type: 'line',
    data: { labels: [], datasets: [] },
    options: {
      ...defaultOpts,
      plugins: { ...defaultOpts.plugins, legend: { display: true, labels: { color: '#475569', font: { size: 10, family: "'JetBrains Mono'" } } } },
      elements: { point: { radius: 1 }, line: { tension: 0.2, borderWidth: 2 } },
    }
  });
}

function updateCharts(data) {
  const settled = data.settled || [];
  const cats = data.categories || {};

  // 1. Cumulative P&L (total + per-category lines)
  if (settled.length > 0) {
    // Total cumulative
    let cumPnl = 0;
    const totalLine = settled.map(t => { cumPnl += t.pnl; return cumPnl; });
    const labels = settled.map(t => (t.settled_at || '').slice(5, 16).replace('T', ' '));

    // Per-category cumulative
    const catCums = {};
    for (const c of CATS) catCums[c.key] = { cum: 0, data: [] };
    for (const t of settled) {
      for (const c of CATS) {
        if (t.category === c.key) catCums[c.key].cum += t.pnl;
        catCums[c.key].data.push(catCums[c.key].cum);
      }
    }

    const datasets = [{
      label: 'Total', data: totalLine.map(v => +v.toFixed(2)),
      borderColor: '#e2e8f0', backgroundColor: 'rgba(226,232,240,0.1)',
      fill: true, borderWidth: 2
    }];
    for (const c of CATS) {
      if (catCums[c.key].cum !== 0 || catCums[c.key].data.some(v => v !== 0)) {
        datasets.push({
          label: c.label, data: catCums[c.key].data.map(v => +v.toFixed(2)),
          borderColor: c.color, backgroundColor: 'transparent',
          borderWidth: 1.5, borderDash: [4, 2]
        });
      }
    }

    charts.pnl.data.labels = labels;
    charts.pnl.data.datasets = datasets;
    charts.pnl.update('none');
  }

  // 2. P&L by category (horizontal bar)
  const catLabels = CATS.map(c => c.label);
  const catPnls = CATS.map(c => +(cats[c.key]?.total_pnl || 0).toFixed(2));
  const catColors = CATS.map((c, i) => catPnls[i] >= 0 ? '#22c55e' : '#ef4444');
  charts.catPnl.data.labels = catLabels;
  charts.catPnl.data.datasets = [{ data: catPnls, backgroundColor: catColors, borderRadius: 4 }];
  charts.catPnl.update('none');

  // 3. Win/Loss stacked bar
  charts.winLoss.data.labels = catLabels;
  charts.winLoss.data.datasets = [
    { label: 'Wins', data: CATS.map(c => cats[c.key]?.wins || 0), backgroundColor: '#22c55e', borderRadius: 4 },
    { label: 'Losses', data: CATS.map(c => cats[c.key]?.losses || 0), backgroundColor: '#ef4444', borderRadius: 4 },
  ];
  charts.winLoss.update('none');

  // 4. Edge distribution
  if (settled.length > 0) {
    const bins = [0,5,10,15,20,25,30,35,40,50,60,80];
    const counts = new Array(bins.length).fill(0);
    for (const t of settled) {
      const e = Math.abs(t.edge_pct || 0);
      for (let i = bins.length - 1; i >= 0; i--) {
        if (e >= bins[i]) { counts[i]++; break; }
      }
    }
    const binLabels = bins.map((b, i) => i < bins.length - 1 ? `${b}-${bins[i+1]}%` : `${b}%+`);
    charts.edgeDist.data.labels = binLabels;
    charts.edgeDist.data.datasets = [{ data: counts, backgroundColor: '#6366f1', borderRadius: 4 }];
    charts.edgeDist.update('none');
  }

  // 5. P&L by hour of day
  const hourPnl = new Array(24).fill(0);
  for (const t of settled) {
    const h = parseInt((t.settled_at || '').slice(11, 13));
    if (!isNaN(h)) hourPnl[h] += t.pnl;
  }
  charts.hour.data.labels = Array.from({length:24}, (_,i) => `${String(i).padStart(2,'0')}:00`);
  charts.hour.data.datasets = [{
    data: hourPnl.map(v => +v.toFixed(2)),
    backgroundColor: hourPnl.map(v => v >= 0 ? '#22c55e' : '#ef4444'),
    borderRadius: 4
  }];
  charts.hour.update('none');
}

// ── Shadow Trade Analysis ────────────────────────────────────────────────────

function renderShadowAnalysis(data) {
  const shadow = data.shadow_settled || [];
  const pending = data.shadow_pending || 0;
  const el = document.getElementById('shadow-summary');

  if (shadow.length === 0) {
    el.innerHTML = `Tracking ${pending} pending shadow trades. Waiting for settlements to build calibration data...`;
    return;
  }

  // Summary stats
  const totalShadow = shadow.length;
  const wins = shadow.filter(t => t.won === 1).length;
  const wr = (wins / totalShadow * 100).toFixed(1);
  const totalPnl = shadow.reduce((s, t) => s + (t.hypothetical_pnl || 0), 0);
  const avgEdge = (shadow.reduce((s, t) => s + (t.gross_edge_pct || 0), 0) / totalShadow).toFixed(1);

  // Find optimal threshold: lowest edge bucket with >50% win rate and positive cumulative P&L
  const edgeBuckets = [0, 3, 5, 8, 10, 15, 20, 25, 30, 40];
  let optimalThreshold = '?';
  for (const minE of edgeBuckets) {
    const above = shadow.filter(t => t.gross_edge_pct >= minE);
    if (above.length >= 5) {
      const aboveWins = above.filter(t => t.won === 1).length;
      const abovePnl = above.reduce((s, t) => s + (t.hypothetical_pnl || 0), 0);
      if (aboveWins / above.length > 0.5 && abovePnl > 0) {
        optimalThreshold = minE + '%';
        break;
      }
    }
  }

  const pnlColor = totalPnl >= 0 ? '#22c55e' : '#ef4444';
  el.innerHTML =
    `<span style="color:#e2e8f0">${totalShadow}</span> settled shadows | ` +
    `<span style="color:${pnlColor}">$${totalPnl >= 0 ? '+' : ''}${totalPnl.toFixed(2)}</span> hypothetical P&L | ` +
    `<span style="color:#e2e8f0">${wr}%</span> win rate | ` +
    `avg edge <span style="color:#e2e8f0">${avgEdge}%</span> | ` +
    `<span style="color:#94a3b8">${pending} pending</span> | ` +
    `suggested threshold: <span style="color:#6366f1;font-weight:700">${optimalThreshold}</span>`;

  // 1. Calibration chart: model fair value buckets vs actual win rate
  const calBuckets = [
    {lo: 0.05, hi: 0.20, label: '5-20%'},
    {lo: 0.20, hi: 0.35, label: '20-35%'},
    {lo: 0.35, hi: 0.50, label: '35-50%'},
    {lo: 0.50, hi: 0.65, label: '50-65%'},
    {lo: 0.65, hi: 0.80, label: '65-80%'},
    {lo: 0.80, hi: 0.95, label: '80-95%'},
  ];
  const calLabels = [];
  const calPredicted = [];
  const calActual = [];
  const calCounts = [];
  for (const b of calBuckets) {
    const inBucket = shadow.filter(t => t.model_fair >= b.lo && t.model_fair < b.hi);
    if (inBucket.length < 2) continue;
    const midpoint = ((b.lo + b.hi) / 2 * 100);
    const actualWR = inBucket.filter(t => t.won === 1).length / inBucket.length * 100;
    calLabels.push(b.label + ` (n=${inBucket.length})`);
    calPredicted.push(+midpoint.toFixed(1));
    calActual.push(+actualWR.toFixed(1));
  }
  charts.calibration.data.labels = calLabels;
  charts.calibration.data.datasets = [
    { label: 'Model Predicted', data: calPredicted, backgroundColor: 'rgba(99,102,241,0.5)', borderColor: '#6366f1', borderWidth: 1, borderRadius: 4 },
    { label: 'Actual Win Rate', data: calActual, backgroundColor: 'rgba(34,197,94,0.5)', borderColor: '#22c55e', borderWidth: 1, borderRadius: 4 },
  ];
  charts.calibration.update('none');

  // 2. Missed profit by edge bucket
  const profitBuckets = [
    {lo: 0, hi: 5, label: '0-5%'},
    {lo: 5, hi: 10, label: '5-10%'},
    {lo: 10, hi: 15, label: '10-15%'},
    {lo: 15, hi: 20, label: '15-20%'},
    {lo: 20, hi: 25, label: '20-25%'},
    {lo: 25, hi: 35, label: '25-35%'},
    {lo: 35, hi: 60, label: '35-60%'},
  ];
  const profitLabels = [];
  const profitData = [];
  const profitColors = [];
  const profitCounts = [];
  for (const b of profitBuckets) {
    const inBucket = shadow.filter(t => t.gross_edge_pct >= b.lo && t.gross_edge_pct < b.hi);
    if (inBucket.length === 0) { profitLabels.push(b.label); profitData.push(0); profitColors.push('#334155'); profitCounts.push(0); continue; }
    const pnl = inBucket.reduce((s, t) => s + (t.hypothetical_pnl || 0), 0);
    profitLabels.push(b.label + ` (n=${inBucket.length})`);
    profitData.push(+pnl.toFixed(2));
    profitColors.push(pnl >= 0 ? '#22c55e' : '#ef4444');
    profitCounts.push(inBucket.length);
  }
  charts.missedProfit.data.labels = profitLabels;
  charts.missedProfit.data.datasets = [{ data: profitData, backgroundColor: profitColors, borderRadius: 4 }];
  charts.missedProfit.update('none');

  // 3. Cumulative shadow P&L over time (total + by threshold level)
  if (shadow.length > 0) {
    const sorted = [...shadow].sort((a, b) => (a.settled_at || '').localeCompare(b.settled_at || ''));
    const labels = sorted.map(t => (t.settled_at || '').slice(5, 16).replace('T', ' '));

    const thresholds = [
      {min: 0, label: 'All > 0%', color: '#94a3b8'},
      {min: 5, label: '> 5%', color: '#6366f1'},
      {min: 10, label: '> 10%', color: '#22d3ee'},
      {min: 15, label: '> 15%', color: '#eab308'},
      {min: 25, label: '> 25%', color: '#22c55e'},
    ];

    const datasets = [];
    for (const th of thresholds) {
      let cum = 0;
      const line = sorted.map(t => {
        if (t.gross_edge_pct >= th.min) cum += (t.hypothetical_pnl || 0);
        return +cum.toFixed(2);
      });
      datasets.push({
        label: th.label, data: line, borderColor: th.color,
        backgroundColor: 'transparent', borderWidth: th.min === 0 ? 2 : 1.5,
        borderDash: th.min === 25 ? [] : [4, 2],
      });
    }

    charts.shadowPnl.data.labels = labels;
    charts.shadowPnl.data.datasets = datasets;
    charts.shadowPnl.update('none');
  }
}

// ── Fetch & refresh ──────────────────────────────────────────────────────────

async function refresh() {
  try {
    const r = await fetch('/api/data');
    const data = await r.json();
    lastData = data;
    renderSummary(data);
    renderBankroll(data);
    renderCatTable(data);
    renderTradesTable(data);
    updateCharts(data);
    renderShadowAnalysis(data);
  } catch (e) {
    console.error('Refresh error:', e);
  }
}

initCharts();
refresh();
setInterval(refresh, REFRESH_MS);
</script>
</body>
</html>"""


# ── HTTP Server ───────────────────────────────────────────────────────────────

class DashboardHandler(http.server.BaseHTTPRequestHandler):
    refresh_sec = DEFAULT_REFRESH

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/api/data":
            data = get_dashboard_data()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(data, default=str).encode())

        elif parsed.path == "/" or parsed.path == "/index.html":
            html = DASHBOARD_HTML.replace("__REFRESH__", str(self.refresh_sec))
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(html.encode())

        else:
            self.send_error(404)

    def do_POST(self):
        parsed = urlparse(self.path)

        if parsed.path == "/api/config":
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length > 0 else {}

            category = body.get("category", "")
            min_edge = body.get("min_edge_pct")

            if not category or min_edge is None:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Missing category or min_edge_pct"}).encode())
                return

            # Clamp to safe range
            min_edge = max(5.0, min(50.0, float(min_edge)))

            overrides = load_overrides()
            if category not in overrides:
                overrides[category] = {}
            overrides[category]["min_edge_pct"] = min_edge
            save_overrides(overrides)

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True, "category": category, "min_edge_pct": min_edge}).encode())

        else:
            self.send_error(404)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress per-request logging noise


def main():
    parser = argparse.ArgumentParser(description="Edge Scanner Control Dashboard")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"Port (default {DEFAULT_PORT})")
    parser.add_argument("--refresh", type=int, default=DEFAULT_REFRESH, help=f"Auto-refresh seconds (default {DEFAULT_REFRESH})")
    args = parser.parse_args()

    DashboardHandler.refresh_sec = args.refresh

    db_status = "FOUND" if DB_PATH.exists() else "NOT FOUND"

    print(f"\n{'='*50}")
    print(f"  Edge Scanner Control Dashboard")
    print(f"{'='*50}")
    print(f"  URL:      http://localhost:{args.port}")
    print(f"  Refresh:  {args.refresh}s")
    print(f"  Database: {DB_PATH}")
    print(f"  DB:       {db_status}")
    print(f"{'='*50}")
    print(f"  Press Ctrl+C to stop.\n")

    server = http.server.HTTPServer(("0.0.0.0", args.port), DashboardHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.shutdown()


if __name__ == "__main__":
    main()
