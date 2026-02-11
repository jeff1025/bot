#!/usr/bin/env python3
"""
Vol Model Calibration Dashboard — Live Auto-Updating
=====================================================
Drop this file in your project folder alongside vol_kalshi_trades.jsonl
and sim_trades.jsonl. Run it and open http://localhost:8050 in your browser.

Usage:
    python vol_dashboard.py
    python vol_dashboard.py --port 8050
    python vol_dashboard.py --refresh 15        (seconds between auto-refresh)

Reads:
    - vol_kalshi_trades.jsonl  (live Kalshi trades)
    - sim_trades.jsonl         (paper trades, optional)

No pip installs needed — uses only Python stdlib.
"""

import http.server
import json
import os
import sys
import argparse
from pathlib import Path
from urllib.parse import urlparse, parse_qs

# Config
DATA_DIR = Path(os.path.expanduser("~")) / ".arb_data"
LIVE_FILE = DATA_DIR / "vol_kalshi_trades.jsonl"
SIM_FILE = DATA_DIR / "sim_trades.jsonl"
SIM_V2_FILE = DATA_DIR / "sim_trades_v2.jsonl"
DEFAULT_PORT = 8050
DEFAULT_REFRESH = 30  # seconds


def load_trades(filepath: Path, source: str) -> list:
    """Load settled trades from a JSONL file."""
    if not filepath.exists():
        return []
    trades = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                t = json.loads(line)
            except json.JSONDecodeError:
                continue
            if t.get("event") != "SETTLE":
                continue
            # Normalize field names across live trader and paper trader schemas
            # Live: market_price, implied_vol, seconds_left, timestamp, blend_info
            # Paper: mkt_price, iv, sec_left, ts, (no blend_info)
            mkt = t.get("market_price") or t.get("mkt_price") or t.get("fill_price", 0) or 0
            iv = t.get("implied_vol") or t.get("iv", 0) or 0
            sec = t.get("seconds_left") or t.get("sec_left", 0) or 0
            ts = t.get("timestamp") or t.get("ts", "")
            trades.append({
                "tid": t.get("trade_id", "?"),
                "source": source,
                "asset": t.get("asset", "?"),
                "dir": t.get("dir", "?"),
                "mkt": round(mkt, 4),
                "fair": round(t.get("model_fair", 0) or 0, 4),
                "won": bool(t.get("won", False)),
                "edge": round(t.get("edge_pct", 0) or 0, 1),
                "mv": round(t.get("model_vol", 0) or 0, 4),
                "iv": round(iv, 4),
                "bi": t.get("blend_info", "") or "",
                "sec": round(sec),
                "pnl": round(t.get("pnl", 0) or 0, 4),
                "cost": round(t.get("cost", 0) or 0, 4),
                "ts": ts,
            })
    return trades


def get_all_trades() -> dict:
    """Load trades from all sources, return as JSON-ready dict."""
    live = load_trades(LIVE_FILE, "live")
    sim = load_trades(SIM_FILE, "paper")
    bell = load_trades(SIM_V2_FILE, "bell")
    # Sort each by timestamp for proper cumulative ordering
    for group in [live, sim, bell]:
        group.sort(key=lambda t: t.get("ts", ""))
    return {
        "live": live,
        "paper": sim,
        "bell": bell,
        "live_file": str(LIVE_FILE),
        "sim_file": str(SIM_FILE),
        "bell_file": str(SIM_V2_FILE),
        "live_exists": LIVE_FILE.exists(),
        "sim_exists": SIM_FILE.exists(),
        "bell_exists": SIM_V2_FILE.exists(),
    }


DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>Vol Model Calibration — Live</title>
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
    max-width: 960px; margin: 0 auto 16px;
    display: flex; align-items: baseline; justify-content: space-between; flex-wrap: wrap; gap: 8px;
  }
  .header h1 { font-size: 18px; font-weight: 700; color: #f8fafc; }
  .header .meta { font-size: 11px; color: #64748b; }
  .header .meta .live-dot {
    display: inline-block; width: 7px; height: 7px; border-radius: 50%;
    background: #22c55e; margin-right: 4px; animation: pulse 2s infinite;
  }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }

  .controls {
    max-width: 960px; margin: 0 auto 12px;
    display: flex; gap: 6px; flex-wrap: wrap; align-items: center;
  }
  .controls .group { display: flex; gap: 4px; align-items: center; }
  .controls .group-label { font-size: 10px; color: #475569; margin-right: 4px; text-transform: uppercase; letter-spacing: 0.05em; }
  .controls .spacer { flex: 1; }
  button {
    padding: 5px 14px; border-radius: 6px; border: none; cursor: pointer;
    font-size: 11px; font-weight: 600; font-family: inherit;
    background: #1e293b; color: #64748b; transition: all 0.15s;
  }
  button.active { background: #6366f1; color: #fff; }
  button.active-btc { background: #f7931a; color: #fff; }
  button.active-eth { background: #627eea; color: #fff; }
  button.active-sol { background: #9945ff; color: #fff; }
  button.active-all { background: #475569; color: #fff; }
  button.toggle-on { background: #334155; color: #e2e8f0; border: 1.5px solid #6366f1; }
  button.toggle-off { background: #1e293b; color: #475569; border: 1.5px solid #334155; opacity: 0.6; }
  button .shape { margin-right: 4px; font-size: 13px; }

  .chart-wrap {
    max-width: 960px; margin: 0 auto 12px;
    background: #1e293b; border-radius: 12px; border: 1px solid #334155;
    padding: 16px;
  }
  .chart-wrap .subtitle { font-size: 11px; color: #64748b; margin-bottom: 12px; }
  canvas { width: 100% !important; }

  .insight {
    max-width: 960px; margin: 0 auto 12px;
    background: #1a1a2e; border-radius: 10px; padding: 14px 16px;
    border: 1px solid #2d2d4a; font-size: 12px; line-height: 1.7; color: #94a3b8;
  }
  .insight .title { font-weight: 700; color: #a78bfa; margin-bottom: 4px; }

  .stats {
    max-width: 960px; margin: 0 auto 16px;
    display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px;
  }
  .stat-card {
    background: #1e293b; border-radius: 10px; padding: 14px 16px; border: 1px solid #334155;
  }
  .stat-card .label { font-size: 10px; color: #64748b; margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.05em; }
  .stat-card .value { font-size: 22px; font-weight: 700; }
  .stat-card .sub { font-size: 10px; color: #475569; margin-top: 2px; }

  /* Date range picker */
  .date-range-bar {
    max-width: 960px; margin: 0 auto 12px;
    background: #1e293b; border-radius: 10px; padding: 10px 14px;
    border: 1px solid #334155;
    display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
  }
  .date-range-bar .dr-label {
    font-size: 10px; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em;
    white-space: nowrap;
  }
  .date-range-bar input[type="datetime-local"] {
    background: #0f172a; color: #e2e8f0; border: 1px solid #334155;
    border-radius: 6px; padding: 5px 8px; font-size: 11px; font-family: inherit;
    outline: none; transition: border-color 0.15s;
  }
  .date-range-bar input[type="datetime-local"]:focus {
    border-color: #6366f1;
  }
  .date-range-bar input[type="datetime-local"]::-webkit-calendar-picker-indicator {
    filter: invert(0.6);
    cursor: pointer;
  }
  .date-range-bar .dr-presets {
    display: flex; gap: 4px; margin-left: 4px;
  }
  .date-range-bar .dr-presets button {
    padding: 4px 10px; font-size: 10px; border-radius: 5px;
  }
  .date-range-bar .dr-presets button.active { background: #6366f1; color: #fff; }
  .date-range-bar .dr-sep {
    color: #475569; font-size: 12px; margin: 0 2px;
  }
  .date-range-bar .dr-info {
    font-size: 10px; color: #475569; margin-left: auto; white-space: nowrap;
  }
</style>
</head>
<body>

<div class="header">
  <h1>Vol Model Calibration</h1>
  <div class="meta">
    <span class="live-dot"></span>Auto-refresh every <span id="refresh-interval">__REFRESH__</span>s
    · Last update: <span id="last-update">—</span>
    · <span id="trade-count">—</span>
  </div>
</div>

<div class="controls">
  <div class="group" id="view-tabs"></div>
  <div class="spacer"></div>
  <div class="group">
    <span class="group-label">Source</span>
    <div id="source-toggles"></div>
  </div>
  <div class="group">
    <span class="group-label">Asset</span>
    <div id="asset-tabs" style="display:flex;gap:4px;"></div>
  </div>
</div>

<div class="date-range-bar">
  <span class="dr-label">Range</span>
  <div class="dr-presets" id="dr-presets">
    <button data-preset="1h">1H</button>
    <button data-preset="4h">4H</button>
    <button data-preset="12h">12H</button>
    <button data-preset="24h">24H</button>
    <button data-preset="3d">3D</button>
    <button data-preset="7d">7D</button>
    <button data-preset="30d">30D</button>
    <button data-preset="all" class="active">ALL</button>
  </div>
  <span class="dr-sep">│</span>
  <input type="datetime-local" id="dr-from" step="900" title="Start time (15-min steps)"/>
  <span class="dr-sep">→</span>
  <input type="datetime-local" id="dr-to" step="900" title="End time (15-min steps)"/>
  <span class="dr-info" id="dr-info"></span>
</div>

<div class="stats" id="stats-row"></div>

<div class="chart-wrap">
  <div class="subtitle" id="chart-subtitle"></div>
  <canvas id="mainChart" height="380"></canvas>
</div>

<div class="insight" id="insight-box"></div>

<script>
const REFRESH_SEC = __REFRESH__;
const COLORS = {
  win: "#22c55e", loss: "#ef4444", model: "#8b5cf6", market: "#f59e0b",
  grid: "#1e293b", bg: "#0f172a", surface: "#1e293b", text: "#e2e8f0",
  muted: "#64748b", BTC: "#f7931a", ETH: "#627eea", SOL: "#9945ff",
};

// Source config: shape, label, symbol for legend
const SOURCES = {
  live:  { shape: "circle",   symbol: "●", label: "Live $",  color: "#6366f1" },
  paper: { shape: "triangle", symbol: "▲", label: "Paper 📋", color: "#06b6d4" },
  bell:  { shape: "rectRot",  symbol: "◆", label: "Bell v2 🔔", color: "#f59e0b" },
};

let allData = { live: [], paper: [] };
let currentView = "scatter";
let sourcesEnabled = { live: true, paper: true, bell: true };
let currentAsset = "all";
let chart = null;

// Date range state
let dateRangeFrom = null;   // Date object or null (=beginning of time)
let dateRangeTo = null;     // Date object or null (=now)
let activePreset = "all";

function initDateRange() {
  const presetBtns = document.querySelectorAll("#dr-presets button");
  presetBtns.forEach(b => {
    b.onclick = () => {
      activePreset = b.dataset.preset;
      applyPreset(activePreset);
      updatePresetStyles();
      render();
    };
  });

  const fromInput = document.getElementById("dr-from");
  const toInput = document.getElementById("dr-to");

  fromInput.addEventListener("change", () => {
    activePreset = "custom";
    dateRangeFrom = fromInput.value ? new Date(fromInput.value) : null;
    updatePresetStyles();
    render();
  });
  toInput.addEventListener("change", () => {
    activePreset = "custom";
    dateRangeTo = toInput.value ? new Date(toInput.value) : null;
    updatePresetStyles();
    render();
  });
}

function applyPreset(preset) {
  const fromInput = document.getElementById("dr-from");
  const toInput = document.getElementById("dr-to");

  if (preset === "all") {
    dateRangeFrom = null;
    dateRangeTo = null;
    fromInput.value = "";
    toInput.value = "";
    return;
  }

  const now = new Date();
  // Snap 'to' to the next 15-min boundary
  dateRangeTo = null; // means "now"
  toInput.value = "";

  const hours = { "1h": 1, "4h": 4, "12h": 12, "24h": 24, "3d": 72, "7d": 168, "30d": 720 };
  const h = hours[preset] || 0;
  const from = new Date(now.getTime() - h * 3600000);
  // Snap down to 15-min boundary
  from.setMinutes(Math.floor(from.getMinutes() / 15) * 15, 0, 0);
  dateRangeFrom = from;
  fromInput.value = toLocalISO(from);
}

function toLocalISO(d) {
  // Format as YYYY-MM-DDTHH:MM for datetime-local input
  const pad = n => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function updatePresetStyles() {
  document.querySelectorAll("#dr-presets button").forEach(b => {
    b.className = b.dataset.preset === activePreset ? "active" : "";
  });
}

function updateDateRangeInfo(filteredCount, totalCount) {
  const info = document.getElementById("dr-info");
  if (activePreset === "all") {
    info.textContent = "";
  } else {
    info.textContent = `${filteredCount} of ${totalCount} trades in range`;
  }
}

function buildTabs() {
  const views = [
    { id: "scatter", label: "Price Scatter" },
    { id: "calibration", label: "Calibration" },
    { id: "vol", label: "Vol Scatter" },
    { id: "cumulative", label: "Cumulative P&L" },
  ];
  const assets = [
    { id: "all", label: "ALL" },
    { id: "BTC", label: "BTC" },
    { id: "ETH", label: "ETH" },
    { id: "SOL", label: "SOL" },
  ];

  const vt = document.getElementById("view-tabs");
  views.forEach(v => {
    const b = document.createElement("button");
    b.textContent = v.label;
    b.dataset.id = v.id;
    b.onclick = () => { currentView = v.id; render(); };
    vt.appendChild(b);
  });

  const st = document.getElementById("source-toggles");
  st.style.display = "flex";
  st.style.gap = "4px";
  Object.entries(SOURCES).forEach(([id, cfg]) => {
    const b = document.createElement("button");
    b.innerHTML = `<span class="shape">${cfg.symbol}</span>${cfg.label}`;
    b.dataset.id = id;
    b.onclick = () => {
      sourcesEnabled[id] = !sourcesEnabled[id];
      // Don't allow all off
      if (!Object.values(sourcesEnabled).some(v => v)) sourcesEnabled[id] = true;
      render();
    };
    st.appendChild(b);
  });

  const at = document.getElementById("asset-tabs");
  assets.forEach(a => {
    const b = document.createElement("button");
    b.textContent = a.label;
    b.dataset.id = a.id;
    b.onclick = () => { currentAsset = a.id; render(); };
    at.appendChild(b);
  });
}

function updateTabStyles() {
  document.querySelectorAll("#view-tabs button").forEach(b => {
    b.className = b.dataset.id === currentView ? "active" : "";
  });
  document.querySelectorAll("#source-toggles button").forEach(b => {
    b.className = sourcesEnabled[b.dataset.id] ? "toggle-on" : "toggle-off";
  });
  document.querySelectorAll("#asset-tabs button").forEach(b => {
    const id = b.dataset.id;
    if (id === currentAsset) {
      b.className = id === "all" ? "active-all" : `active-${id.toLowerCase()}`;
    } else {
      b.className = "";
    }
  });
}

function getFiltered() {
  let trades = [];
  if (sourcesEnabled.live) trades.push(...allData.live);
  if (sourcesEnabled.paper) trades.push(...allData.paper);
  if (sourcesEnabled.bell) trades.push(...(allData.bell || []));
  if (currentAsset !== "all") trades = trades.filter(t => t.asset === currentAsset);
  // Sort by timestamp for proper chronological ordering
  trades.sort((a, b) => (a.ts || "").localeCompare(b.ts || ""));

  const totalBeforeDateFilter = trades.length;

  // Apply date range filter
  if (dateRangeFrom || dateRangeTo) {
    trades = trades.filter(t => {
      if (!t.ts) return false;
      const tradeTime = new Date(t.ts);
      if (dateRangeFrom && tradeTime < dateRangeFrom) return false;
      if (dateRangeTo && tradeTime > dateRangeTo) return false;
      return true;
    });
  }

  updateDateRangeInfo(trades.length, totalBeforeDateFilter);
  return trades;
}

function getFilteredBySource(source) {
  let trades = [...(allData[source] || [])];
  if (currentAsset !== "all") trades = trades.filter(t => t.asset === currentAsset);
  return trades;
}

function sourceLabel() {
  const parts = [];
  if (sourcesEnabled.live) parts.push("live");
  if (sourcesEnabled.paper) parts.push("paper");
  if (sourcesEnabled.bell) parts.push("bell v2");
  return parts.join(" + ");
}

function renderStats(trades) {
  const wins = trades.filter(t => t.won).length;
  const losses = trades.length - wins;
  const wr = trades.length > 0 ? (wins / trades.length * 100).toFixed(0) : "—";
  const pnl = trades.reduce((s, t) => s + t.pnl, 0).toFixed(2);
  const withIV = trades.filter(t => t.iv > 0);
  const avgRatio = withIV.length > 0
    ? (withIV.reduce((s, t) => s + t.mv / t.iv, 0) / withIV.length).toFixed(2)
    : "—";
  const avgFair = trades.length > 0 ? trades.reduce((s, t) => s + t.fair, 0) / trades.length * 100 : 0;
  const actualWR = trades.length > 0 ? wins / trades.length * 100 : 0;
  const calGap = (avgFair - actualWR).toFixed(0);
  const avgMkt = trades.length > 0 ? (trades.reduce((s,t)=>s+t.mkt,0)/trades.length*100).toFixed(0) : "—";

  const cards = [
    { label: "Record", value: `${wins}W / ${losses}L`, sub: `${wr}% win rate`, color: wins > losses ? COLORS.win : COLORS.loss },
    { label: "Total P&L", value: `$${pnl}`, sub: `${trades.length} trades`, color: parseFloat(pnl) >= 0 ? COLORS.win : COLORS.loss },
    { label: "Avg Model/IV", value: `${avgRatio}x`, sub: "model vol ÷ implied vol", color: COLORS.model },
    { label: "Cal. Gap", value: `${calGap}pp`, sub: "model predicted − actual WR", color: "#f97316" },
    { label: "Avg Mkt Price", value: `${avgMkt}¢`, sub: "average entry price", color: COLORS.market },
  ];

  const row = document.getElementById("stats-row");
  row.innerHTML = cards.map(c => `
    <div class="stat-card">
      <div class="label">${c.label}</div>
      <div class="value" style="color:${c.color}">${c.value}</div>
      <div class="sub">${c.sub}</div>
    </div>
  `).join("");

  document.getElementById("trade-count").textContent = `${trades.length} trades (${sourceLabel()})`;
}

function destroyChart() {
  if (chart) { chart.destroy(); chart = null; }
}

// Build scatter datasets split by source × outcome
function makeScatterDatasets(trades, xKey, yKey) {
  const datasets = [];
  const enabledSources = Object.entries(sourcesEnabled).filter(([k,v]) => v).map(([k]) => k);

  enabledSources.forEach(src => {
    const cfg = SOURCES[src];
    const srcTrades = trades.filter(t => t.source === src);

    datasets.push({
      label: `${cfg.symbol} ${cfg.label} — Win`,
      data: srcTrades.filter(t => t.won).map(t => ({ x: t[xKey] * 100, y: t[yKey] * 100, ...t })),
      backgroundColor: COLORS.win + "cc",
      borderColor: COLORS.win,
      borderWidth: src !== "live" ? 1.5 : 0,
      pointStyle: cfg.shape,
      pointRadius: 7,
      pointHoverRadius: 10,
    });
    datasets.push({
      label: `${cfg.symbol} ${cfg.label} — Loss`,
      data: srcTrades.filter(t => !t.won).map(t => ({ x: t[xKey] * 100, y: t[yKey] * 100, ...t })),
      backgroundColor: COLORS.loss + "cc",
      borderColor: COLORS.loss,
      borderWidth: src !== "live" ? 1.5 : 0,
      pointStyle: cfg.shape,
      pointRadius: 7,
      pointHoverRadius: 10,
    });
  });

  return datasets;
}

function scatterTooltip(ctx) {
  const d = ctx.raw;
  if (!d.tid) return "";
  const srcCfg = SOURCES[d.source] || {};
  return [
    `${srcCfg.symbol || ""} ${d.tid} ${d.asset} ${d.dir.toUpperCase()} — ${d.won ? "WIN" : "LOSS"}`,
    `Market: ${(d.mkt*100).toFixed(0)}¢ → Model: ${(d.fair*100).toFixed(0)}¢`,
    `Edge: ${d.edge}% | σ: ${(d.mv*100).toFixed(0)}% vs IV: ${(d.iv*100).toFixed(0)}%`,
    `Source: ${srcCfg.label || d.source}`,
    d.bi ? d.bi : "",
  ].filter(Boolean);
}

function renderScatter(trades) {
  destroyChart();
  document.getElementById("chart-subtitle").innerHTML =
    'Each point = one trade. Diagonal = model agrees with market. ' +
    '<span style="color:' + COLORS.win + '">Green</span> = win, ' +
    '<span style="color:' + COLORS.loss + '">Red</span> = loss. ' +
    '● = Live, ▲ = Paper';

  const datasets = makeScatterDatasets(trades, "mkt", "fair");
  datasets.push({
    label: "Model = Market",
    data: [{ x: 0, y: 0 }, { x: 100, y: 100 }],
    type: "line", borderColor: "#475569", borderDash: [6, 4],
    borderWidth: 1.5, pointRadius: 0, fill: false,
  });

  const ctx = document.getElementById("mainChart").getContext("2d");
  chart = new Chart(ctx, {
    type: "scatter",
    data: { datasets },
    options: {
      responsive: true,
      scales: {
        x: { title: { display: true, text: "Market Price (¢)", color: COLORS.muted },
             min: 0, max: 100, grid: { color: "#334155" }, ticks: { color: COLORS.muted } },
        y: { title: { display: true, text: "Model Fair Value (¢)", color: COLORS.muted },
             min: 0, max: 100, grid: { color: "#334155" }, ticks: { color: COLORS.muted } },
      },
      plugins: {
        tooltip: { callbacks: { label: scatterTooltip } },
        legend: {
          labels: {
            color: COLORS.muted,
            usePointStyle: true,
            pointStyleWidth: 14,
          }
        },
      }
    }
  });

  document.getElementById("insight-box").innerHTML =
    '<div class="title">Reading this chart</div>' +
    'Dots above the diagonal = model thinks contract is worth more than market (our "edge"). ' +
    'If well-calibrated, green and red would mix evenly at each price level. ' +
    '● = live trades, ▲ = paper trades. Toggle sources on/off to compare.';
}

function renderCalibration(trades) {
  destroyChart();
  document.getElementById("chart-subtitle").textContent =
    "Bars: actual win rate. Purple dots: model prediction. Amber dots: market price.";

  const buckets = [
    { lo: 0, hi: 10, label: "0-10¢" }, { lo: 10, hi: 20, label: "10-20¢" },
    { lo: 20, hi: 30, label: "20-30¢" }, { lo: 30, hi: 40, label: "30-40¢" },
    { lo: 40, hi: 50, label: "40-50¢" }, { lo: 50, hi: 60, label: "50-60¢" },
    { lo: 60, hi: 80, label: "60-80¢" }, { lo: 80, hi: 100, label: "80-100¢" },
  ];

  const data = buckets.map(b => {
    const inB = trades.filter(t => t.fair * 100 >= b.lo && t.fair * 100 < b.hi);
    const w = inB.filter(t => t.won).length;
    const n = inB.length;
    return {
      label: b.label, n, wins: w,
      wr: n > 0 ? w / n * 100 : null,
      avgFair: n > 0 ? inB.reduce((s, t) => s + t.fair, 0) / n * 100 : null,
      avgMkt: n > 0 ? inB.reduce((s, t) => s + t.mkt, 0) / n * 100 : null,
    };
  }).filter(b => b.n > 0);

  const ctx = document.getElementById("mainChart").getContext("2d");
  chart = new Chart(ctx, {
    type: "bar",
    data: {
      labels: data.map(d => `${d.label} (${d.n})`),
      datasets: [
        {
          label: "Actual Win Rate",
          data: data.map(d => d.wr),
          backgroundColor: data.map(d => d.wr >= d.avgFair ? COLORS.win + "aa" : COLORS.loss + "aa"),
          borderRadius: 4, barPercentage: 0.65,
        },
        {
          label: "Model Predicted",
          data: data.map(d => d.avgFair),
          type: "line", borderColor: COLORS.model, backgroundColor: COLORS.model,
          borderWidth: 2.5, pointRadius: 6, fill: false, tension: 0.2,
        },
        {
          label: "Market Price",
          data: data.map(d => d.avgMkt),
          type: "line", borderColor: COLORS.market, backgroundColor: COLORS.market,
          borderWidth: 2.5, borderDash: [6, 3], pointRadius: 6, fill: false, tension: 0.2,
        },
      ]
    },
    options: {
      responsive: true,
      scales: {
        x: { grid: { color: "#334155" }, ticks: { color: COLORS.muted } },
        y: { min: 0, max: 100, title: { display: true, text: "Win Rate %", color: COLORS.muted },
             grid: { color: "#334155" }, ticks: { color: COLORS.muted } },
      },
      plugins: {
        tooltip: {
          callbacks: {
            afterBody: (items) => {
              const idx = items[0]?.dataIndex;
              if (idx === undefined) return "";
              const d = data[idx];
              return `${d.wins}W / ${d.n - d.wins}L`;
            }
          }
        },
        legend: { labels: { color: COLORS.muted } },
      }
    }
  });

  document.getElementById("insight-box").innerHTML =
    '<div class="title">Calibration chart</div>' +
    '<span style="color:#8b5cf6">Purple line</span> = what model predicts. ' +
    '<span style="color:#f59e0b">Amber dashed</span> = what we paid. ' +
    'Bars = actual outcomes. Green = model underestimated (profitable). ' +
    'Red = model overestimated (losing). Gap between purple line and bars = calibration error.';
}

function renderVolScatter(trades) {
  destroyChart();
  document.getElementById("chart-subtitle").innerHTML =
    "Model vol (Binance) vs market implied vol (Kalshi). Above diagonal = model runs hotter. " +
    "● = Live, ▲ = Paper, ◆ = Bell v2";

  const withIV = trades.filter(t => t.iv > 0);
  const datasets = makeScatterDatasets(withIV, "iv", "mv");
  datasets.push({
    label: "Model = Market",
    data: [{ x: 0, y: 0 }, { x: 100, y: 100 }],
    type: "line", borderColor: "#475569", borderDash: [6, 4],
    borderWidth: 1.5, pointRadius: 0, fill: false,
  });

  const ctx = document.getElementById("mainChart").getContext("2d");
  chart = new Chart(ctx, {
    type: "scatter",
    data: { datasets },
    options: {
      responsive: true,
      scales: {
        x: { title: { display: true, text: "Market Implied Vol %", color: COLORS.muted },
             min: 0, grid: { color: "#334155" }, ticks: { color: COLORS.muted } },
        y: { title: { display: true, text: "Model Vol % (Binance)", color: COLORS.muted },
             min: 0, grid: { color: "#334155" }, ticks: { color: COLORS.muted } },
      },
      plugins: {
        tooltip: { callbacks: { label: scatterTooltip } },
        legend: {
          labels: {
            color: COLORS.muted,
            usePointStyle: true,
            pointStyleWidth: 14,
          }
        },
      }
    }
  });

  document.getElementById("insight-box").innerHTML =
    '<div class="title">Vol mismatch</div>' +
    'Almost every dot above diagonal — Binance-derived vol consistently exceeds market IV. ' +
    'Wins cluster closer to diagonal (smaller disagreement). ' +
    '● = live trades, ▲ = paper trades, ◆ = bell v2. Compare shapes to see if one source behaves differently.';
}

function renderCumulative(trades) {
  destroyChart();
  document.getElementById("chart-subtitle").innerHTML =
    "Cumulative P&L over time. Each point = one settled trade. " +
    "● = Live, ▲ = Paper, ◆ = Bell v2";

  // Build per-source cumulative lines
  const datasets = [];
  const enabledSources = Object.entries(sourcesEnabled).filter(([k,v]) => v).map(([k]) => k);

  // Overall cumulative
  let cumAll = 0;
  const allPts = trades.map((t, i) => {
    cumAll += t.pnl;
    return { x: i + 1, y: parseFloat(cumAll.toFixed(2)), ...t };
  });

  datasets.push({
    label: "Total P&L",
    data: allPts, borderColor: "#e2e8f0", backgroundColor: "#e2e8f044",
    borderWidth: 2.5, fill: true, tension: 0.15,
    pointStyle: allPts.map(p => SOURCES[p.source]?.shape || "circle"),
    pointRadius: 3.5,
    pointBackgroundColor: allPts.map(p => p.won ? COLORS.win : COLORS.loss),
  });

  // Per-source cumulative
  enabledSources.forEach(src => {
    const cfg = SOURCES[src];
    let cum = 0;
    const pts = [];
    trades.forEach((t, i) => {
      if (t.source === src) {
        cum += t.pnl;
        pts.push({ x: i + 1, y: parseFloat(cum.toFixed(2)) });
      }
    });
    datasets.push({
      label: `${cfg.symbol} ${cfg.label}`,
      data: pts, borderColor: cfg.color + "88",
      borderWidth: 1.5, pointRadius: 0, fill: false, tension: 0.15, borderDash: [4, 3],
    });
  });

  // Blend type breakdown
  let cumBlended = 0, cumConserv = 0, cumPre = 0;
  const blendPts = [], consPts = [], prePts = [];
  trades.forEach((t, i) => {
    if (t.bi && t.bi.startsWith("blended(")) {
      cumBlended += t.pnl;
      blendPts.push({ x: i + 1, y: parseFloat(cumBlended.toFixed(2)) });
    } else if (t.bi && t.bi.startsWith("conservative")) {
      cumConserv += t.pnl;
      consPts.push({ x: i + 1, y: parseFloat(cumConserv.toFixed(2)) });
    } else {
      cumPre += t.pnl;
      prePts.push({ x: i + 1, y: parseFloat(cumPre.toFixed(2)) });
    }
  });

  datasets.push(
    { label: "Blended (model>IV)", data: blendPts, borderColor: COLORS.loss + "55", borderWidth: 1, pointRadius: 0, fill: false, tension: 0.15, borderDash: [2, 2] },
    { label: "Conservative (model≤IV)", data: consPts, borderColor: COLORS.model + "55", borderWidth: 1, pointRadius: 0, fill: false, tension: 0.15, borderDash: [2, 2] },
    { label: "Pre-blend", data: prePts, borderColor: COLORS.win + "55", borderWidth: 1, pointRadius: 0, fill: false, tension: 0.15, borderDash: [2, 2] },
  );

  const ctx = document.getElementById("mainChart").getContext("2d");
  chart = new Chart(ctx, {
    type: "line",
    data: { datasets },
    options: {
      responsive: true,
      scales: {
        x: { type: 'linear', title: { display: true, text: "Trade #", color: COLORS.muted },
             grid: { color: "#334155" }, ticks: { color: COLORS.muted } },
        y: { title: { display: true, text: "Cumulative P&L ($)", color: COLORS.muted },
             grid: { color: "#334155" }, ticks: { color: COLORS.muted } },
      },
      plugins: {
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const d = ctx.raw;
              if (!d.tid) return `$${ctx.parsed.y.toFixed(2)}`;
              const srcCfg = SOURCES[d.source] || {};
              return [
                `${srcCfg.symbol || ""} ${d.tid} ${d.asset} ${d.dir?.toUpperCase() || ""} — ${d.won ? "WIN" : "LOSS"}`,
                `Trade P&L: $${d.pnl?.toFixed(2)} | Cumulative: $${ctx.parsed.y.toFixed(2)}`,
              ];
            }
          }
        },
        legend: {
          labels: {
            color: COLORS.muted,
            usePointStyle: true,
          }
        },
      }
    }
  });

  document.getElementById("insight-box").innerHTML =
    '<div class="title">P&L trajectory</div>' +
    'Solid line = overall cumulative. Dashed colored lines = per source. ' +
    'Thin dashed lines = breakdown by blend type. ' +
    'Toggle sources on/off to isolate live vs paper performance.';
}

function render() {
  updateTabStyles();
  const trades = getFiltered();
  renderStats(trades);

  if (currentView === "scatter") renderScatter(trades);
  else if (currentView === "calibration") renderCalibration(trades);
  else if (currentView === "vol") renderVolScatter(trades);
  else if (currentView === "cumulative") renderCumulative(trades);
}

async function fetchData() {
  try {
    const resp = await fetch("/api/trades");
    allData = await resp.json();
    document.getElementById("last-update").textContent = new Date().toLocaleTimeString();

    // Set date input bounds from available trade data
    const allTs = [...(allData.live||[]), ...(allData.paper||[]), ...(allData.bell||[])]
      .map(t => t.ts).filter(Boolean).sort();
    if (allTs.length > 0) {
      const fromInput = document.getElementById("dr-from");
      const toInput = document.getElementById("dr-to");
      const earliest = new Date(allTs[0]);
      const latest = new Date(allTs[allTs.length - 1]);
      fromInput.min = toLocalISO(earliest);
      fromInput.max = toLocalISO(latest);
      toInput.min = toLocalISO(earliest);
      toInput.max = toLocalISO(latest);
      // Set placeholder via title
      fromInput.title = `Earliest: ${earliest.toLocaleString()}`;
      toInput.title = `Latest: ${latest.toLocaleString()}`;
    }

    render();
  } catch (e) {
    console.error("Fetch failed:", e);
  }
}

buildTabs();
initDateRange();
fetchData();
setInterval(fetchData, REFRESH_SEC * 1000);
</script>
</body>
</html>"""


class DashboardHandler(http.server.BaseHTTPRequestHandler):
    refresh_sec = DEFAULT_REFRESH

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/api/trades":
            data = get_all_trades()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(data).encode())

        elif parsed.path == "/" or parsed.path == "/index.html":
            html = DASHBOARD_HTML.replace("__REFRESH__", str(self.refresh_sec))
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(html.encode())

        else:
            self.send_error(404)

    def log_message(self, format, *args):
        # Suppress per-request logging noise
        pass


def main():
    parser = argparse.ArgumentParser(description="Vol Model Calibration Dashboard")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"Port (default {DEFAULT_PORT})")
    parser.add_argument("--refresh", type=int, default=DEFAULT_REFRESH, help=f"Auto-refresh seconds (default {DEFAULT_REFRESH})")
    args = parser.parse_args()

    DashboardHandler.refresh_sec = args.refresh

    print(f"╔══════════════════════════════════════════╗")
    print(f"║   Vol Model Calibration Dashboard        ║")
    print(f"╠══════════════════════════════════════════╣")
    print(f"║  http://localhost:{str(args.port):<24s}║")
    print(f"║  Auto-refresh: {str(args.refresh) + 's':<25s}║")
    print(f"╠══════════════════════════════════════════╣")
    live_status = '✓ ' + LIVE_FILE.name if LIVE_FILE.exists() else '✗ not found'
    sim_status = '✓ ' + SIM_FILE.name if SIM_FILE.exists() else '✗ not found'
    bell_status = '✓ ' + SIM_V2_FILE.name if SIM_V2_FILE.exists() else '✗ not found'
    print(f"║  Live:  {live_status:<32s}║")
    print(f"║  Paper: {sim_status:<32s}║")
    print(f"║  Bell:  {bell_status:<32s}║")
    print(f"╚══════════════════════════════════════════╝")
    print(f"\nPress Ctrl+C to stop.\n")

    server = http.server.HTTPServer(("0.0.0.0", args.port), DashboardHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.shutdown()


if __name__ == "__main__":
    main()
