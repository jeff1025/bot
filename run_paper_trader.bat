@echo off
title Vol Model Paper Trader
echo ============================================================
echo  Vol Model Paper Trader
echo  Binance tick data + PM market prices + Black-Scholes
echo  Paper trades when model finds edge, tracks outcomes
echo ============================================================
echo.

cd /d "%~dp0"

REM Proxy credentials now loaded from config.json by the Python scripts.
REM Set env vars here only as fallback if config.json proxy section is missing.
REM To configure: add "residential_proxy" and "binance_proxy" sections to config.json
for /f "tokens=*" %%a in ('python -c "import json;c=json.load(open('config.json'));p=c.get('binance_proxy',{});print(f\"http://{p['username']}:{p['password']}@{p['host']}:{p['port']}\" if p.get('enabled') else '')" 2^>nul') do set BINANCE_PROXY=%%a
for /f "tokens=*" %%a in ('python -c "import json;c=json.load(open('config.json'));p=c.get('residential_proxy',{});print(f\"http://{p['username']}:{p['password']}@{p['host']}:{p['port']}\" if p.get('enabled') else '')" 2^>nul') do set PM_PROXY=%%a

pip install aiohttp httpx --quiet 2>nul

python vol_paper_trader.py

pause
