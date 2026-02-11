@echo off
echo ============================================
echo   Vol Model LIVE Trader - KALSHI
echo   Starting...
echo ============================================
echo.

cd /d "%~dp0"

REM Binance geo-blocks US IPs — route through residential proxy
REM Proxy credentials now loaded from config.json
for /f "tokens=*" %%a in ('python -c "import json;c=json.load(open('config.json'));p=c.get('residential_proxy',{});print(f\"http://{p['username']}:{p['password']}@{p['host']}:{p['port']}\" if p.get('enabled') else '')" 2^>nul') do set BINANCE_PROXY=%%a

python vol_live_trader_kalshi.py

echo.
pause
