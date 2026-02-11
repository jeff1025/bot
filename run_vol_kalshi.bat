@echo off
echo ============================================
echo   Vol Model LIVE Trader - KALSHI
echo   Starting...
echo ============================================
echo.

cd /d "%~dp0"

REM Binance geo-blocks US IPs — route through residential proxy
set BINANCE_PROXY=http://ufngmejp:jf9a4s0axthn@82.23.103.113:7840

python vol_live_trader_kalshi.py

echo.
pause
