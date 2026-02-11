@echo off
title Vol Model Paper Trader
echo ============================================================
echo  Vol Model Paper Trader
echo  Binance tick data + PM market prices + Black-Scholes
echo  Paper trades when model finds edge, tracks outcomes
echo ============================================================
echo.

cd /d "%~dp0"

set BINANCE_PROXY=http://ufngmejp:jf9a4s0axthn@195.40.137.202:5923
set PM_PROXY=http://ufngmejp:jf9a4s0axthn@82.23.103.113:7840

pip install aiohttp httpx --quiet 2>nul

python vol_paper_trader.py

pause
