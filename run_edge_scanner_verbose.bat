@echo off
title Kalshi Microedge Scanner - VERBOSE MODE
color 0E

echo ============================================================
echo   KALSHI MICROEDGE SCANNER — VERBOSE MODE
echo   15-min + Hourly BTC/ETH/SOL Markets
echo   Shows ALL scan results (even empty ones)
echo ============================================================
echo.

cd /d "%~dp0"

python edge_scanner.py --verbose %*

echo.
echo Scanner stopped.
pause
