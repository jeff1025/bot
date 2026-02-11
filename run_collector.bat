@echo off
echo ============================================================
echo   STRIKE PRICE COLLECTOR
echo   Run this first, keep it running while the bot runs
echo ============================================================

REM Check Python
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Python not found. Install Python 3.10+
    pause
    exit /b 1
)

echo [OK] Python found

REM Install websocket-client if needed
pip show websocket-client >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo Installing websocket-client...
    pip install websocket-client
)

echo ============================================================
echo   Starting collector... Keep this window open!
echo ============================================================

python strike_collector.py

pause
