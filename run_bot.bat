@echo off
title Crypto Arbitrage Scanner - Polymarket/Kalshi
color 0A

echo ============================================================
echo   CRYPTO ARBITRAGE SCANNER
echo   15-min BTC/ETH/SOL Markets  
echo   Polymarket / Kalshi
echo ============================================================
echo.

REM ============================================================
REM KILL GHOST INSTANCES - Prevent double-trading
REM ============================================================
echo Checking for ghost instances...
set KILLED=0

REM Method 1: Kill any python process running arb_scanner.py
for /f "tokens=2" %%i in ('wmic process where "commandline like '%%arb_scanner%%' and name like '%%python%%'" get processid 2^>nul ^| findstr /r "[0-9]"') do (
    echo   Killing ghost PID: %%i
    taskkill /PID %%i /F >nul 2>&1
    set KILLED=1
)

REM Method 2: PowerShell fallback (more reliable pattern matching)
powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { $_.Name -like '*python*' -and $_.CommandLine -like '*arb_scanner*' } | ForEach-Object { Write-Host '  Killing ghost PID:' $_.ProcessId; Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }" 2>nul

REM Clean up stale lock file
if exist "%TEMP%\arb_scanner.lock" (
    echo   Removing stale lock file
    del /f "%TEMP%\arb_scanner.lock" >nul 2>&1
)

if %KILLED%==1 (
    echo   Waiting for cleanup...
    timeout /t 3 /nobreak >nul
    echo [OK] Ghost instances killed
) else (
    echo [OK] No ghost instances found
)
echo.

REM ============================================================
REM PURGE CACHE - Prevent stale bytecode
REM ============================================================
echo Cleaning bytecode cache...
for /d /r "%~dp0" %%d in (__pycache__) do (
    rd /s /q "%%d" 2>nul
)
del /s /q "%~dp0*.pyc" 2>nul
echo [OK] Cache cleaned
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python is not installed or not in PATH
    echo.
    echo Please install Python 3.11+ from https://python.org
    echo Make sure to check "Add Python to PATH" during installation
    echo.
    pause
    exit /b 1
)

echo [OK] Python found

REM Check if dependencies are installed
pip show httpx >nul 2>&1
if errorlevel 1 (
    echo.
    echo Installing required packages...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo [ERROR] Failed to install dependencies
        pause
        exit /b 1
    )
)

echo [OK] Dependencies installed
echo.

REM Check config files
if not exist "config.json" (
    echo [ERROR] config.json not found!
    pause
    exit /b 1
)

if not exist "kalshi_private_key.pem" (
    echo [ERROR] kalshi_private_key.pem not found!
    pause
    exit /b 1
)

echo ============================================================
echo   Starting scanner... Close window to stop
echo ============================================================
echo.

python arb_scanner.py

echo.
echo Scanner stopped.
pause
