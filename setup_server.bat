@echo off
echo ============================================================
echo   MACRO ORCHESTRATOR - SERVER Setup
echo   Run this AS ADMINISTRATOR on the server PC
echo ============================================================
echo.

:: Check admin rights
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Right-click this file ^> Run as administrator
    pause
    exit /b 1
)

echo [1/5] Enabling Remote Scheduled Tasks Management...
netsh advfirewall firewall set rule group="Remote Scheduled Tasks Management" new enable=yes >nul 2>&1
if %errorlevel% neq 0 (
    netsh advfirewall firewall add rule name="Schtasks RPC" dir=in action=allow protocol=TCP localport=135 >nul 2>&1
    netsh advfirewall firewall add rule name="Schtasks RPC Dyn" dir=in action=allow protocol=TCP localport=49152-65535 >nul 2>&1
)
echo       OK

echo [2/5] Enabling File and Printer Sharing...
netsh advfirewall firewall set rule group="File and Printer Sharing" new enable=yes >nul 2>&1
echo       OK

echo [3/5] Starting Task Scheduler service...
sc config Schedule start= auto >nul 2>&1
net start Schedule >nul 2>&1
echo       OK

echo [4/5] Starting WMI service...
sc config winmgmt start= auto >nul 2>&1
net start winmgmt >nul 2>&1
echo       OK

echo [5/5] Checking Python...
python --version >nul 2>&1
if %errorlevel%==0 (
    echo       OK: Python found
) else (
    echo       WARNING: Python not found. Install Python 3.10+ from python.org
)

echo.
echo ============================================================
echo   SERVER SETUP COMPLETE
echo.
echo   Computer Name: %COMPUTERNAME%
echo.
echo   NEXT STEPS:
echo   1. Open CMD in macro_run folder
echo   2. pip install -r requirements.txt
echo   3. Double-click run.bat
echo   4. Open http://localhost:8000
echo.
echo   IMPORTANT: This PC also needs setup_remote.bat
echo   if it will run macros on itself (e.g. HOPC560)
echo ============================================================
pause
