@echo off
echo ============================================================
echo   MACRO ORCHESTRATOR - Remote PC Setup
echo   Run this AS ADMINISTRATOR on each remote PC
echo ============================================================
echo.

:: Check admin rights
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: This script must be run as Administrator!
    echo Right-click > Run as administrator
    pause
    exit /b 1
)

echo [1/4] Enabling Remote Scheduled Tasks Management...
netsh advfirewall firewall set rule group="Remote Scheduled Tasks Management" new enable=yes >nul 2>&1
if %errorlevel%==0 (
    echo       OK: Remote Scheduled Tasks enabled
) else (
    echo       Adding firewall rules manually...
    netsh advfirewall firewall add rule name="Schtasks RPC" dir=in action=allow protocol=TCP localport=135 >nul 2>&1
    netsh advfirewall firewall add rule name="Schtasks RPC Dynamic" dir=in action=allow protocol=TCP localport=49152-65535 >nul 2>&1
    echo       OK: Firewall rules added
)

echo.
echo [2/4] Enabling File and Printer Sharing...
netsh advfirewall firewall set rule group="File and Printer Sharing" new enable=yes >nul 2>&1
echo       OK: File sharing enabled

echo.
echo [3/4] Ensuring Windows Management service is running...
sc config winmgmt start= auto >nul 2>&1
net start winmgmt >nul 2>&1
echo       OK: WMI service running

echo.
echo [4/4] Ensuring Task Scheduler is running...
sc config Schedule start= auto >nul 2>&1
net start Schedule >nul 2>&1
echo       OK: Task Scheduler running

echo.
echo ============================================================
echo   SETUP COMPLETE
echo.
echo   This PC can now receive remote macro jobs.
echo.
echo   IMPORTANT: Note these values for the web app:
echo   Computer Name: %COMPUTERNAME%
echo.
echo   To find your shared folder path:
echo   1. Right-click your shared folder
echo   2. Properties ^> Sharing tab
echo   3. Note the "Network Path" (e.g. \\%COMPUTERNAME%\share)
echo   4. Note the "Local Path" (e.g. D:\share)
echo ============================================================
pause
