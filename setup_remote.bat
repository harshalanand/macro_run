@echo off
echo ============================================================
echo   MACRO ORCHESTRATOR - Remote PC Setup
echo   Run this AS ADMINISTRATOR on EVERY PC that will run macros
echo   (including the server PC itself like HOPC560)
echo ============================================================
echo.

:: Check admin rights
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Right-click this file ^> Run as administrator
    pause
    exit /b 1
)

echo ============================================================
echo   STEP 1: FIREWALL SETTINGS
echo ============================================================
echo.

echo [1a] Enabling Remote Scheduled Tasks Management...
netsh advfirewall firewall set rule group="Remote Scheduled Tasks Management" new enable=yes >nul 2>&1
if %errorlevel% neq 0 (
    echo       Adding rules manually...
    netsh advfirewall firewall add rule name="MO_Schtasks_RPC" dir=in action=allow protocol=TCP localport=135 >nul 2>&1
    netsh advfirewall firewall add rule name="MO_Schtasks_Dynamic" dir=in action=allow protocol=TCP localport=49152-65535 >nul 2>&1
)
echo       DONE

echo [1b] Enabling File and Printer Sharing...
netsh advfirewall firewall set rule group="File and Printer Sharing" new enable=yes >nul 2>&1
echo       DONE

echo.
echo ============================================================
echo   STEP 2: SERVICES
echo ============================================================
echo.

echo [2a] Task Scheduler = auto start...
sc config Schedule start= auto >nul 2>&1
net start Schedule >nul 2>&1
echo       DONE

echo [2b] WMI service = auto start...
sc config winmgmt start= auto >nul 2>&1
net start winmgmt >nul 2>&1
echo       DONE

echo [2c] Server service (for file sharing) = auto start...
sc config LanmanServer start= auto >nul 2>&1
net start LanmanServer >nul 2>&1
echo       DONE

echo.
echo ============================================================
echo   STEP 3: VERIFY
echo ============================================================
echo.

echo   Computer Name : %COMPUTERNAME%
echo.

:: Check if Excel is installed
where excel.exe >nul 2>&1
if %errorlevel%==0 (
    echo   Excel         : INSTALLED
) else (
    reg query "HKLM\SOFTWARE\Microsoft\Office" /s 2>nul | findstr /i "excel" >nul 2>&1
    if %errorlevel%==0 (
        echo   Excel         : INSTALLED (registry found)
    ) else (
        echo   Excel         : NOT FOUND - Install Microsoft Excel!
    )
)

:: Check shared folders
echo.
echo   Shared folders on this PC:
net share | findstr /v "^The " | findstr /v "^---" | findstr /v "command completed"
echo.

echo ============================================================
echo   SETUP COMPLETE!
echo.
echo   NOW go to Macro Orchestrator web app and set:
echo.
echo   System Name  : %COMPUTERNAME%
echo   Remote Path  : (local path of shared folder, e.g. D:\test)
echo   Username     : .\administrator
echo   Password     : (your admin password)
echo.
echo   To find Remote Path:
echo   1. Right-click your shared folder
echo   2. Properties ^> Sharing tab
echo   3. The folder path shown = your Remote Path
echo.
echo   Example:
echo     Shared Folder : \\%COMPUTERNAME%\test
echo     Remote Path   : D:\test
echo ============================================================
pause
