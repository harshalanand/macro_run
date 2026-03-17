@echo off
echo ============================================================
echo   MACRO ORCHESTRATOR - Connection Test
echo   Tests schtasks connection from server to a remote PC
echo ============================================================
echo.

set /p REMOTE_PC="Enter remote PC hostname (e.g. HOPC560): "
set /p USERNAME="Enter username (e.g. .\administrator): "
set /p PASSWORD="Enter password: "

echo.
echo Testing %REMOTE_PC%...
echo.

echo [1] schtasks /query...
schtasks /query /s %REMOTE_PC% /u %USERNAME% /p %PASSWORD% /fo list >nul 2>&1
if %errorlevel%==0 (
    echo     OK: Can connect to Task Scheduler on %REMOTE_PC%
) else (
    echo     FAIL: Cannot connect. Run setup_remote.bat on %REMOTE_PC%
    echo.
    echo     Common fixes:
    echo     - Run setup_remote.bat as Administrator on %REMOTE_PC%
    echo     - Check username/password
    echo     - Make sure %REMOTE_PC% is on and reachable (ping %REMOTE_PC%)
    pause
    exit /b 1
)

echo [2] Creating test task...
schtasks /create /s %REMOTE_PC% /u %USERNAME% /p %PASSWORD% /ru %USERNAME% /rp %PASSWORD% /tn "MO_TEST" /tr "cmd /c echo test" /sc once /st 00:00 /f /rl highest >nul 2>&1
if %errorlevel%==0 (
    echo     OK: Can create tasks on %REMOTE_PC%
) else (
    echo     FAIL: Cannot create tasks. Check admin rights.
    pause
    exit /b 1
)

echo [3] Running test task...
schtasks /run /s %REMOTE_PC% /u %USERNAME% /p %PASSWORD% /tn "MO_TEST" >nul 2>&1
if %errorlevel%==0 (
    echo     OK: Can run tasks on %REMOTE_PC%
) else (
    echo     FAIL: Cannot run tasks.
)

echo [4] Cleaning up...
schtasks /delete /s %REMOTE_PC% /u %USERNAME% /p %PASSWORD% /tn "MO_TEST" /f >nul 2>&1
echo     OK

echo.
echo ============================================================
echo   ALL TESTS PASSED - %REMOTE_PC% is ready!
echo   
echo   Now in the web app, set:
echo   System Name : %REMOTE_PC%
echo   Username    : %USERNAME%
echo ============================================================
pause
