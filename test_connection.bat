@echo off
echo ============================================================
echo   MACRO ORCHESTRATOR - Connection Test
echo ============================================================
echo.

set /p REMOTE_PC="Enter remote PC hostname (e.g. HOPC560): "
set /p USERNAME="Enter username (e.g. .\administrator): "
set /p PASSWORD="Enter password: "

echo.
echo Testing %REMOTE_PC%...
echo.

set IS_LOCAL=0
if /i "%REMOTE_PC%"=="%COMPUTERNAME%" set IS_LOCAL=1

if %IS_LOCAL%==1 (
    echo   NOTE: %REMOTE_PC% is THIS machine.
    echo.
)

echo [1] schtasks /query...
if %IS_LOCAL%==1 (
    schtasks /query /fo list >nul 2>&1
) else (
    schtasks /query /s %REMOTE_PC% /u %USERNAME% /p %PASSWORD% /fo list >nul 2>&1
)
if %errorlevel%==0 (
    echo     OK: Task Scheduler accessible
) else (
    echo     FAIL: Run setup_remote.bat on %REMOTE_PC% as admin
    pause & exit /b 1
)

echo [2] schtasks /create...
if %IS_LOCAL%==1 (
    schtasks /create /tn "MO_TEST" /tr "cmd /c echo test" /sc once /st 00:00 /f /rl highest /ru %USERNAME% /rp %PASSWORD% 2>&1
) else (
    schtasks /create /s %REMOTE_PC% /u %USERNAME% /p %PASSWORD% /tn "MO_TEST" /tr "cmd /c echo test" /sc once /st 00:00 /f /rl highest /ru %USERNAME% /rp %PASSWORD% 2>&1
)
if %errorlevel%==0 (
    echo     OK: Can create tasks
) else (
    echo     FAIL: See error above
    echo.
    echo     TIP: Try username as %REMOTE_PC%\administrator instead of .\administrator
    pause & exit /b 1
)

echo [3] schtasks /run...
if %IS_LOCAL%==1 (
    schtasks /run /tn "MO_TEST" >nul 2>&1
) else (
    schtasks /run /s %REMOTE_PC% /u %USERNAME% /p %PASSWORD% /tn "MO_TEST" >nul 2>&1
)
if %errorlevel%==0 (
    echo     OK: Can run tasks
) else (
    echo     FAIL: Cannot run
)

echo [4] Cleanup...
if %IS_LOCAL%==1 (
    schtasks /delete /tn "MO_TEST" /f >nul 2>&1
) else (
    schtasks /delete /s %REMOTE_PC% /u %USERNAME% /p %PASSWORD% /tn "MO_TEST" /f >nul 2>&1
)
echo     OK

echo.
echo ============================================================
echo   ALL TESTS PASSED - %REMOTE_PC% is ready!
echo ============================================================
pause
