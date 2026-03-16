@echo off
echo ============================================
echo   Macro Orchestrator v3 - Starting...
echo ============================================
echo.
cd /d "%~dp0"
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
) else (
    echo Creating virtual environment...
    python -m venv venv
    call venv\Scripts\activate.bat
    pip install -r requirements.txt
)
echo.
echo Server: http://localhost:8000
echo Press Ctrl+C to stop
echo.
python -m uvicorn main:app --host 0.0.0.0 --port 8000
