@echo off
echo ============================================
echo   Macro Orchestrator - DEV MODE (auto-reload)
echo ============================================
echo.
cd /d "%~dp0"
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
)
echo.
echo Server: http://localhost:8000
echo Auto-reload ON (excludes *.db files)
echo.
python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload --reload-exclude "*.db" --reload-exclude "*.db-wal" --reload-exclude "*.db-shm" --reload-exclude "compiled_output" --reload-exclude "uploads"
