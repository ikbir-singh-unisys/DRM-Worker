@echo off
cd /d C:\DrmWorker

:: Create venv if not exists
if not exist "venv\" (
    py -3.13 -m venv venv
)

:: Activate the virtual environment
call venv\Scripts\activate.bat

:: Run the FastAPI worker
python main.py

pause
