@echo off

:: Change to your project directory
cd /d C:\DrmWorker


:: Start the FastAPI worker using PM2 and virtual env's Python
pm2 start venv\Scripts\python.exe --name drm-worker -- main.py

:: Optionally save the PM2 process list (for automatic resurrection)
pm2 save

:: Exit the batch script
exit
