@echo off
SET SOURCE=https://modernstreaming.s3.ap-south-1.amazonaws.com/DrmWorker.zip
SET ZIP_PATH=C:\DrmWorker.zip
SET DESTINATION=C:\
SET DESTINATION_Scripts=C:\Scripts\
SET BAT_FILE=%DESTINATION_Scripts%\run_app.bat

:: Download the ZIP file using PowerShell
powershell -Command "Invoke-WebRequest -Uri '%SOURCE%' -OutFile '%ZIP_PATH%'"

:: Extract the ZIP using PowerShell
powershell -Command "Expand-Archive -Path '%ZIP_PATH%' -DestinationPath '%DESTINATION%' -Force"

:: Check if batch file exists, then run it
IF EXIST "%BAT_FILE%" (
    START "" /D "%DESTINATION_Scripts%" /MIN "%BAT_FILE%"
) ELSE (
    echo Batch file not found: %BAT_FILE%
)
