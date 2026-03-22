REM --- @echo off
REM --- cd /d D:\recoll
REM --- call .venv\Scripts\activate
REM --- streamlit run library_ui.py
REM --- pause

@echo off
echo ================================
echo Starting Silent Death AI System
echo ================================

REM --------------------------------
REM SAFE START RECOLL
REM --------------------------------
echo Checking Recoll...

tasklist | find /i "recoll.exe" >nul

if errorlevel 1 (
    echo Starting Recoll...
    start "" "D:\Recoll\recoll.exe"
) else (
    echo Recoll already running
)

REM --------------------------------
REM START LM STUDIO
REM --------------------------------
echo Starting LM Studio...
start "" "D:\AI_Models\LM Studio\LM Studio.exe"

echo Waiting for LM Studio to initialize...
timeout /t 15 >nul

REM --------------------------------
REM WAIT FOR AI SERVER
REM --------------------------------
echo Waiting for AI server...

:waitloop
curl http://localhost:1234/v1/models >nul 2>&1

if errorlevel 1 (
    timeout /t 2 >nul
    goto waitloop
)

echo AI server is ready!

REM --------------------------------
REM START YOUR WORKING UI
REM --------------------------------
echo Launching Library Brain...

REM 👉 USE YOUR WORKING ENVIRONMENT DIRECTLY
D:\recoll\.venv\Scripts\python.exe -m streamlit run D:\recoll\library_ui.py

pause