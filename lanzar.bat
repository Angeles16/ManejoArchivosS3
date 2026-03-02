@echo off
cd /d "%~dp0"
set PYTHON_VENV="%~dp0venv\Scripts\python.exe"
%PYTHON_VENV% main.py
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ALERTA] El script fallo con codigo %ERRORLEVEL%
    echo Revisa el archivo errores_sincronizacion.log para mas detalles.
    pause
)