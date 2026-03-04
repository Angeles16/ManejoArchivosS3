@echo off
cd /d "%~dp0"

call poetry run python main.py

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ALERTA] El script fallo con codigo %ERRORLEVEL%
    echo Revisa el archivo errores_sincronizacion.log para mas detalles.
    pause
)