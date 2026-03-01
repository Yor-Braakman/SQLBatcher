@echo off
echo SQL Batcher - Build Executable
echo ===============================
echo.

if not exist venv (
    echo ERROR: Virtual environment not found. Run setup.bat first.
    pause
    exit /b 1
)

echo Activating virtual environment...
call venv\Scripts\activate.bat

echo.
echo Building executable with PyInstaller...
pyinstaller --onefile --windowed --name="SQLBatcher" ^
    --hidden-import=pyodbc ^
    --hidden-import=azure.identity ^
    --hidden-import=PyQt5.QtCore ^
    --hidden-import=PyQt5.QtWidgets ^
    --hidden-import=PyQt5.QtGui ^
    main.py

echo.
echo Build complete!
echo Executable location: dist\SQLBatcher.exe
echo.
pause
