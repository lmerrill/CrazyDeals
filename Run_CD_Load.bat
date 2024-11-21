@echo off

:: Run the Node.js script
node C:\Users\lloyd.merrill\Documents\Node.js\CDLoad.js

:: Display a message to the user
echo Press Ctrl+Shift+C to exit.

:: Continuously check for the Ctrl+Shift+C key combination
:loop
    set KEYPRESS=%errorlevel%
    if %KEYPRESS% EQU 0 (
        timeout /t 1 >nul
        goto loop
    )

:: Exit the script if the key combination is detected
if %KEYPRESS% EQU 233 goto :eof