@echo off
for /f "delims=" %%i in ('pip freeze ^| findstr /v /r "^-e"') do (
    pip uninstall -y %%i
)
