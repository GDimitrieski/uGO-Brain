@echo off
setlocal enabledelayedexpansion

REM =========================================================================
REM  uGO-Brain build script
REM  Produces two executables: planner_service.exe and world_config_gui.exe
REM
REM  Prerequisites:
REM      pip install -r requirements.txt
REM
REM  Usage:
REM      deploy\build.bat                  (build both)
REM      deploy\build.bat planner          (build planner_service only)
REM      deploy\build.bat gui              (build world_config_gui only)
REM =========================================================================

set "PROJECT_ROOT=%~dp0.."
pushd "%PROJECT_ROOT%"

REM Build/dist outside OneDrive to avoid file-locking issues.
set "BUILD_DIR=C:\uGO-build\build"
set "DIST_DIR=C:\uGO-build\dist"
set "BUILD_TARGET=%~1"

echo.
echo ===== uGO-Brain Build =====
echo Project root: %PROJECT_ROOT%
echo Build dir:    %BUILD_DIR%
echo Output:       %DIST_DIR%
echo.

REM -- Build planner_service ------------------------------------------------
if "%BUILD_TARGET%"=="" goto build_planner
if /i "%BUILD_TARGET%"=="planner" goto build_planner
goto skip_planner

:build_planner
echo [1/2] Building planner_service.exe ...
pyinstaller --clean --noconfirm --distpath "%DIST_DIR%" --workpath "%BUILD_DIR%" deploy\planner_service.spec
if errorlevel 1 (
    echo ERROR: planner_service build failed.
    goto :fail
)
echo       planner_service.exe built OK.
echo.

:skip_planner

REM -- Build world_config_gui -----------------------------------------------
if "%BUILD_TARGET%"=="" goto build_gui
if /i "%BUILD_TARGET%"=="gui" goto build_gui
goto skip_gui

:build_gui
echo [2/2] Building world_config_gui.exe ...
pyinstaller --clean --noconfirm --distpath "%DIST_DIR%" --workpath "%BUILD_DIR%" deploy\world_config_gui.spec
if errorlevel 1 (
    echo ERROR: world_config_gui build failed.
    goto :fail
)
echo       world_config_gui.exe built OK.
echo.

:skip_gui

echo ===== Build complete =====
echo.
echo Executables are in:
echo   %DIST_DIR%\planner_service\planner_service.exe
echo   %DIST_DIR%\world_config_gui\world_config_gui.exe
echo.
echo To deploy, run:
echo   deploy\package.bat  C:\uGO
echo.
popd
exit /b 0

:fail
popd
exit /b 1
