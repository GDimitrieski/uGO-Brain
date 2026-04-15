# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for world_config_gui.exe

Build:
    cd <project_root>
    pyinstaller deploy/world_config_gui.spec
"""

import os
import sys
from PyInstaller.utils.hooks import collect_submodules

PROJECT_ROOT = os.path.abspath(os.path.join(SPECPATH, os.pardir))

# CRITICAL: add project root to sys.path so collect_submodules can find
# project packages at build time.
sys.path.insert(0, PROJECT_ROOT)

# ---------------------------------------------------------------------------
# Hidden imports
# ---------------------------------------------------------------------------

hidden = []
hidden += collect_submodules("world")
hidden += collect_submodules("planning")
hidden += [
    "uvicorn",
    "uvicorn.logging",
    "uvicorn.loops",
    "uvicorn.loops.auto",
    "uvicorn.protocols",
    "uvicorn.protocols.http",
    "uvicorn.protocols.http.auto",
    "uvicorn.protocols.websockets",
    "uvicorn.protocols.websockets.auto",
    "uvicorn.lifespan",
    "uvicorn.lifespan.on",
    "uvicorn.lifespan.off",
    "fastapi",
    "fastapi.staticfiles",
    "fastapi.responses",
    "starlette",
    "starlette.responses",
    "starlette.routing",
    "starlette.staticfiles",
    "starlette.middleware",
    "anyio",
    "anyio._backends",
    "anyio._backends._asyncio",
    "requests",
]

# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

a = Analysis(
    [os.path.join(PROJECT_ROOT, "world", "world_config_gui", "server.py")],
    pathex=[PROJECT_ROOT],
    hiddenimports=sorted(set(hidden)),
    datas=[],          # static/ and config files live on disk
    binaries=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["tkinter", "matplotlib", "PIL", "numpy", "scipy", "pandas"],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="world_config_gui",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="world_config_gui",
)
