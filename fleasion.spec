# -*- mode: python ; coding: utf-8 -*-

a = Analysis(
    ['src/fleasion/test.py'],
    pathex=['src/fleasion'],
    binaries=[],
    datas=[
        ('src/fleasion/Fleasion2.ui', '.'),
        ('src/fleasion/terminal.py', '.'),
        ('src/fleasion/modules', 'modules'),
        ('src/fleasion/proxy', 'proxy'),
    ],
    hiddenimports=[
        'PySide6.QtXml',
        'PySide6.QtOpenGLWidgets',
        'PySide6.QtUiTools',
        'cryptography',
        'winreg',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='Fleasion',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    uac_admin=True,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='Fleasion',
)
