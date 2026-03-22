def _is_admin() -> bool:
    import ctypes
    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def _relaunch_as_admin() -> None:
    import ctypes, ctypes.wintypes, sys, os, shutil

    if getattr(sys, "frozen", False):
        exe = sys.executable
        params = " ".join(f'"{a}"' for a in sys.argv[1:]) or None
    else:
        # Use uv to relaunch — it handles venv activation correctly.
        # Calling python.exe directly skips uv's environment setup and causes
        # silent import failures in the elevated process.
        uv_exe = shutil.which("uv") or shutil.which("uv.exe")
        if uv_exe:
            exe = uv_exe
            # Walk up from this file to find the project root (contains pyproject.toml)
            check = os.path.dirname(os.path.abspath(__file__))
            for _ in range(6):
                if os.path.exists(os.path.join(check, "pyproject.toml")):
                    break
                check = os.path.dirname(check)
            params = f'--project "{check}" run fleasion'
        else:
            exe = sys.executable
            params = " ".join(f'"{a}"' for a in sys.argv)

    SEE_MASK_NO_CONSOLE    = 0x00008000
    SEE_MASK_NOCLOSEPROCESS = 0x00000040

    class _SHELLEXECUTEINFOW(ctypes.Structure):
        _fields_ = [
            ("cbSize",         ctypes.wintypes.DWORD),
            ("fMask",          ctypes.wintypes.ULONG),
            ("hwnd",           ctypes.wintypes.HWND),
            ("lpVerb",         ctypes.wintypes.LPCWSTR),
            ("lpFile",         ctypes.wintypes.LPCWSTR),
            ("lpParameters",   ctypes.wintypes.LPCWSTR),
            ("lpDirectory",    ctypes.wintypes.LPCWSTR),
            ("nShow",          ctypes.c_int),
            ("hInstApp",       ctypes.wintypes.HINSTANCE),
            ("lpIDList",       ctypes.c_void_p),
            ("lpClass",        ctypes.wintypes.LPCWSTR),
            ("hkeyClass",      ctypes.wintypes.HKEY),
            ("dwHotKey",       ctypes.wintypes.DWORD),
            ("hIconOrMonitor", ctypes.wintypes.HANDLE),
            ("hProcess",       ctypes.wintypes.HANDLE),
        ]

    sei = _SHELLEXECUTEINFOW()
    sei.cbSize       = ctypes.sizeof(_SHELLEXECUTEINFOW)
    sei.fMask        = SEE_MASK_NO_CONSOLE | SEE_MASK_NOCLOSEPROCESS
    sei.lpVerb       = "runas"
    sei.lpFile       = exe
    sei.lpParameters = params
    sei.lpDirectory  = None
    sei.nShow        = 0  # SW_HIDE — suppress any console window from uv

    ok = ctypes.windll.shell32.ShellExecuteExW(ctypes.byref(sei))
    if ok:
        sys.exit(0)


def main() -> None:
    if not _is_admin():
        _relaunch_as_admin()

    import sys, traceback, ctypes

    try:
        from PySide6.QtWidgets import QApplication
        from PySide6.QtCore import Qt
        from PySide6.QtGui import QSurfaceFormat
        from PySide6.QtOpenGLWidgets import QOpenGLWidget as _QOGLW
        import fleasion.test as _app

        # Force native desktop OpenGL (not ANGLE/D3D translator) BEFORE QApplication.
        # ANGLE doesn't support #version 330 core shaders, so AnimGLWidget's
        # initializeGL would fail silently → _ready stays False → black viewport.
        QApplication.setAttribute(Qt.AA_UseDesktopOpenGL)

        # Set the default surface format before QApplication so the shared GL
        # context (created by AA_ShareOpenGLContexts) uses 3.3 Core profile,
        # matching what AnimGLWidget requests.
        _gl_fmt = QSurfaceFormat()
        _gl_fmt.setVersion(3, 3)
        _gl_fmt.setProfile(QSurfaceFormat.CoreProfile)
        _gl_fmt.setDepthBufferSize(24)
        QSurfaceFormat.setDefaultFormat(_gl_fmt)

        QApplication.setAttribute(Qt.AA_ShareOpenGLContexts)
        app = QApplication(sys.argv)
        _app.app = app
        _app._default_style_name = app.style().objectName()

        saved = _app._load_app_settings()
        if saved.get("style"):
            app.setStyle(saved["style"])

        window = _app.MainWindow()

        if saved.get("light_mode"):
            app.styleHints().setColorScheme(Qt.ColorScheme.Light)
            if hasattr(window, "_tray_light_action"):
                window._tray_light_action.setChecked(True)

        _dummy_gl = _QOGLW(window)
        _dummy_gl.setFixedSize(1, 1)
        _dummy_gl.move(-2, -2)

        if saved.get("show_on_start", True):
            window.show()

        _dummy_gl.hide()
        _app.apply_light_to_mainwindow(window)
        _app.install_lightmode_autorefresh(window)
        sys.exit(app.exec())

    except Exception:
        err = traceback.format_exc()
        ctypes.windll.user32.MessageBoxW(0, err, "Fleasion — startup error", 0x10)
