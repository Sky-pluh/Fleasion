import zipfile
import tempfile
import sys
import os
import importlib.util
import time
import json
import threading
import ctypes
import ssl
import socket
import shutil
from pathlib import Path
from fleasion.terminal import Ui_Form as TerminalUI
from urllib.parse import urlparse

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QCheckBox, QSystemTrayIcon,
    QTableView, QTreeView, QScrollArea, QAbstractScrollArea, QTabWidget, QFrame
)
from PySide6.QtUiTools import QUiLoader
from PySide6.QtCore import QFile, Qt, Signal, QObject, QTimer, QUrl
from PySide6.QtGui import QDesktopServices
import PySide6.QtWidgets as QtWidgets
from PySide6.QtGui import QAction, QIcon, QGuiApplication, QPalette

import winreg
from ctypes import wintypes

try:
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.backends import default_backend
    import datetime as dt
    _CRYPTO_OK = True
except ImportError:
    _CRYPTO_OK = False

from fleasion.proxy.setup import (
    TARGET_DOMAINS, HOME, MITM_CA_PEM, MITM_CA_CERT,
    HOSTS_FILE, HOSTS_MARKER, ROBLOX_PATHS,
    _add_hosts, _remove_hosts, ensure_mitm_cert, install_cert, remove_cert,
)

PROXY = None  # stop event

# Proxy config
LISTEN_PORT = 443
_TMP_DIR = tempfile.mkdtemp(prefix="Fleasion_proxy_")
REAL_IPS = {}
_STOP = threading.Event()
_SRV = None
_ca_cert_obj = None
_ca_key_obj = None
_cert_cache = {}
_ctx_cache = {}        # hostname -> ssl.SSLContext  (server-side, reused)
_cert_lock = threading.Lock()

# Single reusable upstream SSL context — built once, shared across all threads.
_fwd_ctx: ssl.SSLContext | None = None


def _make_fwd_ctx() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _get_server_ctx(hostname: str) -> ssl.SSLContext:
    with _cert_lock:
        if hostname in _ctx_cache:
            return _ctx_cache[hostname]
    pair = _make_cert(hostname)
    if not pair:
        raise RuntimeError(f"cert gen failed for {hostname}")
    nc = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    nc.minimum_version = ssl.TLSVersion.TLSv1_2
    nc.load_cert_chain(pair[0], pair[1])
    with _cert_lock:
        _ctx_cache[hostname] = nc
    return nc




# Colour helpers (for console output)
if sys.platform == "win32":
    os.system("")


def system_is_dark() -> bool:
    app = QGuiApplication.instance()
    if not app:
        return False
    sh = app.styleHints()
    if hasattr(sh, "colorScheme"):
        return sh.colorScheme() == Qt.ColorScheme.Dark
    col = app.palette().color(QPalette.Window)
    return col.lightness() < 128


LIGHT_GLOBAL_QSS = """
QHeaderView::section {
    background-color: #fafafa;
    color: black;
    padding: 4px;
    border: 1px solid #626663;
}

QScrollArea {
    background-color: #f5f5f5;
    border: 1px solid #d0d0d0;
}

QScrollArea > QWidget > QWidget {
    background-color: #f5f5f5;
}

QFrame[frameShape="4"],
QFrame[frameShape="5"] {
    color: #d0d0d0;
}

QTreeView {
    background-color: white;
    border: 1px solid #626663;
    alternate-background-color: #fcfcfc;
    color: black;
    outline: none;
}

QTreeView::item {
    padding: 4px;
    border-bottom: 1px solid #eeeeee;
}

QTreeView::item:selected {
    background-color: #e0e0e0;
    color: black;
}
"""


LIGHT_TREE_QSS = """
QTreeView {
    background-color: white;
    border: 1px solid #626663;
    alternate-background-color: #fcfcfc;
    color: black;
    outline: none;
}

QTreeView::item {
    padding: 4px;
    border-bottom: 1px solid #eeeeee;
}

QTreeView::item:selected {
    background-color: #e0e0e0;
    color: black;
}

QHeaderView::section {
    background-color: #fafafa;
    color: black;
    padding: 4px;
    border: 1px solid #626663;
}

QScrollBar:vertical {
    border: 1px solid #626663;
    background: #fafafa;
    width: 14px;
    margin: 0px;
}

QScrollBar::handle:vertical {
    background: #626663;
    min-height: 20px;
    margin: 2px;
}

QScrollBar::handle:vertical:hover {
    background: #4a4d4b;
}

QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
    height: 0px;
}

QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {
    background: none;
}

QScrollBar:horizontal {
    border: 1px solid #626663;
    background: #fafafa;
    height: 14px;
    margin: 0px;
}

QScrollBar::handle:horizontal {
    background: #626663;
    min-width: 20px;
    margin: 2px;
}

QScrollBar::handle:horizontal:hover {
    background: #4a4d4b;
}

QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {
    width: 0px;
}

QScrollBar::add-page:horizontal, QScrollBar::sub-page:horizontal {
    background: none;
}
"""

LIGHT_TABLE_QSS = """
QHeaderView::section {
    background-color: #fafafa;
    color: black;
    padding: 4px;
    border: 1px solid #626663;
}
"""


def apply_light_to_mainwindow(mainwindow: QWidget):
    if not mainwindow or system_is_dark():
        return

    mainwindow.setStyleSheet(LIGHT_GLOBAL_QSS)
    for tv in mainwindow.findChildren(QTreeView):
        tv.setStyleSheet(LIGHT_TREE_QSS)
    for tv in mainwindow.findChildren(QTableView):
        tv.setStyleSheet(LIGHT_TABLE_QSS)


def install_lightmode_autorefresh(mainwindow: QWidget):
    if not mainwindow or system_is_dark():
        return

    for tabs in mainwindow.findChildren(QTabWidget):
        try:
            tabs.currentChanged.connect(
                lambda _=None: apply_light_to_mainwindow(mainwindow))
        except Exception:
            pass


def _tt_dir() -> str:
    base = os.environ.get("LOCALAPPDATA") or str(
        Path.home() / "AppData" / "Local")
    p = os.path.join(base, "SubplaceJoiner")
    os.makedirs(p, exist_ok=True)
    return p


def _tt_path() -> str:
    return os.path.join(_tt_dir(), "timetracker.json")


def _app_settings_path() -> str:
    return os.path.join(_tt_dir(), "app_settings.json")


def _load_app_settings() -> dict:
    try:
        with open(_app_settings_path(), "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_app_settings(settings: dict):
    try:
        with open(_app_settings_path(), "w", encoding="utf-8") as f:
            json.dump(settings, f, indent=2)
    except Exception as e:
        print(f"[settings] save failed: {e}")


_STARTUP_REG_KEY = r"Software\Microsoft\Windows\CurrentVersion\Run"
_STARTUP_REG_NAME = "Fleasion"


def _get_startup_cmd() -> str:
    exe = sys.executable
    script = os.path.abspath(__file__)
    if os.path.basename(exe).lower() in ("python.exe", "pythonw.exe"):
        return f'"{exe}" "{script}"'
    return f'"{exe}"'


def _is_startup_enabled() -> bool:
    try:
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                             _STARTUP_REG_KEY, 0, winreg.KEY_READ)
        winreg.QueryValueEx(key, _STARTUP_REG_NAME)
        winreg.CloseKey(key)
        return True
    except Exception:
        return False


def _set_startup_enabled(enabled: bool):
    try:
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                             _STARTUP_REG_KEY, 0, winreg.KEY_SET_VALUE)
        if enabled:
            winreg.SetValueEx(key, _STARTUP_REG_NAME, 0,
                              winreg.REG_SZ, _get_startup_cmd())
        else:
            try:
                winreg.DeleteValue(key, _STARTUP_REG_NAME)
            except FileNotFoundError:
                pass
        winreg.CloseKey(key)
    except Exception as e:
        print(f"[startup] registry write failed: {e}")


def load_time_total_seconds() -> int:
    try:
        with open(_tt_path(), "r", encoding="utf-8") as f:
            d = json.load(f)
        return int(d.get("total_seconds", 0))
    except Exception:
        return 0


def save_time_total_seconds(total_seconds: int):
    d = {
        "total_seconds": int(total_seconds),
        "updated_at": int(time.time()),
    }
    try:
        with open(_tt_path(), "w", encoding="utf-8") as f:
            json.dump(d, f, indent=2)
    except Exception as e:
        print(f"[timetracker] save failed: {e}")


def fmt_hms(total_seconds: int) -> str:
    total_seconds = max(0, int(total_seconds))
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


class _TeeStream(QObject):
    text = Signal(str)

    def __init__(self, original):
        super().__init__()
        self._original = original

    def write(self, s):
        try:
            self._original.write(s)
            self._original.flush()
        except Exception:
            pass

        if s:
            self.text.emit(str(s))

    def flush(self):
        try:
            self._original.flush()
        except Exception:
            pass

    def __getattr__(self, name):
        return getattr(self._original, name)


class OutputWindow(QMainWindow):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.ui = TerminalUI()

        central = QWidget()
        self.ui.setupUi(central)
        self.setCentralWidget(central)

        self.setWindowTitle("Output Console")
        self.resize(600, 400)

    def append_text(self, s: str):
        s = s.rstrip("\n")
        if s:
            self.ui.OutputTerminal.appendPlainText(s)


class Interceptor:
    def __init__(self):
        self.modules = []

    def register_module(self, module):
        if module not in self.modules:
            self.modules.append(module)

    def unregister_module(self, module):
        if module in self.modules:
            self.modules.remove(module)

    def _dispatch(self, name, *args):
        for module in list(self.modules):
            fn = getattr(module, name, None)
            if callable(fn):
                try:
                    fn(*args)
                except Exception as e:
                    print(
                        f"[Interceptor] {module.__class__.__name__}.{name} failed: {e}")

    def request(self, flow):
        self._dispatch("request", flow)

    def response(self, flow):
        self._dispatch("response", flow)

    def websocket_message(self, flow):
        self._dispatch("websocket", flow)


INTERCEPTOR = Interceptor()


# UI / ZIP loader
def load_ui(path, parent=None):
    loader = QUiLoader()
    file = QFile(path)
    file.open(QFile.ReadOnly)
    widget = loader.load(file, parent)
    file.close()
    return widget


def load_module(path, parent_tabwidget):
    if os.path.isdir(path):
        tmpdir = path
        keep_tmpdir = False
    else:
        tmpdir_obj = tempfile.TemporaryDirectory()
        tmpdir = tmpdir_obj.name
        keep_tmpdir = True
        with zipfile.ZipFile(path, "r") as z:
            z.extractall(tmpdir)

    manifest_path = os.path.join(tmpdir, "manifest.json")
    if os.path.exists(manifest_path):
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        tab_name = manifest.get("name", "Unnamed Tab")
        tab_description = manifest.get("description", "")
    else:
        tab_name = "Unnamed Tab"
        tab_description = ""

    ui_path = os.path.join(tmpdir, "tab.ui")
    if not os.path.exists(ui_path):
        raise FileNotFoundError(ui_path)

    tab_widget = QWidget()
    layout = QVBoxLayout(tab_widget)
    layout.setContentsMargins(0, 9, 0, 9)
    layout.setSpacing(6)
    layout.addWidget(load_ui(ui_path, tab_widget))

    sys.path.insert(0, tmpdir)

    py_file = os.path.join(tmpdir, "main.py")
    if not os.path.exists(py_file):
        raise FileNotFoundError(py_file)

    module_name = f"tab_module_{os.path.basename(path).replace('.', '_')}"
    spec = importlib.util.spec_from_file_location(module_name, py_file)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    tab_logic = module.Main(tab_widget)
    parent_tabwidget.addTab(tab_widget, tab_name)

    INTERCEPTOR.register_module(tab_logic)

    if keep_tmpdir:
        tab_logic._tmpdir = tmpdir_obj

    tab_logic._tab_name = tab_name
    tab_logic._tab_description = tab_description

    return tab_logic


_MODULE_ORDER = ["cache", "subplace_tab", "randostuff", "credits"]

def load_all_modules(parent_tabwidget, modules_dir="modules"):
    loaded = []

    if not os.path.exists(modules_dir):
        os.makedirs(modules_dir)

    entries = [e for e in os.listdir(modules_dir)
               if e.endswith(".zip") or os.path.isdir(os.path.join(modules_dir, e))]

    def _order_key(name):
        base = name[:-4] if name.endswith(".zip") else name
        try:
            return _MODULE_ORDER.index(base)
        except ValueError:
            return len(_MODULE_ORDER)

    entries.sort(key=_order_key)

    for entry in entries:
        path = os.path.join(modules_dir, entry)
        try:
            tab_logic = load_module(path, parent_tabwidget)
            loaded.append(tab_logic)
        except Exception as e:
            print(f"Failed to load module '{entry}': {e}")

    return loaded


# Hi hi this is how the freaky proxy works
# Uses: hosts file redirect -> custom TLS listener -> forward to real IP
# Calls INTERCEPTOR.request / INTERCEPTOR.response with MockFlow objects

# Mock flow objects

class _MockHeaders:
    """Case-insensitive dict-like header store."""

    def __init__(self, pairs: list[tuple[str, str]]):
        # preserve order; store as list of [key, value]
        self._pairs: list[list[str]] = [[k, v] for k, v in pairs]

    def get(self, key: str, default: str = "") -> str:
        low = key.lower()
        for k, v in self._pairs:
            if k.lower() == low:
                return v
        return default

    def __getitem__(self, key: str) -> str:
        low = key.lower()
        for k, v in self._pairs:
            if k.lower() == low:
                return v
        raise KeyError(key)

    def __setitem__(self, key: str, value: str):
        low = key.lower()
        for pair in self._pairs:
            if pair[0].lower() == low:
                pair[1] = str(value)
                return
        self._pairs.append([key, str(value)])

    def __contains__(self, key: str) -> bool:
        return any(k.lower() == key.lower() for k, _ in self._pairs)

    def items(self):
        return [(k, v) for k, v in self._pairs]

    def __iter__(self):
        return iter(k for k, _ in self._pairs)


class MockRequest:
    def __init__(self, raw_bytes: bytes, sni_hostname: str):
        sep = raw_bytes.find(b"\r\n\r\n")
        if sep == -1:
            sep = len(raw_bytes)
            body_bytes = b""
        else:
            body_bytes = raw_bytes[sep + 4:]

        header_text = raw_bytes[:sep].decode("utf-8", errors="replace")
        lines = header_text.split("\r\n")

        req_parts = lines[0].split(" ", 2)
        self._method = req_parts[0] if req_parts else "GET"
        self._path = req_parts[1] if len(req_parts) > 1 else "/"

        pairs = []
        self._host_header = sni_hostname
        for line in lines[1:]:
            if ":" in line:
                k, _, v = line.partition(":")
                pairs.append((k.strip(), v.strip()))
                if k.strip().lower() == "host":
                    self._host_header = v.strip()

        self.headers = _MockHeaders(pairs)

        self._req_encoding = self.headers.get("Content-Encoding", "").lower()
        self._raw_content = b""   # initialise before property setter
        self.raw_content = body_bytes   # uses the property to update both fields

        self._sni = sni_hostname
        self._url_override: str | None = None

        scheme = "https"
        self.pretty_url = f"{scheme}://{self._host_header}{self._path}"

    @property
    def raw_content(self) -> bytes:
        return self._raw_content

    @raw_content.setter
    def raw_content(self, value: bytes):
        self._raw_content = value
        enc = getattr(self, "_req_encoding", "")
        if enc:
            dec = MockResponse._decompress(value, enc)
            self.content = dec if dec is not None else value
        else:
            self.content = value

    @property
    def url(self) -> str:
        return self._url_override or self.pretty_url

    @url.setter
    def url(self, value: str):
        self._url_override = value

    def rebuild_bytes(self) -> bytes:
        # If URL was rewritten to a different path, update the request line
        if self._url_override:
            parsed = urlparse(self._url_override)
            path = parsed.path
            if parsed.query:
                path += "?" + parsed.query
        else:
            path = self._path

        lines = [f"{self._method} {path} HTTP/1.1"]

        body = self.raw_content or b""

        for k, v in self.headers.items():
            low = k.lower()
            if low == "content-length":
                continue   # recompute below
            if low == "connection":
                continue   # we force close
            lines.append(f"{k}: {v}")

        lines.append(f"Content-Length: {len(body)}")
        lines.append("Connection: close")
        lines.append("")
        lines.append("")

        return "\r\n".join(lines).encode("utf-8") + body

    def get_forward_target(self) -> tuple[str, str]:
        """Returns (hostname_for_sni, real_ip_to_connect_to)."""
        if self._url_override:
            parsed = urlparse(self._url_override)
            host = parsed.hostname or self._host_header
        else:
            host = self._host_header
        ip = REAL_IPS.get(host) or host
        return host, ip


class MockResponse:
    def __init__(self, raw_bytes: bytes):
        sep = raw_bytes.find(b"\r\n\r\n")
        if sep == -1:
            self.status_code = 0
            self.headers = _MockHeaders([])
            self._content = raw_bytes
            self._raw = raw_bytes
            self._modified = False
            self._encoding = ""
            return

        header_text = raw_bytes[:sep].decode("utf-8", errors="replace")
        lines = header_text.split("\r\n")

        first_parts = lines[0].split(" ", 2)
        self.status_code = int(first_parts[1]) if len(first_parts) > 1 else 0

        pairs = []
        for line in lines[1:]:
            if ":" in line:
                k, _, v = line.partition(":")
                pairs.append((k.strip(), v.strip()))

        self.headers = _MockHeaders(pairs)
        self._content = raw_bytes[sep + 4:]   # raw (possibly compressed) body
        self._raw = raw_bytes
        self._modified = False
        self._encoding = self.headers.get("Content-Encoding", "").lower()

    @property
    def content(self) -> bytes:
        if self._encoding:
            decompressed = self._decompress(self._content, self._encoding)
            if decompressed is not None:
                self.headers._pairs = [
                    [k, v] for k, v in self.headers._pairs
                    if k.lower() != "content-encoding"
                ]
                self._content = decompressed
                self._encoding = ""
        return self._content

    @content.setter
    def content(self, value: bytes):
        self._content = value
        self._modified = True
        self._encoding = ""   # caller provides plain bytes

    @staticmethod
    def _decompress(data: bytes, encoding: str) -> bytes | None:
        try:
            if "gzip" in encoding:
                import gzip as _gzip
                return _gzip.decompress(data)
            if "zstd" in encoding:
                import zstandard as _zstd
                return _zstd.ZstdDecompressor().decompress(data, max_output_size=64 * 1024 * 1024)
            if "deflate" in encoding:
                import zlib as _zlib
                try:
                    return _zlib.decompress(data)
                except _zlib.error:
                    return _zlib.decompress(data, -15)
            if "br" in encoding:
                import brotli as _brotli
                return _brotli.decompress(data)
        except ImportError:
            pass
        except Exception:
            pass
        return None

    def rebuild_bytes(self) -> bytes:
        if not self._modified:
            return self._raw

        sep = self._raw.find(b"\r\n\r\n")
        status_line = "HTTP/1.1 200 OK"
        if sep != -1:
            status_line = self._raw[:sep].split(
                b"\r\n", 1)[0].decode("utf-8", errors="replace")

        new_lines = [status_line]
        skip = {"content-length", "transfer-encoding", "content-encoding"}
        for k, v in self.headers._pairs:
            if k.lower() not in skip:
                new_lines.append(f"{k}: {v}")
        new_lines.append(f"Content-Length: {len(self.content)}")
        new_lines.append("Connection: close")
        new_lines.append("")
        new_lines.append("")

        return "\r\n".join(new_lines).encode("utf-8") + self.content


class MockSyntheticResponse:

    def __init__(self, status_code: int, content: bytes, headers: dict):
        self.status_code = status_code
        self.content = content if isinstance(
            content, bytes) else content.encode("utf-8")
        self.headers = _MockHeaders(list(headers.items()))

    def rebuild_bytes(self) -> bytes:
        reason = {200: "OK", 404: "Not Found",
                  502: "Bad Gateway"}.get(self.status_code, "")
        lines = [f"HTTP/1.1 {self.status_code} {reason}"]
        for k, v in self.headers.items():
            lines.append(f"{k}: {v}")
        lines.append(f"Content-Length: {len(self.content)}")
        lines.append("Connection: close")
        lines.append("")
        lines.append("")
        return "\r\n".join(lines).encode("utf-8") + self.content


class MockFlow:
    def __init__(self, request: MockRequest):
        self.request = request
        self.response = None   # set by addon for synthetic response


# evil cert gen

def _load_ca():
    global _ca_cert_obj, _ca_key_obj
    if _ca_cert_obj is not None:
        return True
    if not _CRYPTO_OK:
        print("[proxy] cryptography package not installed — cannot generate certs")
        return False
    if not os.path.isfile(MITM_CA_PEM):
        print(f"[proxy] mitmproxy CA not found: {MITM_CA_PEM}")
        return False
    try:
        data = Path(MITM_CA_PEM).read_bytes()
        _ca_key_obj = serialization.load_pem_private_key(
            data, password=None, backend=default_backend())
        _ca_cert_obj = x509.load_pem_x509_certificate(data, default_backend())
        return True
    except Exception as e:
        print(f"[proxy] Failed to load CA: {e}")
        return False


def _make_cert(hostname: str) -> tuple[str, str] | None:
    with _cert_lock:
        if hostname in _cert_cache:
            return _cert_cache[hostname]
        if not _load_ca():
            return None
        try:
            key = rsa.generate_private_key(
                public_exponent=65537, key_size=2048, backend=default_backend())
            now = dt.datetime.now(dt.timezone.utc)
            cert = (
                x509.CertificateBuilder()
                .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, hostname)]))
                .issuer_name(_ca_cert_obj.subject)
                .public_key(key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(now - dt.timedelta(days=1))
                .not_valid_after(now + dt.timedelta(days=365))
                .add_extension(x509.SubjectAlternativeName([x509.DNSName(hostname)]), critical=False)
                .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
                .sign(_ca_key_obj, hashes.SHA256(), default_backend())
            )
            cert_pem = cert.public_bytes(serialization.Encoding.PEM)
            key_pem = key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.TraditionalOpenSSL,
                serialization.NoEncryption(),
            )
            safe = hostname.replace(".", "_")
            cert_file = os.path.join(_TMP_DIR, f"{safe}.crt")
            key_file = os.path.join(_TMP_DIR, f"{safe}.key")
            Path(cert_file).write_bytes(cert_pem)
            Path(key_file).write_bytes(key_pem)
            _cert_cache[hostname] = (cert_file, key_file)
            return cert_file, key_file
        except Exception as e:
            print(f"[proxy] cert gen failed for {hostname}: {e}")
            return None


# HTTP helpers

def _recv_until_headers(sock, timeout=10.0) -> tuple[bytes, bytes]:
    sock.settimeout(timeout)
    chunks = []
    total = b""
    while b"\r\n\r\n" not in total:
        chunk = sock.recv(16384)
        if not chunk:
            break
        chunks.append(chunk)
        total = b"".join(chunks)
    sep = total.find(b"\r\n\r\n")
    if sep == -1:
        return total, b""
    return total[:sep], total[sep + 4:]


def _recv_exactly(sock, n: int, already: bytes = b"", timeout=15.0) -> bytes:
    sock.settimeout(timeout)
    chunks = [already] if already else []
    received = len(already)
    while received < n:
        chunk = sock.recv(min(65536, n - received))
        if not chunk:
            break
        chunks.append(chunk)
        received += len(chunk)
    return b"".join(chunks)


def _dechunk(body: bytes) -> bytes:
    out = bytearray()
    i = 0
    while i < len(body):
        # Find end of chunk-size line
        end = body.find(b"\r\n", i)
        if end == -1:
            break
        size_line = body[i:end].split(
            b";", 1)[0].strip()  # strip chunk-extensions
        if not size_line:
            break
        try:
            chunk_size = int(size_line, 16)
        except ValueError:
            break
        if chunk_size == 0:
            break
        i = end + 2                          # skip \r\n after size
        out.extend(body[i: i + chunk_size])
        i += chunk_size + 2                  # skip trailing \r\n after data
    return bytes(out)


def _recv_full_response(sock, timeout=15.0) -> bytes:
    sock.settimeout(timeout)
    chunks = []
    total = b""
    while b"\r\n\r\n" not in total:
        chunk = sock.recv(65536)
        if not chunk:
            return total
        chunks.append(chunk)
        total = b"".join(chunks)

    sep = total.find(b"\r\n\r\n")
    buf = total          # alias so rest of function is unchanged
    header_text = buf[:sep].decode("utf-8", errors="replace")
    body_so_far = buf[sep + 4:]
    lines = header_text.split("\r\n")
    status_code = 0
    try:
        status_code = int(lines[0].split(" ", 2)[1])
    except Exception:
        pass

    content_length = None
    transfer_encoding = ""
    for line in lines[1:]:
        low = line.lower()
        if low.startswith("content-length:"):
            try:
                content_length = int(line.split(":", 1)[1].strip())
            except ValueError:
                pass
        if low.startswith("transfer-encoding:"):
            transfer_encoding = line.split(":", 1)[1].strip().lower()

    prefix = buf[:sep + 4]

    if status_code in (204, 304) or 100 <= status_code < 200:
        return prefix + body_so_far

    if "chunked" in transfer_encoding:
        body_chunks = [body_so_far] if body_so_far else []
        while True:
            try:
                chunk = sock.recv(65536)
            except (socket.timeout, OSError):
                break
            if not chunk:
                break
            body_chunks.append(chunk)
            if chunk.endswith(b"0\r\n\r\n") or b"\r\n0\r\n\r\n" in chunk:
                break
        body_so_far = b"".join(body_chunks)
        # Decode the chunked framing - callers expect plain body bytes
        decoded = _dechunk(body_so_far)
        # add Content-Length so MockResponse and everything downstream is happy :D
        # strip trailing \r\n\r\n
        hdr_text = prefix[:-4].decode("utf-8", errors="replace")
        new_lines = []
        for line in hdr_text.split("\r\n"):
            if line.lower().startswith("transfer-encoding"):
                continue
            new_lines.append(line)
        new_lines.append(f"Content-Length: {len(decoded)}")
        new_prefix = "\r\n".join(new_lines).encode("utf-8") + b"\r\n\r\n"
        return new_prefix + decoded

    if content_length is not None:
        body_so_far = _recv_exactly(sock, content_length, already=body_so_far)
        return prefix + body_so_far

    body_chunks = [body_so_far] if body_so_far else []
    while True:
        try:
            chunk = sock.recv(65536)
            if not chunk:
                break
            body_chunks.append(chunk)
        except (socket.timeout, OSError):
            break
    return prefix + b"".join(body_chunks)


# Per-connection handler

def _handle_connection(raw_conn: socket.socket, addr):
    ssl_conn = None
    fwd_ssl = None
    fwd_raw = None
    try:
        # TLS with SNI-based cert swap
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2

        default = _make_cert(TARGET_DOMAINS[0])
        if default is None:
            raw_conn.close()
            return
        ctx.load_cert_chain(default[0], default[1])

        def sni_cb(ssl_socket, server_name, _ctx):
            if not server_name:
                return
            try:
                ssl_socket.context = _get_server_ctx(server_name)
            except Exception as e:
                print(f"[proxy] sni swap failed ({server_name}): {e}")

        ctx.set_servername_callback(sni_cb)

        try:
            ssl_conn = ctx.wrap_socket(raw_conn, server_side=True)
        except (ssl.SSLError, OSError):
            raw_conn.close()
            return

        hostname = ssl_conn.server_hostname or TARGET_DOMAINS[0]

        # Read HTTP request
        try:
            header_bytes, leftover = _recv_until_headers(ssl_conn)
        except (socket.timeout, OSError):
            return
        if not header_bytes:
            return

        # Parse Content-Length so we can read full body
        header_text = header_bytes.decode("utf-8", errors="replace")
        content_length = 0
        for line in header_text.split("\r\n")[1:]:
            if line.lower().startswith("content-length:"):
                try:
                    content_length = int(line.split(":", 1)[1].strip())
                except ValueError:
                    pass

        body = leftover
        if content_length > 0 and len(body) < content_length:
            body = _recv_exactly(ssl_conn, content_length, already=body)

        raw_request = header_bytes + b"\r\n\r\n" + body

        # Build mock flow
        mock_req = MockRequest(raw_request, hostname)
        mock_flow = MockFlow(mock_req)

        # Call request interceptors
        INTERCEPTOR.request(mock_flow)

        # Check if addon set a synthetic response
        if mock_flow.response is not None:
            syn = mock_flow.response
            # Could be MockSyntheticResponse or a real mitmproxy Response object
            if hasattr(syn, "rebuild_bytes"):
                response_bytes = syn.rebuild_bytes()
            else:
                try:
                    status = syn.status_code
                    content = syn.content if isinstance(
                        syn.content, bytes) else b""
                    hdrs = dict(syn.headers) if syn.headers else {}
                    synth = MockSyntheticResponse(status, content, hdrs)
                    response_bytes = synth.rebuild_bytes()
                except Exception as e:
                    print(f"[proxy] synthetic response extraction failed: {e}")
                    response_bytes = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"

            ssl_conn.settimeout(10.0)
            ssl_conn.sendall(response_bytes)
            return

        fwd_host, real_ip = mock_req.get_forward_target()

        try:
            fwd_raw = socket.create_connection((real_ip, 443), timeout=10.0)
            fwd_ssl = _fwd_ctx.wrap_socket(fwd_raw, server_hostname=fwd_host)
        except Exception as e:
            print(f"[proxy] connect failed -> {fwd_host} ({real_ip}): {e}")
            return

        outgoing = mock_req.rebuild_bytes()
        fwd_ssl.settimeout(10.0)
        fwd_ssl.sendall(outgoing)

        try:
            response_raw = _recv_full_response(fwd_ssl)
        except Exception as e:
            print(f"[proxy] recv error: {e}")
            response_raw = b""

        if not response_raw:
            return

        mock_flow.response = MockResponse(response_raw)
        INTERCEPTOR.response(mock_flow)

        out_bytes = mock_flow.response.rebuild_bytes()
        ssl_conn.settimeout(10.0)
        ssl_conn.sendall(out_bytes)

    except Exception as e:
        print(f"[proxy] handler error: {e}")
    finally:
        for s in (fwd_ssl, fwd_raw, ssl_conn, raw_conn):
            try:
                if s:
                    s.close()
            except Exception:
                pass


# TCP listener

def _run_server():
    global _SRV
    _SRV = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _SRV.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        _SRV.bind(("0.0.0.0", LISTEN_PORT))
    except OSError as e:
        print(f"[proxy] Cannot bind port {LISTEN_PORT}: {e}")
        print("  Is something else on port 443? (IIS, another proxy, etc.)")
        _STOP.set()
        return

    _SRV.listen(256)
    _SRV.settimeout(1.0)
    print(
        f"[proxy] Listening on 0.0.0.0:{LISTEN_PORT} for {', '.join(TARGET_DOMAINS)}")

    while not _STOP.is_set():
        try:
            conn, addr = _SRV.accept()
        except socket.timeout:
            continue
        except OSError:
            break
        threading.Thread(target=_handle_connection,
                         args=(conn, addr), daemon=True).start()

    _SRV.close()


# Proxy startup / shutdown

def start_proxy_t():
    def runner():
        try:
            # Remove any stale hosts entries from a previous crashed/force-killed
            # run BEFORE resolving IPs - otherwise gethostbyname() returns
            # 127.0.0.1 and REAL_IPS gets poisoned, causing every forwarded
            # connection to loop back to the proxy itself (recv timeout spam +
            # empty cache finder).
            _remove_hosts()

            # Resolve real IPs before hosts redirect
            for d in TARGET_DOMAINS:
                try:
                    REAL_IPS[d] = socket.gethostbyname(d)
                    print(f"[proxy] resolved {d} -> {REAL_IPS[d]}")
                except Exception:
                    print(f"[proxy] WARNING: could not resolve {d}")

            # Generate CA if needed, inject into Roblox
            ensure_mitm_cert()
            if not _load_ca():
                raise RuntimeError("Failed to load mitmproxy CA")

            global _fwd_ctx
            _fwd_ctx = _make_fwd_ctx()

            # Pre-generate leaf certs
            for d in TARGET_DOMAINS:
                _make_cert(d)

            install_cert()
            _add_hosts()

            # Start listener
            _run_server()

        except Exception as ex:
            import traceback
            traceback.print_exc()
            ctypes.windll.user32.MessageBoxW(
                0,
                f"Failed to start proxy:\n{ex}\n\nThe program will now close.",
                "Proxy error",
                0x10
            )
            os._exit(1)

    threading.Thread(target=runner, daemon=True).start()


def stop_proxy():
    global PROXY
    _STOP.set()
    _remove_hosts()
    remove_cert()
    try:
        shutil.rmtree(_TMP_DIR, ignore_errors=True)
    except Exception:
        pass
    PROXY = None


# Main window

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.loaded_modules = []

        self._loader = QUiLoader()

        ui_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "Fleasion2.ui"
        )

        file = QFile(ui_path)

        if not file.exists():
            raise FileNotFoundError(f"UI not found: {ui_path}")

        if not file.open(QFile.ReadOnly):
            raise RuntimeError(f"Failed to open UI: {ui_path}")

        loaded_mainwindow = self._loader.load(file, None)
        file.close()

        if loaded_mainwindow is None:
            raise RuntimeError(
                f"Failed to load UI (invalid/corrupt .ui?): {ui_path}")

        central = loaded_mainwindow.centralWidget()
        if central is None:
            raise RuntimeError("UI has no centralWidget")

        self.setCentralWidget(central)
        self.setMinimumSize(800, 350)

        _icon_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fleasionlogo2.ico")
        if os.path.exists(_icon_path):
            self.setWindowIcon(QIcon(_icon_path))

        self.tabs = central.findChild(QtWidgets.QTabWidget, "tabWidget")
        if self.tabs is None:
            raise RuntimeError("tabWidget not found in UI")

        self.tabs.currentChanged.connect(self.on_tab_changed)

        chk = central.findChild(QCheckBox, "checkBox")
        if chk is None:
            raise RuntimeError("checkBox not found in UI")

        chk.toggled.connect(self.set_always_on_top)
        self._output_buffer = []
        self._output_window = None

        self._stdout_orig = sys.stdout
        self._stderr_orig = sys.stderr

        self._stdout_tee = _TeeStream(self._stdout_orig)
        self._stderr_tee = _TeeStream(self._stderr_orig)

        self._stdout_tee.text.connect(self._on_output_text)
        self._stderr_tee.text.connect(self._on_output_text)

        sys.stdout = self._stdout_tee
        sys.stderr = self._stderr_tee
        self.setup_tray()

        self._tt_total = load_time_total_seconds()
        self._tt_session_start = time.time()

        self._tt_timer = QTimer(self)
        self._tt_timer.timeout.connect(self._tick_timetracker)
        self._tt_timer.start(1000)

        self._tt_last_save = time.time()

        start_proxy_t()
        base_dir = os.path.dirname(os.path.abspath(__file__))
        modules_dir = os.path.join(base_dir, "modules")
        self.loaded_modules = load_all_modules(self.tabs, modules_dir)

    def set_always_on_top(self, enabled: bool):
        if sys.platform.startswith("win"):
            self.createWinId()

            user32 = ctypes.windll.user32
            user32.SetWindowPos.argtypes = [
                wintypes.HWND, wintypes.HWND,
                wintypes.INT, wintypes.INT, wintypes.INT, wintypes.INT,
                wintypes.UINT
            ]
            user32.SetWindowPos.restype = wintypes.BOOL

            HWND_TOPMOST = wintypes.HWND(-1)
            HWND_NOTOPMOST = wintypes.HWND(-2)
            SWP_NOMOVE = 0x0002
            SWP_NOSIZE = 0x0001
            SWP_NOACTIVATE = 0x0010
            SWP_SHOWWINDOW = 0x0040

            hwnd = wintypes.HWND(int(self.winId()))
            ok = user32.SetWindowPos(
                hwnd,
                HWND_TOPMOST if enabled else HWND_NOTOPMOST,
                0, 0, 0, 0,
                SWP_NOMOVE | SWP_NOSIZE | SWP_NOACTIVATE | SWP_SHOWWINDOW
            )

            if not ok:
                self.setWindowFlag(Qt.WindowStaysOnTopHint, enabled)
                self.show()
            return

        self.setWindowFlag(Qt.WindowStaysOnTopHint, enabled)
        self.show()

    def _on_output_text(self, s: str):
        self._output_buffer.append(s)
        if len(self._output_buffer) > 5000:
            self._output_buffer = self._output_buffer[-5000:]

        if self._output_window is not None:
            self._output_window.append_text(s)

    def _tick_timetracker(self):
        elapsed = int(time.time() - self._tt_session_start)
        total_now = self._tt_total + elapsed

        if self._output_window is not None:
            try:
                self._output_window.ui.label_2.setText(
                    f"Time wasted with this program open: {fmt_hms(total_now)}"
                )
            except Exception:
                pass

        if time.time() - self._tt_last_save >= 10:
            save_time_total_seconds(total_now)
            self._tt_last_save = time.time()

    def open_output_window(self):
        if self._output_window is None:
            self._output_window = OutputWindow(self)
            for line in self._output_buffer:
                self._output_window.append_text(line)

        self._output_window.show()
        self._output_window.raise_()
        self._output_window.activateWindow()
        try:
            elapsed = int(time.time() - self._tt_session_start)
            total_now = self._tt_total + elapsed
            self._output_window.ui.label_2.setText(
                f"Time wasted with this program open: {fmt_hms(total_now)}"
            )
        except Exception:
            pass

    def setup_tray(self):
        if not QSystemTrayIcon.isSystemTrayAvailable():
            return
        icon_path = os.path.join(os.path.dirname(
            os.path.abspath(__file__)), "fleasionlogo2.ico")

        if os.path.exists(icon_path):
            icon = QIcon(icon_path)
        else:
            icon = self.style().standardIcon(QtWidgets.QStyle.SP_ComputerIcon)

        self.tray = QSystemTrayIcon(icon, self)
        self.tray.setToolTip("Fleasion")

        menu = QtWidgets.QMenu()

        act_title = QAction("Fleasion v2.0", self)
        act_title.setEnabled(False)
        menu.addAction(act_title)
        menu.addSeparator()

        act_show = QAction("Show", self)
        act_show.triggered.connect(self.show_from_tray)
        menu.addAction(act_show)

        act_output = QAction("Output", self)
        act_output.triggered.connect(self.open_output_window)
        menu.addAction(act_output)
        menu.addSeparator()

        act_discord = QAction("Discord", self)
        act_discord.triggered.connect(lambda: QDesktopServices.openUrl(QUrl("http://discord.gg/invite/hXyhKehEZF")))
        menu.addAction(act_discord)

        act_donate = QAction("Donate", self)
        act_donate.triggered.connect(lambda: QDesktopServices.openUrl(QUrl("https://ko-fi.com/fleasion")))
        menu.addAction(act_donate)

        menu.addSeparator()

        act_light = QAction("Light Mode", self)
        act_light.setCheckable(True)
        act_light.setChecked(not system_is_dark())
        act_light.triggered.connect(
            lambda checked: self._toggle_light_mode(checked))
        menu.addAction(act_light)
        self._tray_light_action = act_light

        from PySide6.QtWidgets import QStyleFactory
        style_menu = QtWidgets.QMenu("Style", menu)
        for style_name in QStyleFactory.keys():
            act = QAction(style_name, self)
            act.triggered.connect(
                lambda _checked, s=style_name: self._apply_style(s))
            if style_name.lower() == _default_style_name.lower():
                act.setText(f"{style_name} (default)")
            style_menu.addAction(act)
        menu.addMenu(style_menu)
        menu.addSeparator()

        act_delete_db = QAction("Delete DB", self)
        def _tray_delete_db():
            for m in getattr(self, "loaded_modules", []):
                if hasattr(m, "delete_roblox_db"):
                    m.delete_roblox_db()
                    break
        act_delete_db.triggered.connect(_tray_delete_db)
        menu.addAction(act_delete_db)

        settings_menu = QtWidgets.QMenu("Settings", menu)

        act_startup = QAction("Run on Startup", self)
        act_startup.setCheckable(True)
        act_startup.setChecked(_is_startup_enabled())
        act_startup.triggered.connect(
            lambda checked: _set_startup_enabled(checked))
        settings_menu.addAction(act_startup)

        act_del_db = QAction("Delete DB on start", self)
        act_del_db.setCheckable(True)
        act_del_db.setChecked(_load_app_settings().get("del_db_on_start", False))
        act_del_db.triggered.connect(lambda checked: self._set_del_db_on_start(checked))
        settings_menu.addAction(act_del_db)

        act_clear_finder = QAction("Clear finder on start", self)
        act_clear_finder.setCheckable(True)
        act_clear_finder.setChecked(_load_app_settings().get("clear_finder_on_start", False))
        act_clear_finder.triggered.connect(lambda checked: self._set_clear_finder_on_start(checked))
        settings_menu.addAction(act_clear_finder)

        act_show_on_start = QAction("Show on start", self)
        act_show_on_start.setCheckable(True)
        act_show_on_start.setChecked(_load_app_settings().get("show_on_start", True))
        act_show_on_start.triggered.connect(lambda checked: self._set_show_on_start(checked))
        settings_menu.addAction(act_show_on_start)

        menu.addMenu(settings_menu)
        menu.addSeparator()

        act_quit = QAction("Quit", self)
        act_quit.triggered.connect(self.quit_app)
        menu.addAction(act_quit)
        self.tray.setContextMenu(menu)
        self.tray.activated.connect(self.on_tray_activated)
        self.tray.show()

    def _apply_style(self, style_name: str):
        app.setStyle(style_name)
        s = _load_app_settings()
        s["style"] = style_name
        _save_app_settings(s)

    def _toggle_light_mode(self, enabled: bool):
        hints = app.styleHints()
        if enabled:
            hints.setColorScheme(Qt.ColorScheme.Light)
            apply_light_to_mainwindow(self)
        else:
            hints.setColorScheme(Qt.ColorScheme.Unknown)
            for w in app.allWidgets():
                if isinstance(w, (QTreeView, QTableView)):
                    w.setStyleSheet("")
            for w in app.topLevelWidgets():
                w.setStyleSheet("")
        s = _load_app_settings()
        s["light_mode"] = enabled
        _save_app_settings(s)

    def _set_del_db_on_start(self, enabled: bool):
        s = _load_app_settings()
        s["del_db_on_start"] = enabled
        _save_app_settings(s)

    def _set_clear_finder_on_start(self, enabled: bool):
        s = _load_app_settings()
        s["clear_finder_on_start"] = enabled
        _save_app_settings(s)

    def _set_show_on_start(self, enabled: bool):
        s = _load_app_settings()
        s["show_on_start"] = enabled
        _save_app_settings(s)

    def show_from_tray(self):
        self.show()
        self.raise_()
        self.activateWindow()

    def on_tray_activated(self, reason):
        if reason == QSystemTrayIcon.DoubleClick:
            self.show_from_tray()

    def quit_app(self):
        try:
            if hasattr(self, "tray") and self.tray:
                self.tray.hide()
        except Exception:
            pass

        self.cleanup()
        try:
            sys.stdout = self._stdout_orig
            sys.stderr = self._stderr_orig
        except Exception:
            pass
        try:
            elapsed = int(time.time() - self._tt_session_start)
            save_time_total_seconds(self._tt_total + elapsed)
        except Exception:
            pass
        QtWidgets.QApplication.quit()

    def event(self, event):
        super().event(event)
        for module in self.loaded_modules:
            if hasattr(module, "event"):
                module.event(event)
        return True

    def on_tab_changed(self, index):
        current_widget = self.tabs.widget(index)
        for module in self.loaded_modules:
            if hasattr(module, "_tab_name") and module.tab_widget is current_widget:
                if hasattr(module, "on_focus"):
                    try:
                        module.on_focus()
                    except Exception as e:
                        print(f"[on_focus] {module._tab_name} failed: {e}")

    def cleanup(self):
        print("Stopping proxy...")
        stop_proxy()

        for module in self.loaded_modules:
            if hasattr(module, "cleanup_before_exit"):
                try:
                    module.cleanup_before_exit()
                except Exception as e:
                    print(f"Module cleanup failed: {e}")

        self.loaded_modules.clear()

        import gc
        gc.collect()

        print("Cleanup done.")

    def closeEvent(self, event):
        if hasattr(self, "tray") and self.tray and self.tray.isVisible():
            event.ignore()
            self.hide()
            try:
                self.tray.showMessage(
                    "Fleasion",
                    "Still running in the system tray. Right-click the tray icon to quit.",
                    QSystemTrayIcon.Information,
                    2000
                )
            except Exception:
                pass
            return
        try:
            self.cleanup()
        except Exception as e:
            print(f"Error during cleanup: {e}")

        event.accept()


if __name__ == "__main__":
    QApplication.setAttribute(Qt.AA_ShareOpenGLContexts)
    app = QApplication(sys.argv)
    _default_style_name = app.style().objectName()
    _saved_settings = _load_app_settings()
    if _saved_settings.get("style"):
        app.setStyle(_saved_settings["style"])
    window = MainWindow()
    if _saved_settings.get("light_mode"):
        app.styleHints().setColorScheme(Qt.ColorScheme.Light)
        if hasattr(window, "_tray_light_action"):
            window._tray_light_action.setChecked(True)
    # Create a hidden dummy QOpenGLWidget before show() so Qt sets WS_CLIPCHILDREN
    # on the native HWND during initial creation — prevents full-window flash on
    # the first real GL widget (mesh/anim preview).
    from PySide6.QtOpenGLWidgets import QOpenGLWidget as _QOGLW
    _dummy_gl = _QOGLW(window)
    _dummy_gl.setFixedSize(1, 1)
    _dummy_gl.move(-2, -2)
    if _saved_settings.get("show_on_start", True):
        window.show()
    _dummy_gl.hide()
    apply_light_to_mainwindow(window)
    install_lightmode_autorefresh(window)
    sys.exit(app.exec())
