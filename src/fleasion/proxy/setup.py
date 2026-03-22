"""Proxy environment setup: hosts file redirection and Roblox cert injection."""

import os
import glob
from pathlib import Path

try:
    from mitmproxy import certs as mitm_certs
except Exception:
    mitm_certs = None

HOME = os.environ.get("USERPROFILE", os.path.expanduser("~"))
MITM_CA_PEM = os.path.join(HOME, ".mitmproxy", "mitmproxy-ca.pem")
MITM_CA_CERT = os.path.join(HOME, ".mitmproxy", "mitmproxy-ca-cert.pem")
HOSTS_FILE = r"C:\Windows\System32\drivers\etc\hosts"
HOSTS_MARKER = "# Fleasion-proxy"
TARGET_DOMAINS = [
    "assetdelivery.roblox.com",
    "fts.rbxcdn.com",
    "gamejoin.roblox.com",
]
ROBLOX_PATHS = {
    "Roblox":    os.path.join(HOME, "AppData", "Local", "Roblox",    "Versions"),
    "Bloxstrap": os.path.join(HOME, "AppData", "Local", "Bloxstrap", "Versions"),
    "Fishstrap": os.path.join(HOME, "AppData", "Local", "Fishstrap", "Versions"),
    "Voidstrap": os.path.join(HOME, "AppData", "Local", "Voidstrap", "RblxVersions"),
    "Plexity":   os.path.join(HOME, "AppData", "Local", "Plexity"),
}


# Hosts file

def _add_hosts():
    text = Path(HOSTS_FILE).read_text(encoding="utf-8", errors="replace")
    lines = [l for l in text.splitlines() if HOSTS_MARKER not in l]
    for d in TARGET_DOMAINS:
        lines.append(f"127.0.0.1 {d} {HOSTS_MARKER}")
    Path(HOSTS_FILE).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[proxy] hosts: redirected {', '.join(TARGET_DOMAINS)}")


def _remove_hosts():
    try:
        text = Path(HOSTS_FILE).read_text(encoding="utf-8", errors="replace")
        lines = [l for l in text.splitlines() if HOSTS_MARKER not in l]
        Path(HOSTS_FILE).write_text("\n".join(lines) + "\n", encoding="utf-8")
        print("[proxy] hosts: cleaned up")
    except Exception as e:
        print(f"[proxy] hosts cleanup error: {e}")


# Roblox cacert.pem injection

def ensure_mitm_cert() -> bool:
    """Generate mitmproxy CA if it doesn't exist yet."""
    confdir = Path.home() / ".mitmproxy"
    confdir.mkdir(exist_ok=True)
    if mitm_certs is not None:
        try:
            mitm_certs.CertStore.from_store(str(confdir), "mitmproxy", 2048)
        except Exception as e:
            print(f"[proxy] CertStore.from_store failed: {e}")
    return (confdir / "mitmproxy-ca-cert.pem").exists()


def install_cert() -> bool:
    if not ensure_mitm_cert():
        return False
    if not os.path.isfile(MITM_CA_CERT):
        return False
    our_cert = Path(MITM_CA_CERT).read_text(encoding="utf-8").strip()

    for name, base in ROBLOX_PATHS.items():
        if not os.path.isdir(base):
            continue
        try:
            for entry in os.scandir(base):
                if not entry.is_dir():
                    continue
                if glob.glob(os.path.join(entry.path, "*PlayerBeta.exe")):
                    cacert_path = os.path.join(entry.path, "ssl", "cacert.pem")
                    if not os.path.isfile(cacert_path):
                        continue
                    content = Path(cacert_path).read_text(encoding="utf-8")
                    if our_cert in content:
                        content = content.replace(our_cert, "")
                    content = content.rstrip() + "\n\n" + our_cert + "\n"
                    Path(cacert_path).write_text(content, encoding="utf-8")
                    print(
                        f"[proxy] cert injected -> {name}: ...{cacert_path[-50:]}")
        except PermissionError:
            pass
    return True


def remove_cert():
    if not os.path.isfile(MITM_CA_CERT):
        return
    try:
        our_cert = Path(MITM_CA_CERT).read_text(encoding="utf-8").strip()
        for name, base in ROBLOX_PATHS.items():
            if not os.path.isdir(base):
                continue
            try:
                for entry in os.scandir(base):
                    if not entry.is_dir():
                        continue
                    if glob.glob(os.path.join(entry.path, "*PlayerBeta.exe")):
                        cacert_path = os.path.join(
                            entry.path, "ssl", "cacert.pem")
                        if not os.path.isfile(cacert_path):
                            continue
                        content = Path(cacert_path).read_text(encoding="utf-8")
                        if our_cert in content:
                            content = content.replace(
                                our_cert, "").rstrip() + "\n"
                            Path(cacert_path).write_text(
                                content, encoding="utf-8")
                            print(f"[proxy] cert removed from {name}")
            except PermissionError:
                pass
    except Exception as e:
        print(f"[proxy] cert remove error: {e}")
