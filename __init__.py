import subprocess
import sys

def _ensure_package(
    import_name: str, pip_name: str | None = None, required: bool = True
) -> None:
    try:
        __import__(import_name)
        return
    except Exception:
        pass

    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "-U", pip_name or import_name]
        )
    except Exception:
        if required:
            raise


_ensure_package("requests", required=True)
# Optional feature: browser session cookie extraction.
_ensure_package("browser_cookie3", "browser-cookie3", required=False)
