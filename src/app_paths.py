from pathlib import Path


_SRC_DIR = Path(__file__).resolve().parent
_LOG_DIR = _SRC_DIR / "logs"
_SETTINGS_DIR = _SRC_DIR / "settings"


def get_log_path(filename: str) -> Path:
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    return _LOG_DIR / filename


def get_settings_path(filename: str) -> Path:
    return _SETTINGS_DIR / filename
