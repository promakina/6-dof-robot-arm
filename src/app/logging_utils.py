import logging
import os
from typing import Optional

from app_paths import get_log_path

_FILE_HANDLERS: dict[str, logging.Handler] = {}


def _get_file_logger(
    name: str,
    log_filename: str,
    env_var: str,
    level: Optional[str] = None,
) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s:%(name)s:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = _FILE_HANDLERS.get(log_filename)
    if file_handler is None:
        file_handler = logging.FileHandler(get_log_path(log_filename), mode="w", encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)
        _FILE_HANDLERS[log_filename] = file_handler

    # Console output is intentionally disabled; use log files only.
    logger.addHandler(file_handler)
    logger.propagate = False

    resolved_level = level or os.getenv(env_var, "INFO")
    logger.setLevel(resolved_level.upper())
    return logger


def get_stream_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    return _get_file_logger(name, "streaming_motion.log", "ROBOT_STREAM_LOG_LEVEL", level)


def get_ik_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    return _get_file_logger(name, "ik_solution.log", "ROBOT_IK_LOG_LEVEL", level)


def get_keyframe_logger(name: str = "keyframes", level: Optional[str] = None) -> logging.Logger:
    return _get_file_logger(name, "keyframe_interpolation.log", "ROBOT_KEYFRAME_LOG_LEVEL", level)
