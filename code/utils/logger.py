import logging
from pathlib import Path
from datetime import datetime


def setup_logging(
    log_file=None,
    log_level=logging.INFO,
    console=True,
    reset_file=True,
    logger_name=None,
):
    """
    Setup a simple file logger for the application.

    Args:
        log_file: Path to the log file. If None, creates a timestamped log file.
        log_level: Logging level.
        console: Whether to also log to console.
        reset_file: If True, reset the log file on setup.
        logger_name: If provided, configure only this named logger.
    """
    if log_file is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = Path("logs") / f"app_{timestamp}.log"
    else:
        log_file = Path(log_file)

    log_file.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    logger.propagate = logger_name is None

    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    file_mode = "w" if reset_file else "a"
    file_handler = logging.FileHandler(log_file, mode=file_mode, encoding='utf-8')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def get_logger(name=__name__):
    return logging.getLogger(name)
