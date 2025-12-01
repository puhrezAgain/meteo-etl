import logging
import sys

def configure_logger() -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    handler = logging.StreamHandler(sys.stderr)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

