import logging
from logging.handlers import RotatingFileHandler
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.propagate = False

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")

os.makedirs(LOG_DIR, exist_ok=True)

file_handler = RotatingFileHandler(
    os.path.join(LOG_DIR, "scheduler.log"),
    maxBytes=5_000_000,  # 5 MB
    backupCount=15,  # keep up to 15 log files
)
stream_handler = logging.StreamHandler()

formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)
