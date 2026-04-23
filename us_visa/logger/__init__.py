import sys
from pathlib import Path
from datetime import datetime
import logging



BASE_DIR = Path(__file__).resolve().parents[2]

log_dir = BASE_DIR / "logs"
log_dir.mkdir(exist_ok=True)


log_file = log_dir / f"{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"

# root logger config
logging.basicConfig(
    format="[ %(asctime)s ] %(name)s - %(levelname)s - %(message)s",
    level=logging.DEBUG,
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

# pymongo log config
logging.getLogger("pymongo").setLevel(logging.ERROR)
logging.getLogger("pymongo.topology").setLevel(logging.ERROR)
logging.getLogger("pymongo.connection").setLevel(logging.ERROR)
logging.getLogger("pymongo.command").setLevel(logging.ERROR)
