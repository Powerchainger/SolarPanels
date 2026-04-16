import os
import time
import signal
import logging
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

from EnPhase.cloud_measurements import main


# ----------------------------
# LOGGING
# ----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("scheduler")
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

file_handler = logging.FileHandler(os.path.join(LOG_DIR, "scheduler.log"))
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)


# ----------------------------
# SCHEDULER
# ----------------------------
scheduler = BackgroundScheduler()
running = True


def job():
    logger.info("Starting hourly inverter data fetch")
    try:
        main()
        logger.info("Job completed successfully")
    except Exception as e:
        logger.exception(f"Job failed: {e}")


def shutdown(sig=None, frame=None):
    global running
    logger.info("Shutdown signal received → stopping scheduler...")
    running = False
    scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")


# Register Ctrl+C handler
signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def start():
    scheduler.add_job(
        job,
        "interval",
        hours=1,
        next_run_time=datetime.now(),
        max_instances=1,
        coalesce=True,
    )

    scheduler.start()
    logger.info("Scheduler started (Ctrl+C to stop)")

    try:
        while running:
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown()


if __name__ == "__main__":
    start()