import time
import signal
import logging
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from Growatt.measurements import fetch_and_log

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("growatt_scheduler")

scheduler = BackgroundScheduler()
running = True

def job():
    logger.info("Starting Growatt data fetch...")
    try:
        fetch_and_log()
        logger.info("Growatt job successful.")
    except Exception as e:
        logger.error(f"Growatt job failed: {e}")

def shutdown(sig, frame):
    global running
    logger.info("Stopping Growatt scheduler...")
    scheduler.shutdown()
    running = False

signal.signal(signal.SIGINT, shutdown)

if __name__ == "__main__":
    scheduler.add_job(job,
                       "interval", 
                       minutes=5,
                       next_run_time=datetime.now())
    scheduler.start()
    logger.info("Growatt Scheduler Started (Press Ctrl+C to stop)")
    
    try:
        while running:
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown(None, None)