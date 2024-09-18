import os
os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'

import redis
from rq import Worker, Queue, Connection
from dotenv import load_dotenv
from logger import setup_logger
import traceback

load_dotenv()
logger = setup_logger(__name__)

redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
redis_conn = redis.from_url(redis_url)

try:
    redis_conn.ping()
    logger.info("Successfully connected to Redis")
except redis.ConnectionError:
    logger.error("Failed to connect to Redis")

def exception_handler(job, exc_type, exc_value, traceback):
    logger.error(f"Error in job {job.id}: {exc_type.__name__}: {exc_value}")
    logger.error(traceback.format_exc())

if __name__ == '__main__':
    logger.info("Starting worker")
    with Connection(redis_conn):
        worker = Worker(Queue('default'), exception_handlers=[exception_handler])
        logger.info("Worker started, waiting for jobs...")
        worker.work()



