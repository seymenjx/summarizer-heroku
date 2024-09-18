import os
from redis import Redis
from rq import Worker, Queue, Connection
from dotenv import load_dotenv
from logger import setup_logger

load_dotenv()
logger = setup_logger(__name__)

redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
redis_conn = Redis.from_url(redis_url)

if __name__ == '__main__':
    logger.info("Starting worker")
    with Connection(redis_conn):
        worker = Worker(Queue('default'))
        logger.info("Worker started, waiting for jobs...")
        worker.work()



