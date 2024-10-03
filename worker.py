import asyncio
import redis
import json
import logging
import os
import signal
from summarizer.core import run_summarize_files_from_s3
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
queue_name = 'default'
job_status_key = 'job_status'
current_job_id = None

async def process_job(job):
    global current_job_id
    current_job_id = job['id']
    bucket_name = job['bucket_name']
    prefix = job.get('prefix', '')
    max_files = job.get('max_files', 100)
    processed_files = 0

    logger.info(f"Processing job: bucket={bucket_name}, prefix={prefix}, max_files={max_files}")

    try:
        await run_summarize_files_from_s3(bucket_name, prefix, max_files)
        logger.info(f"Job completed: {current_job_id}")
    except Exception as e:
        logger.error(f"Error processing job: {str(e)}")
        redis_client = redis.Redis.from_url(redis_url)
        redis_client.hset(job_status_key, current_job_id, json.dumps({"error": str(e)}))

async def main():
    redis_client = redis.Redis.from_url(redis_url)
    while True:
        _, job_data = await asyncio.to_thread(redis_client.blpop, queue_name)
        job = json.loads(job_data)
        await process_job(job)

def handle_shutdown(signum, frame):
    logger.info(f"Received shutdown signal: {signum}. Saving job status...")
    if current_job_id:
        redis_client = redis.Redis.from_url(redis_url)
        redis_client.hset(job_status_key, current_job_id, json.dumps({"status": "stopped"}))
    asyncio.get_event_loop().stop()

if __name__ == '__main__':
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    asyncio.run(main())
