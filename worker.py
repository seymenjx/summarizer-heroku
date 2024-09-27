import asyncio
import redis
import json
import logging
import sys
import os
from summarizer.core import run_summarize_files_from_s3  # Updated import
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')  # Make sure this matches the Flask app's Redis URL
queue_name = 'default'
results_key = 'summarization_results'
benchmark_key = 'summarization_benchmark'

async def process_job(job):
    bucket_name = job['bucket_name']
    prefix = job.get('prefix', '')
    max_files = job.get('max_files', 100)
    job_id = job['id']

    logger.info(f"Processing job: bucket={bucket_name}, prefix={prefix}, max_files={max_files}")

    try:
        result = await run_summarize_files_from_s3(bucket_name, prefix, max_files)
        
        # Store the result in Redis
        redis_client = redis.Redis.from_url(redis_url)
        redis_client.set(f"{results_key}:{job_id}", json.dumps(result), ex=3600)  # 1 hour expiration
        logger.info(f"Job completed: {job_id}")
    except Exception as e:
        logger.error(f"Error processing job: {str(e)}")
        redis_client = redis.Redis.from_url(redis_url)
        redis_client.set(f"{results_key}:{job_id}", json.dumps({"error": str(e)}), ex=3600)

async def main():
    redis_client = redis.Redis.from_url(redis_url)
    while True:
        _, job_data = await asyncio.to_thread(redis_client.blpop, queue_name)
        job = json.loads(job_data)
        await process_job(job)
        # Set expiration time for job data
        redis_client.set(f"job:{job['id']}", json.dumps(job), ex=3600)  # 1 hour expiration

if __name__ == '__main__':
    asyncio.run(main())
