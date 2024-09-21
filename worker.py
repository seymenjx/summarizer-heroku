import asyncio
import redis
import json
import logging
import sys
from summarizer.core import run_summarize_files_from_s3  # Updated import

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

redis_url = 'redis://localhost:6379'  # Make sure this matches the Flask app's Redis URL
queue_name = 'default'
results_key = 'summarization_results'

async def process_job(job_data):
    try:
        bucket_name = job_data['bucket_name']
        prefix = job_data.get('prefix', '')
        max_files = job_data.get('max_files', 100)
        logger.info(f"Processing job: bucket={bucket_name}, prefix={prefix}, max_files={max_files}")
        
        redis_client = redis.Redis.from_url(redis_url)
        
        summaries = run_summarize_files_from_s3(bucket_name, prefix, max_files)
        async for partial_result in summaries:
            logger.info(f"Received partial result: {partial_result}")
            current_results = json.loads(redis_client.get(results_key) or '{}')
            current_results.update(partial_result)
            redis_client.set(results_key, json.dumps(current_results))
            logger.info(f"Partial result stored: {partial_result}")
        
        logger.info(f"Job completed successfully. All summaries stored in Redis under key: {results_key}")
        return "Job completed"
    except Exception as e:
        logger.error(f"Error processing job: {str(e)}", exc_info=True)
        return f"Error: {str(e)}"

async def worker():
    redis_client = redis.Redis.from_url(redis_url)
    logger.info(f"Worker started, listening on queue: {queue_name}")
    
    while True:
        try:
            logger.debug("Waiting for job...")
            result = redis_client.blpop(queue_name, timeout=5)
            if result is None:
                continue
            
            _, job_data = result
            job = json.loads(job_data.decode('utf-8'))
            logger.info(f"Received job: {job}")
            
            result = await process_job(job)
            
            if result:
                logger.info(f"Job completed successfully: {result}")
            else:
                logger.warning("Job failed")
        except Exception as e:
            logger.error(f"Error in worker loop: {str(e)}", exc_info=True)
        
        await asyncio.sleep(1)  # Small delay to prevent tight loop

def test_redis_connection():
    try:
        redis_client = redis.Redis.from_url(redis_url)
        redis_client.ping()
        logger.info("Redis connection successful")
        
        # Check queue length
        queue_length = redis_client.llen(queue_name)
        logger.info(f"Current queue length: {queue_length}")
        
        # Check queue contents
        queue_contents = redis_client.lrange(queue_name, 0, -1)
        logger.info(f"Queue contents: {queue_contents}")
    except redis.ConnectionError:
        logger.error("Failed to connect to Redis", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    asyncio.run(worker())





