import asyncio
import redis
import json
import logging
import sys
from summarizer.core import run_summarize_files_from_s3  # Updated import

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

redis_url = 'redis://localhost:6379'  # Make sure this matches the Flask app's Redis URL
queue_name = 'default'
results_key = 'summarization_results'
benchmark_key = 'summarization_benchmark'

async def process_job(job_data):
    try:
        start_time = asyncio.get_event_loop().time()
        job_id = job_data['id']
        bucket_name = job_data['bucket_name']
        prefix = job_data.get('prefix', '')
        max_files = job_data.get('max_files', 100)
        logger.info(f"Processing job: bucket={bucket_name}, prefix={prefix}, max_files={max_files}")
        
        redis_client = redis.Redis.from_url(redis_url)
        
        async for partial_result in run_summarize_files_from_s3(bucket_name, prefix, max_files):
            current_results = json.loads(redis_client.get(f"{results_key}:{job_id}") or '{}')
            current_results.update(partial_result)
            redis_client.set(f"{results_key}:{job_id}", json.dumps(current_results))
        
        end_time = asyncio.get_event_loop().time()
        processing_time = end_time - start_time
        logger.info(f"Job completed successfully in {processing_time:.2f} seconds. All summaries stored in Redis under key: {results_key}:{job_id}")
        
        # Store processing time in benchmark_key
        redis_client.hset(benchmark_key, job_id, processing_time)
        
        return f"Job completed in {processing_time:.2f} seconds"
    except Exception as e:
        logger.error(f"Error processing job: {str(e)}", exc_info=True)
        return f"Error: {str(e)}"

async def main():
    redis_client = redis.Redis.from_url(redis_url)
    while True:
        _, job_data = await asyncio.to_thread(redis_client.blpop, queue_name)
        job = json.loads(job_data)
        await process_job(job)

if __name__ == '__main__':
    asyncio.run(main())
