from flask import Flask, Response, request, jsonify
import redis
import json
import logging
import os
import uuid
from redis import Redis
from rq import Queue, Worker
from summarizer.core import run_summarize_files_from_s3  # Updated import
import asyncio
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
redis_client = redis.Redis.from_url(redis_url)
redis_client.config_set('maxmemory-policy', 'allkeys-lru')
queue_name = 'default'
results_key = 'summarization_results'
benchmark_key = 'summarization_benchmark'

@app.route('/summarize', methods=['POST'])
def summarize():
    data = request.json
    logger.info(f"Received request body: {data}")  # Log the entire request body
    bucket_name = data['bucket_name']
    prefix = data.get('prefix', '')
    max_files = data.get('max_files', 100)

    job_id = str(uuid.uuid4())  # Generate a unique ID for the job
    job_data = {
        'id': job_id,
        'bucket_name': bucket_name,
        'prefix': prefix,
        'max_files': max_files
    }

    # Push the job to Redis with expiration time
    redis_client.rpush(queue_name, json.dumps(job_data))
    redis_client.set(f"job:{job_id}", json.dumps(job_data), ex=3600)  # 1 hour expiration
    logger.info(f"Job enqueued: {job_data}")

    return jsonify({'job_id': job_id}), 202

@app.route('/get_summaries', methods=['GET'])
def get_summaries():
    try:
        data = request.args
        if not data:
            return jsonify({"error": "Invalid request parameters"}), 400

        job_id = data.get('job_id')
        if not job_id:
            return jsonify({"error": "Missing 'job_id' in request"}), 400

        # Fetch summarized data from Redis using the job ID
        redis_key = f"{results_key}:{job_id}"
        logger.info(f"Fetching summaries from Redis with key: {redis_key}")
        summaries = redis_client.get(redis_key)
        if not summaries:
            logger.warning(f"No summaries found for job ID: {job_id}")
            return jsonify({"error": "No summaries found for the given job ID"}), 404

        summaries = json.loads(summaries)
        return jsonify(summaries)
    except Exception as e:
        logger.error(f"Error in get_summaries: {str(e)}")
        return jsonify({"error": "Failed to fetch summaries"}), 500



@app.route('/get_benchmark', methods=['GET'])
def get_benchmark():
    try:
        # Fetch all benchmark data from Redis
        benchmark_data = redis_client.hgetall(benchmark_key)
        
        if not benchmark_data:
            return jsonify({"message": "No benchmark data available"}), 404

        # Convert byte strings to regular strings and floats
        results = {
            job_id.decode('utf-8'): float(processing_time)
            for job_id, processing_time in benchmark_data.items()
        }

        # Calculate some statistics
        times = list(results.values())
        stats = {
            "total_jobs": len(times),
            "average_time": sum(times) / len(times),
            "min_time": min(times),
            "max_time": max(times)
        }

        return jsonify({
            "benchmark_results": results,
            "statistics": stats
        })

    except Exception as e:
        logger.error(f"Error in get_benchmark: {str(e)}")
        return jsonify({"error": "Failed to fetch benchmark results"}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
