from flask import Flask, Response, request, jsonify
import redis
import json
import logging
import os
import uuid
from redis import Redis
from rq import Queue, Worker
from summarizer.core import run_summarize_files_from_s3  # Updated import

app = Flask(__name__)

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

redis_url = 'redis://localhost:6379'
redis_client = redis.Redis.from_url(redis_url)
queue_name = 'default'
results_key = 'summarization_results'

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

    # Push the job to Redis
    redis_client.rpush(queue_name, json.dumps(job_data))
    logger.info(f"Job enqueued: {job_data}")

    return jsonify({'job_id': job_id}), 202

@app.route('/get_summaries', methods=['GET'])
def get_summaries():
    summaries = redis_client.get(results_key)
    if summaries:
        try:
            summaries_dict = json.loads(summaries)
            return jsonify(summaries_dict), 200
        except json.JSONDecodeError:
            logger.error("Failed to decode summaries JSON")
            return jsonify({"error": "Invalid summary data"}), 500
    else:
        return jsonify({"error": "No summaries available"}), 404

if __name__ == '__main__':
    app.run(debug=True, port=5001)

