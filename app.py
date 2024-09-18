import os
import boto3
from botocore.exceptions import ClientError
from flask import Flask, request, jsonify
from rq import Queue
from redis import Redis
from dotenv import load_dotenv
from summarizer import summarize_files_from_s3
from logger import setup_logger
from rq.job import Job
from rq_dashboard import RQDashboard

load_dotenv()

app = Flask(__name__)
logger = setup_logger(__name__)

print("Starting application...")
logger.debug("This is a debug message")
logger.info("This is an info message")
logger.warning("This is a warning message")
logger.error("This is an error message")
print("Logger set up")

# Initialize Redis connection
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
try:
    redis_conn = Redis.from_url(redis_url)
    redis_conn.ping()
    q = Queue(connection=redis_conn)
    logger.info("Successfully connected to Redis")
except Exception as e:
    logger.error(f"Error connecting to Redis: {str(e)}")

# Initialize S3 client
s3_client = boto3.client('s3')

@app.route('/summarize', methods=['POST'])
def summarize():
    logger.info("Summarize endpoint called")
    data = request.json
    bucket_name = data.get('bucket_name')
    prefix = data.get('prefix', '')
    max_files = data.get('max_files', 100)
    max_workers = data.get('max_workers', 10)

    logger.info(f"Received summarization request for bucket: {bucket_name}, prefix: {prefix}")
    
    # Test S3 connection
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
        logger.info(f"Successfully listed objects in S3 bucket. Response: {response}")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"S3 ClientError: {error_code} - {error_message}")
        return jsonify({'error': f'S3 Error: {error_code} - {error_message}'}), 403
    except Exception as e:
        logger.error(f"Unexpected error accessing S3: {str(e)}")
        return jsonify({'error': f'Unexpected error accessing S3: {str(e)}'}), 500

    # Enqueue job
    try:
        job = q.enqueue(summarize_files_from_s3, bucket_name, prefix, max_files, max_workers)
        logger.info(f"Successfully enqueued job with ID: {job.id}")
        return jsonify({'job_id': job.id}), 202
    except Exception as e:
        logger.error(f"Error enqueueing job: {str(e)}")
        return jsonify({'error': f'Error enqueueing job: {str(e)}'}), 500

@app.route('/cancel/<job_id>', methods=['POST'])
def cancel_job(job_id):
    try:
        job = Job.fetch(job_id, connection=redis_conn)
        job.cancel()
        return jsonify({'message': f'Job {job_id} has been cancelled'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 400

RQDashboard(app)

if __name__ == '__main__':
    logger.info("Starting Flask application")
    app.run(debug=True)

