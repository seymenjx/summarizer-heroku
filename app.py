import os
import logging
from flask import Flask, jsonify, request
from rq import Queue
from redis import Redis
from rq.job import Job
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
import sys
from summarizer import summarize_files_from_s3

# Load environment variables from .env file
load_dotenv()

def setup_logging():
    """
    Set up logging configuration for the application.
    """
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Console Handler for stdout
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    
    # File Handler with rotation
    file_handler = RotatingFileHandler('app.log', maxBytes=10240, backupCount=5)
    file_handler.setFormatter(log_formatter)
    
    # Get the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Remove existing handlers to avoid duplication
    root_logger.handlers = []
    
    # Add handlers
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    
    return root_logger

# Initialize Flask application
app = Flask(__name__)
logger = setup_logging()

# Initialize Redis connection
# Use the REDIS_URL provided by Heroku if available
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
try:
    redis_conn = Redis.from_url(redis_url)
    q = Queue(connection=redis_conn)
except Exception as e:
    logger.error(f"Failed to connect to Redis: {str(e)}")
    # Fallback to in-memory queue for development
    from fakeredis import FakeStrictRedis
    redis_conn = FakeStrictRedis()
    q = Queue(connection=redis_conn)

@app.route('/')
def home():
    """
    Home route to confirm the application is running.
    """
    logger.info("Home route accessed")
    return "Hello, World! The summarizer app is running."

@app.route('/summarize', methods=['POST'])
def summarize():
    """
    Endpoint to initiate the summarization process.
    """
    logger.info("Summarize endpoint called")
    data = request.json
    
    # Validate input data
    if not data or 'bucket_name' not in data:
        logger.error("Invalid request: missing bucket_name")
        return jsonify({'error': 'Missing bucket_name'}), 400
    
    bucket_name = data['bucket_name']
    prefix = data.get('prefix', '')
    max_files = data.get('max_files', 100)
    max_workers = data.get('max_workers', 10)

    # Validate max_files and max_workers
    try:
        max_files = int(max_files)
        max_workers = int(max_workers)
    except ValueError:
        logger.error("Invalid max_files or max_workers value")
        return jsonify({'error': 'Invalid max_files or max_workers value'}), 400

    logger.info(f"Received request: bucket={bucket_name}, prefix={prefix}, max_files={max_files}, max_workers={max_workers}")
    
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
    """
    Endpoint to cancel a running job.
    """
    logger.info(f"Attempting to cancel job {job_id}")
    try:
        job = Job.fetch(job_id, connection=redis_conn)
        if job.is_finished:
            logger.warning(f"Job {job_id} has already finished")
            return jsonify({'message': f'Job {job_id} has already finished'}), 400
        elif job.is_failed:
            logger.warning(f"Job {job_id} has already failed")
            return jsonify({'message': f'Job {job_id} has already failed'}), 400
        else:
            job.cancel()
            logger.info(f"Job {job_id} has been cancelled")
            return jsonify({'message': f'Job {job_id} has been cancelled'}), 200
    except Exception as e:
        logger.error(f"Error cancelling job {job_id}: {str(e)}")
        return jsonify({'error': str(e)}), 400

if __name__ == '__main__':
    # Potential risk: Debug mode should be disabled in production
    app.run(debug=False, host='0.0.0.0')

