import os
from flask import Flask, jsonify, request
from rq import Queue
from redis import Redis
from dotenv import load_dotenv
from rq.job import Job

load_dotenv()

app = Flask(__name__)

# Initialize Redis connection
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
redis_conn = Redis.from_url(redis_url)
q = Queue(connection=redis_conn)

@app.route('/')
def home():
    return "Hello, World! The summarizer app is running."

@app.route('/summarize', methods=['POST'])
def summarize():
    data = request.json
    bucket_name = data.get('bucket_name')
    prefix = data.get('prefix', '')
    max_files = data.get('max_files', 100)
    max_workers = data.get('max_workers', 10)

    # Here you would typically enqueue a job, but for now, let's just return a message
    return jsonify({
        'message': 'Summarization request received',
        'bucket': bucket_name,
        'prefix': prefix,
        'max_files': max_files,
        'max_workers': max_workers
    }), 202

@app.route('/cancel/<job_id>', methods=['POST'])
def cancel_job(job_id):
    try:
        job = Job.fetch(job_id, connection=redis_conn)
        if job.is_finished:
            return jsonify({'message': f'Job {job_id} has already finished'}), 400
        elif job.is_failed:
            return jsonify({'message': f'Job {job_id} has already failed'}), 400
        else:
            job.cancel()
            return jsonify({'message': f'Job {job_id} has been cancelled'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True)

