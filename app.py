from flask import Flask, request, jsonify
from rq import Queue
from redis import Redis
from worker import summarize_files_from_s3

app = Flask(__name__)
redis_conn = Redis()
q = Queue(connection=redis_conn)

@app.route('/summarize', methods=['POST'])
def summarize():
    data = request.json
    bucket_name = data.get('bucket_name')
    prefix = data.get('prefix', '')
    max_files = data.get('max_files', 100)
    max_workers = data.get('max_workers', 10)

    job = q.enqueue(summarize_files_from_s3, bucket_name, prefix, max_files, max_workers)
    return jsonify({'job_id': job.id}), 202

@app.route('/job/<job_id>', methods=['GET'])
def get_job_status(job_id):
    job = q.fetch_job(job_id)
    if job is None:
        return jsonify({'status': 'not found'}), 404
    
    status = {
        'job_id': job.id,
        'status': job.get_status(),
        'result': job.result,
    }
    return jsonify(status)

if __name__ == '__main__':
    app.run(debug=True)

