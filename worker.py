import os
from rq import Worker, Queue, Connection
from redis import Redis
from summarizer import summarize_files_from_s3

redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
conn = Redis.from_url(redis_url)

if __name__ == '__main__':
    with Connection(conn):
        worker = Worker(Queue('default'))
        worker.work()



