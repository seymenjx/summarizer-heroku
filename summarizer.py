import os
import asyncio
import boto3
from together import Together
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import logging

logger = logging.getLogger(__name__)

# Initialize Together AI client
together = Together(os.getenv('TOGETHER_API_KEY'))

async def summarize_text(text):
    prompt = f"Summarize the following text:\n\n{text}\n\nSummary:"
    try:
        response = await together.complete(prompt=prompt, model="togethercomputer/llama-2-70b-chat", max_tokens=200)
        return response['output']['choices'][0]['text'].strip()
    except Exception as e:
        logger.error(f"Error in summarize_text: {str(e)}")
        return None

async def process_file(s3_client, bucket, key):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        summary = await summarize_text(content)
        return key, summary
    except Exception as e:
        logger.error(f"Error processing file {key}: {str(e)}")
        return key, None

async def process_batch(s3_client, bucket, keys):
    tasks = [process_file(s3_client, bucket, key) for key in keys]
    return await asyncio.gather(*tasks)

def summarize_files_from_s3(bucket_name, prefix='', max_files=100, max_workers=10):
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    
    summaries = {}
    files_processed = 0
    
    async def process_pages():
        nonlocal files_processed
        loop = asyncio.get_event_loop()
        
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' not in page:
                continue
            
            keys = [obj['Key'] for obj in page['Contents'] if obj['Key'].endswith('.txt')]
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                batch_size = min(len(keys), 10)  # Process up to 10 files per batch
                for i in range(0, len(keys), batch_size):
                    batch = keys[i:i+batch_size]
                    results = await loop.run_in_executor(executor, partial(asyncio.run, process_batch(s3_client, bucket_name, batch)))
                    
                    for key, summary in results:
                        if summary:
                            summaries[key] = summary
                            files_processed += 1
                            if files_processed >= max_files:
                                return
    
    asyncio.run(process_pages())
    
    return summaries

if __name__ == '__main__':
    # For testing purposes
    results = summarize_files_from_s3('your-bucket-name', max_files=20, max_workers=5)
    for key, summary in results.items():
        print(f"File: {key}\nSummary: {summary}\n")