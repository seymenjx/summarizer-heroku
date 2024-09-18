import os
import asyncio
import boto3
from together import Together
import logging
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import aiohttp

logger = logging.getLogger(__name__)

# Initialize Together AI client
load_dotenv()
together = Together()

async def summarize_text(text, session):
    prompt = f"Summarize the following text:\n\n{text}\n\nSummary:"
    try:
        async with session.post(
            "https://api.together.xyz/inference",
            json={
                "model": "togethercomputer/llama-2-70b-chat",
                "prompt": prompt,
                "max_tokens": 200,
            },
            headers={"Authorization": f"Bearer {os.getenv('TOGETHER_API_KEY')}"}
        ) as response:
            if response.status == 200:
                result = await response.json()
                return result['output']['choices'][0]['text'].strip()
            else:
                logger.error(f"API request failed with status {response.status}")
                return None
    except Exception as e:
        logger.error(f"Error in summarize_text: {str(e)}")
        return None

async def process_file(s3_client, bucket, key, session):
    try:
        response = await s3_client.get_object(Bucket=bucket, Key=key)
        content = await response['Body'].read()
        content = content.decode('utf-8')
        summary = await summarize_text(content, session)
        return key, summary
    except Exception as e:
        logger.error(f"Error processing file {key}: {str(e)}")
        return key, None

async def summarize_files_from_s3(bucket_name, prefix='', max_files=100, max_workers=10):
    try:
        session = aioboto3.Session()
        async with session.client('s3') as s3_client:
            paginator = s3_client.get_paginator('list_objects_v2')
            summaries = {}
            files_processed = 0
            
            async with aiohttp.ClientSession() as api_session:
                async for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                    if 'Contents' not in page:
                        continue
                    
                    keys = [obj['Key'] for obj in page['Contents'] if obj['Key'].lower().endswith(('.txt', '.log', '.md'))]
                    
                    tasks = [process_file(s3_client, bucket_name, key, api_session) for key in keys[:max_files - files_processed]]
                    results = await asyncio.gather(*tasks)
                    
                    for key, summary in results:
                        if summary:
                            summaries[key] = summary
                            files_processed += 1
                            if files_processed >= max_files:
                                return summaries
            
            return summaries
    except Exception as e:
        logger.error(f"Error in summarize_files_from_s3: {str(e)}")
        return {}

# This function will be called by RQ
def run_summarize_files_from_s3(bucket_name, prefix='', max_files=100, max_workers=10):
    return asyncio.run(summarize_files_from_s3(bucket_name, prefix, max_files, max_workers))

if __name__ == '__main__':
    # For testing purposes
    bucket_name = os.getenv('TEST_BUCKET_NAME')
    if not bucket_name:
        logger.error("TEST_BUCKET_NAME environment variable is not set")
    else:
        results = summarize_files_from_s3(bucket_name, max_files=20, max_workers=5)
        for key, summary in results.items():
            print(f"File: {key}\nSummary: {summary}\n")