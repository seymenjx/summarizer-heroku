import os
import asyncio
import boto3
from together import Together
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import logging
from botocore.exceptions import ClientError
from dotenv import load_dotenv
logger = logging.getLogger(__name__)

# Initialize Together AI client
load_dotenv()
together = Together()

async def summarize_text(text):
    # Prepare the prompt for text summarization
    prompt = f"Summarize the following text:\n\n{text}\n\nSummary:"
    try:
        # Use Together AI to generate a summary
        response = await together.complete(prompt=prompt, model="togethercomputer/llama-2-70b-chat", max_tokens=200)
        return response['output']['choices'][0]['text'].strip()
    except Exception as e:
        logger.error(f"Error in summarize_text: {str(e)}")
        return None

async def process_file(s3_client, bucket, key):
    try:
        # Retrieve the file content from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        # Generate a summary for the file content
        summary = await summarize_text(content)
        return key, summary
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.error(f"File not found: {key}")
        else:
            logger.error(f"Error processing file {key}: {str(e)}")
        return key, None
    except UnicodeDecodeError:
        logger.error(f"Unable to decode file {key}. It might not be a text file.")
        return key, None
    except Exception as e:
        logger.error(f"Unexpected error processing file {key}: {str(e)}")
        return key, None

async def process_batch(s3_client, bucket, keys):
    # Create tasks for processing multiple files concurrently
    tasks = [process_file(s3_client, bucket, key) for key in keys]
    return await asyncio.gather(*tasks)

def summarize_files_from_s3(bucket_name, prefix='', max_files=100, max_workers=10):
    try:
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {str(e)}")
        return {}

    summaries = {}
    files_processed = 0
    
    async def process_pages():
        nonlocal files_processed
        loop = asyncio.get_event_loop()
        
        try:
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                if 'Contents' not in page:
                    continue
                
                # Filter for text files
                keys = [obj['Key'] for obj in page['Contents'] if obj['Key'].lower().endswith(('.txt', '.log', '.md'))]
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    batch_size = min(len(keys), 10)  # Process up to 10 files per batch
                    for i in range(0, len(keys), batch_size):
                        batch = keys[i:i+batch_size]
                        # Process a batch of files concurrently
                        results = await loop.run_in_executor(executor, partial(asyncio.run, process_batch(s3_client, bucket_name, batch)))
                        
                        for key, summary in results:
                            if summary:
                                summaries[key] = summary
                                files_processed += 1
                                if files_processed >= max_files:
                                    return
        except ClientError as e:
            logger.error(f"S3 client error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error during processing: {str(e)}")
    
    # Run the asynchronous processing
    try:
        asyncio.run(process_pages())
    except Exception as e:
        logger.error(f"Error in asyncio execution: {str(e)}")
    
    return summaries

if __name__ == '__main__':
    # For testing purposes
    bucket_name = os.getenv('TEST_BUCKET_NAME')
    if not bucket_name:
        logger.error("TEST_BUCKET_NAME environment variable is not set")
    else:
        results = summarize_files_from_s3(bucket_name, max_files=20, max_workers=5)
        for key, summary in results.items():
            print(f"File: {key}\nSummary: {summary}\n")