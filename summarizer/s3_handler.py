import aiobotocore.session
import asyncio
import logging
import json
import os

logger = logging.getLogger(__name__)

async def get_last_processed_file(bucket, prefix):
    session = aiobotocore.session.get_session()
    async with session.create_client('s3', region_name='eu-north-1') as client:
        paginator = client.get_paginator('list_objects_v2')
        last_key = None
        async for result in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}summarizer/"):
            for content in sorted(result.get('Contents', []), key=lambda x: x['LastModified'], reverse=True):
                last_key = content['Key']
                return last_key
    return None

async def get_s3_files(bucket, prefix, max_files):
    logger.info(f"Starting to list files in bucket: {bucket}, prefix: {prefix}")
    session = aiobotocore.session.get_session()
    async with session.create_client('s3', region_name='eu-north-1') as client:
        paginator = client.get_paginator('list_objects_v2')
        file_count = 0
        try:
            async for result in paginator.paginate(Bucket=bucket, Prefix=prefix):
                logger.info(f"Received a page of results with {len(result.get('Contents', []))} items")
                for content in result.get('Contents', []):
                    if content['Key'].endswith('/'):  # Skip directories
                        logger.info(f"Skipping directory: {content['Key']}")
                        continue
                    if content['Key'] == '9/b+V+I9J23s3P2ZRZ9TX6XNE3RP301xQ7VtHBvU':
                        logger.info(f"Skipping specific file: {content['Key']}")
                        continue
                    logger.info(f"Yielding file: {content['Key']}")
                    yield content['Key']
                    file_count += 1
                    if max_files and file_count >= max_files:
                        logger.info(f"Reached max_files limit of {max_files}")
                        return
        except Exception as e:
            logger.error(f"Error listing S3 files: {str(e)}")
    logger.info(f"Finished listing files. Total files found: {file_count}")

async def get_file_content(bucket, key):
    logger.info(f"Fetching content for file: {key}")
    session = aiobotocore.session.get_session()
    async with session.create_client('s3', region_name='eu-north-1') as client:
        try:
            response = await client.get_object(Bucket=bucket, Key=key)
            async with response['Body'] as stream:
                content = await stream.read()
            logger.info(f"Successfully fetched content for file: {key}")
            return content
        except Exception as e:
            logger.error(f"Error fetching content for file {key}: {str(e)}")
            raise

async def check_summary_exists(bucket, key):
    session = aiobotocore.session.get_session()  # Use get_session to create a session
    async with session.create_client('s3', region_name='eu-north-1') as client:  # Set the correct region here
        try:
            await client.head_object(Bucket=bucket, Key=f"summaries/{key}")
            return True
        except client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.error(f"Error checking if summary exists: {str(e)}")
                return False
        except Exception as e:
            logger.error(f"Error checking if summary exists: {str(e)}")
            return False
            return False

async def upload_summary_to_s3(bucket, key, summary):
    session = aiobotocore.session.get_session()  # Use get_session to create a session
    async with session.create_client('s3', region_name='eu-north-1') as client:  # Set the correct region here
        try:
            summary_key = f"summaries/{key}"
            await client.put_object(Bucket=bucket, Key=summary_key, Body=str(summary).encode('utf-8'))
            logger.info(f"Successfully uploaded summary to {summary_key}")
        except Exception as e:
            logger.error(f"Error uploading summary to S3: {str(e)}")

# Example usage
async def main():
    bucket = 'emsaller'
    prefix = ''
    max_files = 5

    async for key in get_s3_files(bucket, prefix, max_files):
        content = await get_file_content(bucket, key)
        logger.info(f"Successfully retrieved content for {key} (length: {len(content)})")

if __name__ == "__main__":
    asyncio.run(main())
