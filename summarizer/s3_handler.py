import aiobotocore.session
import asyncio
import logging

logger = logging.getLogger(__name__)

async def get_s3_files(bucket, prefix, max_files):
    session = aiobotocore.session.get_session()  # Use get_session to create a session
    async with session.create_client('s3', region_name='eu-north-1') as client:  # Set the correct region here
        paginator = client.get_paginator('list_objects_v2')
        async for result in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for content in result.get('Contents', []):
                if content['Key'] == '9/b+V+I9J23s3P2ZRZ9TX6XNE3RP301xQ7VtHBvU':
                    continue  # Skip this file
                yield content['Key']
                if max_files and max_files <= 0:
                    return
                max_files -= 1

async def get_file_content(bucket, key):
    session = aiobotocore.session.get_session()  # Use get_session to create a session
    async with session.create_client('s3', region_name='eu-north-1') as client:  # Set the correct region here
        response = await client.get_object(Bucket=bucket, Key=key)
        async with response['Body'] as stream:
            return await stream.read()

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
