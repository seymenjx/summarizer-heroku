import aioboto3
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

async def get_s3_files(bucket_name, prefix='', max_files=100):
    session = aioboto3.Session()
    async with session.client('s3') as s3_client:
        paginator = s3_client.get_paginator('list_objects_v2')
        files_yielded = 0

        logger.info(f"Listing files in bucket: {bucket_name}, prefix: {prefix}")

        async for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get('Contents', []):
                if files_yielded >= max_files:
                    return
                
                key = obj['Key']
                if key.lower().endswith(('.txt', '.log', '.md')):
                    yield key
                    files_yielded += 1
                else:
                    logger.debug(f"Skipping file with unsupported extension: {key}")

        logger.info(f"Total files yielded: {files_yielded}")
        
        if files_yielded == 0:
            logger.warning(f"No files were yielded from bucket: {bucket_name}, prefix: {prefix}")

async def get_file_content(bucket_name, key):
    session = aioboto3.Session()
    async with session.client('s3') as s3_client:
        try:
            response = await s3_client.get_object(Bucket=bucket_name, Key=key)
            content = await response['Body'].read()
            decoded_content = content.decode('utf-8')
            logger.info(f"Successfully retrieved content for {key} (length: {len(decoded_content)})")
            return decoded_content
        except Exception as e:
            logger.error(f"Error retrieving file content for {key}: {str(e)}", exc_info=True)
            return f"Dosya içeriği alınamadı: {str(e)}"
