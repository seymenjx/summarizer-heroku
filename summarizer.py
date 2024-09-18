import os
import boto3
from together import Together
import tiktoken  # For token counting
from dotenv import load_dotenv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_exponential
import json
from datetime import datetime
import logging

start_time = time.time()
load_dotenv()

# Initialize Together AI client
client = Together(api_key=os.environ.get("TOGETHER_API_KEY"))

# Initialize S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
)

# Initialize tokenizer
tokenizer = tiktoken.get_encoding("cl100k_base")  # Assuming GPT-like tokenizer

def list_files_in_s3(bucket_name, prefix=''):
    """
    List all files in an S3 bucket.
    """
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        files = [content['Key'] for content in response.get('Contents', [])]
        return files
    except Exception as e:
        print(f"Error listing files in S3: {str(e)}")
        return []

def read_file_from_s3(bucket_name, file_key):
    """
    Read a file from an S3 bucket.

    This function attempts to retrieve and read the contents of a file stored in an S3 bucket.
    It uses the boto3 S3 client to interact with AWS S3.

    Args:
        bucket_name (str): The name of the S3 bucket containing the file.
        file_key (str): The key (path) of the file within the S3 bucket.

    Returns:
        str or None: The contents of the file as a string if successful, None if an error occurs.

    Raises:
        No exceptions are raised directly, but errors are caught and logged.
    """
    try:
        # Attempt to get the object from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        
        # Read the content of the file
        file_content = response['Body'].read().decode('utf-8')
        
        # Note: This assumes the file is a UTF-8 encoded text file.
        # For binary files or other encodings, you might need to modify this part.
        
        return file_content
    except Exception as e:
        # Log any errors that occur during the process
        print(f"Error reading file {file_key} from S3: {str(e)}")
        
        # Return None to indicate that the file couldn't be read
        return None

def count_tokens(text):
    """
    Count the number of tokens in the text using GPT tokenizer.
    """
    return len(tokenizer.encode(text))

def chunk_text(text, max_tokens=8000):
    chunks = []
    current_chunk = ""
    current_tokens = 0
    
    for sentence in text.split('. '):
        sentence_tokens = count_tokens(sentence)
        if current_tokens + sentence_tokens > max_tokens:
            chunks.append(current_chunk.strip())
            current_chunk = sentence
            current_tokens = sentence_tokens
        else:
            current_chunk += ". " + sentence
            current_tokens += sentence_tokens
    
    if current_chunk:
        chunks.append(current_chunk.strip())
    
    return chunks

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def summarize_chunk(chunk):
    try:
        stream = client.chat.completions.create(
            model="seymenjx/meta-llama/Meta-Llama-3-8B-cb316f32",
            messages=[{"role": "user", "content": f"Summarize the following text:\n{chunk}"}],
            stream=True,
            max_tokens=1000  # Limit the summary length
        )
        summary = "".join(getattr(chunk.choices[0].delta, "content", "") for chunk in stream)
        return summary if summary else "No summary received."
    except Exception as e:
        print(f"Error summarizing chunk: {str(e)}")
        raise  # Re-raise the exception to trigger a retry

def store_summary_in_s3(bucket_name, file_key, summary, input_tokens, output_tokens):
    summary_key = f"summaries/{file_key}"
    
    # Prepare metadata
    metadata = {
        'original_file': file_key,
        'input_tokens': str(input_tokens),
        'output_tokens': str(output_tokens),
        'summarized_at': datetime.utcnow().isoformat()
    }
    
    try:
        # Store the summary with metadata
        s3.put_object(
            Bucket=bucket_name,
            Key=summary_key,
            Body=summary,
            ContentType='text/plain',
            Metadata=metadata
        )
        return summary_key
    except Exception as e:
        print(f"Error storing summary for {file_key}: {str(e)}")
        return None

def summarize_text_with_together_ai(text):
    chunks = chunk_text(text)
    summaries = []
    
    for chunk in chunks:
        summary = summarize_chunk(chunk)
        summaries.append(summary)
    
    final_summary = " ".join(summaries)
    return final_summary if final_summary else "No summary received."

def process_single_file(bucket_name, file_key):
    summary_key = f"summaries/{file_key}"
    
    # Check if summary already exists
    try:
        s3.head_object(Bucket=bucket_name, Key=summary_key)
        print(f"Summary already exists for {file_key}. Skipping summarization.")
        return file_key, None, None, summary_key
    except:
        # Summary doesn't exist, proceed with summarization
        file_content = read_file_from_s3(bucket_name, file_key)
        if not file_content:
            return file_key, None, None, None

        input_tokens = count_tokens(file_content)
        summary = summarize_text_with_together_ai(file_content)
        
        if summary:
            output_tokens = count_tokens(summary)
            summary_key = store_summary_in_s3(bucket_name, file_key, summary, input_tokens, output_tokens)
            return file_key, input_tokens, output_tokens, summary_key
        return file_key, input_tokens, None, None

def summarize_files_from_s3(bucket_name, prefix='', max_files=100, max_workers=10):
    logger.info(f"Starting summarization for bucket: {bucket_name}, prefix: {prefix}")
    files = list_files_in_s3(bucket_name, prefix)
    logger.info(f"Found {len(files)} files")
    
    if not files:
        print("No files found in the S3 bucket.")
        return
    
    total_input_tokens = 0
    total_output_tokens = 0
    file_count = 0

    summaries = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {executor.submit(process_single_file, bucket_name, file_key): file_key for file_key in files[:max_files]}
        
        for future in as_completed(future_to_file):
            file_key = future_to_file[future]
            try:
                file_key, input_tokens, output_tokens, summary_key = future.result()
                if summary_key:
                    file_count += 1
                    summaries[file_key] = summary_key
                    print(f"Processed file {file_count}/{len(files)}: {file_key}")
                    if input_tokens is not None:
                        total_input_tokens += input_tokens
                        if output_tokens:
                            total_output_tokens += output_tokens
                        else:
                            print(f"No summary received for file: {file_key}")
                    else:
                        print(f"Summary already existed for file: {file_key}")
                else:
                    print(f"Skipping file due to read error: {file_key}")
            except Exception as e:
                print(f"Error processing {file_key}: {str(e)}")

    print(f"\nProcessed {file_count} files.")
    print(f"Total input tokens: {total_input_tokens}")
    print(f"Total output tokens: {total_output_tokens}")
    return total_input_tokens, total_output_tokens, summaries

def get_summary_from_s3(bucket_name, summary_key):
    """
    Retrieve a summary and its metadata from an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        summary_key (str): The key of the summary object in the bucket.

    Returns:
        tuple: A tuple containing the summary content (str) and metadata (dict),
               or (None, None) if an error occurs.
    """
    try:
        # Attempt to get the object from S3
        response = s3.get_object(Bucket=bucket_name, Key=summary_key)
        
        # Read the content of the object and decode it from bytes to string
        summary_content = response['Body'].read().decode('utf-8')
        
        # Extract the metadata from the response
        metadata = response['Metadata']
        
        # Return both the summary content and metadata
        return summary_content, metadata
    except Exception as e:
        # If any error occurs during the process, print the error and return None values
        print(f"Error retrieving summary {summary_key}: {str(e)}")
        return None, None

def setup_logger(name):
    logger = logging.getLogger(name)
    # ... rest of the logger setup ...
    return logger

logger = setup_logger(__name__)