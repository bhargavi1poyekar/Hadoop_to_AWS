from botocore.exceptions import NoCredentialsError
import boto3

def upload_file_to_s3(s3_client: boto3.client, bucket_name: str, file_key: str, file_content: str) -> None:
    """
    Upload file content to S3.

    Args:
        s3_client (boto3.client): The S3 client to use.
        bucket_name (str): The name of the S3 bucket.
        file_key (str): The S3 object key.
        file_content (str): The content of the file to upload.

    Raises:
        NoCredentialsError: If AWS credentials are not available.
        Exception: If uploading to S3 fails.
    """
    try:
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=file_content, ChecksumAlgorithm='SHA256')
    except NoCredentialsError:
        print("AWS credentials not available")
        raise
    except Exception as e:
        print(f"Failed to upload to S3: {e}")
        raise