from botocore.exceptions import NoCredentialsError, ClientError
import boto3
from typing import Union, BinaryIO

def upload_file_to_s3(
    s3_client: boto3.client,
    bucket_name: str,
    file_key: str,
    file_content: Union[str, bytes, BinaryIO],
) -> dict:
    """
    Uploads file content to an S3 bucket with checksum validation.

    This function handles the upload of data to Amazon S3 while automatically
    calculating a SHA256 checksum for data integrity verification.

    Args:
        s3_client: Authenticated boto3 S3 client instance
        bucket_name: Name of the destination S3 bucket
        file_key: S3 object key/path (e.g., 'folder/data.txt')
        file_content: Content to upload (can be string, bytes, or file-like object)

    Returns:
        dict: S3 response

    Raises:
        NoCredentialsError: When AWS credentials are invalid or missing
        ClientError: For S3-specific errors (access denied, bucket not found)
        ValueError: If bucket_name or file_key are empty
        TypeError: If file_content is not str, bytes, or file-like object
        RuntimeError: For other unexpected errors during upload
    """
    try:
        response = s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=file_content,
            ChecksumAlgorithm='SHA256'
        )
        return {
            'ETag': response.get('ETag'),
            'ChecksumSHA256': response.get('ChecksumSHA256')
        }
    except NoCredentialsError:
        print("AWS credentials not available")
        raise
    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"S3 upload failed (Code: {error_code}): {e.response['Error']['Message']}")
        raise
    except Exception as e:
        print(f"Failed to upload to S3: {e}")
        raise