import os
from dotenv import load_dotenv
import boto3
from botocore.exceptions import NoCredentialsError
from hdfs import InsecureClient
from typing import Dict, Tuple


def load_environment_variables() -> Dict[str, str]:
    """
    Load environment variables from .env file.

    Returns:
        Dict[str, str]: A dictionary containing environment variables and their values.
    """
    load_dotenv()
    return {
        'hdfs_url': os.getenv('HDFS_URL'),
        'hdfs_user': os.getenv('HDFS_USER'),
        'hdfs_path': os.getenv('HDFS_PATH'),
        's3_bucket_name': os.getenv('S3_BUCKET_NAME'),
        's3_file_key': os.getenv('S3_FILE_KEY'),
        'aws_access_key': os.getenv('AWS_ACCESS_KEY'),
        'aws_secret_key': os.getenv('AWS_SECRET_KEY'),
        'aws_region': os.getenv('AWS_REGION')
    }


def initialize_clients(config: Dict[str, str]) -> Tuple[InsecureClient, boto3.client]:
    """
    Initialize HDFS and S3 clients using the provided configuration.

    Args:
        config (Dict[str, str]): Configuration dictionary containing HDFS and S3 details.

    Returns:
        Tuple[InsecureClient, boto3.client]: A tuple containing HDFS client and S3 client.
    """
    hdfs_client = InsecureClient(config['hdfs_url'], user=config['hdfs_user'])
    s3_client = boto3.client('s3', 
                             region_name=config['aws_region'],
                             aws_access_key_id=config['aws_access_key'],
                             aws_secret_access_key=config['aws_secret_key'])
    return hdfs_client, s3_client


def read_file_from_hdfs(hdfs_client: InsecureClient, hdfs_path: str) -> str:
    """
    Read file content from HDFS.

    Args:
        hdfs_client (InsecureClient): The HDFS client to use.
        hdfs_path (str): The path of the file in HDFS.

    Returns:
        str: The content of the file.

    Raises:
        Exception: If reading from HDFS fails.
    """
    try:
        with hdfs_client.read(hdfs_path, encoding='utf-8') as reader:
            return reader.read()
    except Exception as e:
        print(f"Failed to read from HDFS: {e}")
        raise


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
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=file_content)
    except NoCredentialsError:
        print("AWS credentials not available")
        raise
    except Exception as e:
        print(f"Failed to upload to S3: {e}")
        raise


def main() -> None:
    """
    Main function to coordinate HDFS and S3 operations.
    """
    config = load_environment_variables()
    hdfs_client, s3_client = initialize_clients(config)

    try:
        file_content = read_file_from_hdfs(hdfs_client, config['hdfs_path'])
        print("File content from HDFS:", file_content)
        upload_file_to_s3(s3_client, config['s3_bucket_name'], config['s3_file_key'], file_content)
        print("File successfully copied from HDFS to S3.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
