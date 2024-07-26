import os
from dotenv import load_dotenv
import boto3
from botocore.exceptions import NoCredentialsError
from hdfs import InsecureClient
from typing import Dict, Tuple
import subprocess


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


def get_file_group(file_path: str) -> str:
    """
    Retrieve the group associated with a file in HDFS.

    Args:
        file_path (str): The path to the file in HDFS.

    Returns:
        str: The group name associated with the file, or None if an error occurs.
    """
    try:
        result = subprocess.run(['hadoop', 'fs', '-stat', '%g', file_path], capture_output=True, text=True, check=True)
        print(result)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error getting file group:{e}")
        return None


def get_user_groups(user: str) -> list[str]:
    """
    Retrieve the groups that a user belongs to on the local system.

    Args:
        user (str): The username to query.

    Returns:
        list[str]: A list of group names that the user belongs to.
    """
    try:
        result = subprocess.run(['id', '-Gn', user], capture_output=True, text=True, check=True)
        return result.stdout.strip().split()
    except subprocess.CalledProcessError as e:
        print(f"Error getting user groups: {e}")
        return []


def check_user_access(file_path: str, user: str) -> bool:
    """
    Check if a user has access to a file in HDFS based on group membership.

    Args:
        file_path (str): The path to the file in HDFS.
        user (str): The username to check.

    Returns:
        bool: True if the user’s group matches the file’s group, False otherwise.
    """
    file_group = get_file_group(file_path)
    if not file_group:
        return False
    user_groups = get_user_groups(user)
    if file_group in user_groups:
        return True
    return False 

def main() -> None:
    """
    Main function to coordinate HDFS and S3 operations.
    """
    config = load_environment_variables()
    hdfs_client, s3_client = initialize_clients(config)

    try:
        valid_access = check_user_access(config['hdfs_path'],  config['hdfs_user'])
        if valid_access:
            file_content = read_file_from_hdfs(hdfs_client, config['hdfs_path'])
            print("File content from HDFS:", file_content)
            upload_file_to_s3(s3_client, config['s3_bucket_name'], config['s3_file_key'], file_content)
            print("File successfully copied from HDFS to S3.")
        else:
            print("The User doesn't have access to the HDFS file")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
