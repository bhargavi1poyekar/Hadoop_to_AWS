from src.security import get_secret
from src.encryption import KMSEncryption
from src.integrity import generate_checksum, verify_checksum
from src.monitoring import TransferMonitor
from src.notifications import TransferNotifier
from datetime import datetime
import logging
import os
from dotenv import load_dotenv
import boto3
from botocore.exceptions import NoCredentialsError
from hdfs import InsecureClient
from typing import Dict, Tuple
import subprocess

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# def load_environment_variables() -> Dict[str, str]:
#     """
#     Load environment variables from .env file.

#     Returns:
#         Dict[str, str]: A dictionary containing environment variables and their values.
#     """
#     load_dotenv()
#     return {
#         'hdfs_url': os.getenv('HDFS_URL'),
#         'hdfs_user': os.getenv('HDFS_USER'),
#         'hdfs_path': os.getenv('HDFS_PATH'),
#         's3_bucket_name': os.getenv('S3_BUCKET_NAME'),
#         's3_file_key': os.getenv('S3_FILE_KEY'),
#         'aws_access_key': os.getenv('AWS_ACCESS_KEY'),
#         'aws_secret_key': os.getenv('AWS_SECRET_KEY'),
#         'aws_region': os.getenv('AWS_REGION')
#     }


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

# def main() -> None:
#     """
#     Main function to coordinate HDFS and S3 operations.
#     """
#     config = load_environment_variables()
    

#     try:
#         valid_access = check_user_access(config['hdfs_path'],  config['hdfs_user'])
#         if valid_access:
#             file_content = read_file_from_hdfs(hdfs_client, config['hdfs_path'])
#             print("File content from HDFS:", file_content)
#             upload_file_to_s3(s3_client, config['s3_bucket_name'], config['s3_file_key'], file_content)
#             print("File successfully copied from HDFS to S3.")
#         else:
#             print("The User doesn't have access to the HDFS file")
#     except Exception as e:
#         print(f"An error occurred: {e}")

def main():
    # Load config from Secrets Manager instead of .env
    try:
        secrets = get_secret('hdfs_s3_transfer', os.getenv('AWS_REGION'))
        config = {
            'hdfs_url': secrets['HDFS_URL'],
            'hdfs_user': secrets['HDFS_USER'],
            'hdfs_path': secrets['HDFS_PATH'],
            's3_bucket_name': secrets['S3_BUCKET_NAME'],
            's3_file_key': secrets['S3_FILE_KEY'],
            'aws_region': secrets['AWS_REGION'],
            'kms_key_id': secrets['KMS_KEY_ID'],
            'sns_topic_arn': secrets['SNS_TOPIC_ARN']
        }
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return

    # Initialize all components
    monitor = TransferMonitor(config['aws_region'])
    notifier = TransferNotifier(config['aws_region'], config['sns_topic_arn'])
    kms = KMSEncryption(config['aws_region'], config['kms_key_id'])
    
    start_time = datetime.now()

    hdfs_client, s3_client = initialize_clients(config)
    
    try:
        # Existing access check
        if not check_user_access(config['hdfs_path'], config['hdfs_user']):
            raise Exception("User lacks access permissions")
            
        # Read file with enhanced logging
        logger.info(f"Reading file from HDFS: {config['hdfs_path']}")
        file_content = read_file_from_hdfs(hdfs_client, config['hdfs_path'])
        
        # Generate checksum
        checksum = generate_checksum(file_content.encode('utf-8'))
        logger.info(f"Generated checksum: {checksum}")
        
        # Encrypt content
        encrypted_data = kms.encrypt_data(file_content.encode('utf-8'))
        logger.info("File encrypted successfully")
        
        # Upload to S3
        upload_file_to_s3(
            s3_client,
            config['s3_bucket_name'],
            config['s3_file_key'],
            encrypted_data['ciphertext']
        )
        logger.info("File uploaded to S3")
        
        # Verify transfer
        downloaded = s3_client.get_object(
            Bucket=config['s3_bucket_name'],
            Key=config['s3_file_key']
        )
        if not verify_checksum(downloaded['Body'].read(), checksum):
            raise Exception("Checksum verification failed")
        
        # Log success
        transfer_time = (datetime.now() - start_time).total_seconds()
        monitor.log_transfer_stats(
            file_size=len(file_content),
            duration=transfer_time,
            status='Success'
        )
        
        notifier.send_notification(
            "Transfer Successful",
            notifier.format_transfer_message(
                config['hdfs_path'],
                "SUCCESS",
                f"Transferred in {transfer_time:.2f} seconds"
            )
        )
        
    except Exception as e:
        logger.error(f"Transfer failed: {e}")
        monitor.log_transfer_stats(0, 0, 'Failed')
        notifier.send_notification(
            "Transfer Failed",
            notifier.format_transfer_message(
                config['hdfs_path'],
                "FAILED",
                str(e)
            )
        )

if __name__ == "__main__":
    main()
