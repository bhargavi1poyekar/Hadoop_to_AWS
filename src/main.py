from src.config import get_ssm_parameters
from src.encryption import Encryption
from src.monitoring import TransferMonitor
from src.notifications import TransferNotifier
from src.sts import assume_role
from src.hdfs import read_file_from_hdfs, get_file_group, get_user_groups, check_user_access
from src.upload_s3 import upload_file_to_s3
from datetime import datetime
import logging
import os
from dotenv import load_dotenv
import boto3
from botocore.exceptions import NoCredentialsError
from hdfs import InsecureClient
from typing import Dict, Tuple
import subprocess

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def initialize_clients(config: Dict[str, str], **creds) -> Tuple[InsecureClient, boto3.client]:
    """
    Initialize HDFS and S3 clients using the provided configuration.

    Args:
        config (Dict[str, str]): Configuration dictionary containing HDFS and S3 details.

    Returns:
        Tuple[InsecureClient, boto3.client]: A tuple containing HDFS client and S3 client.
    """

    hdfs_client = InsecureClient(config['hdfs_url'], user=config['hdfs_user'])

    s3_client = boto3.client(
        's3',
        region_name=config['aws_region'],
        **creds
    )


    return hdfs_client, s3_client


def main():
    # Load config from SSM

    creds = assume_role(
        role_arn=os.getenv("HDFS_IAM_ROLE_ARN"),
        session_name="hdfs-s3-transfer",
        region=os.getenv("AWS_REGION")
    )

    config = get_ssm_parameters('/hdfs-s3-transfer/', os.getenv('AWS_REGION'), **creds)

    # Initialize all components
    monitor = TransferMonitor(config['aws_region'], **creds)
    notifier = TransferNotifier(config['aws_region'], config['sns_topic_arn'], **creds)
    cipher = Encryption(config['aws_region'], config['encryption_key'])
    
    start_time = datetime.now()

    hdfs_client, s3_client = initialize_clients(config, **creds)
    
    try:
        # Existing access check
        if not check_user_access(config['hdfs_path'], config['hdfs_user']):
            raise Exception("User lacks access permissions")
            
        # Read file with enhanced logging
        logger.info(f"Reading file from HDFS: {config['hdfs_path']}")
        file_content = read_file_from_hdfs(hdfs_client, config['hdfs_path'])
        
        
        # Encrypt content
        encrypted_data = cipher.encrypt_data(file_content.encode('utf-8'))
        logger.info("File encrypted successfully")
        
        # Upload to S3
        upload_file_to_s3(
            s3_client,
            config['s3_bucket_name'],
            config['s3_file_key'],
            encrypted_data['ciphertext']
        )

        logger.info("File uploaded to S3")
        
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
