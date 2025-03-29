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
    Initialize and return HDFS and S3 clients with the given configuration.
    
    Args:
        config: Dictionary containing configuration parameters with keys:
            - hdfs_url: URL of the HDFS NameNode (e.g., 'http://namenode:9870')
            - hdfs_user: Username for HDFS operations
            - aws_region: AWS region for S3 operations
        **creds: Additional credentials to pass to boto3 client from STS.
        
    Returns:
        Tuple containing:
            - hdfs_client: Configured HDFS client instance
            - s3_client: Configured boto3 S3 client instance
    """

    hdfs_client = InsecureClient(config['hdfs_url'], user=config['hdfs_user'])

    s3_client = boto3.client(
        's3',
        region_name=config['aws_region'],
        **creds
    )

    return hdfs_client, s3_client


def main():
    """
    Main execution flow for HDFS to S3 file transfer with encryption.
    
    Workflow:
    1. Assumes IAM role using STS for temporary credentials
    2. Retrieves configuration from AWS SSM Parameter Store
    3. Initializes monitoring and notification components
    4. Checks user access permissions in HDFS
    5. Reads file content from HDFS
    6. Encrypts file content using Fernet encryption
    7. Uploads encrypted file to S3 with checksum validation
    8. Logs transfer metrics and sends notifications
    
    Environment Variables Required:
        - HDFS_IAM_ROLE_ARN: ARN of IAM role to assume
        - AWS_REGION: AWS region for services
        - SNS_TOPIC_ARN: ARN for SNS notifications (optional)
        
    Raises:
        Exception: If any step in the transfer process fails
    """

    # Assume IAM role for temporary credentials
    creds = assume_role(
        role_arn=os.getenv("HDFS_IAM_ROLE_ARN"),
        session_name="hdfs-s3-transfer",
        region=os.getenv("AWS_REGION")
    )

    # Get Configs from SSM Parameter Store
    config = get_ssm_parameters('/hdfs-s3-transfer/', os.getenv('AWS_REGION'), **creds)

    # Initialize all components
    monitor = TransferMonitor(config['aws_region'], **creds)
    notifier = TransferNotifier(config['aws_region'], config['sns_topic_arn'], **creds)
    cipher = Encryption(config['aws_region'], config['encryption_key'])
    
    start_time = datetime.now()

    hdfs_client, s3_client = initialize_clients(config, **creds)
    
    try:
        # --- ACCESS VALIDATION ---
        if not check_user_access(config['hdfs_path'], config['hdfs_user']):
            raise Exception("User lacks access permissions")
            
        # --- FILE READ ---
        logger.info(f"Reading file from HDFS: {config['hdfs_path']}")
        file_content = read_file_from_hdfs(hdfs_client, config['hdfs_path'])
        
        # --- ENCRYPTION ---
        encrypted_data = cipher.encrypt_data(file_content.encode('utf-8'))
        logger.info("File encrypted successfully")
        
        # --- S3 UPLOAD ---
        upload_file_to_s3(
            s3_client,
            config['s3_bucket_name'],
            config['s3_file_key'],
            encrypted_data['ciphertext']
        )

        logger.info("File uploaded to S3")
        
        # --- MONITORING ---
        transfer_time = (datetime.now() - start_time).total_seconds()
        monitor.log_transfer_stats(
            file_size=len(file_content),
            duration=transfer_time,
            status='Success'
        )
        
        # --- NOTIFICATION ---
        notifier.send_notification(
            "Transfer Successful",
            notifier.format_transfer_message(
                config['hdfs_path'],
                "SUCCESS",
                f"Transferred in {transfer_time:.2f} seconds"
            )
        )
        
    except Exception as e:
        # --- ERROR HANDLING ---
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
