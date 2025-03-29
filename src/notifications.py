import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from typing import Dict, Any

class TransferNotifier:
    """
    Handles sending notifications via AWS Simple Notification Service (SNS).
    
    This class provides methods to:
    - Send notifications to a specified SNS topic
    - Format standardized transfer notification messages
    - Handle AWS credential management through boto3
    
    Attributes:
        client (boto3.client): Initialized SNS client
        topic_arn (str): ARN of the SNS topic to publish to
    """

    def __init__(self, region_name: str, topic_arn: str, **creds: Dict[str, str]):
        """
        Initialize the TransferNotifier with SNS client configuration.
        
        Args:
            region_name: AWS region name where the SNS topic resides (e.g., 'us-east-1')
            topic_arn: Amazon Resource Name (ARN) of the SNS topic
            **creds: Optional AWS credentials (AccessKeyId, SecretAccessKey, SessionToken)
                    If not provided, will use default boto3 credential chain.
                    
        Raises:
            ClientError: If SNS client initialization fails
            ValueError: If topic_arn is invalid
        """
        self.client = boto3.client('sns', region_name=region_name, **creds)
        self.topic_arn = topic_arn

    def send_notification(self, subject: str, message: str) -> bool:
        """
        Publish a notification message to the configured SNS topic.
        
        Args:
            subject: The subject line of the notification (max 100 chars)
            message: The body content of the notification
            
        Returns:
            bool: True if notification was successfully sent
            
        Raises:
            ClientError: If SNS publish operation fails
            ValueError: If subject or message are empty
            RuntimeError: For unexpected errors during publishing
        """
        try:
            self.client.publish(
                TopicArn=self.topic_arn,
                Message=message,
                Subject=subject
            )
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_msg = e.response['Error']['Message']
            raise ClientError(
                f"SNS publish failed (Code: {error_code}): {error_msg}"
            ) from e
        except Exception as e:
            raise RuntimeError(f"Unexpected error sending notification: {str(e)}") from e

    def format_transfer_message(self, file_path: str, status: str, details: str = "") -> str:
        """
        Generate a standardized notification message for transfer operations.
        
        Args:
            file_path: Path of the file being transferred
            status: Current status of the transfer ('SUCCESS', 'FAILED', etc.)
            details: Additional information about the transfer
            
        Returns:
            str: Formatted message with timestamp and transfer details
            
        """
        return f"""
        HDFS to S3 Transfer {status}
        File: {file_path}
        Timestamp: {datetime.now().isoformat()}
        Details: {details}
        """