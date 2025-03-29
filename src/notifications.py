import boto3
from botocore.exceptions import ClientError
from datetime import datetime

class TransferNotifier:
    def __init__(self, region_name: str, topic_arn: str, **creds):
        self.client = boto3.client('sns', region_name=region_name, **creds)
        self.topic_arn = topic_arn

    def send_notification(self, subject: str, message: str) -> bool:
        """Send SNS notification"""
        try:
            self.client.publish(
                TopicArn=self.topic_arn,
                Message=message,
                Subject=subject
            )
            return True
        except ClientError as e:
            raise e

    def format_transfer_message(self, file_path: str, status: str, details: str = "") -> str:
        """Format standardized transfer message"""
        return f"""
        HDFS to S3 Transfer {status}
        File: {file_path}
        Timestamp: {datetime.now().isoformat()}
        Details: {details}
        """