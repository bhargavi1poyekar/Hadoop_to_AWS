import boto3
from datetime import datetime
from botocore.exceptions import ClientError

class TransferMonitor:
    def __init__(self, region_name: str, **creds):
        self.client = boto3.client('cloudwatch', region_name=region_name, **creds)
        self.namespace = "HDFS/S3/Transfer"

    def log_metric(self, metric_name: str, value: float, dimensions: list = None):
        """Log custom metric to CloudWatch"""
        try:
            self.client.put_metric_data(
                Namespace=self.namespace,
                MetricData=[{
                    'MetricName': metric_name,
                    'Dimensions': dimensions or [],
                    'Value': value,
                    'Timestamp': datetime.now()
                }]
            )
        except ClientError as e:
            raise e

    def log_transfer_stats(self, file_size: int, duration: float, status: str):
        """Log standard transfer metrics"""
        dimensions = [{'Name': 'Status', 'Value': status}]
        self.log_metric('FileSizeBytes', file_size, dimensions)
        self.log_metric('TransferDuration', duration, dimensions)
        self.log_metric('TransferCount', 1, dimensions)