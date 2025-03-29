import boto3
from datetime import datetime
from botocore.exceptions import ClientError
from typing import List, Dict, Optional

class TransferMonitor:
    """
    Provides monitoring capabilities by logging metrics to Amazon CloudWatch.
    
    This class enables tracking of file transfer operations through custom metrics,
    including file sizes, transfer durations, and success/failure counts. All metrics
    are published under the 'HDFS/S3/Transfer' namespace.
    """

    def __init__(self, region_name: str, **creds: Dict[str, str]):
        """
        Initialize the CloudWatch monitoring client.

        Args:
            region_name: AWS region name (e.g., 'us-east-1') where metrics will be stored
            **creds: AWS credentials (AccessKeyId, SecretAccessKey, SessionToken)

        Raises:
            ClientError: If CloudWatch client initialization fails
        """
        self.client = boto3.client('cloudwatch', region_name=region_name, **creds)
        self.namespace = "HDFS/S3/Transfer"

    def log_metric(self, metric_name: str, value: float, dimensions: List[Dict[str, str]] = None) -> None:
        """
        Log a custom metric to CloudWatch.

        Args:
            metric_name: Name of the metric to log (e.g., 'FileSizeBytes')
            value: Numeric value to record for the metric
            dimensions: Optional list of dimension dictionaries 

        Raises:
            ClientError: If CloudWatch API call fails with AWS-specific errors
            ValueError: If metric_name is empty or value is not numeric
            RuntimeError: For unexpected errors during metric submission
        """
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
            error_code = e.response['Error']['Code']
            raise ClientError(
                f"CloudWatch metric submission failed (Code: {error_code}): {e.response['Error']['Message']}"
            ) from e
        except Exception as e:
            raise RuntimeError(f"Unexpected error logging metric: {str(e)}") from e

    def log_transfer_stats(self, file_size: int, duration: float, status: str) -> None:
        """
        Log standard transfer metrics with status dimension.

        Args:
            file_size: Size of transferred file in bytes
            duration: Transfer duration in seconds (float for precision)
            status: Transfer status ('Success' or 'Failed')

        Raises:
            ClientError: If any CloudWatch API call fails
            ValueError: If invalid status provided
        """
        dimensions = [{'Name': 'Status', 'Value': status}]
        self.log_metric('FileSizeBytes', file_size, dimensions)
        self.log_metric('TransferDuration', duration, dimensions)
        self.log_metric('TransferCount', 1, dimensions)