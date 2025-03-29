import boto3
from typing import Dict, Optional
from botocore.exceptions import ClientError, BotoCoreError
import logging

# Configure logging
logger = logging.getLogger(__name__)

def assume_role(role_arn: str, session_name: str, region: str) -> Dict[str, str]:
    """
    Assume an IAM role and return temporary security credentials.
    
    This function uses AWS Security Token Service (STS) to obtain temporary credentials
    for accessing AWS resources with the permissions of the specified IAM role.

    Args:
        role_arn: The Amazon Resource Name (ARN) of the role to assume
                  (e.g., 'arn:aws:iam::123456789012:role/S3TransferRole')
        session_name: An identifier for the assumed role session (used in CloudTrail logs)
                     (e.g., 'hdfs-s3-transfer-session')
        region: AWS region where the STS endpoint is located
                (e.g., 'us-east-1')

    Returns:
        Dictionary containing temporary credentials:

    Raises:
        ClientError: If AWS service call fails (e.g., permission denied)
        BotoCoreError: For network or configuration issues
        ValueError: If role_arn or session_name are invalid
    """
    if not role_arn.startswith('arn:aws:iam::'):
        raise ValueError(f"Invalid role ARN format: {role_arn}")
    if not session_name.replace('-', '').isalnum():
        raise ValueError("Session name must be alphanumeric with optional hyphens")

    try:
        sts = boto3.client('sts', region_name=region)
        response = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName=session_name
        )

        creds = {
            'aws_access_key_id': response['Credentials']['AccessKeyId'],
            'aws_secret_access_key': response['Credentials']['SecretAccessKey'],
            'aws_session_token': response['Credentials']['SessionToken']
        }
        
        logger.info(
            f"Successfully assumed role {role_arn}\n"
            f"Session: {session_name}\n"
        )
        return creds

    except ClientError as e:
        logger.error(
            f"STS AssumeRole failed for {role_arn}\n"
            f"Error Code: {e.response['Error']['Code']}\n"
            f"Message: {e.response['Error']['Message']}"
        )
        raise RuntimeError(f"Failed to assume role: {e.response['Error']['Message']}") from e
    except BotoCoreError as e:
        logger.error(f"STS connection error: {str(e)}")
        raise RuntimeError("STS service unavailable") from e