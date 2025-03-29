import boto3
import json
from botocore.exceptions import ClientError
from src.sts import assume_role

def get_ssm_parameters(prefix: str, region: str, **creds) -> dict:
    """
    Fetch parameters from AWS SSM Parameter Store.
    Returns them as a dict with keys matching parameter names after the prefix.
    """

    # role_arn = os.getenv("HDFS_IAM_ROLE_ARN")
    # creds = assume_role(role_arn, "ssm-parameter-access", region)
        
    # Step 2: Create SSM client with temporary credentials
    ssm = boto3.client('ssm', region_name=region, **creds)

    # ssm = boto3.client('ssm', region_name=region)
    try:
        response = ssm.get_parameters_by_path(
            Path=prefix,
            WithDecryption=True  # For SecureString parameters
        )
        # Transform list of parameters into a dict
        return {
            param['Name'].split('/')[-1]: param['Value']
            for param in response['Parameters']
        }
    except ClientError as e:
        raise Exception(f"SSM Error: {e}")