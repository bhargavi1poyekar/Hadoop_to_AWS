import boto3
from botocore.exceptions import ClientError
from src.sts import assume_role

def get_ssm_parameters(prefix: str, region: str, **creds) -> dict:
    """
    Retrieves parameters from AWS Systems Manager (SSM) Parameter Store.
    
    Fetches all parameters under the specified path prefix and returns them as a dictionary
    with simplified keys (removing the prefix path). Automatically decrypts SecureString parameters.

    Args:
        prefix: The path prefix for parameters (e.g., '/hdfs-s3-transfer/')
        region: AWS region where parameters are stored (e.g., 'us-east-1')
        **creds: Temporary AWS credentials (typically from STS assume_role) containing:
            - aws_access_key_id
            - aws_secret_access_key
            - aws_session_token

    Returns:
        Dictionary of parameter names (without prefix) to their values

    Raises:
        Exception: If SSM access fails or parameters can't be retrieved
    """

    # Initialize SSM client
    ssm = boto3.client('ssm', region_name=region, **creds)

    try:
        # Retrieve all parameters under the specified path
        response = ssm.get_parameters_by_path(
            Path=prefix,
            WithDecryption=True,  # Decrypt SecureString parameters
            Recursive=True        # Get all parameters under the path
        )
        
        # Transform into a clean dictionary {param_name: param_value}
        return {
            param['Name'].removeprefix(prefix): param['Value']
            for param in response['Parameters']
        }
    except ClientError as error:
        error_msg = f"SSM Parameter Store access failed: {error.response['Error']['Message']}"
        raise RuntimeError(error_msg) from error
    except KeyError as error:
        raise RuntimeError("Malformed SSM response format") from error