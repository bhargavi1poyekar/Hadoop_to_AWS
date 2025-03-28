import boto3
import json
from botocore.exceptions import ClientError

def get_secret(secret_name: str, region_name: str) -> dict:
    """
    Retrieve secrets from AWS Secrets Manager
    """
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        return json.loads(get_secret_value_response['SecretString'])
    except ClientError as e:
        raise e

def rotate_credentials(secret_name: str, region_name: str):
    """
    Trigger credential rotation
    """
    client = boto3.client('secretsmanager', region_name=region_name)
    try:
        client.rotate_secret(SecretId=secret_name)
        return True
    except ClientError as e:
        raise e