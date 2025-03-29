import boto3

def assume_role(role_arn: str, session_name: str, region: str):
    """Assume an IAM role and return temporary credentials"""
    sts = boto3.client('sts', region_name=region)
    try:
        response = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName=session_name
        )

        creds = {
            'aws_access_key_id': response['Credentials']['AccessKeyId'],
            'aws_secret_access_key': response['Credentials']['SecretAccessKey'],
            'aws_session_token': response['Credentials']['SessionToken']
        }
        
        return creds
    except Exception as e:
        logger.error(f"STS AssumeRole failed: {e}")
        raise
