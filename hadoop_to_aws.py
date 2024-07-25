from hdfs import InsecureClient
import boto3
from botocore.exceptions import NoCredentialsError
import os
from dotenv import load_dotenv


load_dotenv()

hdfs_url = os.getenv('HDFS_URL')
hdfs_user = os.getenv('HDFS_USER')
hdfs_path = os.getenv('HDFS_PATH')
s3_bucket_name = os.getenv('S3_BUCKET_NAME')
s3_file_key = os.getenv('S3_FILE_KEY')
aws_access_key = os.getenv('AWS_ACCESS_KEY')
aws_secret_key = os.getenv('AWS_SECRET_KEY')
aws_region = os.getenv('AWS_REGION')

hdfs_client = InsecureClient(hdfs_url, user=hdfs_user)

s3_client = boto3.client('s3', region_name=aws_region,
			aws_access_key_id=aws_access_key,
			aws_secret_access_key=aws_secret_key)

try:
	with hdfs_client.read(hdfs_path, encoding='utf-8') as reader:
		file_content =reader.read()
	
	print(file_content)
	
	s3_client.put_object(Bucket=s3_bucket_name, Key=s3_file_key, Body=file_content)
	
	
	print("File successfully copied from HDFS to S3.")

except NoCredentialsError:
	print("Credentials not available")
except Exception as e:
	print(f"An error occured as {e}")
