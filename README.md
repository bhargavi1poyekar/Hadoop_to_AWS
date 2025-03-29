# HDFS to S3 Data Transfer Application
This Python application securely transfers files from HDFS to AWS S3 by first verifying user access permissions against the file’s group membership. It performs client-side encryption using Fernet (AES-128-CBC) with keys retrieved from AWS SSM Parameter Store, assumes temporary IAM credentials via STS for secure S3 access, and validates file integrity with checksums. The transfer is monitored via CloudWatch metrics, with success/failure notifications sent through SNS, ensuring an auditable and permission-controlled pipeline without AWS KMS dependencies. All configurations (bucket paths, encryption keys) are dynamically fetched from SSM.

![](https://i.postimg.cc/Y9nWqnZJ/system-design-drawio.png)

## Features

The file can only be accessed by the users in the company. 

1. User Access Verification (src/hdfs.py)
* Validates if the current user belongs to the file's group in HDFS.
* Uses Linux system groups (id -Gn) and HDFS permissions (hadoop fs -stat %g).
* Prevents unauthorized file access before transfer begins.

2. Client-Side Encryption (src/encyrption.py)
* Fernet (AES-128-CBC) symmetric encryption.
* Encryption keys:
    * Pre-fetched from AWS SSM Parameter Store (already decrypted).
    * Passed as strings and converted to bytes.
* Methods:
    * encrypt_data(): Returns ciphertext + key ID.
    * decrypt_data(): Reverses the process.

3. AWS Credential Management (src/sts.py)
* Uses STS (Security Token Service) to assume IAM roles.
* Generates temporary credentials for secure S3 access.
* Follows least-privilege principles.

4. S3 Upload (src/upload_s3.py)
* Uses boto3 with:
    * Temporary STS credentials.
    * Checksum verification.
    * Error handling for network issues.

5. Configuration Management (src/config.py)
* All dynamic values (paths, keys, bucket names) fetched from AWS SSM Parameter Store.
* Eliminates hardcoded credentials in the codebase.

6. Monitoring & Alerting (src/monitoring.py, src/notifications.py)
* CloudWatch: Logs transfer metrics (latency, success/failure rates).
* SNS: Sends real-time notifications to ops team on completion/failure.

## AWS Permissions and Setup:

1. IAM ROLE:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket-name",
        "arn:aws:s3:::your-bucket-name/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "sts:AssumeRole"
      ],
      "Resource": "arn:aws:iam::123456789012:role/S3TransferRole"
    }
  ]
}
```
2. S3 Bucket Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::123456789012:role/S3TransferRole"
      },
      "Action": [
        "s3:PutObject",
        "s3:GetObject"
      ],
      "Resource": "arn:aws:s3:::your-bucket-name/*"
    }
  ]
}
```
   
## Installation and Running.

1. Setting up and activating virtual environment. 
    ```
    python3 -m venv .venv
    source .venv/bin/activate
    ```

2. Install the required packages and libraries.
    ```
    pip install -r requirements.txt
    ```
3. Upload the env_template_file config variables to SSM Paramater store. 

4. Run the application.
    ```
    python -m src.main
    ```

5. Successful Transfer Log:
* INFO:botocore.credentials:Found credentials in shared credentials file: ~/.aws/credentials
* INFO:hdfs.client:Instantiated <InsecureClient(url='http://localhost:9870')>.
* INFO:__main__:Reading file from HDFS: /user/bpoyeka1/local_file.txt
* INFO:hdfs.client:Reading file '/user/bpoyeka1/local_file.txt'.
* INFO:__main__:File encrypted successfully
* INFO:__main__:File uploaded to S3



