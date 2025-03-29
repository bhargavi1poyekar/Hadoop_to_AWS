# HDFS to S3 Data Transfer Application
This project automates the transfer of files from Hadoop Distributed File System (HDFS) to Amazon S3 by checking if the User has required permission to the file. The application is built with Python and leverages Hadoop and AWS services for efficient data handling and secure operations.

![](https://i.postimg.cc/5ydpWx39/system-design-drawio-1.png)

## Solution

The file can only be accessed by the users in the company. 

1. Check User Access:
    * Gets the group associated with the file.
    * Checks the user's group.
    * If the user is member of the file's group -> gives permission. 

2. Reads the content of the file from HDFS.
    * Uses the HDFS client to open and read the file content.

3. Upload File to S3
    * Uses the S3 client to put the file content into the specified bucket and key with appropriate bucket policy.
    * Configure Bucket Policies and IAM

    
    Bucket Policy that allows everyone to read, but allows only specific users to modify:

    ```
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::aws-test-bucket/*"
            },
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": [
                        "arn:aws:iam::account_id:user/user1",
                        "arn:aws:iam::account_id:user/user2",
                    ]
                },
                "Action": [
                    "s3:PutObject",
                    "s3:DeleteObject"
                ],
                "Resource": "arn:aws:s3:::aws-test-bucket/*"
            }
        ]
    }

If want a group of people to access this bucket, can add IAM policy that allows the IAM group access to modify this particular bucket. 

IAM Policy:

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": "arn:aws:s3:::your-bucket-name/*"
        }
    ]
}
```

Before setting up the project, ensure you have the following:

- **Python 3.6+** installed on your system.
- **Hadoop** and **AWS CLI** (for S3 operations) installed if you want to set up HDFS and interact with S3.
- **AWS account** with proper IAM roles for S3 access.


## Set Environment Variables

1. Create a .env file in the project directory and add:

    ```
    HDFS_URL=http://your-hdfs-namenode:50070
    HDFS_USER=your-hdfs-username
    HDFS_PATH=/path/to/your/hdfs/file
    S3_BUCKET_NAME=your-s3-bucket-name
    S3_FILE_KEY=path/in/s3/file.txt
    AWS_ACCESS_KEY=YOUR_AWS_ACCESS_KEY
    AWS_SECRET_KEY=YOUR_AWS_SECRET_KEY
    AWS_REGION=us-west-2
    ```
3. Activate Virtual Environment:
    ```
    source venv/bin/activate

2. Run the Application

    ```
    python -m src.main



    ```


