# HDFS to S3 Data Transfer Application
This project automates the transfer of files from Hadoop Distributed File System (HDFS) to Amazon S3. The application is built with Python and leverages Hadoop and AWS services for efficient data handling and secure operations.

![](https://i.postimg.cc/rFRQtjGv/system-design-drawio.png)

## Table of Contents
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Hadoop Setup](#hadoop-setup)
- [S3 Bucket Setup](#s3-bucket-setup)
- [Usage](#usage)

## Prerequisites

Before setting up the project, ensure you have the following:

- **Python 3.6+** installed on your system.
- **Hadoop** and **AWS CLI** (for S3 operations) installed if you want to set up HDFS and interact with S3.
- **AWS account** with proper IAM roles for S3 access.

## Installation

1. Clone the Repository**

   ```sh
   git clone https://github.com/bhargavi1poyekar/Hadoop_to_AWS.git
   cd Hadoop_to_AWS

2. Create and Activate a Virtual Environment

        python -m venv venv
        source venv/bin/activate 

3. Install Dependencies

    Install the required Python packages from requirements.txt:

        pip install -r requirements.txt


## Hadoop Setup
If Hadoop is not already set up on your system, follow these steps:

1. Install Java and Download Hadoop 
    ```sh
    sudo apt update
    sudo apt install openjdk-8-jdk wget
    
    wget https://downloads.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz
    tar -xzvf hadoop-3.3.1.tar.gz
    sudo mv hadoop-3.3.1 /usr/local/hadoop

2. Configure Hadoop

    Edit Hadoop environment variables:

    ```
    sudo nano /usr/local/hadoop/etc/hadoop/hadoop-env.sh
    ```
    Add the following line:
    ```sh
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

3. Configure core-site.xml:

```
    sudo nano /usr/local/hadoop/etc/hadoop/core-site.xml
```

```sh
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```
4. Configure hdfs-site.xml:

```
sudo nano /usr/local/hadoop/etc/hadoop/hdfs-site.xml
```
Add:

```
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
```

5. Configure mapred-site.xml:

    Copy code

```
    cp /usr/local/hadoop/etc/hadoop/mapred-site.xml.template /usr/local/hadoop/etc/hadoop/mapred-site.xml

    sudo nano /usr/local/hadoop/etc/hadoop/mapred-site.xml

```
```
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
```

6. Configure yarn-site.xml:
```
sudo nano /usr/local/hadoop/etc/hadoop/yarn-site.xml
```

```
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
    <value>org.apache.hadoop.mapred.ShuffleHandler</value>
  </property>
</configuration>
```

7. Format HDFS and Start Hadoop

```
/usr/local/hadoop/bin/hdfs namenode -format
/usr/local/hadoop/sbin/start-dfs.sh
/usr/local/hadoop/sbin/start-yarn.sh
```

8. Verify Hadoop Installation

Open your browser and go to:

NameNode: http://localhost:50070

ResourceManager: http://localhost:8088

## Create an S3 Bucket

1. Log in to the AWS Management Console.
2. Navigate to S3 and click Create bucket.
3. Enter a bucket name and choose a region.
4. Configure bucket settings as needed and click Create bucket.
5. Configure Bucket Policies and IAM

### Create IAM Users:

1. Navigate to IAM in the AWS Management Console.
2. Create new users and assign them appropriate permissions.
3. Set Bucket Policies
4. Set IAM Policies:


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

2. Run the Application

    ```
    python3 hadoop_to_aws.py
    ```
