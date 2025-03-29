from hdfs import InsecureClient
import subprocess

def read_file_from_hdfs(hdfs_client: InsecureClient, hdfs_path: str) -> str:
    """
    Read file content from HDFS.

    Args:
        hdfs_client (InsecureClient): The HDFS client to use.
        hdfs_path (str): The path of the file in HDFS.

    Returns:
        str: The content of the file.

    Raises:
        Exception: If reading from HDFS fails.
    """
    try:
        with hdfs_client.read(hdfs_path, encoding='utf-8') as reader:
            return reader.read()
    except Exception as e:
        print(f"Failed to read from HDFS: {e}")
        raise


def get_file_group(file_path: str) -> str:
    """
    Retrieve the group associated with a file in HDFS.

    Args:
        file_path (str): The path to the file in HDFS.

    Returns:
        str: The group name associated with the file, or None if an error occurs.
    """
    try:
        result = subprocess.run(['hadoop', 'fs', '-stat', '%g', file_path], capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error getting file group:{e}")
        return None


def get_user_groups(user: str) -> list[str]:
    """
    Retrieve the groups that a user belongs to on the local system.

    Args:
        user (str): The username to query.

    Returns:
        list[str]: A list of group names that the user belongs to.
    """
    try:
        result = subprocess.run(['id', '-Gn', user], capture_output=True, text=True, check=True)
        return result.stdout.strip().split()
    except subprocess.CalledProcessError as e:
        print(f"Error getting user groups: {e}")
        return []


def check_user_access(file_path: str, user: str) -> bool:
    """
    Check if a user has access to a file in HDFS based on group membership.

    Args:
        file_path (str): The path to the file in HDFS.
        user (str): The username to check.

    Returns:
        bool: True if the user’s group matches the file’s group, False otherwise.
    """
    file_group = get_file_group(file_path)
    if not file_group:
        return False
    user_groups = get_user_groups(user)
    if file_group in user_groups:
        return True
    return False 
