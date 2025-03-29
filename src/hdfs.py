from hdfs import InsecureClient
import subprocess
import logging
from typing import List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_file_from_hdfs(hdfs_client: InsecureClient, hdfs_path: str) -> str:
    """
    Read and return the contents of a file from HDFS with UTF-8 encoding.

    Args:
        hdfs_client: Authenticated InsecureClient instance for HDFS access
        hdfs_path: Absolute path to the file in HDFS (e.g., '/user/data/file.txt')

    Returns:
        Decoded string content of the file

    Raises:
        HdfsError: If file doesn't exist or isn't readable
        UnicodeDecodeError: If file contains invalid UTF-8 sequences
        Exception: For other unexpected errors
    """
    try:
        with hdfs_client.read(hdfs_path, encoding='utf-8') as reader:
            content = reader.read()
            logger.info(f"Successfully read {len(content)} bytes from {hdfs_path}")
            return content
    except Exception as e:
        logger.error(f"HDFS read failed for {hdfs_path}: {str(e)}")
        raise


def get_file_group(file_path: str) -> str:
    """
    Retrieve the group ownership of an HDFS file using Hadoop CLI.

    Args:
        file_path: Absolute HDFS path (e.g., '/user/data/file.txt')

    Returns:
        Group name if successful, None otherwise
    """
    try:
        result = subprocess.run(
            ['hadoop', 'fs', '-stat', '%g', file_path],
            capture_output=True,
            text=True,
            check=True
        )
        group = result.stdout.strip()
        logger.debug(f"File {file_path} belongs to group: {group}")
        return group
    except subprocess.CalledProcessError as e:
        logger.error(f"HDFS group check failed for {file_path}: {e.stderr.strip()}")
        return None


def get_user_groups(user: str) -> List[str]:
    """
    Get system group memberships for a user via 'id' command.

    Args:
        user: System username to check

    Returns:
        List of group names (empty list on failure)
    """
    try:
        result = subprocess.run(
            ['id', '-Gn', user],
            capture_output=True,
            text=True,
            check=True
        )
        groups = result.stdout.strip().split()
        logger.debug(f"User {user} belongs to groups: {groups}")
        return groups
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to get groups for user {user}: {e.stderr.strip()}")
        return []


def check_user_access(file_path: str, user: str) -> bool:
    """
    Verify if a user has group-based access to an HDFS file.

    Args:
        file_path: HDFS path to check
        user: Username to validate

    Returns:
        bool: True if user's groups include the file's group owner

    Logic Flow:
        1. Get file's group owner
        2. Get user's system groups
        3. Check for intersection
    """

    file_group = get_file_group(file_path)
    if not file_group:
        logger.warning(f"Could not determine group for {file_path}")
        return False

    user_groups = get_user_groups(user)
    access_granted = file_group in user_groups

    logger.info(
        f"Access check for {user} to {file_path}: "
        f"File Group={file_group}, User Groups={user_groups}, "
        f"Access={'GRANTED' if access_granted else 'DENIED'}"
    )
    return access_granted
