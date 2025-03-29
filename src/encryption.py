from cryptography.fernet import Fernet
from typing import Dict
import os
import boto3
from botocore.exceptions import ClientError

class Encryption:
    def __init__(self, region_name: str = None, encryption_key: str = None):
        """
        Initialize with encryption key from pre-fetched config.
        - `region_name`: Unused (kept for compatibility).
        - `encryption_key`: Fernet key (already decrypted from SSM).
        """
        try:
            self.key = encryption_key.encode()  # Convert string key to bytes
            self.cipher = Fernet(self.key)
        except Exception as e:
            raise

    def encrypt_data(self, data: bytes) -> Dict[str, bytes]:
        """Encrypt data using local Fernet (AES-128-CBC)"""
        try:
            ciphertext = self.cipher.encrypt(data)
            return {
                'ciphertext': ciphertext,
                'key_id': self.key
            }
        except Exception as e:
            raise e

    def decrypt_data(self, ciphertext: bytes) -> bytes:
        """Decrypt data using local Fernet"""
        try:
            return self.cipher.decrypt(ciphertext)
        except Exception as e:
            raise e