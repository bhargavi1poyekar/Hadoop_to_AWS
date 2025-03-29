from cryptography.fernet import Fernet
from typing import Dict, Optional
import os
import boto3
from botocore.exceptions import ClientError
import logging


class Encryption:
    """
    Provides symmetric encryption/decryption using Fernet (AES-128-CBC).
    
    Fernet guarantees:
    - Confidentiality: AES-128-CBC encryption
    - Integrity: HMAC-SHA256 signing
    - Timestamp: Verification of message freshness
    
    Requires pre-shared keys managed through AWS SSM Parameter Store.
    """

    def __init__(self, region_name: str = None, encryption_key: str = None):
        """
        Initialize encryption handler with a Fernet key.
        
        Args:
            region_name: Unused parameter (maintained for backward compatibility)
            encryption_key: Base64-encoded Fernet key
            
        Raises:
            ValueError: If key is invalid or empty
            TypeError: If key has wrong type
            cryptography.fernet.InvalidToken: If key is malformed
        """
        if not encryption_key:
            raise ValueError("Encryption key cannot be empty")
            
        try:
            self.key = encryption_key.encode()  # Convert to bytes if provided as string
            self.cipher = Fernet(self.key)  # Will raise InvalidToken for bad keys
        except Exception as e:
            logging.error(f"Encryption initialization failed: {str(e)}")
            raise

    def encrypt_data(self, data: bytes) -> Dict[str, bytes]:
        """
        Encrypt data using Fernet symmetric encryption.
        
        Args:
            data: Raw bytes to encrypt (max ~4GB due to Fernet spec)
            
        Returns:
            Dictionary with:
            - 'ciphertext': Encrypted bytes
            - 'key_id': The key identifier (raw bytes)
            
        Raises:
            TypeError: If input isn't bytes
            ValueError: If data is empty
            cryptography.fernet.InvalidToken: On encryption failure
        """
        if not data:
            raise ValueError("Data cannot be empty")
        if not isinstance(data, bytes):
            raise TypeError(f"Expected bytes, got {type(data).__name__}")

        try:
            ciphertext = self.cipher.encrypt(data)
            return {
                'ciphertext': ciphertext,
                'key_id': self.key  # For tracking which key was used
            }
        except Exception as e:
            logging.error(f"Encryption failed: {str(e)}")
            raise


    def decrypt_data(self, ciphertext: bytes) -> bytes:
        """
        Decrypt Fernet-encrypted data.
        
        Args:
            ciphertext: Encrypted bytes from encrypt_data()
            
        Returns:
            Original plaintext bytes
            
        Raises:
            TypeError: If input isn't bytes
            ValueError: If token is malformed
            cryptography.fernet.InvalidToken: If token is invalid/expired
        """
        if not ciphertext:
            raise ValueError("Ciphertext cannot be empty")
        if not isinstance(ciphertext, bytes):
            raise TypeError(f"Expected bytes, got {type(ciphertext).__name__}")

        try:
            return self.cipher.decrypt(ciphertext)
        except Exception as e:
            logging.error(f"Decryption failed: {str(e)}")
            raise