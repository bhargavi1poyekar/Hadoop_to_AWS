import boto3
from botocore.exceptions import ClientError

class KMSEncryption:
    def __init__(self, region_name: str, key_id: str):
        self.client = boto3.client('kms', region_name=region_name)
        self.key_id = key_id

    def encrypt_data(self, data: bytes) -> dict:
        """Encrypt data using KMS"""
        try:
            response = self.client.encrypt(
                KeyId=self.key_id,
                Plaintext=data
            )
            return {
                'ciphertext': response['CiphertextBlob'],
                'key_id': response['KeyId']
            }
        except ClientError as e:
            raise e

    def decrypt_data(self, ciphertext: bytes) -> bytes:
        """Decrypt data using KMS"""
        try:
            response = self.client.decrypt(
                CiphertextBlob=ciphertext
            )
            return response['Plaintext']
        except ClientError as e:
            raise e