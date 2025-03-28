import hashlib

def generate_checksum(data: bytes, algorithm: str = 'sha256') -> str:
    """Generate checksum for data integrity verification"""
    hash_obj = hashlib.new(algorithm)
    hash_obj.update(data)
    return hash_obj.hexdigest()

def verify_checksum(original_data: bytes, received_checksum: str) -> bool:
    """Verify data against checksum"""
    current_checksum = generate_checksum(original_data)
    return current_checksum == received_checksum