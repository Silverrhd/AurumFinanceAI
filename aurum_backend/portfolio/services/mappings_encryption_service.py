import os
import io
import pandas as pd
from cryptography.fernet import Fernet
import logging

logger = logging.getLogger(__name__)

class MappingsEncryptionService:
    """
    Simple service to encrypt/decrypt the Mappings.xlsx file.
    
    This works like a digital safe:
    1. We encrypt the file once and store it safely
    2. When needed, we unlock it in memory (never save unlocked version to disk)
    3. We read it like a normal Excel file
    4. Memory is automatically cleared when done
    """

    def __init__(self):
        self.key = self._get_encryption_key()
        self.cipher = Fernet(self.key)

    def _get_encryption_key(self):
        """Get the encryption key from environment variables"""
        key_string = os.environ.get('MAPPINGS_ENCRYPTION_KEY')
        if not key_string:
            raise ValueError(
                "MAPPINGS_ENCRYPTION_KEY not found in environment variables. "
                "Please add it to your .env file or AWS environment."
            )
        return key_string.encode()

    def encrypt_file(self, input_file_path: str, output_file_path: str):
        """
        Encrypt a file and save the encrypted version.
        
        Args:
            input_file_path: Path to original Mappings.xlsx
            output_file_path: Path to save encrypted version
        """
        try:
            # Read the original file
            with open(input_file_path, 'rb') as f:
                original_data = f.read()

            # Encrypt it
            encrypted_data = self.cipher.encrypt(original_data)

            # Save encrypted version
            with open(output_file_path, 'wb') as f:
                f.write(encrypted_data)

            logger.info(f"Successfully encrypted {input_file_path} -> {output_file_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to encrypt file: {e}")
            return False

    def read_encrypted_excel(self, encrypted_file_path: str, sheet_name: str = None):
        """
        Read an encrypted Excel file directly into pandas DataFrame.
        
        This is the magic part - it decrypts the file in memory only,
        reads it like a normal Excel file, then automatically cleans up.
        
        Args:
            encrypted_file_path: Path to encrypted Mappings.xlsx.encrypted
            sheet_name: Which sheet to read (e.g., 'MS', 'JPM', 'Safra')
        
        Returns:
            pandas DataFrame with the data
        """
        try:
            # Read encrypted file
            with open(encrypted_file_path, 'rb') as f:
                encrypted_data = f.read()

            # Decrypt in memory (never touches disk)
            decrypted_data = self.cipher.decrypt(encrypted_data)

            # Create memory stream (like a temporary file in RAM)
            memory_stream = io.BytesIO(decrypted_data)

            # Read as normal Excel file
            df = pd.read_excel(memory_stream, sheet_name=sheet_name)

            # Memory is automatically cleared when this function ends
            logger.debug(f"Successfully read sheet '{sheet_name}' from encrypted file")
            return df

        except Exception as e:
            logger.error(f"Failed to read encrypted Excel file: {e}")
            raise