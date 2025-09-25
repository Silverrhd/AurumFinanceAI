#!/usr/bin/env python3
import os
import sys
sys.path.append('aurum_backend')

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv('aurum_backend/.env')

from portfolio.services.mappings_encryption_service import MappingsEncryptionService

def main():
    # Paths
    original_file = "aurum_backend/data/excel/input_files/Mappings.xlsx"
    encrypted_file = "aurum_backend/data/excel/input_files/Mappings.xlsx.encrypted"

    # Check if original exists
    if not os.path.exists(original_file):
        print(f"Error: {original_file} not found!")
        return False

    # Encrypt it
    service = MappingsEncryptionService()
    success = service.encrypt_file(original_file, encrypted_file)

    if success:
        print(f"✅ Successfully encrypted Mappings.xlsx")
        print(f"📁 Original file: {original_file}")
        print(f"🔒 Encrypted file: {encrypted_file}")
        print(f"")
        print(f"Next steps:")
        print(f"1. Delete the original file: rm '{original_file}'")
        print(f"2. Add to Git: git add '{encrypted_file}'")
        print(f"3. Update .gitignore to prevent future xlsx files")
    else:
        print(f"❌ Failed to encrypt file")

    return success

if __name__ == "__main__":
    main()