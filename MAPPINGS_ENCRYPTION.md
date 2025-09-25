# Mappings.xlsx Encryption Security System

## Overview

This system encrypts the sensitive `Mappings.xlsx` file containing client account information using AES-256 encryption. The encrypted file is stored in Git while the decryption key is kept secure in environment variables.

## File Structure

```
aurum_backend/data/excel/input_files/
├── Mappings.xlsx.backup      # Local editable copy (not in Git)
├── Mappings.xlsx.encrypted   # Encrypted version (in Git)
└── [no Mappings.xlsx]        # Original removed for security
```

## Security Model

- **Encrypted File**: Stored in Git repository, unreadable without key
- **Encryption Key**: Stored in environment variables (separate from code)
- **Backup File**: Local only, excluded from Git via .gitignore
- **Encryption**: AES-256 (same standard used by banks and governments)

## Development Setup

### 1. Environment Variable

The system requires `MAPPINGS_ENCRYPTION_KEY` in your environment:

```bash
# In aurum_backend/.env
MAPPINGS_ENCRYPTION_KEY=your_generated_key_here
```

### 2. Verify Setup

Test that encryption is working:

```bash
cd aurum_backend
python3 manage.py shell -c "
from portfolio.services.mappings_encryption_service import MappingsEncryptionService
service = MappingsEncryptionService()
df = service.read_encrypted_excel('data/excel/input_files/Mappings.xlsx.encrypted', sheet_name='MS')
print(f'✅ Successfully read {len(df)} rows from MS sheet')
"
```

## Editing Workflow

When you need to add or modify client/account mappings:

### Step 1: Edit the Backup File

```bash
# Open the editable backup in Excel
open aurum_backend/data/excel/input_files/Mappings.xlsx.backup
```

Make your changes in Excel:
- Add new clients
- Update account numbers  
- Modify account names
- Add new bank sheets if needed
- Save the file

### Step 2: Re-encrypt the Updated File

```bash
# Copy backup to working file
cp aurum_backend/data/excel/input_files/Mappings.xlsx.backup aurum_backend/data/excel/input_files/Mappings.xlsx

# Run encryption script
python3 encrypt_mappings.py

# Clean up temporary file
rm aurum_backend/data/excel/input_files/Mappings.xlsx
```

### Step 3: Deploy Changes

```bash
# Add encrypted file to Git
git add aurum_backend/data/excel/input_files/Mappings.xlsx.encrypted

# Commit with descriptive message
git commit -m "Update client mappings: added XYZ client accounts"

# Deploy to production
git push
```

## Production Deployment

### Environment Setup

Set the encryption key on your production server:

```bash
# Option 1: Environment variable
export MAPPINGS_ENCRYPTION_KEY="your_production_key_here"

# Option 2: Add to your deployment script
echo 'MAPPINGS_ENCRYPTION_KEY="your_key"' >> /path/to/production/.env
```

### Security Best Practices

1. **Different Keys**: Use different encryption keys for development vs production
2. **Key Storage**: Consider AWS Secrets Manager for production key storage
3. **Key Backup**: Securely backup encryption keys in case of server changes
4. **Access Control**: Limit who has access to production environment variables

## Key Rotation (Future)

If you ever need to rotate encryption keys:

### 1. Generate New Key

```python
from cryptography.fernet import Fernet
new_key = Fernet.generate_key()
print(f"New encryption key: {new_key.decode()}")
```

### 2. Re-encrypt with New Key

```bash
# Update environment with new key
export MAPPINGS_ENCRYPTION_KEY="new_key_here"

# Re-encrypt the backup file
cp aurum_backend/data/excel/input_files/Mappings.xlsx.backup aurum_backend/data/excel/input_files/Mappings.xlsx
python3 encrypt_mappings.py
rm aurum_backend/data/excel/input_files/Mappings.xlsx

# Deploy new encrypted file
git add aurum_backend/data/excel/input_files/Mappings.xlsx.encrypted
git commit -m "Rotate encryption key for Mappings.xlsx"
git push
```

### 3. Update Production

```bash
# Update production environment with new key
# Then restart services to pick up new key
```

## Troubleshooting

### Common Issues

**Error: "MAPPINGS_ENCRYPTION_KEY not found"**
- Solution: Verify environment variable is set in `.env` file or production environment

**Error: "Failed to decrypt file"**
- Solution: Check that the encryption key matches the one used to encrypt the file
- If keys were rotated, ensure production has the new key

**Error: "Mappings.xlsx file not found"**
- Solution: System now looks for `Mappings.xlsx.encrypted` automatically
- Ensure encrypted file exists in the input_files directory

### Recovery Process

If you lose the encryption key but have the original backup:

1. Generate a new encryption key
2. Use the backup file to re-encrypt with new key
3. Update environment variables with new key
4. Deploy new encrypted file

## Technical Details

### Files Modified

The encryption system updates these files to use encrypted mappings:

**Transformers:**
- `portfolio/preprocessing/transformers/ms_transformer.py`
- `portfolio/preprocessing/transformers/jpm_transformer.py`
- `portfolio/preprocessing/transformers/safra_transformer.py`
- `portfolio/preprocessing/transformers/citi_transformer.py`

**Combiners:**
- `portfolio/preprocessing/combiners/hsbc_enricher.py`
- `portfolio/preprocessing/combiners/lombard_combiner.py`
- `portfolio/preprocessing/combiners/lombard_enricher.py`

**Main Script:**
- `portfolio/preprocessing/preprocess.py`

### Encryption Service

The `MappingsEncryptionService` provides:
- `encrypt_file()`: Encrypt a file and save encrypted version
- `read_encrypted_excel()`: Read encrypted Excel file directly into pandas DataFrame

### Memory Safety

- Files are decrypted only in memory (RAM)
- No temporary unencrypted files written to disk
- Memory is automatically cleared after reading

## Maintenance

### Regular Tasks

- **Backup Verification**: Periodically verify that backup file can be opened in Excel
- **Key Security**: Ensure encryption keys are securely stored and backed up
- **Access Review**: Review who has access to production environment variables

### Monitoring

- Check logs for encryption/decryption errors
- Monitor file sizes to ensure encrypted files are being generated correctly
- Verify all transformers can successfully read encrypted mappings

---

## Quick Reference

**Edit mappings**: Open `Mappings.xlsx.backup` in Excel  
**Re-encrypt**: `python3 encrypt_mappings.py`  
**Test**: `python3 manage.py shell -c "...test_code..."`  
**Deploy**: `git add encrypted_file && git commit && git push`