"""Bank detection from filename patterns."""

import re
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

class BankDetector:
    """Detects bank from filename patterns."""
    
    BANK_PATTERNS = {
        'JPM': r'^JPM_',
        'CS': r'^CS_',
        'CSC': r'^CSC_',
        'JB': r'^JB_',
        'MS': r'^MS_',
        'Valley': r'^Valley_',
        'Pershing': r'^Pershing_',
        'HSBC': r'^HSBC_',
        'LO': r'^LO_',
        'Safra': r'^Safra_',
        'IDB': r'^IDB_',
        'Banchile': r'^Banchile_',
        'ALT': r'^alt_'
    }
    
    # Banks that use individual files per client/account
    INDIVIDUAL_FILE_BANKS = {'Valley', 'CS', 'CSC', 'JB', 'Pershing', 'HSBC', 'LO', 'IDB'}
    
    @classmethod
    def detect_bank(cls, filename: str) -> Optional[str]:
        """
        Detect bank from filename.
        
        Args:
            filename: Name of the file
            
        Returns:
            Bank code (e.g., 'JPM') or None if not detected
        """
        for bank_code, pattern in cls.BANK_PATTERNS.items():
            if re.match(pattern, filename, re.IGNORECASE):
                logger.info(f"Detected bank '{bank_code}' from filename '{filename}'")
                return bank_code
        
        logger.warning(f"Could not detect bank from filename '{filename}'")
        return None
    
    @classmethod
    def extract_date_from_filename(cls, filename: str) -> Optional[str]:
        """
        Extract date from filename pattern.
        
        Args:
            filename: Name of the file (e.g., 'JPM_securities_26_05_2025.xlsx')
            
        Returns:
            Date string (e.g., '26_05_2025') or None if not found
        """
        # Pattern to match date in format DD_MM_YYYY
        date_pattern = r'(\d{2}_\d{2}_\d{4})'
        match = re.search(date_pattern, filename)
        
        if match:
            date_str = match.group(1)
            logger.info(f"Extracted date '{date_str}' from filename '{filename}'")
            return date_str
        
        logger.warning(f"Could not extract date from filename '{filename}'")
        return None
    
    @classmethod
    def is_individual_file_bank(cls, bank_code: str) -> bool:
        """
        Check if bank uses individual files per client/account.
        
        Args:
            bank_code: Bank code (e.g., 'Valley')
            
        Returns:
            True if bank uses individual files, False if unified files
        """
        return bank_code in cls.INDIVIDUAL_FILE_BANKS
    
    @classmethod
    def extract_client_account_from_filename(cls, filename: str) -> Optional[Tuple[str, str, str]]:
        """
        Extract bank, client, account from individual file pattern.
        
        Args:
            filename: Individual file pattern (e.g., 'Valley_HZ_Greige_Securities_27_05_2025.xlsx')
            
        Returns:
            Tuple of (bank, client, account) or None if pattern doesn't match
        """
        # Pattern: Bank_Client_Account_Type_DD_MM_YYYY.xlsx
        # Example: Valley_HZ_Greige_Securities_27_05_2025.xlsx
        pattern = r'^([A-Za-z]+)_([A-Za-z0-9]+)_([A-Za-z0-9]+)_(?:[Ss]ecurities|transactions|unitcost)_\d{2}_\d{2}_\d{4}\.xlsx?$'
        
        match = re.match(pattern, filename, re.IGNORECASE)
        if match:
            bank, client, account = match.groups()
            logger.debug(f"Extracted from '{filename}': bank='{bank}', client='{client}', account='{account}'")
            return (bank, client, account)
        
        logger.warning(f"Could not extract client/account from filename '{filename}'")
        return None 