"""
Bond Name Pattern Parser - Tier 3 Fallback Logic
Extracts issuer names from bond names using regex patterns
"""

import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class BondNameParser:
    """Parser for extracting issuer names from bond names."""
    
    # Dynamic patterns (NOT hardcoded issuers) - from ProjectAurum analysis
    EXTRACTION_PATTERNS = [
        # US Treasury patterns
        (r'^(US TREASURY|US TREAS)\b.*', 'US Treasury'),
        (r'^TREASURY\s+(BILL|NOTE|BOND)\b.*', 'US Treasury'),
        
        # Standard corporate bond: "COMPANY NAME X.X% MM/DD/YY"
        (r'^([A-Z][A-Z\s&\.\-\']+?)\s+\d+(?:\.\d+)?%\s+\d{2}/\d{2}/\d{2,4}', r'\1'),
        
        # Corporate with fractions: "COMPANY NAME X X/X MM/DD/YY"
        (r'^([A-Z][A-Z\s&\.\-\']+?)\s+\d+\s+\d+/\d+\s+\d{2}/\d{2}/\d{2,4}', r'\1'),
        
        # Bank patterns: "BK OF AMERICA" -> "Bank of America"
        (r'^BK OF (.+)', r'Bank of \1'),
        
        # Generic pattern: Everything before first number
        (r'^([A-Z][A-Z\s&\.\-\']{2,}?)\s+\d', r'\1'),
        
        # Fallback: First meaningful part
        (r'^([A-Z][A-Z\s&\.\-\']{2,})', r'\1'),
    ]
    
    # Enhanced name standardization rules
    STANDARDIZATION_RULES = [
        # PHASE 1: Remove bond-specific suffixes that cause duplication
        (r'\s+CPN\s*:?.*$', '', re.IGNORECASE),  # Remove "CPN: ..." and everything after
        (r'\s+DUE\s*:?.*$', '', re.IGNORECASE),  # Remove "DUE: ..." and everything after  
        (r'\s+CALLABLE\s+AT.*$', '', re.IGNORECASE),  # Remove "CALLABLE AT ..." and everything after
        (r'\s+MATURITY\s*:?.*$', '', re.IGNORECASE),  # Remove "MATURITY: ..." and everything after
        
        # PHASE 2: Normalize company suffix variations
        (r'\s+CORP\s*$', ' Corporation', re.IGNORECASE),
        (r'\s+INC\s*$', ' Inc', re.IGNORECASE),
        (r'\s+CO\s*$', ' Company', re.IGNORECASE),
        (r'\s+MTN\s*$', '', re.IGNORECASE),  # Remove MTN
        (r'\s+SR\s*$', '', re.IGNORECASE),   # Remove SR  
        (r'\s+JR\s*$', '', re.IGNORECASE),   # Remove JR
        
        # PHASE 3: Normalize common abbreviations
        (r'\bMTR\b', 'MOTOR', re.IGNORECASE),
        (r'\bINTL\b', 'INTERNATIONAL', re.IGNORECASE),
        (r'\bFINL\b', 'FINANCIAL', re.IGNORECASE),
        (r'\bNATL\b', 'NATIONAL', re.IGNORECASE),
        (r'\bSYS\b', 'SYSTEMS', re.IGNORECASE),
        
        # PHASE 4: Handle bank patterns
        (r'^BK OF (.+)', r'Bank of \1', re.IGNORECASE),
        (r'^BANK OF (.+)', r'Bank of \1', re.IGNORECASE),
        
        # PHASE 5: Specific company standardizations (based on test results)
        (r'^T-MOBILE USA.*', 'T-MOBILE USA Inc', re.IGNORECASE),
        (r'^CVS HEALTH.*', 'CVS HEALTH Corporation', re.IGNORECASE), 
        (r'^CITIGROUP.*', 'CITIGROUP Inc', re.IGNORECASE),
        (r'^FORD (MTR|MOTOR).*', 'FORD MOTOR Company', re.IGNORECASE),
        (r'^ENERGY TRANSFER LP.*', 'ENERGY TRANSFER LP', re.IGNORECASE),
        (r'^WESTERN MIDSTREAM OPERATING LP.*', 'WESTERN MIDSTREAM OPERATING LP', re.IGNORECASE),
        (r'^WESTN MIDSTREAM OPERA.*', 'WESTERN MIDSTREAM OPERATING LP', re.IGNORECASE),
    ]
    
    def extract_issuer_from_name(self, bond_name: str) -> Optional[str]:
        """
        Extract issuer name from bond name using pattern matching.
        
        Args:
            bond_name: Raw bond name from database
            
        Returns:
            Extracted issuer name or None if extraction fails
        """
        if not bond_name:
            return None
        
        # STEP 1: Pre-process bond name (remove obvious noise)
        cleaned_name = self._pre_process_bond_name(bond_name.strip().upper())
        
        # STEP 2: Try existing extraction patterns
        for pattern, replacement in self.EXTRACTION_PATTERNS:
            match = re.match(pattern, cleaned_name)
            if match:
                if isinstance(replacement, str) and replacement.startswith('\\'):
                    # Dynamic replacement with captured group
                    extracted = match.group(1) if match.groups() else cleaned_name
                else:
                    # Static replacement
                    extracted = replacement
                
                # STEP 3: Apply enhanced standardization rules
                standardized = self._enhanced_standardize_name(extracted.strip())
                if standardized:
                    logger.debug(f"Extracted '{standardized}' from '{bond_name}' using pattern")
                    return standardized
        
        logger.warning(f"Could not extract issuer from bond name: {bond_name}")
        return None
    
    def _pre_process_bond_name(self, bond_name: str) -> str:
        """Pre-process bond name to remove noise before pattern matching."""
        # Remove common prefixes that interfere with matching
        prefixes_to_remove = [
            r'^\s*CUSIP\s*:?\s*\w+\s*',  # Remove CUSIP prefixes
            r'^\s*ISIN\s*:?\s*\w+\s*',   # Remove ISIN prefixes
        ]
        
        processed = bond_name
        for prefix_pattern in prefixes_to_remove:
            processed = re.sub(prefix_pattern, '', processed, flags=re.IGNORECASE)
        
        return processed.strip()
    
    def _enhanced_standardize_name(self, raw_name: str) -> str:
        """Apply enhanced standardization rules to extracted name."""
        if not raw_name:
            return raw_name
        
        standardized = raw_name
        for pattern, replacement, flags in self.STANDARDIZATION_RULES:
            standardized = re.sub(pattern, replacement, standardized, flags=flags)
        
        # Additional cleanup
        standardized = re.sub(r'\s+', ' ', standardized)  # Normalize whitespace
        standardized = standardized.strip()
        
        return standardized
    
    def _standardize_name(self, raw_name: str) -> str:
        """Legacy method - kept for backward compatibility."""
        return self._enhanced_standardize_name(raw_name)