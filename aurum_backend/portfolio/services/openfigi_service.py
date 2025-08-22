"""
OpenFIGI API Integration Service for Bond Issuer Extraction
Implements 4-tier extraction strategy with 7-day caching
"""

import requests
import logging
import re
from typing import Optional, Dict
from django.conf import settings
from django.core.cache import cache

logger = logging.getLogger(__name__)

class OpenFIGIService:
    """Service for OpenFIGI API integration with ProjectAurum compatibility."""
    
    BASE_URL = 'https://api.openfigi.com/v3/mapping'
    API_KEY = 'bf21060a-0568-489e-8622-efcaf02e52cf'  # ProjectAurum key
    CACHE_TIMEOUT = 7 * 24 * 3600  # 7 days (604800 seconds)
    REQUEST_TIMEOUT = 10  # seconds
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'X-OPENFIGI-APIKEY': self.API_KEY
        })
    
    def get_issuer_by_cusip(self, cusip: str) -> Optional[str]:
        """
        Get issuer name by CUSIP using OpenFIGI API with caching.
        
        Args:
            cusip: CUSIP identifier (9 or 12 digit)
            
        Returns:
            Clean issuer name or None if not found
        """
        if not cusip or len(cusip) < 9:
            return None
        
        # Normalize CUSIP (use first 9 digits)
        normalized_cusip = cusip[:9].upper()
        
        # Check cache first (Tier 1)
        cache_key = f"openfigi_cusip_{normalized_cusip}"
        cached_result = cache.get(cache_key)
        if cached_result is not None:
            logger.debug(f"Cache hit for CUSIP {normalized_cusip}: {cached_result}")
            return cached_result if cached_result != "NOT_FOUND" else None
        
        try:
            # Make API request (Tier 2)
            issuer_name = self._fetch_from_api(normalized_cusip)
            
            if issuer_name:
                # Clean and cache result
                cleaned_name = self._clean_issuer_name(issuer_name)
                cache.set(cache_key, cleaned_name, self.CACHE_TIMEOUT)
                logger.info(f"OpenFIGI success for {normalized_cusip}: {cleaned_name}")
                return cleaned_name
            else:
                # Cache negative result to avoid repeated API calls
                cache.set(cache_key, "NOT_FOUND", self.CACHE_TIMEOUT)
                logger.warning(f"OpenFIGI no data for CUSIP {normalized_cusip}")
                return None
                
        except Exception as e:
            logger.error(f"OpenFIGI API error for CUSIP {normalized_cusip}: {e}")
            # Cache negative result on error
            cache.set(cache_key, "NOT_FOUND", self.CACHE_TIMEOUT // 24)  # Shorter cache on error
            return None
    
    def _fetch_from_api(self, cusip: str) -> Optional[str]:
        """Fetch issuer data from OpenFIGI API."""
        payload = [{
            'idType': 'ID_CUSIP',
            'idValue': cusip,
            'exchCode': 'US'  # Focus on US market
        }]
        
        response = self.session.post(
            self.BASE_URL,
            json=payload,
            timeout=self.REQUEST_TIMEOUT
        )
        
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0 and 'data' in data[0]:
                figi_data = data[0]['data']
                if figi_data and len(figi_data) > 0:
                    # Get issuer name from first result
                    return figi_data[0].get('name') or figi_data[0].get('issuer')
        
        return None
    
    def _clean_issuer_name(self, raw_name: str) -> str:
        """Clean issuer name using ProjectAurum patterns."""
        if not raw_name:
            return raw_name
        
        # Apply cleaning patterns from ProjectAurum
        cleaning_patterns = [
            (r'\s+N/B\s*$', ''),  # Remove N/B suffix
            (r'\s+CORP\s*$', ' Corporation'),  # Standardize CORP
            (r'\s+INC\s*$', ' Inc'),  # Standardize INC
            (r'\s+CO\s*$', ' Company'),  # Standardize CO
            (r'\s+LLC\s*$', ' LLC'),  # Keep LLC
            (r'\s+LP\s*$', ' LP'),  # Keep LP
            (r'\s+LTD\s*$', ' Limited'),  # Standardize LTD
            (r'\s+MTN\s*$', ''),  # Remove MTN (Medium Term Note)
            (r'\s+SR\s*$', ''),  # Remove SR (Senior)
        ]
        
        cleaned = raw_name.strip()
        for pattern, replacement in cleaning_patterns:
            cleaned = re.sub(pattern, replacement, cleaned, flags=re.IGNORECASE)
        
        return cleaned.strip()