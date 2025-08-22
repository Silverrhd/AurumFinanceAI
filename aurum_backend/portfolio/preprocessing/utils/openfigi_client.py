"""
OpenFIGI API Client for AurumFinance portfolio management.
Provides comprehensive CUSIP and security lookup capabilities with caching and error handling.
"""

import logging
import time
import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import requests

logger = logging.getLogger(__name__)


class OpenFIGIClient:
    """
    OpenFIGI API client with caching, batch processing, and comprehensive error handling.
    Supports CUSIP lookups, name-based searches, and security data enrichment.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the OpenFIGI client.
        
        Args:
            api_key: OpenFIGI API key for authentication (if None, uses configuration)
        """
        if api_key is None:
            from ...config import get_config
            config = get_config()
            self.api_key = config.api_clients.openfigi_api_key
            self.base_url = config.api_clients.openfigi_base_url
            self.min_request_interval = config.api_clients.openfigi_rate_limit
        else:
            self.api_key = api_key
            self.base_url = "https://api.openfigi.com/v3"
        self.session = requests.Session()
        self.session.headers.update({
            'X-OPENFIGI-APIKEY': self.api_key,
            'Content-Type': 'application/json'
        })
        
        # Internal cache to avoid duplicate API calls
        self._cache = {}
        self._cache_expiry = {}
        self.cache_timeout = timedelta(hours=24)  # Cache for 24 hours
        
        # Statistics tracking
        self.stats = {
            'api_calls': 0,
            'cache_hits': 0,
            'successful_lookups': 0,
            'failed_lookups': 0,
            'batch_requests': 0
        }
        
        # Rate limiting
        self.last_request_time = 0
        if not hasattr(self, 'min_request_interval'):
            self.min_request_interval = 0.1  # 100ms between requests (fallback)
        
        logger.info("OpenFIGI client initialized successfully")
    
    def batch_lookup(self, cusips: List[str], id_type: str = "ID_CUSIP") -> Dict[str, Dict]:
        """
        Perform batch CUSIP lookups using OpenFIGI API.
        
        Args:
            cusips: List of CUSIP identifiers to lookup
            id_type: Type of identifier (default: "ID_CUSIP")
            
        Returns:
            Dict mapping CUSIP to API response data
        """
        try:
            if not cusips:
                return {}
                
            logger.info(f"Starting batch lookup for {len(cusips)} CUSIPs")
            
            # Filter out invalid/empty CUSIPs and check cache
            valid_cusips = []
            results = {}
            
            for cusip in cusips:
                cusip = str(cusip).strip()
                if not cusip or cusip == '0':
                    continue
                    
                # Check cache first
                cached_result = self._get_from_cache(cusip)
                if cached_result:
                    results[cusip] = cached_result
                    self.stats['cache_hits'] += 1
                else:
                    valid_cusips.append(cusip)
            
            if not valid_cusips:
                logger.info("All CUSIPs found in cache or invalid")
                return results
            
            # Split into batches of 100 (OpenFIGI limit)
            batch_size = 100
            for i in range(0, len(valid_cusips), batch_size):
                batch = valid_cusips[i:i + batch_size]
                batch_results = self._process_batch(batch, id_type)
                results.update(batch_results)
            
            logger.info(f"Batch lookup completed: {len(results)} results")
            return results
            
        except Exception as e:
            logger.error(f"Error in batch lookup: {str(e)}")
            # Return error responses for all requested CUSIPs
            return {cusip: {'error': f'Batch lookup failed: {str(e)}'} for cusip in cusips}
    
    def lookup_by_cusip(self, cusip: str) -> Dict:
        """
        Lookup a single CUSIP using OpenFIGI API.
        
        Args:
            cusip: CUSIP identifier to lookup
            
        Returns:
            Dict containing security information or error details
        """
        try:
            cusip = str(cusip).strip()
            if not cusip or cusip == '0':
                return {'error': 'Invalid CUSIP provided'}
            
            # Check cache first
            cached_result = self._get_from_cache(cusip)
            if cached_result:
                self.stats['cache_hits'] += 1
                return cached_result
            
            # Perform single CUSIP lookup
            batch_result = self.batch_lookup([cusip])
            return batch_result.get(cusip, {'error': 'CUSIP lookup failed'})
            
        except Exception as e:
            logger.error(f"Error looking up CUSIP {cusip}: {str(e)}")
            return {'error': f'CUSIP lookup error: {str(e)}'}
    
    def lookup_by_name(self, name: str) -> Dict:
        """
        Lookup security by name when CUSIP lookup fails.
        
        Args:
            name: Security name to search for
            
        Returns:
            Dict containing security information or error details
        """
        try:
            if not name or not isinstance(name, str):
                return {'error': 'Invalid security name provided'}
            
            name = name.strip()
            if not name:
                return {'error': 'Empty security name provided'}
            
            # Check cache using name as key
            cache_key = f"name_{name}"
            cached_result = self._get_from_cache(cache_key)
            if cached_result:
                self.stats['cache_hits'] += 1
                return cached_result
            
            logger.info(f"Performing name-based lookup for: {name}")
            
            # Rate limiting
            self._rate_limit()
            
            # Prepare search request
            search_data = [{
                'query': name,
                'start': 0,
                'num': 5  # Get top 5 results
            }]
            
            response = self.session.post(
                f"{self.base_url}/search",
                json=search_data,
                timeout=30
            )
            
            self.stats['api_calls'] += 1
            
            if response.status_code == 200:
                search_results = response.json()
                
                if search_results and len(search_results) > 0 and search_results[0].get('data'):
                    # Get the first result
                    first_result = search_results[0]['data'][0]
                    
                    # Transform to expected format
                    result = self._transform_search_result(first_result)
                    
                    # Cache the result
                    self._add_to_cache(cache_key, result)
                    
                    self.stats['successful_lookups'] += 1
                    logger.info(f"Name lookup successful for: {name}")
                    return result
                else:
                    error_msg = f'No results found for name: {name}'
                    result = {'error': error_msg}
                    self._add_to_cache(cache_key, result)
                    self.stats['failed_lookups'] += 1
                    return result
            else:
                error_msg = f'Name search API error: {response.status_code}'
                logger.error(f"{error_msg} for name: {name}")
                return {'error': error_msg}
                
        except Exception as e:
            logger.error(f"Error in name lookup for '{name}': {str(e)}")
            return {'error': f'Name lookup error: {str(e)}'}
    
    def get_client_stats(self) -> Dict:
        """
        Get client usage statistics.
        
        Returns:
            Dict containing API usage statistics
        """
        return {
            'api_calls': self.stats['api_calls'],
            'cache_hits': self.stats['cache_hits'],
            'successful_lookups': self.stats['successful_lookups'],
            'failed_lookups': self.stats['failed_lookups'],
            'batch_requests': self.stats['batch_requests'],
            'cache_entries': len(self._cache),
            'hit_rate': (self.stats['cache_hits'] / max(1, self.stats['cache_hits'] + self.stats['api_calls'])) * 100
        }
    
    def _process_batch(self, cusips: List[str], id_type: str) -> Dict[str, Dict]:
        """
        Process a single batch of CUSIP lookups.
        
        Args:
            cusips: List of CUSIPs in this batch
            id_type: Identifier type
            
        Returns:
            Dict mapping CUSIP to response data
        """
        try:
            # Rate limiting
            self._rate_limit()
            
            # Prepare batch request
            mapping_jobs = []
            for cusip in cusips:
                mapping_jobs.append({
                    'idType': id_type,
                    'idValue': cusip
                    # Removed exchCode - let OpenFIGI API handle exchange detection automatically
                })
            
            logger.debug(f"Sending batch request for {len(mapping_jobs)} CUSIPs")
            
            response = self.session.post(
                f"{self.base_url}/mapping",
                json=mapping_jobs,
                timeout=30
            )
            
            self.stats['api_calls'] += 1
            self.stats['batch_requests'] += 1
            
            if response.status_code == 200:
                api_responses = response.json()
                return self._process_batch_response(cusips, api_responses)
            else:
                logger.error(f"Batch API request failed with status {response.status_code}")
                return {cusip: {'error': f'API request failed: {response.status_code}'} for cusip in cusips}
                
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            return {cusip: {'error': f'Batch processing error: {str(e)}'} for cusip in cusips}
    
    def _process_batch_response(self, cusips: List[str], api_responses: List) -> Dict[str, Dict]:
        """
        Process the response from a batch API call.
        
        Args:
            cusips: Original list of CUSIPs
            api_responses: API response data
            
        Returns:
            Dict mapping CUSIP to processed response data
        """
        results = {}
        
        for i, cusip in enumerate(cusips):
            try:
                if i < len(api_responses):
                    response_data = api_responses[i]
                    
                    if response_data.get('error'):
                        # API returned an error for this CUSIP
                        result = {'error': response_data['error']}
                        self.stats['failed_lookups'] += 1
                    elif response_data.get('data') and len(response_data['data']) > 0:
                        # Successfully retrieved data
                        first_result = response_data['data'][0]
                        result = self._transform_api_result(first_result)
                        self.stats['successful_lookups'] += 1
                    else:
                        # No data found
                        result = {'error': f'No data found for CUSIP: {cusip}'}
                        self.stats['failed_lookups'] += 1
                else:
                    # Response array too short
                    result = {'error': 'No response data available'}
                    self.stats['failed_lookups'] += 1
                
                results[cusip] = result
                self._add_to_cache(cusip, result)
                
            except Exception as e:
                logger.error(f"Error processing response for CUSIP {cusip}: {str(e)}")
                results[cusip] = {'error': f'Response processing error: {str(e)}'}
        
        return results
    
    def _transform_api_result(self, api_data: Dict) -> Dict:
        """
        Transform OpenFIGI API response to expected format.
        
        Args:
            api_data: Raw API response data
            
        Returns:
            Dict in expected transformer format
        """
        try:
            result = {
                'security_type': api_data.get('securityType', 'Unknown'),
                'security_type2': api_data.get('securityType2', ''),
                'ticker': api_data.get('ticker', ''),
                'maturity': api_data.get('maturity', ''),
                'coupon': self._parse_coupon(api_data.get('coupon')),
                'name': api_data.get('name', ''),
                'exchCode': api_data.get('exchCode', ''),
                'compositeFIGI': api_data.get('compositeFIGI', ''),
                'shareClassFIGI': api_data.get('shareClassFIGI', ''),
                'figi': api_data.get('figi', '')
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error transforming API result: {str(e)}")
            return {'error': f'Result transformation error: {str(e)}'}
    
    def _transform_search_result(self, search_data: Dict) -> Dict:
        """
        Transform OpenFIGI search result to expected format.
        
        Args:
            search_data: Raw search response data
            
        Returns:
            Dict in expected transformer format
        """
        try:
            result = {
                'security_type': search_data.get('securityType', 'Unknown'),
                'security_type2': search_data.get('securityType2', ''),
                'ticker': search_data.get('ticker', ''),
                'maturity': search_data.get('maturity', ''),
                'coupon': self._parse_coupon(search_data.get('coupon')),
                'name': search_data.get('name', ''),
                'exchCode': search_data.get('exchCode', ''),
                'figi': search_data.get('figi', '')
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error transforming search result: {str(e)}")
            return {'error': f'Search result transformation error: {str(e)}'}
    
    def _parse_coupon(self, coupon_data: Any) -> Optional[float]:
        """
        Parse coupon rate from API response.
        
        Args:
            coupon_data: Coupon data from API
            
        Returns:
            Float coupon rate or None
        """
        try:
            if coupon_data is None:
                return None
            
            # Handle numeric values
            if isinstance(coupon_data, (int, float)):
                return float(coupon_data)
            
            # Handle string values
            if isinstance(coupon_data, str):
                coupon_str = coupon_data.strip().replace('%', '')
                if coupon_str:
                    return float(coupon_str)
            
            return None
            
        except (ValueError, TypeError):
            return None
    
    def _get_from_cache(self, key: str) -> Optional[Dict]:
        """
        Get result from cache if not expired.
        
        Args:
            key: Cache key
            
        Returns:
            Cached result or None
        """
        if key in self._cache:
            expiry_time = self._cache_expiry.get(key)
            if expiry_time and datetime.now() < expiry_time:
                return self._cache[key]
            else:
                # Remove expired entry
                del self._cache[key]
                if key in self._cache_expiry:
                    del self._cache_expiry[key]
        
        return None
    
    def _add_to_cache(self, key: str, result: Dict) -> None:
        """
        Add result to cache with expiration.
        
        Args:
            key: Cache key
            result: Result to cache
        """
        self._cache[key] = result
        self._cache_expiry[key] = datetime.now() + self.cache_timeout
    
    def _rate_limit(self) -> None:
        """
        Implement rate limiting to respect API limits.
        """
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def clear_cache(self) -> None:
        """Clear all cached results."""
        self._cache.clear()
        self._cache_expiry.clear()
        logger.info("OpenFIGI client cache cleared")
    
    def get_cache_size(self) -> int:
        """Get current cache size."""
        return len(self._cache)