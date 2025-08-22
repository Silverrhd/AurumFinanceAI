"""
Mindicador API Client for Chilean currency conversion.
Provides conversion capabilities for CLP (Chilean Peso) and UF (Chilean Unit of Account) to USD.
"""

import logging
import time
from typing import Dict, Optional, Any
from datetime import datetime, timedelta
import requests

logger = logging.getLogger(__name__)


class MindicadorClient:
    """
    Chilean Mindicador API client for currency conversion.
    Supports conversion from CLP and UF to USD with caching and error handling.
    """
    
    def __init__(self):
        """Initialize the Mindicador client."""
        try:
            from ...config import get_config
            config = get_config()
            self.base_url = config.api_clients.mindicador_base_url
            self.min_request_interval = config.api_clients.mindicador_rate_limit
        except ImportError:
            # Fallback if config not available
            self.base_url = "https://mindicador.cl/api"
            self.min_request_interval = 1.0
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'AurumFinance-Portfolio/1.0'
        })
        
        # Internal cache to avoid duplicate API calls
        self._cache = {}
        self._cache_expiry = {}
        self.cache_timeout = timedelta(hours=6)  # Cache for 6 hours (rates change daily)
        
        # Statistics tracking
        self.stats = {
            'api_calls': 0,
            'cache_hits': 0,
            'successful_conversions': 0,
            'failed_conversions': 0,
            'usd_passthrough': 0
        }
        
        # Rate limiting
        self.last_request_time = 0
        if not hasattr(self, 'min_request_interval'):
            self.min_request_interval = 1.0  # 1 second between requests (fallback)
        
        # Exchange rate cache
        self._rates = {}
        
        logger.info("MindicadorClient initialized successfully")
    
    def convert_to_usd(self, value: float, currency: str) -> float:
        """
        Convert a value from Chilean currency to USD.
        
        Args:
            value: The amount to convert
            currency: Source currency code ('CLP', 'UF', or 'USD')
            
        Returns:
            float: Converted value in USD
        """
        try:
            # Validate inputs
            if not isinstance(value, (int, float)) or value == 0:
                return 0.0
            
            value = float(value)
            currency = str(currency).strip().upper()
            
            # Handle USD passthrough
            if currency == 'USD':
                self.stats['usd_passthrough'] += 1
                return value
            
            # Handle supported Chilean currencies
            if currency in ['CLP', 'UF']:
                exchange_rate = self._get_exchange_rate(currency)
                if exchange_rate and exchange_rate > 0:
                    converted_value = value / exchange_rate
                    self.stats['successful_conversions'] += 1
                    logger.debug(f"Converted {value} {currency} to {converted_value:.2f} USD (rate: {exchange_rate})")
                    return converted_value
                else:
                    logger.warning(f"No valid exchange rate available for {currency}, returning 0.0")
                    self.stats['failed_conversions'] += 1
                    return 0.0
            else:
                logger.warning(f"Unsupported currency: {currency}, returning original value")
                self.stats['failed_conversions'] += 1
                return value  # Return original value for unsupported currencies
                
        except Exception as e:
            logger.error(f"Error converting {value} {currency} to USD: {str(e)}")
            self.stats['failed_conversions'] += 1
            return 0.0
    
    def _get_exchange_rate(self, currency: str) -> Optional[float]:
        """
        Get current exchange rate for currency to USD.
        
        Args:
            currency: Currency code ('CLP' or 'UF')
            
        Returns:
            float: Exchange rate (currency per USD) or None if unavailable
        """
        try:
            # Check cache first
            cache_key = f"rate_{currency}"
            cached_rate = self._get_from_cache(cache_key)
            if cached_rate is not None:
                self.stats['cache_hits'] += 1
                return cached_rate
            
            # Fetch from API
            rate = self._fetch_exchange_rate(currency)
            
            if rate:
                # Cache the rate
                self._add_to_cache(cache_key, rate)
                return rate
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting exchange rate for {currency}: {str(e)}")
            return None
    
    def _fetch_exchange_rate(self, currency: str) -> Optional[float]:
        """
        Fetch current exchange rate from Mindicador API.
        
        Args:
            currency: Currency code ('CLP' or 'UF')
            
        Returns:
            float: Exchange rate or None
        """
        try:
            # Rate limiting
            self._rate_limit()
            
            # Map currencies to Mindicador API endpoints
            currency_endpoints = {
                'CLP': 'dolar',  # USD/CLP rate
                'UF': 'uf'       # UF value in CLP
            }
            
            if currency not in currency_endpoints:
                logger.warning(f"Unsupported currency for API fetch: {currency}")
                return None
            
            endpoint = currency_endpoints[currency]
            url = f"{self.base_url}/{endpoint}"
            
            logger.debug(f"Fetching exchange rate for {currency} from {url}")
            
            response = self.session.get(url, timeout=10)
            self.stats['api_calls'] += 1
            
            if response.status_code == 200:
                data = response.json()
                
                if currency == 'CLP':
                    # For CLP, get the current USD value
                    if 'serie' in data and data['serie']:
                        latest_data = data['serie'][0]  # Most recent entry
                        usd_clp_rate = latest_data.get('valor')
                        if usd_clp_rate:
                            logger.info(f"Fetched USD/CLP rate: {usd_clp_rate}")
                            return float(usd_clp_rate)
                
                elif currency == 'UF':
                    # For UF, we need both UF->CLP and CLP->USD rates
                    if 'serie' in data and data['serie']:
                        latest_data = data['serie'][0]  # Most recent entry
                        uf_clp_rate = latest_data.get('valor')
                        
                        if uf_clp_rate:
                            # Get USD/CLP rate to calculate UF/USD
                            usd_clp_rate = self._fetch_exchange_rate('CLP')
                            if usd_clp_rate:
                                # UF to USD = (UF to CLP) / (USD to CLP)
                                uf_usd_rate = float(uf_clp_rate) / float(usd_clp_rate)
                                logger.info(f"Calculated UF/USD rate: {uf_usd_rate} (UF/CLP: {uf_clp_rate}, USD/CLP: {usd_clp_rate})")
                                return uf_usd_rate
                
                logger.warning(f"Could not extract rate from API response for {currency}")
                return None
                
            else:
                logger.error(f"API request failed for {currency}: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching exchange rate for {currency}: {str(e)}")
            return None
    
    def _get_from_cache(self, key: str) -> Optional[float]:
        """
        Get rate from cache if not expired.
        
        Args:
            key: Cache key
            
        Returns:
            Cached rate or None
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
    
    def _add_to_cache(self, key: str, rate: float) -> None:
        """
        Add rate to cache with expiration.
        
        Args:
            key: Cache key
            rate: Exchange rate to cache
        """
        self._cache[key] = rate
        self._cache_expiry[key] = datetime.now() + self.cache_timeout
    
    def _rate_limit(self) -> None:
        """
        Implement rate limiting to be respectful to the free API.
        """
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def get_client_stats(self) -> Dict[str, Any]:
        """
        Get client usage statistics.
        
        Returns:
            Dict containing API usage statistics
        """
        total_requests = self.stats['cache_hits'] + self.stats['api_calls']
        return {
            'api_calls': self.stats['api_calls'],
            'cache_hits': self.stats['cache_hits'],
            'successful_conversions': self.stats['successful_conversions'],
            'failed_conversions': self.stats['failed_conversions'],
            'usd_passthrough': self.stats['usd_passthrough'],
            'cache_entries': len(self._cache),
            'hit_rate': (self.stats['cache_hits'] / max(1, total_requests)) * 100
        }
    
    def clear_cache(self) -> None:
        """Clear all cached rates."""
        self._cache.clear()
        self._cache_expiry.clear()
        logger.info("MindicadorClient cache cleared")
    
    def get_supported_currencies(self) -> list:
        """Get list of supported currencies."""
        return ['CLP', 'UF', 'USD']
    
    def is_currency_supported(self, currency: str) -> bool:
        """Check if currency is supported."""
        return str(currency).strip().upper() in self.get_supported_currencies()