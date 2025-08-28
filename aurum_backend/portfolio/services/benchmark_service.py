"""
Benchmark data service for fetching ETF performance data.
Handles VOO (S&P 500) and AGG (Fixed Income) benchmark data using Alpha Vantage API.
"""
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from django.core.cache import cache
import time

logger = logging.getLogger(__name__)

class BenchmarkService:
    """Fetches and normalizes benchmark ETF data for portfolio comparison using Alpha Vantage."""
    
    BENCHMARK_SYMBOLS = {
        'VOO': 'Vanguard S&P 500 ETF',
        'AGG': 'iShares Core US Aggregate Bond ETF'
    }
    
    CACHE_TIMEOUT = 86400  # 24 hours in seconds
    ALPHA_VANTAGE_API_KEY = '9JLWYMA9KRFYRI6F'
    ALPHA_VANTAGE_BASE_URL = 'https://www.alphavantage.co/query'
    
    def get_benchmark_data(self, start_date: str, end_date: str) -> Dict[str, List[Dict]]:
        """
        Fetch benchmark data for VOO and AGG from inception to end_date.
        
        Args:
            start_date: Portfolio inception date (2025-05-29)
            end_date: Latest portfolio data date (e.g., 2025-08-21)
            
        Returns:
            Dict with benchmark data normalized to base 1000
        """
        benchmark_data = {}
        
        for symbol in self.BENCHMARK_SYMBOLS.keys():
            try:
                data = self._fetch_etf_data(symbol, start_date, end_date)
                if data:
                    normalized_data = self._normalize_to_base_1000(data, start_date)
                    benchmark_data[symbol] = normalized_data
                    logger.info(f"Successfully fetched {len(normalized_data)} data points for {symbol}")
                else:
                    logger.warning(f"No data returned for {symbol}")
                    
            except Exception as e:
                logger.error(f"Error fetching {symbol} data: {str(e)}")
                
        return benchmark_data
    
    def _fetch_etf_data(self, symbol: str, start_date: str, end_date: str) -> Optional[List[Tuple]]:
        """Fetch ETF price data from Alpha Vantage with caching."""
        cache_key = f"benchmark_av_{symbol}_{start_date}_{end_date}"
        cached_data = cache.get(cache_key)
        
        if cached_data:
            logger.debug(f"Using cached Alpha Vantage data for {symbol}")
            return cached_data
            
        try:
            # Rate limiting: Alpha Vantage allows 5 requests per minute
            time.sleep(0.2)  # Small delay between requests
            
            # Fetch data from Alpha Vantage
            params = {
                'function': 'TIME_SERIES_DAILY',
                'symbol': symbol,
                'apikey': self.ALPHA_VANTAGE_API_KEY,
                'outputsize': 'full'  # Get all historical data, not just last 100 days
            }
            
            response = requests.get(self.ALPHA_VANTAGE_BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Check for API errors
            if 'Error Message' in data:
                logger.error(f"Alpha Vantage error for {symbol}: {data['Error Message']}")
                return None
                
            if 'Note' in data:
                logger.warning(f"Alpha Vantage rate limit for {symbol}: {data['Note']}")
                return None
                
            if 'Time Series (Daily)' not in data:
                logger.error(f"Unexpected Alpha Vantage response for {symbol}: {list(data.keys())}")
                return None
            
            time_series = data['Time Series (Daily)']
            
            # Convert to list of (date, close_price) tuples
            # Filter data within our date range
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            filtered_data = []
            for date_str, values in time_series.items():
                date_dt = datetime.strptime(date_str, '%Y-%m-%d')
                if start_dt <= date_dt <= end_dt:
                    close_price = float(values['4. close'])
                    filtered_data.append((date_str, close_price))
            
            # Sort by date (oldest first)
            filtered_data.sort(key=lambda x: x[0])
            
            if not filtered_data:
                logger.warning(f"No data for {symbol} in date range {start_date} to {end_date}")
                return None
            
            # Cache for 24 hours
            cache.set(cache_key, filtered_data, self.CACHE_TIMEOUT)
            logger.info(f"Cached {len(filtered_data)} Alpha Vantage data points for {symbol}")
            
            return filtered_data
            
        except Exception as e:
            logger.error(f"Alpha Vantage API error for {symbol}: {str(e)}")
            return None
    
    def _normalize_to_base_1000(self, price_data: List[Tuple], start_date: str) -> List[Dict]:
        """
        Normalize price data to base 1000 (same as portfolio logic).
        
        Args:
            price_data: List of (date, price) tuples
            start_date: Portfolio inception date for base normalization
            
        Returns:
            List of normalized data points with base 1000
        """
        if not price_data:
            return []
            
        # Find the first price (base price for normalization)
        first_price = price_data[0][1]  # First close price
        
        normalized_data = []
        for date_str, close_price in price_data:
            # Apply same base 1000 logic as portfolio
            cumulative_value = 1000 * (close_price / first_price)
            
            normalized_data.append({
                'date': date_str,
                'cumulative_value': round(cumulative_value, 2),
                'price': close_price
            })
            
        return normalized_data
    
    def test_api_connection(self) -> bool:
        """Test if Alpha Vantage API is accessible."""
        try:
            # Test with a simple, reliable ticker
            params = {
                'function': 'TIME_SERIES_DAILY',
                'symbol': 'SPY',
                'apikey': self.ALPHA_VANTAGE_API_KEY,
                'outputsize': 'compact'  # Just last 100 days for test
            }
            
            response = requests.get(self.ALPHA_VANTAGE_BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Check for API errors
            if 'Error Message' in data or 'Note' in data:
                return False
                
            return 'Time Series (Daily)' in data
            
        except Exception as e:
            logger.error(f"Alpha Vantage API connection test failed: {str(e)}")
            return False