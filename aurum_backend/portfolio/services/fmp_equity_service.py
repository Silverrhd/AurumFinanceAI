"""
FMP API Integration Service for Equity Analysis
Implements rate limiting and 30-day JSON caching
"""

import requests
import logging
import os
import json
import hashlib
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from django.conf import settings

logger = logging.getLogger(__name__)


# Comprehensive UCITS to US ETF Mapping Dictionary
# Maps European UCITS ETFs to their US-domiciled equivalents for data lookup
UCITS_TO_US_MAPPING = {
    # S&P 500 Core Trackers
    'CSPX': 'IVV',          # iShares Core S&P 500 UCITS → iShares Core S&P 500
    'CSPX LN': 'IVV',       # Same with London exchange suffix
    'VUSA': 'VOO',          # Vanguard S&P 500 UCITS → Vanguard S&P 500
    'VUAA': 'VOO',          # Vanguard S&P 500 UCITS (Acc) → Vanguard S&P 500
    'SPY5': 'SPY',          # SPDR S&P 500 UCITS → SPDR S&P 500
    'CSTNL': 'IVV',         # iShares Core S&P 500 UCITS → iShares Core S&P 500

    # Nasdaq 100 Trackers
    'EQQQ': 'QQQ',          # Invesco EQQQ Nasdaq 100 UCITS → Invesco QQQ
    'EQAC': 'QQQ',          # Invesco EQQQ Nasdaq 100 UCITS (Acc) → Invesco QQQ
    'CNDX': 'QQQ',          # iShares Nasdaq 100 UCITS → Invesco QQQ

    # Total Market / Broad Trackers
    'VWRA': 'VT',           # Vanguard FTSE All-World UCITS (Acc) → Vanguard Total World
    'VWRL': 'VT',           # Vanguard FTSE All-World UCITS (Dist) → Vanguard Total World
    'IRRRF': 'ACWI',        # iShares MSCI World UCITS → iShares MSCI ACWI

    # Europe / Regional Trackers
    'IEUR': 'VGK',          # iShares Core MSCI Europe UCITS → Vanguard FTSE Europe
    'ISF': 'ISF',           # iShares Core FTSE 100 → No US equivalent (UK-specific)
    'CCAU': 'EWC',          # iShares MSCI Canada UCITS → iShares MSCI Canada

    # Emerging Markets
    'EIMI': 'IEMG',         # iShares Core EM IMI UCITS → iShares Core MSCI EM
    'ICHN': 'MCHI',         # iShares MSCI China UCITS → iShares MSCI China
    'NDIA': 'INDA',         # iShares MSCI India UCITS → iShares MSCI India
    'CPXJ': 'EPP',          # iShares MSCI Pacific ex-Japan UCITS → iShares MSCI Pacific ex-Japan

    # iShares S&P 500 Sector UCITS ETFs → Select Sector SPDR ETFs
    'IUCD': 'XLY',          # iShares S&P 500 Consumer Discretionary UCITS → Consumer Discretionary SPDR
    'IUCD LN': 'XLY',       # Same with London exchange suffix
    'IUCS': 'XLP',          # iShares S&P 500 Consumer Staples UCITS → Consumer Staples SPDR
    'IUCS LN': 'XLP',       # Same with London exchange suffix
    'IUFS': 'XLF',          # iShares S&P 500 Financials UCITS → Financial SPDR
    'IUHC': 'XLV',          # iShares S&P 500 Healthcare UCITS → Healthcare SPDR
    'IUIS': 'XLI',          # iShares S&P 500 Industrials UCITS → Industrial SPDR
    'IUIT': 'XLK',          # iShares S&P 500 Info Tech UCITS → Technology SPDR
    'IUCM': 'XLC',          # iShares S&P 500 Communication UCITS → Communication Services SPDR
    'IUUS': 'XLU',          # iShares S&P 500 Utilities UCITS → Utilities SPDR

    # Xtrackers MSCI USA Sector UCITS ETFs → Select Sector SPDR ETFs
    'XUCD': 'XLY',          # Xtrackers MSCI USA Consumer Discretionary UCITS → Consumer Discretionary SPDR
    'XUFN': 'XLF',          # Xtrackers MSCI USA Financials UCITS → Financial SPDR
    'XUHC': 'XLV',          # Xtrackers MSCI USA Healthcare UCITS → Healthcare SPDR
    'XUTC': 'XLK',          # Xtrackers MSCI USA Info Tech UCITS → Technology SPDR
    'XUCM': 'XLC',          # Xtrackers MSCI USA Communication Services UCITS → Communication Services SPDR

    # iShares MSCI Europe Sector ETFs → Vanguard/iShares US Sector Equivalents
    'ESIN.DE': 'XLI',       # iShares MSCI Europe Industrials → Industrial SPDR
    'ESIH.DE': 'XLV',       # iShares MSCI Europe Healthcare → Healthcare SPDR
    'ESI1.DE': 'XLY',       # iShares MSCI Europe Consumer Discretionary → Consumer Discretionary SPDR
    'ESIS.DE': 'XLP',       # iShares MSCI Europe Consumer Staples → Consumer Staples SPDR
    'ESIF.DE': 'XLF',       # iShares MSCI Europe Financials → Financial SPDR
    'ESIT.DE': 'XLK',       # iShares MSCI Europe Info Tech → Technology SPDR

    # Thematic / Strategy ETFs
    'ICLN': 'ICLN',         # iShares Global Clean Energy → Same ticker exists in US
    'IEMXF': 'ACWV',        # iShares Edge MSCI World Min Vol → iShares MSCI Global Min Vol
}


class FMPRateLimiter:
    """Token bucket rate limiter for FMP API (300 req/min)."""

    def __init__(self, max_requests: int = 300, time_window: int = 60):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []

    def wait_if_needed(self):
        """Wait if rate limit would be exceeded."""
        import time

        now = time.time()
        # Remove requests older than time window
        self.requests = [req_time for req_time in self.requests
                        if now - req_time < self.time_window]

        if len(self.requests) >= self.max_requests:
            oldest_request = self.requests[0]
            wait_time = self.time_window - (now - oldest_request)
            if wait_time > 0:
                logger.warning(f"Rate limit reached, waiting {wait_time:.2f}s")
                time.sleep(wait_time + 0.1)  # Small buffer

        self.requests.append(now)


class FMPCache:
    """30-day JSON cache with MD5 keys."""

    def __init__(self, cache_file: str = 'fmp_cache.json', expire_days: int = 30):
        self.cache_file = Path(settings.BASE_DIR) / 'data' / cache_file
        self.expire_days = expire_days
        self.cache_data = {}
        self.stats = {'hits': 0, 'misses': 0, 'expired': 0}
        self._load_cache()

    def _load_cache(self):
        """Load cache from disk."""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r') as f:
                    self.cache_data = json.load(f)
                logger.info(f"Loaded FMP cache with {len(self.cache_data)} entries")
            except Exception as e:
                logger.warning(f"Could not load FMP cache: {e}")
                self.cache_data = {}
        else:
            logger.info("No existing FMP cache found. Starting with empty cache.")

    def _save_cache(self):
        """Save cache to disk."""
        try:
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache_data, f, indent=2, default=str)
            logger.debug(f"FMP cache saved with {len(self.cache_data)} entries")
        except IOError as e:
            logger.error(f"Could not save FMP cache: {e}")

    def _generate_key(self, identifier: str, endpoint_type: str) -> str:
        """Generate cache key from identifier and endpoint type."""
        combined = f"{endpoint_type}:{identifier.upper().strip()}"
        return hashlib.md5(combined.encode()).hexdigest()

    def _is_expired(self, cached_entry: Dict) -> bool:
        """Check if cache entry is expired."""
        if 'timestamp' not in cached_entry:
            return True

        cache_time = datetime.fromisoformat(cached_entry['timestamp'])
        expiry_time = cache_time + timedelta(days=self.expire_days)
        return datetime.now() > expiry_time

    def get(self, identifier: str, endpoint_type: str) -> Optional[Dict]:
        """Get cached response for identifier."""
        cache_key = self._generate_key(identifier, endpoint_type)

        if cache_key in self.cache_data:
            cached_entry = self.cache_data[cache_key]

            if self._is_expired(cached_entry):
                self.stats['expired'] += 1
                del self.cache_data[cache_key]
                self._save_cache()
                return None

            self.stats['hits'] += 1
            logger.debug(f"FMP cache HIT for {endpoint_type}:{identifier}")
            return cached_entry.get('data')

        self.stats['misses'] += 1
        logger.debug(f"FMP cache MISS for {endpoint_type}:{identifier}")
        return None

    def set(self, identifier: str, data: Dict, endpoint_type: str):
        """Cache response for identifier."""
        cache_key = self._generate_key(identifier, endpoint_type)

        self.cache_data[cache_key] = {
            'identifier': identifier,
            'endpoint_type': endpoint_type,
            'data': data,
            'timestamp': datetime.now().isoformat()
        }
        self._save_cache()
        logger.debug(f"Cached FMP response for {endpoint_type}:{identifier}")


class FMPEquityService:
    """
    FMP API client for equity analysis.

    Free tier: 250 requests/day, ~300/minute burst
    30-day cache minimizes API usage
    """

    # HARDCODED API KEY (Following OpenFIGI pattern)
    # Replace this placeholder with your actual FMP API key
    API_KEY = 'R4Ha75HU1HmKoiFqfuYHuTFCwOchsgOo'
    BASE_URL = 'https://financialmodelingprep.com/api'

    # Hardcoded SPY sector weights (fallback if API fails)
    HARDCODED_SPY_WEIGHTS = {
        'Information Technology': 32.91,
        'Financials': 13.93,
        'Consumer Discretionary': 10.83,
        'Communication Services': 9.62,
        'Health Care': 9.62,
        'Industrials': 7.86,
        'Consumer Staples': 5.89,
        'Energy': 3.00,
        'Utilities': 2.49,
        'Real Estate': 2.14,
        'Materials': 1.72
    }

    # Sector normalization map (FMP → SPY benchmark)
    SECTOR_NORMALIZATION_MAP = {
        'Technology': 'Information Technology',
        'Financial Services': 'Financials',
        'Consumer Cyclical': 'Consumer Discretionary',
        'Consumer Defensive': 'Consumer Staples',
        'Basic Materials': 'Materials',
        'Healthcare': 'Health Care',
        # Direct matches (no normalization needed)
        'Real Estate': 'Real Estate',
        'Communication Services': 'Communication Services',
        'Energy': 'Energy',
        'Industrials': 'Industrials',
        'Utilities': 'Utilities',
        'Information Technology': 'Information Technology',
        'Financials': 'Financials',
        'Consumer Discretionary': 'Consumer Discretionary',
        'Consumer Staples': 'Consumer Staples',
        'Materials': 'Materials',
        'Health Care': 'Health Care'
    }

    def __init__(self):
        self.rate_limiter = FMPRateLimiter(max_requests=300, time_window=60)
        self.cache = FMPCache(cache_file='fmp_cache.json', expire_days=30)
        self.session = requests.Session()
        self.api_calls_made = 0
        self.errors_encountered = 0
        logger.info("FMP Equity Service initialized successfully")

    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Make HTTP request to FMP API with rate limiting."""
        self.rate_limiter.wait_if_needed()

        if params is None:
            params = {}

        # Add API key to parameters
        params['apikey'] = self.API_KEY

        url = f"{self.BASE_URL}/{endpoint}"

        try:
            logger.debug(f"Making FMP API request to: {endpoint}")
            response = requests.get(url, params=params, timeout=30)
            self.api_calls_made += 1

            if response.status_code == 200:
                data = response.json()
                logger.debug(f"FMP API request successful: {endpoint}")
                return data
            elif response.status_code == 429:
                logger.warning(f"FMP API rate limit exceeded. Status: {response.status_code}")
                self.errors_encountered += 1
                return None
            else:
                logger.warning(f"FMP API request failed. Status: {response.status_code}")
                self.errors_encountered += 1
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"FMP API request exception: {e}")
            self.errors_encountered += 1
            return None

    def get_company_profile(self, ticker: str) -> Optional[Dict]:
        """Get company profile including sector information."""
        if not ticker:
            return None

        ticker = ticker.upper().strip()

        # Check cache first
        cached_data = self.cache.get(ticker, 'profile')
        if cached_data:
            return cached_data

        # Make API request
        endpoint = f"v3/profile/{ticker}"
        response_data = self._make_request(endpoint)

        if response_data and isinstance(response_data, list) and len(response_data) > 0:
            profile_data = response_data[0]

            # Extract and normalize sector
            raw_sector = profile_data.get('sector', 'Unknown')
            normalized_sector = self.SECTOR_NORMALIZATION_MAP.get(raw_sector, raw_sector)

            result = {
                'ticker': profile_data.get('symbol', ticker),
                'company_name': profile_data.get('companyName', ''),
                'sector': normalized_sector,
                'raw_sector': raw_sector,
                'industry': profile_data.get('industry', ''),
                'market_cap': profile_data.get('mktCap', 0)
            }

            # Cache the successful result
            self.cache.set(ticker, result, 'profile')
            return result

        # DON'T cache empty/failed results - return without caching
        # This allows retry on next request in case of temporary API issues
        logger.warning(f"No profile data found for {ticker} - will retry on next request")
        empty_result = {
            'ticker': ticker,
            'sector': 'Unknown',
            'error': 'No data available'
        }
        return empty_result

    def get_etf_holdings(self, ticker: str) -> Optional[List[Dict]]:
        """Get ETF holdings breakdown with UCITS to US mapping fallback."""
        if not ticker:
            return None

        ticker = ticker.upper().strip()

        # Check cache first
        cached_data = self.cache.get(ticker, 'etf_holdings')
        if cached_data:
            return cached_data

        # Make API request - use v3/etf-holder endpoint (confirmed working)
        endpoint = f"v3/etf-holder/{ticker}"
        response_data = self._make_request(endpoint)

        if response_data and isinstance(response_data, list) and len(response_data) > 0:
            holdings = []
            for holding in response_data:
                holdings.append({
                    'symbol': holding.get('asset', ''),
                    'name': holding.get('name', ''),
                    'weight': holding.get('weightPercentage', 0),
                    'shares': holding.get('sharesNumber', 0),
                    'market_value': holding.get('marketValue', 0)
                })

            # Cache successful result (7 days - ETF holdings change slowly)
            self.cache.set(ticker, holdings, 'etf_holdings')
            logger.info(f"Fetched {len(holdings)} holdings for ETF {ticker}")
            return holdings

        # UCITS Mapping Fallback: Try US equivalent if European UCITS ETF
        if ticker in UCITS_TO_US_MAPPING:
            us_equivalent = UCITS_TO_US_MAPPING[ticker]
            logger.info(f"No data for UCITS ETF {ticker}, using US equivalent {us_equivalent}")

            # Recursively call with US equivalent (will use cache if available)
            us_holdings = self.get_etf_holdings(us_equivalent)
            if us_holdings:
                # Cache the result under original ticker too
                self.cache.set(ticker, us_holdings, 'etf_holdings')
                logger.info(f"Successfully mapped {ticker} → {us_equivalent} ({len(us_holdings)} holdings)")
                return us_holdings

        # DON'T cache empty/failed results - return without caching
        logger.warning(f"No ETF holdings found for {ticker} - will retry on next request")
        return []

    def get_etf_sector_weightings(self, ticker: str) -> Optional[Dict[str, float]]:
        """Get ETF sector allocation breakdown with UCITS to US mapping fallback."""
        if not ticker:
            return None

        ticker = ticker.upper().strip()

        # Check cache first
        cached_data = self.cache.get(ticker, 'sector_weightings')
        if cached_data:
            return cached_data

        # Make API request
        endpoint = f"v3/etf-sector-weightings/{ticker}"
        response_data = self._make_request(endpoint)

        if response_data and isinstance(response_data, list):
            sector_weights = {}
            for sector_data in response_data:
                raw_sector = sector_data.get('sector', 'Unknown')
                weight_pct = sector_data.get('weightPercentage', 0)

                # Normalize sector name
                normalized_sector = self.SECTOR_NORMALIZATION_MAP.get(raw_sector, raw_sector)

                # Handle percentage string parsing
                try:
                    weight_str = str(weight_pct).strip()
                    if weight_str.endswith('%'):
                        weight_str = weight_str[:-1]
                    sector_weights[normalized_sector] = float(weight_str)
                except (ValueError, TypeError):
                    logger.warning(f"Invalid sector weight for {ticker}: {weight_pct}")
                    continue

            # Cache the result
            self.cache.set(ticker, sector_weights, 'sector_weightings')
            return sector_weights

        # UCITS Mapping Fallback: Try US equivalent if European UCITS ETF
        if ticker in UCITS_TO_US_MAPPING:
            us_equivalent = UCITS_TO_US_MAPPING[ticker]
            logger.info(f"No sector data for UCITS ETF {ticker}, using US equivalent {us_equivalent}")

            # Recursively call with US equivalent (will use cache if available)
            us_sectors = self.get_etf_sector_weightings(us_equivalent)
            if us_sectors:
                # Cache the result under original ticker too
                self.cache.set(ticker, us_sectors, 'sector_weightings')
                logger.info(f"Successfully mapped {ticker} → {us_equivalent} (sector weightings)")
                return us_sectors

        # DON'T cache empty/failed results - return without caching
        logger.warning(f"No sector weightings found for {ticker} - will retry on next request")
        return {}

    def is_etf(self, ticker: str) -> bool:
        """
        Check if a ticker is an ETF using FMP API.

        Args:
            ticker: Stock ticker to check

        Returns:
            True if ticker is an ETF, False otherwise
        """
        if not ticker:
            return False

        ticker = ticker.upper().strip()

        # Check cache first
        cached_result = self.cache.get(ticker, 'is_etf')
        if cached_result is not None:
            return cached_result

        # Make API request to ETF info endpoint
        endpoint = f"v3/etf-info"
        params = {'symbol': ticker}
        response_data = self._make_request(endpoint, params)

        # If we get data back, it's an ETF
        is_etf_result = bool(response_data and isinstance(response_data, list) and len(response_data) > 0)

        # Cache the result (30 days - this won't change)
        self.cache.set(ticker, is_etf_result, 'is_etf')

        return is_etf_result

    def get_spy_benchmark_allocation(self) -> Dict[str, float]:
        """Get SPY sector allocation for benchmark comparison."""
        # Try to get live data
        spy_weights = self.get_etf_sector_weightings('SPY')

        if spy_weights and len(spy_weights) > 0:
            return spy_weights

        # Fallback to hardcoded weights
        logger.warning("Using hardcoded SPY sector weights (FMP API unavailable)")
        return self.HARDCODED_SPY_WEIGHTS.copy()

    def batch_enrich_equities(self, positions) -> Dict[str, Dict]:
        """
        Batch enrich equity positions with sector data.

        Args:
            positions: QuerySet of Position objects

        Returns:
            Dict mapping ticker to enriched data
        """
        results = {}

        for position in positions:
            ticker = position.asset.ticker
            if not ticker:
                continue

            if ticker in results:
                continue  # Already processed

            profile = self.get_company_profile(ticker)
            if profile:
                results[ticker] = profile

        logger.info(f"Enriched {len(results)} equity tickers")
        logger.info(f"FMP API calls made: {self.api_calls_made}")
        logger.info(f"Cache stats: {self.cache.stats}")

        return results
