"""
ETF Detection Utility
Identifies ETFs from equity positions using FMP API + fallback name patterns
"""

import logging
from typing import List, Tuple
from portfolio.models import Asset

logger = logging.getLogger(__name__)


class ETFDetector:
    """Detect ETFs using FMP API and name-based patterns as fallback."""

    # Enhanced keywords to catch more ETFs
    ETF_KEYWORDS = [
        'ETF', 'FUND', 'INDEX', 'TRUST', 'SELECT SECTOR',
        'ISH', 'ISHARES', 'SPDR', 'VANGUARD', 'INVESCO',
        'PROSHARES', 'POWERSHARES', 'WISDOMTREE', 'ARK',
        'SCHWAB', 'FIDELITY', 'BLACKROCK'
    ]

    def __init__(self, fmp_service=None):
        """
        Initialize ETF detector.

        Args:
            fmp_service: Optional FMPEquityService instance for API-based detection
        """
        self.fmp_service = fmp_service

    def is_etf(self, asset: Asset) -> bool:
        """
        Check if asset is ETF using FMP API first, then name patterns as fallback.

        Args:
            asset: Asset object to check

        Returns:
            True if asset is an ETF
        """
        if not asset:
            return False

        # Method 1: Use FMP API if available and ticker exists
        if self.fmp_service and asset.ticker:
            try:
                is_etf_api = self.fmp_service.is_etf(asset.ticker)
                if is_etf_api:
                    return True
            except Exception as e:
                logger.warning(f"FMP API ETF check failed for {asset.ticker}: {e}")

        # Method 2: Fallback to name-based pattern matching
        if asset.name:
            name = asset.name.upper()
            if any(keyword in name for keyword in self.ETF_KEYWORDS):
                return True

        return False

    def separate_equities(self, positions) -> Tuple[List, List]:
        """
        Separate positions into direct equities vs ETFs.

        Args:
            positions: QuerySet or list of Position objects

        Returns:
            Tuple of (direct_equities, etfs)
        """
        etfs = []
        direct_equities = []

        for position in positions:
            if self.is_etf(position.asset):
                etfs.append(position)
            else:
                direct_equities.append(position)

        logger.info(f"Separated {len(direct_equities)} direct equities and {len(etfs)} ETFs")
        return direct_equities, etfs
