"""
ETF Detection Utility
Identifies ETFs from equity positions using name-based patterns
"""

import logging
from typing import List, Tuple
from portfolio.models import Asset

logger = logging.getLogger(__name__)


class ETFDetector:
    """Detect ETFs using name-based patterns."""

    ETF_KEYWORDS = [
        'ETF', 'FUND', 'INDEX', 'TRUST',
        'ISHARES', 'SPDR', 'VANGUARD', 'INVESCO',
        'PROSHARES', 'POWERSHARES', 'WISDOMTREE'
    ]

    def is_etf(self, asset: Asset) -> bool:
        """
        Check if asset is ETF using name patterns.

        Args:
            asset: Asset object to check

        Returns:
            True if asset is likely an ETF
        """
        if not asset or not asset.name:
            return False

        name = asset.name.upper()
        return any(keyword in name for keyword in self.ETF_KEYWORDS)

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
