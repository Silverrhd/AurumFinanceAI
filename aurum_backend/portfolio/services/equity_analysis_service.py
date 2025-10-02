"""
Equity Analysis Service
Core business logic for equity breakdown analysis
"""

import logging
from typing import Dict, List
from collections import defaultdict
from decimal import Decimal
from .fmp_equity_service import FMPEquityService
from .etf_detector import ETFDetector

logger = logging.getLogger(__name__)


class EquityAnalysisService:
    """Analyze equity positions with sector breakdown."""

    def __init__(self):
        self.fmp_service = FMPEquityService()
        self.etf_detector = ETFDetector()

    def analyze_equity_portfolio(self, positions) -> Dict:
        """
        Main analysis method.

        Args:
            positions: QuerySet of Position objects (Equities only, ALTs excluded)

        Returns:
            {
                'direct_holdings': [...],
                'etf_holdings': [...],
                'direct_sector_exposure': {...},
                'etf_sector_exposure': {...},
                'combined_sector_exposure': {...},
                'spy_benchmark': {...},
                'sector_comparison': {...},
                'total_equity_value': float
            }
        """
        logger.info(f"Analyzing equity portfolio with {positions.count()} positions")

        # Separate direct equities from ETFs
        direct_equities, etfs = self.etf_detector.separate_equities(positions)

        # Calculate total equity value
        total_equity_value = float(sum(pos.market_value for pos in positions))

        # Enrich direct equities with sector data
        direct_holdings_enriched = self._enrich_direct_holdings(direct_equities, total_equity_value)

        # Analyze ETF holdings
        etf_holdings_enriched = self._enrich_etf_holdings(etfs, total_equity_value)

        # Calculate sector exposures
        direct_sector_exposure = self._calculate_direct_sector_exposure(direct_holdings_enriched)
        etf_sector_exposure = self._calculate_etf_sector_exposure(etf_holdings_enriched)
        combined_sector_exposure = self._calculate_combined_exposure(
            direct_sector_exposure,
            etf_sector_exposure
        )

        # Get SPY benchmark
        spy_benchmark = self.fmp_service.get_spy_benchmark_allocation()

        # Compare with SPY
        sector_comparison = self._compare_with_spy(combined_sector_exposure, spy_benchmark)

        return {
            'direct_holdings': direct_holdings_enriched,
            'etf_holdings': etf_holdings_enriched,
            'direct_sector_exposure': direct_sector_exposure,
            'etf_sector_exposure': etf_sector_exposure,
            'combined_sector_exposure': combined_sector_exposure,
            'spy_benchmark': spy_benchmark,
            'sector_comparison': sector_comparison,
            'total_equity_value': total_equity_value
        }

    def _enrich_direct_holdings(self, direct_equities: List, total_equity_value: float) -> List[Dict]:
        """Enrich direct equity holdings with FMP sector data."""
        enriched_holdings = []

        # Batch enrich all tickers
        ticker_data = self.fmp_service.batch_enrich_equities(direct_equities)

        for position in direct_equities:
            ticker = position.asset.ticker
            market_value = float(position.market_value)
            percentage = (market_value / total_equity_value * 100) if total_equity_value > 0 else 0

            # Get sector from FMP data
            sector = 'Unknown'
            company_name = position.asset.name
            if ticker and ticker in ticker_data:
                sector = ticker_data[ticker].get('sector', 'Unknown')
                company_name = ticker_data[ticker].get('company_name', position.asset.name)

            enriched_holdings.append({
                'ticker': ticker or 'N/A',
                'name': company_name,
                'sector': sector,
                'market_value': market_value,
                'percentage': round(percentage, 2),
                'quantity': float(position.quantity)
            })

        # Sort by market value descending
        enriched_holdings.sort(key=lambda x: x['market_value'], reverse=True)

        logger.info(f"Enriched {len(enriched_holdings)} direct equity holdings")
        return enriched_holdings

    def _enrich_etf_holdings(self, etfs: List, total_equity_value: float) -> List[Dict]:
        """Enrich ETF holdings with sector breakdown."""
        enriched_etfs = []

        for position in etfs:
            ticker = position.asset.ticker
            market_value = float(position.market_value)
            percentage = (market_value / total_equity_value * 100) if total_equity_value > 0 else 0

            # Get ETF sector weightings
            sector_breakdown = {}
            if ticker:
                sector_breakdown = self.fmp_service.get_etf_sector_weightings(ticker) or {}

            enriched_etfs.append({
                'ticker': ticker or 'N/A',
                'name': position.asset.name,
                'market_value': market_value,
                'percentage': round(percentage, 2),
                'quantity': float(position.quantity),
                'sector_breakdown': sector_breakdown
            })

        # Sort by market value descending
        enriched_etfs.sort(key=lambda x: x['market_value'], reverse=True)

        logger.info(f"Enriched {len(enriched_etfs)} ETF holdings")
        return enriched_etfs

    def _calculate_direct_sector_exposure(self, direct_holdings: List[Dict]) -> Dict[str, float]:
        """Calculate sector exposure from direct holdings."""
        sector_exposure = defaultdict(float)

        for holding in direct_holdings:
            sector = holding['sector']
            percentage = holding['percentage']
            sector_exposure[sector] += percentage

        return dict(sector_exposure)

    def _calculate_etf_sector_exposure(self, etf_holdings: List[Dict]) -> Dict[str, float]:
        """Calculate sector exposure from ETF holdings."""
        sector_exposure = defaultdict(float)

        for etf in etf_holdings:
            etf_percentage = etf['percentage']  # % of total equity portfolio
            sector_breakdown = etf['sector_breakdown']  # % within the ETF

            for sector, sector_weight in sector_breakdown.items():
                # Calculate indirect exposure: ETF% × Sector% within ETF
                indirect_exposure = (etf_percentage * sector_weight) / 100
                sector_exposure[sector] += indirect_exposure

        return dict(sector_exposure)

    def _calculate_combined_exposure(self, direct: Dict[str, float], etf: Dict[str, float]) -> Dict[str, Dict]:
        """Calculate combined sector exposure from direct and ETF holdings."""
        all_sectors = set(list(direct.keys()) + list(etf.keys()))
        combined = {}

        for sector in all_sectors:
            direct_exp = direct.get(sector, 0.0)
            etf_exp = etf.get(sector, 0.0)
            total_exp = direct_exp + etf_exp

            combined[sector] = {
                'direct': round(direct_exp, 2),
                'etf': round(etf_exp, 2),
                'total': round(total_exp, 2)
            }

        # Sort by total exposure descending
        combined = dict(sorted(combined.items(), key=lambda x: x[1]['total'], reverse=True))

        return combined

    def _compare_with_spy(self, combined_exposure: Dict[str, Dict], spy_benchmark: Dict[str, float]) -> Dict[str, Dict]:
        """Compare portfolio sector allocation with SPY benchmark."""
        all_sectors = set(list(combined_exposure.keys()) + list(spy_benchmark.keys()))
        comparison = {}

        for sector in all_sectors:
            portfolio_exp = combined_exposure.get(sector, {}).get('total', 0.0)
            spy_exp = spy_benchmark.get(sector, 0.0)
            difference = portfolio_exp - spy_exp

            comparison[sector] = {
                'direct': combined_exposure.get(sector, {}).get('direct', 0.0),
                'etf': combined_exposure.get(sector, {}).get('etf', 0.0),
                'total': portfolio_exp,
                'spy': round(spy_exp, 2),
                'diff': round(difference, 2),
                'overweight': difference > 0,
                'underweight': difference < 0
            }

        # Sort by total exposure descending
        comparison = dict(sorted(comparison.items(), key=lambda x: x[1]['total'], reverse=True))

        return comparison
