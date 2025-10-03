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
        # Pass FMP service to ETF detector for API-based detection
        self.etf_detector = ETFDetector(fmp_service=self.fmp_service)

    def analyze_equity_portfolio(self, positions) -> Dict:
        """
        Main analysis method with ETF look-through disaggregation.

        Args:
            positions: QuerySet of Position objects (Equities only, ALTs excluded)

        Returns:
            {
                'aggregated_holdings': [...],  # Stock-level view with direct + ETF contributions
                'direct_holdings': [...],  # DEPRECATED: kept for backward compatibility
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

        # Analyze ETF holdings with sector breakdown
        etf_holdings_enriched = self._enrich_etf_holdings(etfs, total_equity_value)

        # NEW: Perform ETF look-through disaggregation
        aggregated_holdings = self._aggregate_with_etf_lookthrough(
            direct_equities,
            etfs,
            total_equity_value
        )

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
            'aggregated_holdings': aggregated_holdings,  # NEW: Look-through view
            'direct_holdings': direct_holdings_enriched,  # Kept for backward compatibility
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

    def _aggregate_with_etf_lookthrough(self, direct_equities: List, etfs: List, total_equity_value: float) -> List[Dict]:
        """
        Aggregate stock holdings with ETF look-through disaggregation.

        This method "looks through" ETFs to show the underlying stock holdings,
        combining them with any direct holdings of the same stock.

        Args:
            direct_equities: List of direct stock positions
            etfs: List of ETF positions
            total_equity_value: Total portfolio equity value

        Returns:
            List of aggregated holdings with structure:
            [
                {
                    'ticker': 'AAPL',
                    'name': 'Apple Inc',
                    'sector': 'Technology',
                    'direct_value': 100000,
                    'direct_shares': 100,
                    'etf_value': 50000,
                    'etf_contributions': [
                        {'etf_ticker': 'SPY', 'etf_name': 'S&P 500 ETF', 'value': 30000, 'weight': 6.7},
                        {'etf_ticker': 'QQQ', 'etf_name': 'Nasdaq ETF', 'value': 20000, 'weight': 8.2}
                    ],
                    'total_value': 150000,
                    'percentage': 5.5
                },
                ...
            ]
        """
        logger.info("Performing ETF look-through disaggregation...")

        aggregated_map = {}  # ticker -> aggregated data

        # Step 1: Add direct holdings to aggregated map
        for position in direct_equities:
            ticker = position.asset.ticker or position.asset.name
            if not ticker:
                continue

            ticker = ticker.upper().strip()

            # Get sector from FMP
            profile = self.fmp_service.get_company_profile(ticker)
            sector = profile.get('sector', 'Unknown') if profile else 'Unknown'
            company_name = profile.get('company_name', position.asset.name) if profile else position.asset.name

            aggregated_map[ticker] = {
                'ticker': ticker,
                'name': company_name,
                'sector': sector,
                'direct_value': float(position.market_value),
                'direct_shares': float(position.quantity),
                'etf_value': 0.0,
                'etf_contributions': [],
                'total_value': 0.0,
                'percentage': 0.0
            }

        # Step 2: For each ETF, get holdings and distribute to aggregated map
        for etf_position in etfs:
            etf_ticker = etf_position.asset.ticker
            if not etf_ticker:
                logger.warning(f"Skipping ETF without ticker: {etf_position.asset.name}")
                continue

            etf_value = float(etf_position.market_value)
            etf_name = etf_position.asset.name

            # Get ETF holdings from FMP API
            etf_holdings = self.fmp_service.get_etf_holdings(etf_ticker)

            if not etf_holdings:
                logger.warning(f"No holdings data found for ETF {etf_ticker}")
                continue

            # Use Top 10 holdings only for performance and practical relevance
            original_count = len(etf_holdings)
            if original_count > 10:
                etf_holdings = etf_holdings[:10]
                logger.info(f"Using top 10 holdings for {etf_ticker} (out of {original_count} total)")
            else:
                logger.info(f"Processing {original_count} holdings from {etf_ticker}")

            # Distribute ETF value across underlying holdings
            for holding in etf_holdings:
                stock_ticker = holding.get('symbol', '').upper().strip()
                if not stock_ticker:
                    continue

                weight = holding.get('weight', 0) / 100.0  # Convert percentage to decimal
                indirect_value = etf_value * weight

                # Initialize or update aggregated holding
                if stock_ticker not in aggregated_map:
                    # Get sector for this stock
                    profile = self.fmp_service.get_company_profile(stock_ticker)
                    sector = profile.get('sector', 'Unknown') if profile else 'Unknown'
                    stock_name = holding.get('name', stock_ticker)

                    aggregated_map[stock_ticker] = {
                        'ticker': stock_ticker,
                        'name': stock_name,
                        'sector': sector,
                        'direct_value': 0.0,
                        'direct_shares': 0.0,
                        'etf_value': 0.0,
                        'etf_contributions': [],
                        'total_value': 0.0,
                        'percentage': 0.0
                    }

                # Add ETF contribution
                aggregated_map[stock_ticker]['etf_value'] += indirect_value
                aggregated_map[stock_ticker]['etf_contributions'].append({
                    'etf_ticker': etf_ticker,
                    'etf_name': etf_name,
                    'value': indirect_value,
                    'weight': round(weight * 100, 2)
                })

        # Step 3: Calculate totals and percentages
        aggregated_holdings = []
        for ticker, data in aggregated_map.items():
            data['total_value'] = data['direct_value'] + data['etf_value']
            data['percentage'] = round((data['total_value'] / total_equity_value * 100), 2) if total_equity_value > 0 else 0.0
            aggregated_holdings.append(data)

        # Step 4: Sort by total value descending
        aggregated_holdings.sort(key=lambda x: x['total_value'], reverse=True)

        logger.info(f"Aggregated into {len(aggregated_holdings)} unique stock holdings")

        return aggregated_holdings
