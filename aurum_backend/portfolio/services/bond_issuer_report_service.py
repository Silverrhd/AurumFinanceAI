"""
Bond Issuer Weight Report Service - 4-Tier Extraction Logic
Implements ProjectAurum's proven issuer extraction strategy
"""

import logging
from decimal import Decimal
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Optional

from django.db.models import Q
from portfolio.models import Client, PortfolioSnapshot, Position, Report
from .openfigi_service import OpenFIGIService
from .bond_name_parser import BondNameParser
from .enhanced_report_service import EnhancedReportService

logger = logging.getLogger(__name__)

class BondIssuerReportService(EnhancedReportService):
    """Service for generating Bond Issuer Weight reports."""
    
    def __init__(self):
        super().__init__()
        self.openfigi_service = OpenFIGIService()
        self.name_parser = BondNameParser()
        self.concentration_threshold = 20.0  # 20% like ProjectAurum
    
    def generate_bond_issuer_weight_report(self, client_code: str) -> str:
        """
        Generate Bond Issuer Weight Report using 4-tier extraction.
        Uses latest available snapshot data.
        """
        logger.info(f"Generating Bond Issuer Weight report for {client_code}")
        
        try:
            # Get client and latest snapshot
            client = Client.objects.get(code=client_code)
            snapshot = PortfolioSnapshot.objects.filter(
                client=client
            ).order_by('-snapshot_date').first()
            
            if not snapshot:
                raise ValueError(f"No portfolio data found for {client_code}")
            
            # CRITICAL: Filter ONLY actual bonds (Fixed Income with maturity dates)
            # This excludes ETFs and mutual funds which don't have maturity dates
            bond_positions = snapshot.positions.select_related('asset').filter(
                asset__asset_type='Fixed Income',
                asset__maturity_date__isnull=False
            )
            
            if not bond_positions.exists():
                logger.warning(f"No bond positions found for {client_code} (Fixed Income with maturity dates)")
                return self._render_empty_report(client, snapshot)
            
            # Group bonds by issuer using 4-tier extraction
            issuers_data = self._group_bonds_by_issuer_4tier(bond_positions)
            
            # Calculate rankings and percentages
            total_bond_value = sum(
                Decimal(str(issuer['market_value'])) 
                for issuer in issuers_data.values()
            )
            
            ranked_issuers = self._calculate_rankings_and_percentages(
                issuers_data, total_bond_value
            )
            
            # Perform concentration risk analysis
            risk_analysis = self._calculate_risk_analysis(
                ranked_issuers, self.concentration_threshold
            )
            
            # Prepare template context
            context = {
                'client_name': client.name,
                'client_code': client_code,
                'report_date': snapshot.snapshot_date.strftime('%Y-%m-%d'),
                'issuers': ranked_issuers,
                'risk_analysis': risk_analysis,
                'total_bond_value': float(total_bond_value),
                'generation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'logo_base64': self._get_logo_base64()
            }
            
            # Render template
            template = self.jinja_env.get_template('weighted_bond_issuer_template.html')
            html_content = template.render(context)
            
            logger.info(f"Bond Issuer Weight report generated for {client_code}")
            return html_content
            
        except Exception as e:
            logger.error(f"Error generating bond issuer report for {client_code}: {e}")
            raise
    
    def _group_bonds_by_issuer_4tier(self, bond_positions) -> Dict:
        """
        Group bond positions by issuer using 4-tier extraction strategy.
        
        Tier 1: OpenFIGI CUSIP lookup (cached)
        Tier 2: Fresh CUSIP lookup (retry)
        Tier 3: Bond name pattern parsing
        Tier 4: Use full bond name (last resort)
        """
        issuers_data = defaultdict(lambda: {
            'issuer': '',
            'market_value': Decimal('0.00'),
            'face_value': Decimal('0.00'),
            'num_bonds': 0,
            'extraction_method': '',
            'bond_names': [],
            'bonds': []  # NEW: Store individual bond details for expandable rows
        })
        
        extraction_stats = {'tier1_2': 0, 'tier3': 0, 'tier4': 0}
        
        for position in bond_positions:
            asset = position.asset
            issuer_name = None
            extraction_method = 'unknown'
            
            # TIER 1 & 2: OpenFIGI CUSIP lookup (includes retry logic)
            if asset.cusip:
                issuer_name = self.openfigi_service.get_issuer_by_cusip(asset.cusip)
                if issuer_name:
                    extraction_method = 'openfigi'
                    extraction_stats['tier1_2'] += 1
            
            # TIER 3: Bond name pattern parsing (fallback)
            if not issuer_name:
                issuer_name = self.name_parser.extract_issuer_from_name(asset.name)
                if issuer_name:
                    extraction_method = 'name_pattern'
                    extraction_stats['tier3'] += 1
            
            # TIER 4: Use full bond name (last resort)
            if not issuer_name:
                issuer_name = asset.name
                extraction_method = 'full_name'
                extraction_stats['tier4'] += 1
            
            # Aggregate data by issuer
            issuer_data = issuers_data[issuer_name]
            issuer_data['issuer'] = issuer_name
            issuer_data['market_value'] += Decimal(str(position.market_value))
            issuer_data['face_value'] += Decimal(str(position.quantity))  # Face value
            issuer_data['num_bonds'] += 1
            issuer_data['extraction_method'] = extraction_method
            issuer_data['bond_names'].append(asset.name)
            
            # NEW: Store individual bond details for expandable functionality
            bond_detail = {
                'name': asset.name,
                'market_value': float(position.market_value),
                'face_value': float(position.quantity), 
                'coupon_rate': f"{position.coupon_rate or asset.coupon_rate or 0}%" if (position.coupon_rate or asset.coupon_rate) else "0.00%",
                'maturity_date': asset.maturity_date.strftime('%Y-%m-%d') if asset.maturity_date else 'N/A',
                'estimated_annual_income': float(position.estimated_annual_income or 0),
                'custody': f"{asset.bank} {asset.account}".strip() or 'Unknown'
            }
            issuer_data['bonds'].append(bond_detail)
        
        logger.info(f"Extraction stats: {extraction_stats}")
        return dict(issuers_data)
    
    def _calculate_rankings_and_percentages(self, issuers_data: Dict, total_bond_value: Decimal) -> List[Dict]:
        """Calculate rankings and weight percentages."""
        ranked_issuers = []
        
        for issuer_data in issuers_data.values():
            market_value = issuer_data['market_value']
            weight_pct = (market_value / total_bond_value * 100) if total_bond_value > 0 else 0
            
            ranked_issuers.append({
                'issuer': issuer_data['issuer'],
                'market_value': float(market_value),
                'face_value': float(issuer_data['face_value']),
                'num_bonds': issuer_data['num_bonds'],
                'weight_pct': float(weight_pct),
                'extraction_method': issuer_data['extraction_method'],
                'bonds': issuer_data['bonds']  # NEW: Include individual bond details
            })
        
        # Sort by market value descending
        ranked_issuers.sort(key=lambda x: x['market_value'], reverse=True)
        
        # Add rankings
        for i, issuer in enumerate(ranked_issuers, 1):
            issuer['rank'] = i
        
        return ranked_issuers
    
    def _calculate_risk_analysis(self, ranked_issuers: List[Dict], concentration_threshold: float) -> Dict:
        """Calculate concentration risk analysis with 20% threshold."""
        high_concentration_issuers = [
            issuer for issuer in ranked_issuers
            if issuer['weight_pct'] >= concentration_threshold
        ]
        
        max_concentration = ranked_issuers[0] if ranked_issuers else None
        
        return {
            'concentration_threshold': concentration_threshold,
            'high_concentration_count': len(high_concentration_issuers),
            'high_concentration_issuers': high_concentration_issuers,
            'max_concentration': max_concentration
        }
    
    def _render_empty_report(self, client: Client, snapshot: PortfolioSnapshot) -> str:
        """Render empty report when no Fixed Income positions found."""
        context = {
            'client_name': client.name,
            'client_code': client.code,
            'report_date': snapshot.snapshot_date.strftime('%Y-%m-%d'),
            'issuers': [],
            'risk_analysis': {
                'concentration_threshold': self.concentration_threshold,
                'high_concentration_count': 0,
                'high_concentration_issuers': [],
                'max_concentration': None
            },
            'total_bond_value': 0.0,
            'generation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'logo_base64': self._get_logo_base64(),
            'empty_report': True
        }
        
        template = self.jinja_env.get_template('weighted_bond_issuer_template.html')
        return template.render(context)    

    def _get_logo_base64(self) -> str:
        """Get base64 encoded logo for reports."""
        try:
            import base64
            import os
            
            # Try to find logo file
            logo_paths = [
                os.path.join(os.path.dirname(__file__), '..', '..', 'static', 'images', 'logo.png'),
                os.path.join(os.path.dirname(__file__), '..', '..', 'static', 'logo.png'),
                os.path.join(os.path.dirname(__file__), '..', '..', 'templates', 'logo.png'),
            ]
            
            for logo_path in logo_paths:
                if os.path.exists(logo_path):
                    with open(logo_path, 'rb') as logo_file:
                        return base64.b64encode(logo_file.read()).decode('utf-8')
            
            # No logo found
            return ''
            
        except Exception as e:
            logger.warning(f"Could not load logo: {e}")
            return ''