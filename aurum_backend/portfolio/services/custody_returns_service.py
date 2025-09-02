"""
Monthly Returns by Custody Service for AurumFinance.
Calculates Modified Dietz returns at custody level (Client + Bank + Account).
"""

import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Union
from django.db.models import Sum, Q
from django.db import models
from ..models import Position, Transaction, Client
from .modified_dietz_service import ModifiedDietzService
from .cash_flow_service import CashFlowService

logger = logging.getLogger(__name__)


class CustodyReturnsService:
    """
    Calculate Modified Dietz returns at custody level for monthly reports.
    Builds on ModifiedDietzService with bank+account filtering.
    """
    
    def __init__(self):
        """Initialize custody returns calculator."""
        self.dietz_service = ModifiedDietzService()
        self.cash_flow_service = CashFlowService()
        logger.debug("CustodyReturnsService initialized")
    
    def get_client_custodies(self, client_code: str, end_date: date) -> List[Dict]:
        """
        Get all custodies (bank + account combinations) for a client.
        
        Args:
            client_code: Client code (e.g., 'JAV', 'IZ')
            end_date: Date to check for existing positions
            
        Returns:
            List of custody dicts with bank and account
        """
        try:
            custodies = Position.objects.filter(
                snapshot__client__code=client_code,
                snapshot__snapshot_date=end_date
            ).values('bank', 'account').distinct().order_by('bank', 'account')
            
            custody_list = []
            for custody in custodies:
                custody_name = f"{custody['bank']} {custody['account']}"
                custody_list.append({
                    'bank': custody['bank'],
                    'account': custody['account'],
                    'custody_name': custody_name
                })
            
            logger.debug(f"Found {len(custody_list)} custodies for client {client_code}")
            return custody_list
            
        except Exception as e:
            logger.error(f"Error getting custodies for {client_code}: {str(e)}")
            return []
    
    def calculate_custody_return(self, client: str, bank: str, account: str, 
                               start_date: date, end_date: date) -> Dict[str, Any]:
        """
        Calculate Modified Dietz return for a specific custody.
        
        Args:
            client: Client code
            bank: Bank name
            account: Account name
            start_date: Start date of period
            end_date: End date of period
            
        Returns:
            Dict with custody return data
        """
        try:
            # Get period length
            period_days = (end_date - start_date).days
            if period_days <= 0:
                logger.warning(f"Invalid period: {start_date} to {end_date}")
                return self._empty_custody_result(client, bank, account)
            
            # Get portfolio values for this custody only
            start_value = self._get_custody_value_on_date(client, bank, account, start_date)
            end_value = self._get_custody_value_on_date(client, bank, account, end_date)
            
            # Get transactions for this custody during period
            transactions = self._get_custody_transactions_for_period(
                client, bank, account, start_date, end_date
            )
            
            # Calculate external cash flows
            net_external_flows = 0.0
            weighted_external_flows = 0.0
            external_flow_count = 0
            
            for tx in transactions:
                if self.cash_flow_service.is_external_cash_flow(tx):
                    cf_amount = self.cash_flow_service.get_cash_flow_amount(tx)
                    
                    # Calculate time weight
                    tx_date = tx.date
                    if isinstance(tx_date, datetime):
                        tx_date = tx_date.date()
                    
                    days_from_start = (tx_date - start_date).days
                    weight = (period_days - days_from_start) / period_days
                    
                    net_external_flows += cf_amount
                    weighted_external_flows += cf_amount * weight
                    external_flow_count += 1
            
            # Calculate Modified Dietz return
            gain_or_loss = end_value - start_value - net_external_flows
            average_capital = start_value + weighted_external_flows
            
            if average_capital == 0:
                return_percentage = 0.0
            else:
                return_percentage = (gain_or_loss / average_capital) * 100
            
            custody_name = f"{bank} {account}"
            
            logger.debug(f"Custody {custody_name}: {return_percentage:.2f}% return "
                        f"(${gain_or_loss:,.2f} gain on ${average_capital:,.2f} avg capital)")
            
            return {
                'client': client,
                'bank': bank,
                'account': account,
                'custody_name': custody_name,
                'start_value': start_value,
                'end_value': end_value,
                'returns_dollar': gain_or_loss,
                'returns_percentage': return_percentage,
                'net_external_flows': net_external_flows,
                'external_flow_count': external_flow_count
            }
            
        except Exception as e:
            logger.error(f"Error calculating custody return for {client} {bank} {account}: {str(e)}")
            return self._empty_custody_result(client, bank, account)
    
    def generate_client_monthly_returns(self, client_code: str, year: int, month: int) -> Dict[str, Any]:
        """
        Generate monthly returns data for a single client's custodies.
        
        Args:
            client_code: Client code (e.g., 'JAV')
            year: Year (e.g., 2025)
            month: Month (1-12)
            
        Returns:
            Dict with client data and custody returns
        """
        try:
            # Calculate start and end dates
            end_date = self._get_month_end_date(year, month)
            start_date = self._get_previous_month_end_date(year, month)
            
            # Get client info
            client = Client.objects.get(code=client_code)
            
            # Get all custodies for this client
            custodies = self.get_client_custodies(client_code, end_date)
            
            # Calculate returns for each custody
            custody_returns = []
            total_returns_dollar = 0.0
            
            for custody in custodies:
                custody_result = self.calculate_custody_return(
                    client_code, 
                    custody['bank'], 
                    custody['account'], 
                    start_date, 
                    end_date
                )
                # Format values for template display
                custody_result['returns_dollar_formatted'] = f"{custody_result['returns_dollar']:,.2f}"
                custody_result['returns_percentage_formatted'] = f"{custody_result['returns_percentage']:.2f}"
                
                custody_returns.append(custody_result)
                total_returns_dollar += custody_result['returns_dollar']
            
            # Calculate overall client return percentage (for verification)
            client_start_value = sum(c['start_value'] for c in custody_returns)
            client_return_pct = (total_returns_dollar / client_start_value * 100) if client_start_value > 0 else 0.0
            
            return {
                'client_code': client_code,
                'client_name': client.name,
                'year': year,
                'month': month,
                'month_name': date(year, month, 1).strftime('%B'),
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'custody_returns': custody_returns,
                'total_returns_dollar': total_returns_dollar,
                'total_returns_dollar_formatted': f"{total_returns_dollar:,.2f}",
                'total_returns_percentage': client_return_pct,
                'total_returns_percentage_formatted': f"{client_return_pct:.2f}",
                'total_custodies': len(custody_returns)
            }
            
        except Exception as e:
            logger.error(f"Error generating monthly returns for {client_code}: {str(e)}")
            return {
                'client_code': client_code,
                'error': str(e)
            }
    
    def generate_consolidated_monthly_returns(self, year: int, month: int) -> Dict[str, Any]:
        """
        Generate consolidated monthly returns for all clients.
        
        Args:
            year: Year (e.g., 2025)
            month: Month (1-12)
            
        Returns:
            Dict with all clients' returns and custody breakdowns
        """
        try:
            end_date = self._get_month_end_date(year, month)
            
            # Get all clients with positions on end date
            clients = Client.objects.filter(
                snapshots__snapshot_date=end_date
            ).exclude(code='ALL').distinct().order_by('code')
            
            all_clients_data = []
            grand_total_dollar = 0.0
            
            for client in clients:
                client_data = self.generate_client_monthly_returns(client.code, year, month)
                if 'error' not in client_data and client_data['custody_returns']:
                    all_clients_data.append(client_data)
                    grand_total_dollar += client_data['total_returns_dollar']
            
            return {
                'year': year,
                'month': month,
                'month_name': date(year, month, 1).strftime('%B'),
                'clients_data': all_clients_data,
                'total_clients': len(all_clients_data),
                'grand_total_returns_dollar': grand_total_dollar,
                'grand_total_returns_dollar_formatted': f"{grand_total_dollar:,.2f}",
                'report_date': end_date.strftime('%Y-%m-%d')
            }
            
        except Exception as e:
            logger.error(f"Error generating consolidated monthly returns: {str(e)}")
            return {'error': str(e)}
    
    def render_single_client_template(self, report_data: Dict[str, Any]) -> str:
        """
        Render HTML template for single client custody returns.
        Simple static table, no expandable rows.
        """
        from django.template.loader import render_to_string
        
        try:
            return render_to_string('monthly_returns_custody_single.html', report_data)
        except Exception as e:
            logger.error(f"Error rendering single client template: {str(e)}")
            return f"<html><body><h1>Error</h1><p>Failed to render report: {str(e)}</p></body></html>"
    
    def render_consolidated_template(self, report_data: Dict[str, Any]) -> str:
        """
        Render HTML template for consolidated custody returns.
        Expandable table like cash position report.
        """
        from django.template.loader import render_to_string
        
        try:
            return render_to_string('monthly_returns_custody_consolidated.html', report_data)
        except Exception as e:
            logger.error(f"Error rendering consolidated template: {str(e)}")
            return f"<html><body><h1>Error</h1><p>Failed to render report: {str(e)}</p></body></html>"
    
    def _get_custody_value_on_date(self, client: str, bank: str, account: str, target_date: date) -> float:
        """Get portfolio value for specific custody on date."""
        try:
            total_value = Position.objects.filter(
                snapshot__client__code=client,
                snapshot__snapshot_date=target_date,
                bank=bank,
                account=account
            ).aggregate(total=Sum('market_value'))['total']
            
            return float(total_value) if total_value else 0.0
            
        except Exception as e:
            logger.error(f"Error getting custody value for {client} {bank} {account} on {target_date}: {str(e)}")
            return 0.0
    
    def _get_custody_transactions_for_period(self, client: str, bank: str, account: str, 
                                           start_date: date, end_date: date) -> List[Transaction]:
        """Get transactions for specific custody during period."""
        try:
            transactions = Transaction.objects.filter(
                client__code=client,
                bank=bank,
                account=account,
                date__gte=start_date,
                date__lte=end_date
            ).select_related('asset').order_by('date')
            
            return list(transactions)
            
        except Exception as e:
            logger.error(f"Error getting custody transactions for {client} {bank} {account}: {str(e)}")
            return []
    
    def _get_month_end_date(self, year: int, month: int) -> date:
        """Get closest available snapshot date to month-end."""
        from ..models import PortfolioSnapshot
        
        # Get all snapshots for the target month, closest to end first
        snapshots = PortfolioSnapshot.objects.filter(
            snapshot_date__year=year,
            snapshot_date__month=month
        ).values_list('snapshot_date', flat=True).distinct().order_by('-snapshot_date')
        
        if snapshots:
            return snapshots[0]  # Return the latest date in the month
        
        # Fallback to calendar month-end if no data
        if month == 12:
            next_month = date(year + 1, 1, 1)
        else:
            next_month = date(year, month + 1, 1)
        return next_month - timedelta(days=1)
    
    def _get_previous_month_end_date(self, year: int, month: int) -> date:
        """Get closest available snapshot date to previous month-end."""
        from ..models import PortfolioSnapshot
        
        # Calculate previous month
        if month == 1:
            prev_year, prev_month = year - 1, 12
        else:
            prev_year, prev_month = year, month - 1
        
        # Get all snapshots for the previous month, closest to end first
        snapshots = PortfolioSnapshot.objects.filter(
            snapshot_date__year=prev_year,
            snapshot_date__month=prev_month
        ).values_list('snapshot_date', flat=True).distinct().order_by('-snapshot_date')
        
        if snapshots:
            return snapshots[0]  # Return the latest date in the previous month
        
        # Fallback to calendar month-end if no data
        if prev_month == 12:
            next_month = date(prev_year + 1, 1, 1)
        else:
            next_month = date(prev_year, prev_month + 1, 1)
        return next_month - timedelta(days=1)
    
    def _empty_custody_result(self, client: str, bank: str, account: str) -> Dict[str, Any]:
        """Return empty result for failed calculations."""
        return {
            'client': client,
            'bank': bank,
            'account': account,
            'custody_name': f"{bank} {account}",
            'start_value': 0.0,
            'end_value': 0.0,
            'returns_dollar': 0.0,
            'returns_percentage': 0.0,
            'net_external_flows': 0.0,
            'external_flow_count': 0
        }