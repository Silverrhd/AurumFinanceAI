"""
Excel Export Service for AurumFinance
Generates Excel files for positions and transactions data export.
"""

import pandas as pd
import logging
from io import BytesIO
from decimal import Decimal
from typing import Tuple, List, Dict, Any, Optional
from datetime import datetime, date

from django.db import models
from ..models import Position, Transaction, PortfolioSnapshot, Client

logger = logging.getLogger(__name__)


class ExcelExportService:
    """Service for exporting portfolio data to Excel files."""
    
    def __init__(self):
        self.logger = logger
    
    def export_positions_excel(self, client_code: str, snapshot_date: str) -> Tuple[bytes, str]:
        """
        Export positions data to Excel format.
        
        Args:
            client_code: Client code ('ALL' for all clients, or specific code like 'BK')
            snapshot_date: Snapshot date in 'YYYY-MM-DD' format
            
        Returns:
            Tuple of (excel_bytes, filename)
        """
        try:
            self.logger.info(f"Starting positions export for client={client_code}, date={snapshot_date}")
            
            # Parse snapshot date
            snapshot_date_obj = datetime.strptime(snapshot_date, '%Y-%m-%d').date()
            
            # Build query based on client selection
            if client_code == "ALL":
                positions = Position.objects.filter(
                    snapshot__snapshot_date=snapshot_date_obj
                ).select_related('asset', 'snapshot__client').order_by('snapshot__client__code', 'asset__name')
                self.logger.info(f"Querying positions for ALL clients on {snapshot_date}")
            else:
                positions = Position.objects.filter(
                    snapshot__snapshot_date=snapshot_date_obj,
                    snapshot__client__code=client_code
                ).select_related('asset', 'snapshot__client').order_by('asset__name')
                self.logger.info(f"Querying positions for client {client_code} on {snapshot_date}")
            
            if not positions.exists():
                raise ValueError(f"No positions found for client={client_code} on date={snapshot_date}")
            
            # Build export data
            export_data = []
            for position in positions:
                # Calculate unrealized gain
                unrealized_gain_dollar = float(position.market_value - position.cost_basis)
                
                if position.cost_basis > 0:
                    unrealized_gain_percent = (unrealized_gain_dollar / float(position.cost_basis)) * 100
                else:
                    unrealized_gain_percent = 0.0
                
                # Format maturity date if exists
                maturity_date_str = position.asset.maturity_date.strftime('%d/%m/%Y') if position.asset.maturity_date else None
                
                row_data = {
                    'bank': position.bank or '',
                    'client': position.snapshot.client.code,
                    'account': position.account or '',
                    'asset_type': position.asset.asset_type or '',
                    'name': position.asset.name or '',
                    'cost_basis': float(position.cost_basis),
                    'market_value': float(position.market_value),
                    'quantity': float(position.quantity),
                    'price': float(position.price),
                    'ticker': position.asset.ticker or '',
                    'cusip': position.asset.cusip or '',
                    'coupon_rate': float(position.asset.coupon_rate) if position.asset.coupon_rate else None,
                    'maturity_date': maturity_date_str,
                    'unrealized_gain_dollar': unrealized_gain_dollar,
                    'unrealized_gain_percent': unrealized_gain_percent
                }
                export_data.append(row_data)
            
            self.logger.info(f"Processed {len(export_data)} positions for export")
            
            # Create DataFrame with specific column order
            columns_order = [
                'bank', 'client', 'account', 'asset_type', 'name', 'cost_basis', 
                'market_value', 'quantity', 'price', 'ticker', 'cusip', 
                'coupon_rate', 'maturity_date', 'unrealized_gain_dollar', 'unrealized_gain_percent'
            ]
            
            df = pd.DataFrame(export_data, columns=columns_order)
            
            # Generate filename
            date_formatted = snapshot_date_obj.strftime('%d_%m_%Y')
            filename = f"positions_{date_formatted}_{client_code}.xlsx"
            
            # Create Excel file in memory
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Positions', index=False)
                
                # Get the workbook and worksheet for basic formatting
                workbook = writer.book
                worksheet = writer.sheets['Positions']
                
                # Apply basic formatting (headers bold)
                from openpyxl.styles import Font
                header_font = Font(bold=True)
                for cell in worksheet[1]:  # First row (headers)
                    cell.font = header_font
                
                # Auto-adjust column widths
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 30)  # Cap at 30 characters
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            excel_bytes = excel_buffer.getvalue()
            self.logger.info(f"Generated Excel file: {filename} ({len(excel_bytes)} bytes)")
            
            return excel_bytes, filename
            
        except Exception as e:
            self.logger.error(f"Error exporting positions: {e}")
            raise
    
    def export_transactions_excel(self, client_code: str, start_date: str, end_date: str) -> Tuple[bytes, str]:
        """
        Export transactions data to Excel format.
        
        Args:
            client_code: Client code ('ALL' for all clients, or specific code like 'BK')
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
            
        Returns:
            Tuple of (excel_bytes, filename)
        """
        try:
            self.logger.info(f"Starting transactions export for client={client_code}, date_range={start_date} to {end_date}")
            
            # Parse dates
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
            
            # Build query based on client selection
            if client_code == "ALL":
                transactions = Transaction.objects.filter(
                    date__gte=start_date_obj,
                    date__lte=end_date_obj
                ).select_related('asset', 'client').order_by('client__code', 'date', 'asset__name')
                self.logger.info(f"Querying transactions for ALL clients from {start_date} to {end_date}")
            else:
                transactions = Transaction.objects.filter(
                    date__gte=start_date_obj,
                    date__lte=end_date_obj,
                    client__code=client_code
                ).select_related('asset', 'client').order_by('date', 'asset__name')
                self.logger.info(f"Querying transactions for client {client_code} from {start_date} to {end_date}")
            
            if not transactions.exists():
                raise ValueError(f"No transactions found for client={client_code} in date range {start_date} to {end_date}")
            
            # Build export data
            export_data = []
            for transaction in transactions:
                # Format date
                date_formatted = transaction.date.strftime('%d/%m/%Y')
                
                row_data = {
                    'bank': transaction.bank or '',
                    'client': transaction.client.code,
                    'account': transaction.account or '',
                    'date': date_formatted,
                    'transaction_type': transaction.transaction_type or '',
                    'cusip': transaction.asset.cusip if transaction.asset else '',
                    'quantity': float(transaction.quantity) if transaction.quantity else 0.0,
                    'price': float(transaction.price) if transaction.price else 0.0,
                    'amount': float(transaction.amount)
                }
                export_data.append(row_data)
            
            self.logger.info(f"Processed {len(export_data)} transactions for export")
            
            # Create DataFrame with specific column order
            columns_order = [
                'bank', 'client', 'account', 'date', 'transaction_type', 
                'cusip', 'quantity', 'price', 'amount'
            ]
            
            df = pd.DataFrame(export_data, columns=columns_order)
            
            # Generate filename
            start_formatted = start_date_obj.strftime('%d_%m_%Y')
            end_formatted = end_date_obj.strftime('%d_%m_%Y')
            filename = f"transactions_{start_formatted}-{end_formatted}_{client_code}.xlsx"
            
            # Create Excel file in memory
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Transactions', index=False)
                
                # Get the workbook and worksheet for basic formatting
                workbook = writer.book
                worksheet = writer.sheets['Transactions']
                
                # Apply basic formatting (headers bold)
                from openpyxl.styles import Font
                header_font = Font(bold=True)
                for cell in worksheet[1]:  # First row (headers)
                    cell.font = header_font
                
                # Auto-adjust column widths
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 30)  # Cap at 30 characters
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            excel_bytes = excel_buffer.getvalue()
            self.logger.info(f"Generated Excel file: {filename} ({len(excel_bytes)} bytes)")
            
            return excel_bytes, filename
            
        except Exception as e:
            self.logger.error(f"Error exporting transactions: {e}")
            raise
    
    def get_available_export_dates(self) -> Dict[str, Any]:
        """
        Get available dates for export functionality.
        
        Returns:
            Dictionary with snapshot_dates and transaction_date_range
        """
        try:
            # Get available snapshot dates
            snapshot_dates = list(
                PortfolioSnapshot.objects.values_list('snapshot_date', flat=True)
                .distinct()
                .order_by('-snapshot_date')
            )
            
            # Get transaction date range
            transaction_dates = Transaction.objects.aggregate(
                min_date=models.Min('date'),
                max_date=models.Max('date')
            )
            
            # Get available clients
            clients = list(
                Client.objects.values('id', 'code', 'name')
                .order_by('code')
            )
            
            # Format clients to match frontend interface
            formatted_clients = [
                {
                    'id': str(client['id']),
                    'client_code': client['code'],
                    'name': client['name']
                }
                for client in clients
            ]
            
            return {
                'snapshot_dates': [date.strftime('%Y-%m-%d') for date in snapshot_dates],
                'transaction_date_range': {
                    'min_date': transaction_dates['min_date'].strftime('%Y-%m-%d') if transaction_dates['min_date'] else None,
                    'max_date': transaction_dates['max_date'].strftime('%Y-%m-%d') if transaction_dates['max_date'] else None
                },
                'clients': formatted_clients
            }
            
        except Exception as e:
            self.logger.error(f"Error getting available export dates: {e}")
            raise