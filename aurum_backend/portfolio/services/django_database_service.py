"""
Django Database Service for AurumFinance Portfolio Management.
Replaces SQLite database operations with Django ORM operations.
"""

from django.db import transaction
from django.core.exceptions import ObjectDoesNotExist
from ..models import Client, Asset, PortfolioSnapshot, Position, Transaction
import logging
from typing import Optional, Dict, List, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class DjangoPortfolioDatabase:
    """
    Django ORM-based database service for portfolio data.
    Replaces the SQLite-based portfolio_database functionality.
    """

    @transaction.atomic
    def save_snapshot(self, snapshot_date: str, assets_data: List[Dict], positions_data: List[Dict], 
                     transactions_data: List[Dict], portfolio_metrics: Dict, client_code: str) -> PortfolioSnapshot:
        """
        Save portfolio snapshot to Django models.
        
        Args:
            snapshot_date: Date string in YYYY-MM-DD format
            assets_data: List of asset dictionaries
            positions_data: List of position dictionaries  
            transactions_data: List of transaction dictionaries
            portfolio_metrics: Portfolio-level metrics and calculations
            client_code: Client identifier code
            
        Returns:
            PortfolioSnapshot: The created/updated snapshot instance
        """
        logger.info(f"Saving snapshot for client {client_code} on date {snapshot_date}")
        
        # Get or create client
        client, created = Client.objects.get_or_create(
            code=client_code,
            defaults={'name': f'Client {client_code}'}
        )
        if created:
            logger.info(f"Created new client: {client_code}")

        # Create or update snapshot
        snapshot, created = PortfolioSnapshot.objects.get_or_create(
            client=client,
            snapshot_date=snapshot_date,
            defaults={'portfolio_metrics': portfolio_metrics or {}}
        )
        
        if not created:
            # Update existing snapshot
            snapshot.portfolio_metrics = portfolio_metrics or {}
            snapshot.save()
            logger.info(f"Updated existing snapshot for {client_code} on {snapshot_date}")
        else:
            logger.info(f"Created new snapshot for {client_code} on {snapshot_date}")

        # Save assets
        if assets_data:
            self._save_assets(assets_data, client_code)
            logger.info(f"Saved {len(assets_data)} assets")

        # Save positions (clear existing first)
        if positions_data:
            Position.objects.filter(snapshot=snapshot).delete()
            self._save_positions(snapshot, positions_data)
            logger.info(f"Saved {len(positions_data)} positions")

        # Save transactions (accumulate, don't replace)
        if transactions_data:
            self._save_transactions(client, transactions_data)
            logger.info(f"Saved {len(transactions_data)} transactions")

        return snapshot

    def _save_assets(self, assets_data: List[Dict], client_code: str) -> None:
        """Save asset data to Django models with custody-based uniqueness."""
        for asset_data in assets_data:
            ticker = asset_data.get('ticker')
            if not ticker:
                logger.warning(f"Skipping asset with no ticker: {asset_data}")
                continue
                
            # Use custody-based uniqueness
            bank = asset_data.get('bank', '')
            account = asset_data.get('account', '')
            
            # Create or update asset with custody-based uniqueness
            asset, created = Asset.objects.get_or_create(
                ticker=ticker,
                bank=bank,
                account=account,
                client=client_code,
                defaults={
                    'name': asset_data.get('name', ''),
                    'asset_type': asset_data.get('asset_type', ''),
                    'currency': asset_data.get('currency', 'USD'),
                    'isin': asset_data.get('isin'),
                    'cusip': asset_data.get('cusip'),
                    'coupon_rate': asset_data.get('coupon_rate'),
                    'maturity_date': asset_data.get('maturity_date')
                }
            )
            
            if not created:
                # Update existing asset with new data
                asset.name = asset_data.get('name', asset.name)
                asset.asset_type = asset_data.get('asset_type', asset.asset_type)
                asset.currency = asset_data.get('currency', asset.currency)
                asset.isin = asset_data.get('isin', asset.isin)
                asset.cusip = asset_data.get('cusip', asset.cusip)
                asset.coupon_rate = asset_data.get('coupon_rate', asset.coupon_rate)
                asset.maturity_date = asset_data.get('maturity_date', asset.maturity_date)
                asset.save()

    def _save_positions(self, snapshot: PortfolioSnapshot, positions_data: List[Dict]) -> None:
        """Save position data to Django models."""
        for pos_data in positions_data:
            ticker = pos_data.get('ticker')
            if not ticker:
                logger.warning(f"Skipping position with no ticker: {pos_data}")
                continue
                
            # Use custody-based asset matching
            bank = pos_data.get('bank', '')
            account = pos_data.get('account', '')
            client_code = snapshot.client.code
            
            try:
                # Try to find asset with custody-based matching
                asset = Asset.objects.get(
                    ticker=ticker,
                    bank=bank,
                    account=account,
                    client=client_code
                )
                
                position, created = Position.objects.get_or_create(
                    snapshot=snapshot,
                    asset=asset,
                    defaults={
                        'quantity': pos_data.get('quantity', 0),
                        'market_value': pos_data.get('market_value', 0),
                        'cost_basis': pos_data.get('cost_basis', 0),
                        'price': pos_data.get('price', 0),
                        'bank': pos_data.get('bank', ''),
                        'account': pos_data.get('account', ''),
                        'yield_pct': pos_data.get('yield_pct'),
                        'coupon_rate': pos_data.get('coupon_rate'),
                        'maturity_date': pos_data.get('maturity_date')
                    }
                )
                
                if not created:
                    # Update existing position with new data
                    position.quantity = pos_data.get('quantity', position.quantity)
                    position.market_value = pos_data.get('market_value', position.market_value)
                    position.cost_basis = pos_data.get('cost_basis', position.cost_basis)
                    position.price = pos_data.get('price', position.price)
                    position.bank = pos_data.get('bank', position.bank)
                    position.account = pos_data.get('account', position.account)
                    position.yield_pct = pos_data.get('yield_pct', position.yield_pct)
                    position.coupon_rate = pos_data.get('coupon_rate', position.coupon_rate)
                    position.maturity_date = pos_data.get('maturity_date', position.maturity_date)
                    position.save()
            except Asset.DoesNotExist:
                logger.error(f"Asset not found for ticker: {ticker}, bank: {bank}, account: {account}, client: {client_code}")
                continue

    def _save_transactions(self, client: Client, transactions_data: List[Dict]) -> None:
        """Save transaction data to Django models with custody-based asset matching."""
        for tx_data in transactions_data:
            ticker = tx_data.get('ticker')
            transaction_id = tx_data.get('transaction_id', tx_data.get('id', ''))
            
            if not ticker:
                logger.warning(f"Skipping transaction with no ticker: {tx_data}")
                continue
            
            # Use custody-based asset matching
            bank = tx_data.get('bank', '')
            account = tx_data.get('account', '')
            cusip = tx_data.get('cusip', '')
            client_code = client.code
            
            # Try to find existing asset with custody-based matching
            asset = None
            
            # First try: exact match with ticker + bank + account + client
            if bank and account:
                try:
                    asset = Asset.objects.get(
                        ticker=ticker,
                        bank=bank,
                        account=account,
                        client=client_code
                    )
                except Asset.DoesNotExist:
                    pass
            
            # Second try: match by CUSIP if available
            if not asset and cusip:
                try:
                    asset = Asset.objects.filter(
                        cusip=cusip,
                        client=client_code
                    ).first()
                except Asset.DoesNotExist:
                    pass
            
            # Third try: match by ticker only within client
            if not asset:
                try:
                    asset = Asset.objects.filter(
                        ticker=ticker,
                        client=client_code
                    ).first()
                except Asset.DoesNotExist:
                    pass
            
            if not asset:
                logger.error(f"Asset not found for ticker: {ticker}, bank: {bank}, account: {account}, client: {client_code}")
                continue
                
            # Use get_or_create to avoid duplicates
            transaction_obj, created = Transaction.objects.get_or_create(
                transaction_id=transaction_id,
                client=client,
                defaults={
                    'asset': asset,
                    'date': tx_data.get('date'),
                    'transaction_type': tx_data.get('transaction_type', ''),
                    'quantity': tx_data.get('quantity'),
                    'price': tx_data.get('price'),
                    'amount': tx_data.get('amount', 0),
                    'bank': tx_data.get('bank', ''),
                    'account': tx_data.get('account', '')
                }
            )
            
            if not created:
                # Update existing transaction
                transaction_obj.asset = asset
                transaction_obj.date = tx_data.get('date', transaction_obj.date)
                transaction_obj.transaction_type = tx_data.get('transaction_type', transaction_obj.transaction_type)
                transaction_obj.quantity = tx_data.get('quantity', transaction_obj.quantity)
                transaction_obj.price = tx_data.get('price', transaction_obj.price)
                transaction_obj.amount = tx_data.get('amount', transaction_obj.amount)
                transaction_obj.bank = tx_data.get('bank', transaction_obj.bank)
                transaction_obj.account = tx_data.get('account', transaction_obj.account)
                transaction_obj.save()

    def get_snapshot(self, snapshot_date: str, client_code: str) -> Optional[Dict]:
        """
        Retrieve snapshot data from Django models.
        
        Args:
            snapshot_date: Date string in YYYY-MM-DD format
            client_code: Client identifier code
            
        Returns:
            Dict containing snapshot data or None if not found
        """
        try:
            client = Client.objects.get(code=client_code)
            snapshot = PortfolioSnapshot.objects.get(
                client=client,
                snapshot_date=snapshot_date
            )

            positions = Position.objects.filter(snapshot=snapshot).select_related('asset')
            transactions = Transaction.objects.filter(client=client).select_related('asset')

            return {
                'snapshot': snapshot,
                'positions': positions,
                'transactions': transactions,
                'portfolio_metrics': snapshot.portfolio_metrics
            }
        except (Client.DoesNotExist, PortfolioSnapshot.DoesNotExist) as e:
            logger.warning(f"Snapshot not found for client {client_code} on {snapshot_date}: {e}")
            return None

    def get_most_recent_snapshot_date(self, client_code: Optional[str] = None) -> Optional[str]:
        """
        Get most recent snapshot date.
        
        Args:
            client_code: Optional client code to filter by
            
        Returns:
            Most recent snapshot date string or None
        """
        queryset = PortfolioSnapshot.objects.all()
        if client_code:
            try:
                client = Client.objects.get(code=client_code)
                queryset = queryset.filter(client=client)
            except Client.DoesNotExist:
                logger.warning(f"Client not found: {client_code}")
                return None

        latest = queryset.order_by('-snapshot_date').first()
        return str(latest.snapshot_date) if latest else None

    def get_transactions_by_date_range(self, start_date: Optional[str] = None, 
                                     end_date: Optional[str] = None, 
                                     client_code: Optional[str] = None) -> List[Dict]:
        """
        Retrieve transactions for a specific date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format  
            client_code: Client identifier code
            
        Returns:
            List of transaction dictionaries
        """
        queryset = Transaction.objects.all().select_related('asset', 'client')
        
        if client_code:
            try:
                client = Client.objects.get(code=client_code)
                queryset = queryset.filter(client=client)
            except Client.DoesNotExist:
                logger.warning(f"Client not found: {client_code}")
                return []
        
        if start_date:
            queryset = queryset.filter(date__gte=start_date)
        if end_date:
            queryset = queryset.filter(date__lte=end_date)
            
        # Convert to dictionaries for compatibility with existing code
        transactions = []
        for tx in queryset:
            transactions.append({
                'id': tx.transaction_id,
                'transaction_id': tx.transaction_id,
                'date': str(tx.date),
                'asset_id': tx.asset.id,
                'ticker': tx.asset.ticker,
                'transaction_type': tx.transaction_type,
                'quantity': float(tx.quantity) if tx.quantity else 0,
                'price': float(tx.price) if tx.price else 0,
                'amount': float(tx.amount),
                'bank': tx.bank,
                'account': tx.account,
                'client': tx.client.code
            })
            
        logger.info(f"Retrieved {len(transactions)} transactions for date range {start_date} to {end_date}")
        return transactions

    def get_assets_by_identifiers(self, ticker: Optional[str] = None, 
                                isin: Optional[str] = None, 
                                cusip: Optional[str] = None) -> List[Dict]:
        """
        Find assets matching the provided identifiers.
        
        Args:
            ticker: Asset ticker symbol
            isin: International Securities Identification Number
            cusip: CUSIP identifier
            
        Returns:
            List of matching asset dictionaries
        """
        queryset = Asset.objects.all()
        
        if ticker:
            queryset = queryset.filter(ticker=ticker)
        if isin:
            queryset = queryset.filter(isin=isin)
        if cusip:
            queryset = queryset.filter(cusip=cusip)
            
        # Convert to dictionaries for compatibility
        assets = []
        for asset in queryset:
            assets.append({
                'id': asset.id,
                'ticker': asset.ticker,
                'name': asset.name,
                'asset_type': asset.asset_type,
                'currency': asset.currency,
                'isin': asset.isin,
                'cusip': asset.cusip,
                'coupon_rate': float(asset.coupon_rate) if asset.coupon_rate else None,
                'maturity_date': str(asset.maturity_date) if asset.maturity_date else None,
                'active': asset.active
            })
            
        logger.info(f"Found {len(assets)} assets matching identifiers")
        return assets