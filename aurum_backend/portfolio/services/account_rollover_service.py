"""
Account Rollover Service - Enhanced Production-Ready Implementation
Simple "copy & paste" rollover with robust error handling and logging.
"""

import logging
from collections import defaultdict
from typing import Dict, List, Optional
from django.db import transaction
from ..models import Client, Position, PortfolioSnapshot, Transaction

logger = logging.getLogger(__name__)


class AccountRolloverService:
    """
    Production-ready account rollover service.
    Simple copy & paste approach with comprehensive error handling.
    """
    
    def __init__(self):
        self.logger = logger
    
    def detect_missing_accounts(self, target_date: str) -> Dict[str, Dict[str, str]]:
        """
        Find accounts that exist historically but are missing on target date.
        
        Args:
            target_date: Date to check for missing accounts (YYYY-MM-DD)
            
        Returns:
            Dict mapping client_code -> {account_key: last_available_date}
            Example: {'ELP': {'CS_SP': '2025-07-24', 'LO_SP': '2025-07-24'}}
        """
        self.logger.info(f"🔍 Detecting missing accounts for {target_date}")
        
        # Get all historical account combinations
        historical_accounts = Position.objects.filter(
            snapshot__snapshot_date__lt=target_date
        ).values(
            'snapshot__client__code', 'bank', 'account'
        ).distinct()
        
        # Get current accounts on target date
        current_accounts = Position.objects.filter(
            snapshot__snapshot_date=target_date
        ).values(
            'snapshot__client__code', 'bank', 'account'
        ).distinct()
        
        # Build sets for comparison
        historical_set = {
            (acc['snapshot__client__code'], acc['bank'], acc['account'])
            for acc in historical_accounts
        }
        
        current_set = {
            (acc['snapshot__client__code'], acc['bank'], acc['account'])
            for acc in current_accounts
        }
        
        # Find accounts that exist historically but have zero positions now
        potentially_missing_accounts = historical_set - current_set
        
        if not potentially_missing_accounts:
            self.logger.info("✅ No missing accounts detected")
            return {}
        
        self.logger.info(f"🔍 Found {len(potentially_missing_accounts)} accounts with zero positions - applying enhanced detection")
        
        # ENHANCED ROLLOVER LOGIC: Multi-level transaction detection
        rollover_map = defaultdict(dict)
        accounts_excluded_by_enhanced_logic = 0
        
        for client, bank, account in potentially_missing_accounts:
            # Level 1: Check if bank has ANY activity on target date
            bank_has_activity = Transaction.objects.filter(bank=bank, date=target_date).exists()
            
            if not bank_has_activity:
                # Bank file not uploaded, rollover is legitimate
                self.logger.info(f"🔄 {client}-{bank}-{account}: Bank {bank} has no activity - bank file not uploaded")
                self._add_to_rollover_map(rollover_map, client, bank, account, target_date)
                continue
            
            # Level 2: Check if client has activity for this bank on target date
            client_bank_activity = Transaction.objects.filter(
                client__code=client, 
                bank=bank, 
                date=target_date
            ).exists()
            
            if not client_bank_activity:
                # Client missing from bank file, rollover needed
                self.logger.info(f"🔄 {client}-{bank}-{account}: Client missing from {bank} data - rollover needed")
                self._add_to_rollover_map(rollover_map, client, bank, account, target_date)
            else:
                # Client has transactions for this bank = legitimate zero balance (transfers/closures)
                self.logger.info(f"✅ {client}-{bank}-{account}: Client has {bank} transactions - legitimate zero balance, no rollover")
                accounts_excluded_by_enhanced_logic += 1
        
        self.logger.info(f"📊 Enhanced logic excluded {accounts_excluded_by_enhanced_logic} accounts (legitimate zero balances)")
        
        result = dict(rollover_map)
        if result:
            self.logger.info(f"🚨 Final rollover needed for clients: {list(result.keys())}")
        else:
            self.logger.info("✅ No rollover needed after enhanced analysis")
        
        # CLEANUP: Always check for phantom rollover positions, regardless of missing accounts
        self._cleanup_phantom_rollover_positions(target_date, potentially_missing_accounts)
        
        return result
    
    def _cleanup_phantom_rollover_positions(self, target_date: str, potentially_missing_accounts):
        """
        Remove phantom rollover positions when enhanced logic determines no rollover is needed.
        This handles cases where phantom rollovers persist from previous population runs.
        
        Args:
            target_date: Target date to clean up (YYYY-MM-DD)
            potentially_missing_accounts: Set of (client, bank, account) tuples that had zero positions
        """
        self.logger.info(f"🧹 CLEANUP METHOD CALLED - Starting cleanup of phantom rollover positions for {target_date}")
        print(f"🧹 CLEANUP METHOD CALLED - Starting cleanup of phantom rollover positions for {target_date}")
        
        # Find ALL rollover positions on target date, not just missing accounts
        all_rollover_positions = Position.objects.filter(
            snapshot__snapshot_date=target_date,
            is_rolled_over=True
        ).values('snapshot__client__code', 'bank', 'account').distinct()
        
        rollover_accounts = {(pos['snapshot__client__code'], pos['bank'], pos['account']) for pos in all_rollover_positions}
        
        cleaned_accounts = 0
        cleaned_positions = 0
        
        for client, bank, account in rollover_accounts:
            # Only clean accounts where client has transactions (enhanced logic excluded them)
            client_bank_activity = Transaction.objects.filter(
                client__code=client, 
                bank=bank, 
                date=target_date
            ).exists()
            
            if client_bank_activity:
                # This account was excluded by enhanced logic - clean phantom rollovers
                phantom_positions = Position.objects.filter(
                    snapshot__client__code=client,
                    snapshot__snapshot_date=target_date,
                    bank=bank,
                    account=account,
                    is_rolled_over=True
                )
                
                if phantom_positions.exists():
                    position_count = phantom_positions.count()
                    phantom_positions.delete()
                    
                    self.logger.info(f"🗑️  Cleaned {position_count} phantom positions for {client}-{bank}-{account}")
                    cleaned_accounts += 1
                    cleaned_positions += position_count
                    
                    # Clear rollover markers from snapshot if this was the only rolled account
                    try:
                        client_obj = Client.objects.get(code=client)
                        snapshot = PortfolioSnapshot.objects.get(
                            client=client_obj, 
                            snapshot_date=target_date
                        )
                        
                        # Check if any other accounts still have rollover positions
                        remaining_rollovers = Position.objects.filter(
                            snapshot=snapshot,
                            is_rolled_over=True
                        ).exists()
                        
                        if not remaining_rollovers:
                            # No more rollover positions - clear snapshot markers
                            snapshot.has_rolled_accounts = False
                            snapshot.rollover_summary = {}
                            snapshot.save()
                            self.logger.info(f"📸 Cleared rollover markers from {client} snapshot")
                        else:
                            # Update rollover summary to remove this account
                            account_key = f"{bank}_{account}"
                            if account_key in snapshot.rollover_summary:
                                snapshot.rollover_summary.pop(account_key)
                                snapshot.save()
                                self.logger.info(f"📸 Removed {account_key} from {client} rollover summary")
                                
                    except (Client.DoesNotExist, PortfolioSnapshot.DoesNotExist) as e:
                        self.logger.warning(f"⚠️ Could not update snapshot for {client}: {e}")
        
        if cleaned_accounts > 0:
            self.logger.info(f"✅ Cleanup complete: {cleaned_accounts} accounts cleaned, {cleaned_positions} phantom positions removed")
        else:
            self.logger.info("✅ No phantom positions found to clean")
    
    def _add_to_rollover_map(self, rollover_map, client, bank, account, target_date):
        """
        Helper method to add account to rollover map with last available date.
        
        Args:
            rollover_map: Dictionary to add the rollover entry to
            client: Client code
            bank: Bank code
            account: Account code
            target_date: Target date for rollover
        """
        # Find last date this account had data
        last_position = Position.objects.filter(
            snapshot__client__code=client,
            bank=bank,
            account=account,
            snapshot__snapshot_date__lt=target_date
        ).order_by('-snapshot__snapshot_date').first()
        
        if last_position:
            account_key = f"{bank}_{account}"
            rollover_map[client][account_key] = str(last_position.snapshot.snapshot_date)
            self.logger.debug(f"📅 {client}-{bank}-{account}: Will rollover from {last_position.snapshot.snapshot_date}")
    
    def copy_account_positions(self, client_code: str, bank: str, account: str,
                             from_date: str, to_date: str) -> int:
        """
        Copy positions exactly from one date to another with robust error handling.
        
        Args:
            client_code: Client code
            bank: Bank name
            account: Account identifier
            from_date: Source date (YYYY-MM-DD)
            to_date: Target date (YYYY-MM-DD)
            
        Returns:
            Number of positions successfully copied
        """
        # Get source positions
        source_positions = Position.objects.filter(
            snapshot__client__code=client_code,
            snapshot__snapshot_date=from_date,
            bank=bank,
            account=account
        )
        
        if not source_positions.exists():
            self.logger.warning(f"⚠️ No source positions found for {client_code} {bank}_{account} on {from_date}")
            return 0
        
        # Get or create target snapshot
        client = Client.objects.get(code=client_code)
        target_snapshot, created = PortfolioSnapshot.objects.get_or_create(
            client=client,
            snapshot_date=to_date,
            defaults={'has_rolled_accounts': True}
        )
        
        if created:
            self.logger.info(f"📸 Created new snapshot for {client_code} on {to_date}")
        
        # ENHANCEMENT 2: Clear any existing rolled positions for this account/date
        existing_rolled = Position.objects.filter(
            snapshot=target_snapshot,
            bank=bank,
            account=account,
            is_rolled_over=True
        )
        if existing_rolled.exists():
            self.logger.info(f"🧹 Clearing {existing_rolled.count()} existing rolled positions for {bank}_{account}")
            existing_rolled.delete()
        
        # Copy each position exactly with error handling
        positions_copied = 0
        positions_failed = 0
        
        for source_pos in source_positions:
            try:  # ENHANCEMENT 1: Add error handling
                Position.objects.create(
                    snapshot=target_snapshot,
                    asset=source_pos.asset,
                    # EXACT COPY - NO CHANGES
                    quantity=source_pos.quantity,
                    market_value=source_pos.market_value,
                    cost_basis=source_pos.cost_basis,
                    price=source_pos.price,
                    bank=source_pos.bank,
                    account=source_pos.account,
                    yield_pct=source_pos.yield_pct,
                    coupon_rate=source_pos.coupon_rate,
                    maturity_date=source_pos.maturity_date,
                    estimated_annual_income=source_pos.estimated_annual_income,
                    face_value=source_pos.face_value,
                    # Rollover tracking
                    is_rolled_over=True,
                    rolled_from_date=from_date
                )
                positions_copied += 1
                
            except Exception as e:  # ENHANCEMENT 1: Handle individual position failures
                asset_symbol = source_pos.asset.symbol if source_pos.asset else 'Unknown'
                self.logger.error(f"❌ Failed to copy position {source_pos.id} "
                                f"({asset_symbol}): {e}")
                positions_failed += 1
                continue  # Continue with other positions
        
        if positions_failed > 0:
            self.logger.warning(f"⚠️ {positions_failed} positions failed to copy for {bank}_{account}")
        
        self.logger.info(f"✅ Successfully copied {positions_copied} positions for {bank}_{account}")
        return positions_copied
    
    def apply_rollover_for_client(self, client_code: str, target_date: str) -> Dict:
        """
        Apply rollover for all missing accounts of a client with enhanced logging.
        
        Args:
            client_code: Client code to process
            target_date: Target date for rollover (YYYY-MM-DD)
            
        Returns:
            Dict with rollover results and statistics
        """
        missing_accounts = self.detect_missing_accounts(target_date)
        client_missing = missing_accounts.get(client_code, {})
        
        if not client_missing:
            self.logger.info(f"✅ No rollover needed for client {client_code} on {target_date}")
            return {
                'success': True,
                'client': client_code,
                'accounts_rolled': 0,
                'positions_copied': 0,
                'message': 'No rollover needed'
            }
        
        # ENHANCEMENT 3: Better logging
        self.logger.info(f"🔄 Starting rollover for client {client_code} on {target_date}")
        self.logger.info(f"📋 Found {len(client_missing)} missing accounts: {list(client_missing.keys())}")
        
        rollover_log = []
        total_positions = 0
        total_failures = 0
        
        for account_key, from_date in client_missing.items():
            bank, account = account_key.split('_', 1)
            
            # ENHANCEMENT 3: Detailed per-account logging
            self.logger.info(f"  📋 Rolling {account_key}: {from_date} → {target_date}")
            
            try:
                positions_copied = self.copy_account_positions(
                    client_code, bank, account, from_date, target_date
                )
                
                # ENHANCEMENT 3: Success logging
                self.logger.info(f"  ✅ {account_key}: {positions_copied} positions copied")
                
                rollover_log.append({
                    'account': account_key,
                    'rolled_from': from_date,
                    'positions_copied': positions_copied,
                    'success': True
                })
                
                total_positions += positions_copied
                
            except Exception as e:
                self.logger.error(f"  ❌ {account_key}: Rollover failed - {e}")
                rollover_log.append({
                    'account': account_key,
                    'rolled_from': from_date,
                    'positions_copied': 0,
                    'success': False,
                    'error': str(e)
                })
                total_failures += 1
        
        # Update snapshot rollover summary (only successful rollovers)
        successful_rollovers = {
            acc['account']: acc['rolled_from'] 
            for acc in rollover_log if acc['success']
        }
        
        if successful_rollovers:
            try:
                client = Client.objects.get(code=client_code)
                snapshot = PortfolioSnapshot.objects.get(
                    client=client, snapshot_date=target_date
                )
                snapshot.has_rolled_accounts = True
                snapshot.rollover_summary = successful_rollovers
                snapshot.save()
                self.logger.info(f"📸 Updated snapshot rollover summary for {client_code}")
            except Exception as e:
                self.logger.error(f"❌ Failed to update snapshot summary for {client_code}: {e}")
        
        # Final summary logging
        successful_accounts = len([acc for acc in rollover_log if acc['success']])
        self.logger.info(f"🎯 Rollover complete for {client_code}: "
                        f"{successful_accounts}/{len(client_missing)} accounts successful, "
                        f"{total_positions} total positions copied")
        
        return {
            'success': total_failures == 0,
            'client': client_code,
            'accounts_rolled': successful_accounts,
            'accounts_failed': total_failures,
            'positions_copied': total_positions,
            'rollover_details': rollover_log
        }
    
    @transaction.atomic
    def rollover_all_missing_accounts(self, target_date: str) -> Dict:
        """
        Apply rollover for all clients with missing accounts.
        
        Args:
            target_date: Target date for rollover (YYYY-MM-DD)
            
        Returns:
            Dict with comprehensive rollover results
        """
        self.logger.info(f"🚀 Starting comprehensive rollover for {target_date}")
        
        missing_accounts = self.detect_missing_accounts(target_date)
        
        if not missing_accounts:
            self.logger.info("✅ No missing accounts found - no rollover needed")
            return {
                'success': True,
                'clients_processed': 0,
                'accounts_rolled': 0,
                'positions_copied': 0,
                'message': 'No rollover needed'
            }
        
        rollover_results = {}
        rollover_summary = {
            'clients_processed': 0,
            'accounts_rolled': 0,
            'positions_copied': 0,
            'failures': 0
        }
        
        for client_code in missing_accounts.keys():
            try:
                result = self.apply_rollover_for_client(client_code, target_date)
                rollover_results[client_code] = result
                
                # Update summary
                rollover_summary['clients_processed'] += 1
                rollover_summary['accounts_rolled'] += result['accounts_rolled']
                rollover_summary['positions_copied'] += result['positions_copied']
                rollover_summary['failures'] += result.get('accounts_failed', 0)
                
            except Exception as e:
                self.logger.error(f"❌ Rollover failed for client {client_code}: {e}")
                rollover_results[client_code] = {
                    'success': False,
                    'error': str(e)
                }
                rollover_summary['failures'] += 1
        
        self.logger.info(f"🎯 Comprehensive rollover complete: {rollover_summary}")
        
        return {
            'success': rollover_summary['failures'] == 0,
            'summary': rollover_summary,
            'details': rollover_results
        }