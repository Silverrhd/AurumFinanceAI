"""
Management command for testing account rollover functionality.
"""

from django.core.management.base import BaseCommand
from portfolio.services.account_rollover_service import AccountRolloverService
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Test account rollover functionality'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--date', 
            type=str, 
            required=True,
            help='Target date for rollover testing (YYYY-MM-DD)'
        )
        parser.add_argument(
            '--client', 
            type=str, 
            help='Test specific client only'
        )
        parser.add_argument(
            '--dry-run', 
            action='store_true', 
            help='Show what would be rolled over without applying changes'
        )
    
    def handle(self, *args, **options):
        rollover_service = AccountRolloverService()
        target_date = options['date']
        
        self.stdout.write(f"🧪 Testing rollover for {target_date}")
        
        if options['dry_run']:
            # Just show detection results
            self.stdout.write("🔍 DRY RUN - Detection only")
            missing_accounts = rollover_service.detect_missing_accounts(target_date)
            
            if not missing_accounts:
                self.stdout.write(
                    self.style.SUCCESS(f"✅ No missing accounts found for {target_date}")
                )
                return
            
            self.stdout.write(f"🚨 Missing accounts for {target_date}:")
            for client, accounts in missing_accounts.items():
                self.stdout.write(f"  👤 {client}:")
                for account, from_date in accounts.items():
                    self.stdout.write(f"    🏦 {account} (last data: {from_date})")
        
        else:
            # Apply rollover
            if options['client']:
                self.stdout.write(f"🔄 Testing rollover for client: {options['client']}")
                result = rollover_service.apply_rollover_for_client(
                    options['client'], target_date
                )
                self.stdout.write(f"✅ Rollover result: {result}")
            else:
                self.stdout.write("🔄 Testing rollover for all missing accounts")
                results = rollover_service.rollover_all_missing_accounts(target_date)
                self.stdout.write(f"✅ Rollover results: {results}")