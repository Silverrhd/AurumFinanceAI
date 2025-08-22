"""
Django Management Command: Create Client Users
Creates systematic user accounts for all AurumFinance clients.

Usage:
    python manage.py create_client_users [options]

Examples:
    python manage.py create_client_users                    # Create missing users
    python manage.py create_client_users --clean-slate      # Clean up + create all
    python manage.py create_client_users --rotate-passwords # Reset all passwords
    python manage.py create_client_users --dry-run          # Show what would be done
"""

import os
import tempfile
from datetime import datetime

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings

from portfolio.services.user_management_service import ClientUserManagementService


class Command(BaseCommand):
    help = 'Create systematic user accounts for all AurumFinance clients'

    def add_arguments(self, parser):
        parser.add_argument(
            '--clean-slate',
            action='store_true',
            help='Delete inconsistent existing client users before creating new ones',
        )
        parser.add_argument(
            '--rotate-passwords',
            action='store_true',
            help='Reset passwords for existing users (creates missing users too)',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be done without making changes',
        )
        parser.add_argument(
            '--output-file',
            type=str,
            help='Custom path for credentials output file',
        )

    def handle(self, *args, **options):
        """Execute the management command."""
        
        # Setup styling
        self.style_success = self.style.SUCCESS
        self.style_warning = self.style.WARNING
        self.style_error = self.style.ERROR
        
        self.stdout.write(
            self.style_success('🚀 AurumFinance Client User Management System')
        )
        self.stdout.write('=' * 60)
        
        try:
            # Handle dry run
            if options['dry_run']:
                self._handle_dry_run()
                return
            
            # Execute the bulk operation
            results = ClientUserManagementService.bulk_create_all_client_users(
                clean_slate=options['clean_slate'],
                rotate_passwords=options['rotate_passwords']
            )
            
            if results['status'] == 'error':
                raise CommandError(f"Operation failed: {results['message']}")
            
            # Display results
            self._display_results(results)
            
            # Generate credentials file
            self._generate_credentials_file(results, options.get('output_file'))
            
            self.stdout.write(
                self.style_success('\n✅ Client user provisioning completed successfully!')
            )
            
        except Exception as e:
            raise CommandError(f"Command failed: {str(e)}")

    def _handle_dry_run(self):
        """Handle dry run mode - show what would be done."""
        self.stdout.write(
            self.style_warning('🔍 DRY RUN MODE - No changes will be made')
        )
        self.stdout.write('-' * 60)
        
        # Get current user status
        user_list = ClientUserManagementService.list_client_users()
        
        if user_list['status'] == 'error':
            self.stdout.write(
                self.style_error(f"❌ Error: {user_list['message']}")
            )
            return
        
        # Display what would be done
        summary = user_list['summary']
        self.stdout.write(f"📊 Current Status:")
        self.stdout.write(f"   • Total clients: {summary['total_clients']}")
        self.stdout.write(f"   • Existing users: {summary['existing_users']}")
        self.stdout.write(f"   • Missing users: {summary['missing_users']}")
        
        if summary['missing_users'] > 0:
            self.stdout.write(f"\n📝 Would create {summary['missing_users']} new users:")
            
            for user_info in user_list['users']:
                if user_info['status'] == 'missing':
                    self.stdout.write(f"   • {user_info['username']} (Client {user_info['client_code']})")
        else:
            self.stdout.write(
                self.style_success("\n✅ All client users already exist!")
            )

    def _display_results(self, results):
        """Display the results of the bulk operation."""
        self.stdout.write(f"\n📊 Operation Summary:")
        self.stdout.write(f"   • Timestamp: {results['timestamp']}")
        
        summary = results['summary']
        self.stdout.write(f"   • Total clients: {summary['total_clients']}")
        self.stdout.write(f"   • Users created: {summary['created']}")
        self.stdout.write(f"   • Users existed: {summary['existed']}")
        self.stdout.write(f"   • Passwords reset: {summary['reset']}")
        self.stdout.write(f"   • Errors: {summary['errors']}")
        
        # Display cleanup results if performed
        if results['cleanup']:
            cleanup = results['cleanup']
            if cleanup['status'] == 'success' and cleanup.get('deleted_users'):
                self.stdout.write(f"\n🧹 Cleanup Results:")
                self.stdout.write(f"   • Deleted {len(cleanup['deleted_users'])} inconsistent users")
                for deleted in cleanup['deleted_users']:
                    self.stdout.write(f"     - {deleted['username']} ({deleted['reason']})")
        
        # Display any errors
        errors = [user for user in results['users'] if user['status'] == 'error']
        if errors:
            self.stdout.write(f"\n❌ Errors encountered:")
            for error in errors:
                self.stdout.write(
                    self.style_error(f"   • {error['client_code']}: {error['message']}")
                )
        
        # Display successful operations
        successful = [user for user in results['users'] 
                     if user['status'] in ['created', 'reset']]
        
        if successful:
            self.stdout.write(f"\n✅ Successfully processed users:")
            for user in successful:
                status_emoji = "🆕" if user['status'] == 'created' else "🔄"
                action = "Created" if user['status'] == 'created' else "Reset password for"
                self.stdout.write(f"   {status_emoji} {action}: {user['username']}")

    def _generate_credentials_file(self, results, custom_output_file=None):
        """Generate a secure credentials file with usernames and passwords."""
        
        # Get users with passwords (created or reset)
        users_with_passwords = [
            user for user in results['users'] 
            if user['status'] in ['created', 'reset'] and 'password' in user
        ]
        
        if not users_with_passwords:
            self.stdout.write(
                self.style_warning("\n⚠️  No new passwords to output (all users existed)")
            )
            return
        
        # Determine output file path
        if custom_output_file:
            credentials_file = custom_output_file
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            credentials_file = os.path.join(
                tempfile.gettempdir(), 
                f"aurum_client_credentials_{timestamp}.txt"
            )
        
        # Generate credentials file content
        lines = [
            "# AurumFinance Client User Credentials",
            f"# Generated on: {results['timestamp']}",
            "# CONFIDENTIAL - Keep secure and distribute only to authorized personnel",
            f"# Total credentials: {len(users_with_passwords)}",
            "",
            "# Format: username:password",
            ""
        ]
        
        # Sort users by client code for better organization
        users_with_passwords.sort(key=lambda x: x['client_code'])
        
        for user in users_with_passwords:
            action = "CREATED" if user['status'] == 'created' else "PASSWORD RESET"
            lines.append(f"# {user['client_code']} - {action}")
            lines.append(f"{user['username']}:{user['password']}")
            lines.append("")
        
        # Write credentials file
        try:
            with open(credentials_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(lines))
            
            # Set file permissions to be readable only by owner (Unix systems)
            if hasattr(os, 'chmod'):
                os.chmod(credentials_file, 0o600)
            
            self.stdout.write(f"\n📄 Credentials file generated:")
            self.stdout.write(f"   📁 {credentials_file}")
            self.stdout.write(f"   🔐 Contains {len(users_with_passwords)} credentials")
            self.stdout.write(
                self.style_warning("   ⚠️  Keep this file secure and delete after distribution")
            )
            
        except Exception as e:
            self.stdout.write(
                self.style_error(f"❌ Failed to create credentials file: {str(e)}")
            )

    def _display_usage_examples(self):
        """Display usage examples."""
        self.stdout.write("\n📖 Usage Examples:")
        self.stdout.write("   python manage.py create_client_users")
        self.stdout.write("   python manage.py create_client_users --clean-slate")
        self.stdout.write("   python manage.py create_client_users --rotate-passwords")
        self.stdout.write("   python manage.py create_client_users --dry-run")