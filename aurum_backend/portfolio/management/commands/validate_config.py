"""
Django management command to validate AurumFinance configuration.
Run with: python manage.py validate_config
"""

from django.core.management.base import BaseCommand
from django.conf import settings
import json

from portfolio.config import get_config


class Command(BaseCommand):
    help = 'Validate AurumFinance configuration and display settings'

    def add_arguments(self, parser):
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Show detailed configuration information',
        )
        parser.add_argument(
            '--check-apis',
            action='store_true',
            help='Test API connectivity (requires API keys)',
        )

    def handle(self, *args, **options):
        self.stdout.write("🔧 AurumFinance Configuration Validation")
        self.stdout.write("=" * 50)
        
        try:
            # Get configuration
            config = get_config()
            
            # Display basic configuration summary
            summary = config.get_config_summary()
            self.stdout.write("\n📋 Configuration Summary:")
            for key, value in summary.items():
                if isinstance(value, dict):
                    self.stdout.write(f"  {key}:")
                    for sub_key, sub_value in value.items():
                        self.stdout.write(f"    {sub_key}: {sub_value}")
                else:
                    self.stdout.write(f"  {key}: {value}")
            
            # Detailed configuration if requested
            if options['verbose']:
                self.stdout.write("\n🔍 Detailed Configuration:")
                self.stdout.write("-" * 30)
                
                # API Clients
                self.stdout.write("\n🔗 API Client Configuration:")
                openfigi_config = config.get_openfigi_client_config()
                self.stdout.write(f"  OpenFIGI API Key: {'✅ SET' if openfigi_config['api_key'] else '❌ MISSING'}")
                self.stdout.write(f"  OpenFIGI Base URL: {openfigi_config['base_url']}")
                self.stdout.write(f"  OpenFIGI Rate Limit: {openfigi_config['rate_limit']}s")
                
                mindicador_config = config.get_mindicador_client_config()
                self.stdout.write(f"  Mindicador Base URL: {mindicador_config['base_url']}")
                self.stdout.write(f"  Mindicador Rate Limit: {mindicador_config['rate_limit']}s")
                
                # Database
                self.stdout.write("\n💾 Database Configuration:")
                db_config = config.get_database_config()
                for key, value in db_config.items():
                    if 'password' in key.lower():
                        display_value = '✅ SET' if value else '❌ NOT SET'
                    else:
                        display_value = value
                    self.stdout.write(f"  {key}: {display_value}")
                
                # File Processing
                self.stdout.write("\n📁 File Processing Configuration:")
                fp_config = config.file_processing
                self.stdout.write(f"  Max file size: {fp_config.max_file_size_mb}MB")
                self.stdout.write(f"  Allowed extensions: {', '.join(fp_config.allowed_file_extensions)}")
                self.stdout.write(f"  Upload directory: {fp_config.upload_directory}")
                self.stdout.write(f"  Batch size: {fp_config.batch_size}")
                
                # Logging
                self.stdout.write("\n📝 Logging Configuration:")
                log_config = config.logging
                self.stdout.write(f"  Log level: {log_config.log_level}")
                self.stdout.write(f"  Log directory: {log_config.log_directory}")
                self.stdout.write(f"  Log to file: {log_config.log_to_file}")
            
            # API connectivity tests if requested
            if options['check_apis']:
                self.stdout.write("\n🌐 API Connectivity Tests:")
                self.stdout.write("-" * 30)
                
                # Test OpenFIGI
                openfigi_config = config.get_openfigi_client_config()
                if openfigi_config['api_key']:
                    self.stdout.write("  Testing OpenFIGI API...")
                    try:
                        from portfolio.preprocessing.utils.openfigi_client import OpenFIGIClient
                        client = OpenFIGIClient()
                        stats = client.get_client_stats()
                        self.stdout.write("  ✅ OpenFIGI client initialized successfully")
                    except Exception as e:
                        self.stdout.write(f"  ❌ OpenFIGI client error: {str(e)}")
                else:
                    self.stdout.write("  ⏭️  Skipping OpenFIGI test (no API key)")
                
                # Test Mindicador
                self.stdout.write("  Testing Mindicador API...")
                try:
                    from portfolio.preprocessing.utils.mindicador_client import MindicadorClient
                    client = MindicadorClient()
                    stats = client.get_client_stats()
                    self.stdout.write("  ✅ Mindicador client initialized successfully")
                except Exception as e:
                    self.stdout.write(f"  ❌ Mindicador client error: {str(e)}")
            
            # Configuration validation results
            self.stdout.write("\n✅ Configuration validation completed successfully!")
            
            # Warnings for missing configuration
            warnings = []
            if not config.api_clients.openfigi_api_key:
                warnings.append("OPENFIGI_API_KEY not set - Valley and IDB transformers may fail")
            
            if warnings:
                self.stdout.write(f"\n⚠️  Configuration Warnings:")
                for warning in warnings:
                    self.stdout.write(f"  • {warning}")
            
        except Exception as e:
            self.stderr.write(f"❌ Configuration validation failed: {str(e)}")
            raise