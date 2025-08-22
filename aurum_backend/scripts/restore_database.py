#!/usr/bin/env python3
"""
PostgreSQL Database Restore Script for Aurum Finance
Restore database from backup files with verification and safety checks.
"""

import os
import sys
import subprocess
import datetime
import gzip
import logging
from pathlib import Path
from typing import Optional, Dict
import tempfile

# Add Django project to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'aurum_backend.settings')

import django
django.setup()

from django.conf import settings
from django.db import connection


class DatabaseRestore:
    """
    PostgreSQL database restore utility with safety checks.
    """
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.backup_settings = getattr(settings, 'BACKUP_SETTINGS', {})
        self._set_defaults()
    
    def _set_defaults(self):
        """Set default restore configuration."""
        defaults = {
            'BACKUP_LOCATION': settings.BASE_DIR / 'backups',
            'REQUIRE_CONFIRMATION': True,
            'CREATE_PRE_RESTORE_BACKUP': True,
        }
        
        for key, value in defaults.items():
            if key not in self.backup_settings:
                self.backup_settings[key] = value
    
    def _setup_logging(self) -> logging.Logger:
        """Configure logging for restore operations."""
        logger = logging.getLogger('aurum_restore')
        logger.setLevel(logging.INFO)
        
        # Create logs directory if it doesn't exist
        log_dir = settings.BASE_DIR / 'logs'
        log_dir.mkdir(exist_ok=True)
        
        # File handler
        file_handler = logging.FileHandler(log_dir / 'restore.log')
        file_handler.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def list_available_backups(self) -> list:
        """List all available backup files."""
        backup_dir = Path(self.backup_settings['BACKUP_LOCATION'])
        if not backup_dir.exists():
            return []
        
        backups = []
        backup_files = sorted(backup_dir.glob('aurum_backup_*.sql*'), reverse=True)
        
        for backup_file in backup_files:
            try:
                stat = backup_file.stat()
                backups.append({
                    'filename': backup_file.name,
                    'path': str(backup_file),
                    'size': stat.st_size,
                    'size_mb': stat.st_size / (1024 * 1024),
                    'created': datetime.datetime.fromtimestamp(stat.st_ctime),
                    'compressed': backup_file.suffix == '.gz'
                })
            except Exception as e:
                self.logger.error(f"Error reading backup file {backup_file}: {str(e)}")
        
        return backups
    
    def verify_backup_file(self, backup_path: Path) -> Dict:
        """
        Verify backup file integrity and readability.
        
        Args:
            backup_path: Path to backup file
            
        Returns:
            Dict with verification results
        """
        try:
            if not backup_path.exists():
                return {'valid': False, 'error': 'Backup file does not exist'}
            
            # Check file size
            file_size = backup_path.stat().st_size
            if file_size == 0:
                return {'valid': False, 'error': 'Backup file is empty'}
            
            # Try to read first few lines to verify it's a SQL dump
            if backup_path.suffix == '.gz':
                with gzip.open(backup_path, 'rt') as f:
                    first_lines = [f.readline() for _ in range(5)]
            else:
                with open(backup_path, 'r') as f:
                    first_lines = [f.readline() for _ in range(5)]
            
            # Check for PostgreSQL dump header
            header_found = any('PostgreSQL database dump' in line for line in first_lines)
            if not header_found:
                return {'valid': False, 'error': 'File does not appear to be a PostgreSQL dump'}
            
            return {
                'valid': True,
                'file_size': file_size,
                'file_size_mb': file_size / (1024 * 1024),
                'compressed': backup_path.suffix == '.gz'
            }
            
        except Exception as e:
            return {'valid': False, 'error': f'Error verifying backup: {str(e)}'}
    
    def create_pre_restore_backup(self) -> Optional[str]:
        """Create a backup before restore operation."""
        try:
            # Import dynamically to avoid circular import
            import importlib.util
            import sys
            
            # Load backup_database module dynamically
            backup_module_path = os.path.join(os.path.dirname(__file__), 'backup_database.py')
            spec = importlib.util.spec_from_file_location("backup_database", backup_module_path)
            backup_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(backup_module)
            
            self.logger.info("Creating pre-restore backup")
            backup = backup_module.DatabaseBackup()
            result = backup.create_backup('pre_restore')
            
            if result['success']:
                self.logger.info(f"Pre-restore backup created: {result['backup_path']}")
                return result['backup_path']
            else:
                self.logger.error(f"Failed to create pre-restore backup: {result['error']}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error creating pre-restore backup: {str(e)}")
            return None
    
    def restore_database(self, backup_path: str, force: bool = False) -> Dict:
        """
        Restore database from backup file.
        
        Args:
            backup_path: Path to backup file
            force: Skip confirmation prompts
            
        Returns:
            Dict with restore results
        """
        try:
            backup_file = Path(backup_path)
            self.logger.info(f"Starting database restore from: {backup_file.name}")
            
            # Verify backup file
            verification = self.verify_backup_file(backup_file)
            if not verification['valid']:
                return {'success': False, 'error': verification['error']}
            
            self.logger.info(f"Backup file verified: {verification['file_size_mb']:.2f} MB")
            
            # Create pre-restore backup if enabled
            pre_restore_backup = None
            if self.backup_settings['CREATE_PRE_RESTORE_BACKUP']:
                pre_restore_backup = self.create_pre_restore_backup()
                if not pre_restore_backup:
                    return {'success': False, 'error': 'Failed to create pre-restore backup'}
            
            # Get database configuration
            db_config = settings.DATABASES['default']
            
            if db_config['ENGINE'] != 'django.db.backends.postgresql':
                return {'success': False, 'error': 'Only PostgreSQL databases are supported'}
            
            # Prepare SQL file (decompress if needed)
            sql_file = backup_file
            temp_file = None
            
            if backup_file.suffix == '.gz':
                # Create temporary decompressed file
                temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False)
                temp_file.close()
                
                with gzip.open(backup_file, 'rt') as f_in:
                    with open(temp_file.name, 'w') as f_out:
                        f_out.write(f_in.read())
                
                sql_file = Path(temp_file.name)
            
            # Create psql command for restore
            cmd = [
                'psql',
                '--host', db_config.get('HOST', 'localhost'),
                '--port', str(db_config.get('PORT', 5432)),
                '--username', db_config['USER'],
                '--dbname', db_config['NAME'],
                '--file', str(sql_file),
                '--quiet'
            ]
            
            # Set password environment variable
            env = os.environ.copy()
            env['PGPASSWORD'] = db_config['PASSWORD']
            
            # Execute restore
            self.logger.info("Executing database restore...")
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            
            # Clean up temporary file
            if temp_file:
                os.unlink(temp_file.name)
            
            if result.returncode != 0:
                error_msg = f"Restore failed: {result.stderr}"
                self.logger.error(error_msg)
                return {'success': False, 'error': error_msg}
            
            # Verify database connectivity after restore
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM django_migrations")
                    migration_count = cursor.fetchone()[0]
                    self.logger.info(f"Database restore verified: {migration_count} migrations found")
            except Exception as e:
                self.logger.warning(f"Could not verify restore: {str(e)}")
            
            self.logger.info("Database restore completed successfully")
            
            return {
                'success': True,
                'backup_file': str(backup_file),
                'backup_size_mb': verification['file_size_mb'],
                'pre_restore_backup': pre_restore_backup,
                'restore_time': datetime.datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Database restore failed: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def test_restore(self, backup_path: str) -> Dict:
        """
        Test restore operation without actually modifying the database.
        
        Args:
            backup_path: Path to backup file
            
        Returns:
            Dict with test results
        """
        try:
            backup_file = Path(backup_path)
            self.logger.info(f"Testing restore from: {backup_file.name}")
            
            # Verify backup file
            verification = self.verify_backup_file(backup_file)
            if not verification['valid']:
                return {'success': False, 'error': verification['error']}
            
            # Try to parse SQL file structure
            sql_content = ""
            if backup_file.suffix == '.gz':
                with gzip.open(backup_file, 'rt') as f:
                    # Read first 1000 lines to check structure
                    sql_content = ''.join([f.readline() for _ in range(1000)])
            else:
                with open(backup_file, 'r') as f:
                    sql_content = ''.join([f.readline() for _ in range(1000)])
            
            # Basic SQL structure checks
            checks = {
                'has_create_table': 'CREATE TABLE' in sql_content,
                'has_insert_data': 'INSERT INTO' in sql_content,
                'has_constraints': 'ALTER TABLE' in sql_content,
                'has_indexes': 'CREATE INDEX' in sql_content
            }
            
            self.logger.info("Restore test completed successfully")
            
            return {
                'success': True,
                'backup_file': str(backup_file),
                'backup_size_mb': verification['file_size_mb'],
                'structure_checks': checks,
                'test_time': datetime.datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Restore test failed: {str(e)}")
            return {'success': False, 'error': str(e)}


def main():
    """Main restore execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Aurum Finance Database Restore')
    parser.add_argument('--backup', help='Path to backup file to restore')
    parser.add_argument('--list', action='store_true', help='List available backups')
    parser.add_argument('--test', help='Test restore from backup file')
    parser.add_argument('--force', action='store_true', help='Skip confirmation prompts')
    
    args = parser.parse_args()
    
    restore = DatabaseRestore()
    
    if args.list:
        backups = restore.list_available_backups()
        print(f"\nFound {len(backups)} backup files:")
        for i, backup in enumerate(backups, 1):
            print(f"  {i}. {backup['filename']}")
            print(f"     Size: {backup['size_mb']:.2f} MB")
            print(f"     Created: {backup['created']}")
            print(f"     Compressed: {backup['compressed']}")
            print()
        return
    
    if args.test:
        result = restore.test_restore(args.test)
        if result['success']:
            print(f"Restore test passed for: {result['backup_file']}")
            print(f"Backup size: {result['backup_size_mb']:.2f} MB")
            print("Structure checks:", result['structure_checks'])
        else:
            print(f"Restore test failed: {result['error']}")
            sys.exit(1)
        return
    
    if not args.backup:
        print("Error: --backup argument is required for restore operation")
        print("Use --list to see available backups")
        sys.exit(1)
    
    # Confirmation prompt
    if not args.force:
        print(f"\n⚠️  WARNING: This will restore the database from backup!")
        print(f"Backup file: {args.backup}")
        print(f"Current database will be overwritten!")
        
        response = input("\nAre you sure you want to continue? (yes/no): ")
        if response.lower() != 'yes':
            print("Restore cancelled.")
            return
    
    # Perform restore
    result = restore.restore_database(args.backup, args.force)
    
    if result['success']:
        print(f"Database restore completed successfully!")
        print(f"Restored from: {result['backup_file']}")
        print(f"Backup size: {result['backup_size_mb']:.2f} MB")
        if result.get('pre_restore_backup'):
            print(f"Pre-restore backup saved: {result['pre_restore_backup']}")
    else:
        print(f"Database restore failed: {result['error']}")
        sys.exit(1)


if __name__ == '__main__':
    main()