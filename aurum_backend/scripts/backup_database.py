#!/usr/bin/env python3
"""
PostgreSQL Database Backup Script for Aurum Finance
Automated backup with compression, retention policies, and integrity verification.
"""

import os
import sys
import subprocess
import datetime
import gzip
import shutil
import logging
from pathlib import Path
from typing import Optional, Dict, List
import hashlib

# Add Django project to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'aurum_backend.settings')

import django
django.setup()

from django.conf import settings
from django.core.management.base import BaseCommand


class DatabaseBackup:
    """
    Comprehensive PostgreSQL backup utility with retention policies.
    """
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.backup_settings = getattr(settings, 'BACKUP_SETTINGS', {})
        self._set_defaults()
    
    def _set_defaults(self):
        """Set default backup configuration."""
        defaults = {
            'RETENTION_DAYS': 30,
            'WEEKLY_RETENTION': 12,
            'MONTHLY_RETENTION': 12,
            'BACKUP_LOCATION': settings.BASE_DIR / 'backups',
            'REMOTE_BACKUP': False,
            'COMPRESS_BACKUPS': True,
            'VERIFY_BACKUPS': True,
            'MAX_BACKUP_SIZE_GB': 10,
        }
        
        for key, value in defaults.items():
            if key not in self.backup_settings:
                self.backup_settings[key] = value
        
        # Ensure backup directory exists
        Path(self.backup_settings['BACKUP_LOCATION']).mkdir(parents=True, exist_ok=True)
    
    def _setup_logging(self) -> logging.Logger:
        """Configure logging for backup operations."""
        logger = logging.getLogger('aurum_backup')
        logger.setLevel(logging.INFO)
        
        # Create logs directory if it doesn't exist
        log_dir = settings.BASE_DIR / 'logs'
        log_dir.mkdir(exist_ok=True)
        
        # File handler
        file_handler = logging.FileHandler(log_dir / 'backup.log')
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
    
    def create_backup(self, backup_type: str = 'daily') -> Dict:
        """
        Create a PostgreSQL database backup.
        
        Args:
            backup_type: Type of backup ('daily', 'weekly', 'monthly')
            
        Returns:
            Dict with backup results
        """
        try:
            self.logger.info(f"Starting {backup_type} database backup")
            
            # Get database configuration
            db_config = settings.DATABASES['default']
            
            if db_config['ENGINE'] != 'django.db.backends.postgresql':
                raise ValueError("Only PostgreSQL databases are supported")
            
            # Generate backup filename
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_filename = f"aurum_backup_{backup_type}_{timestamp}.sql"
            backup_path = Path(self.backup_settings['BACKUP_LOCATION']) / backup_filename
            
            # Create pg_dump command
            cmd = [
                'pg_dump',
                '--host', db_config.get('HOST', 'localhost'),
                '--port', str(db_config.get('PORT', 5432)),
                '--username', db_config['USER'],
                '--dbname', db_config['NAME'],
                '--verbose',
                '--clean',
                '--no-owner',
                '--no-privileges',
                '--file', str(backup_path)
            ]
            
            # Set password environment variable
            env = os.environ.copy()
            env['PGPASSWORD'] = db_config['PASSWORD']
            
            # Execute backup
            self.logger.info(f"Executing backup to: {backup_path}")
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            
            if result.returncode != 0:
                raise subprocess.CalledProcessError(
                    result.returncode, cmd, result.stdout, result.stderr
                )
            
            # Get backup file size
            backup_size = backup_path.stat().st_size
            backup_size_mb = backup_size / (1024 * 1024)
            
            self.logger.info(f"Backup created successfully: {backup_size_mb:.2f} MB")
            
            # Compress backup if enabled
            if self.backup_settings['COMPRESS_BACKUPS']:
                compressed_path = self._compress_backup(backup_path)
                backup_path = compressed_path
                backup_size = backup_path.stat().st_size
                backup_size_mb = backup_size / (1024 * 1024)
                self.logger.info(f"Backup compressed: {backup_size_mb:.2f} MB")
            
            # Verify backup integrity
            if self.backup_settings['VERIFY_BACKUPS']:
                checksum = self._calculate_checksum(backup_path)
                self.logger.info(f"Backup checksum: {checksum}")
            
            # Check backup size limits
            max_size_bytes = self.backup_settings['MAX_BACKUP_SIZE_GB'] * 1024 * 1024 * 1024
            if backup_size > max_size_bytes:
                self.logger.warning(f"Backup size ({backup_size_mb:.2f} MB) exceeds limit")
            
            backup_info = {
                'success': True,
                'backup_path': str(backup_path),
                'backup_size': backup_size,
                'backup_size_mb': backup_size_mb,
                'backup_type': backup_type,
                'timestamp': timestamp,
                'checksum': checksum if self.backup_settings['VERIFY_BACKUPS'] else None
            }
            
            self.logger.info(f"{backup_type.title()} backup completed successfully")
            return backup_info
            
        except Exception as e:
            self.logger.error(f"Backup failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'backup_type': backup_type
            }
    
    def _compress_backup(self, backup_path: Path) -> Path:
        """Compress backup file using gzip."""
        compressed_path = backup_path.with_suffix(backup_path.suffix + '.gz')
        
        with open(backup_path, 'rb') as f_in:
            with gzip.open(compressed_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # Remove uncompressed file
        backup_path.unlink()
        
        return compressed_path
    
    def _calculate_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum of backup file."""
        sha256_hash = hashlib.sha256()
        
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        
        return sha256_hash.hexdigest()
    
    def cleanup_old_backups(self) -> Dict:
        """
        Clean up old backups according to retention policies.
        
        Returns:
            Dict with cleanup results
        """
        try:
            self.logger.info("Starting backup cleanup")
            
            backup_dir = Path(self.backup_settings['BACKUP_LOCATION'])
            if not backup_dir.exists():
                return {'success': True, 'message': 'No backup directory found'}
            
            now = datetime.datetime.now()
            deleted_files = []
            
            # Get all backup files
            backup_files = list(backup_dir.glob('aurum_backup_*.sql*'))
            
            for backup_file in backup_files:
                try:
                    # Extract timestamp from filename
                    filename = backup_file.name
                    if '_daily_' in filename:
                        retention_days = self.backup_settings['RETENTION_DAYS']
                    elif '_weekly_' in filename:
                        retention_days = self.backup_settings['WEEKLY_RETENTION'] * 7
                    elif '_monthly_' in filename:
                        retention_days = self.backup_settings['MONTHLY_RETENTION'] * 30
                    else:
                        continue  # Skip unknown backup types
                    
                    # Get file modification time
                    file_time = datetime.datetime.fromtimestamp(backup_file.stat().st_mtime)
                    age_days = (now - file_time).days
                    
                    if age_days > retention_days:
                        backup_file.unlink()
                        deleted_files.append(str(backup_file))
                        self.logger.info(f"Deleted old backup: {backup_file.name}")
                
                except Exception as e:
                    self.logger.error(f"Error processing backup file {backup_file}: {str(e)}")
            
            self.logger.info(f"Cleanup completed. Deleted {len(deleted_files)} old backups")
            
            return {
                'success': True,
                'deleted_files': deleted_files,
                'deleted_count': len(deleted_files)
            }
            
        except Exception as e:
            self.logger.error(f"Backup cleanup failed: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def list_backups(self) -> List[Dict]:
        """List all available backups with details."""
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
                    'modified': datetime.datetime.fromtimestamp(stat.st_mtime),
                    'compressed': backup_file.suffix == '.gz'
                })
            except Exception as e:
                self.logger.error(f"Error reading backup file {backup_file}: {str(e)}")
        
        return backups


def main():
    """Main backup execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Aurum Finance Database Backup')
    parser.add_argument('--type', choices=['daily', 'weekly', 'monthly'], 
                       default='daily', help='Backup type')
    parser.add_argument('--cleanup', action='store_true', 
                       help='Clean up old backups')
    parser.add_argument('--list', action='store_true', 
                       help='List available backups')
    
    args = parser.parse_args()
    
    backup = DatabaseBackup()
    
    if args.list:
        backups = backup.list_backups()
        print(f"\nFound {len(backups)} backups:")
        for b in backups:
            print(f"  {b['filename']} - {b['size_mb']:.2f} MB - {b['created']}")
        return
    
    if args.cleanup:
        result = backup.cleanup_old_backups()
        if result['success']:
            print(f"Cleanup completed. Deleted {result['deleted_count']} old backups")
        else:
            print(f"Cleanup failed: {result['error']}")
            sys.exit(1)
        return
    
    # Create backup
    result = backup.create_backup(args.type)
    
    if result['success']:
        print(f"Backup completed successfully:")
        print(f"  File: {result['backup_path']}")
        print(f"  Size: {result['backup_size_mb']:.2f} MB")
        print(f"  Type: {result['backup_type']}")
    else:
        print(f"Backup failed: {result['error']}")
        sys.exit(1)


if __name__ == '__main__':
    main()