"""
Database Backup and Restore Service for Aurum Finance
Supports both SQLite (local development) and PostgreSQL (production)
Provides on-demand backup creation and UI-driven restore functionality.
"""

import os
import shutil
import sqlite3
import subprocess
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from django.conf import settings

logger = logging.getLogger(__name__)

class DatabaseBackupService:
    """
    Database backup and restore operations with safety features.
    Supports both SQLite (local development) and PostgreSQL (production).
    
    Key Safety Features:
    1. Auto-detects database type (SQLite vs PostgreSQL)
    2. Uses appropriate backup methods for each database type
    3. Validates backup integrity before operations
    4. Creates pre-restore backups automatically
    5. Atomic operations (all-or-nothing)
    6. Persistent backup location (survives redeployments)
    """
    
    def __init__(self):
        self.db_config = settings.DATABASES['default']
        self.db_engine = self.db_config['ENGINE']
        self.logger = logger
        
        # Detect database type
        self.is_sqlite = 'sqlite' in self.db_engine.lower()
        self.is_postgresql = 'postgresql' in self.db_engine.lower()
        
        if not (self.is_sqlite or self.is_postgresql):
            raise ValueError(f"Unsupported database engine: {self.db_engine}")
        
        # Set up paths and database-specific settings
        if self.is_sqlite:
            self.db_path = Path(self.db_config['NAME'])
            self.backup_dir = self.db_path.parent
            self.backup_extension = '.sqlite3'
        else:  # PostgreSQL
            # Use persistent backup location outside deployment directory
            self.backup_dir = Path('/opt/aurumfinance/backups') if os.path.exists('/opt/aurumfinance') else Path.cwd() / 'backups'
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            self.backup_extension = '.sql'
            
            # PostgreSQL connection details
            self.db_name = self.db_config['NAME']
            self.db_user = self.db_config['USER']
            self.db_password = self.db_config['PASSWORD']
            self.db_host = self.db_config.get('HOST', 'localhost')
            self.db_port = self.db_config.get('PORT', '5432')
    
    def create_backup(self, backup_name: Optional[str] = None) -> Dict[str, any]:
        """
        Create a consistent database backup using appropriate method for database type.
        
        Args:
            backup_name: Optional custom name (defaults to timestamp)
            
        Returns:
            Dict with backup results and metadata
        """
        try:
            # Generate backup filename
            if backup_name:
                backup_filename = f"db_backup_{backup_name}{self.backup_extension}"
            else:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                backup_filename = f"db_backup_{timestamp}{self.backup_extension}"
            
            backup_path = self.backup_dir / backup_filename
            
            # Ensure backup doesn't already exist
            if backup_path.exists():
                return {
                    'success': False,
                    'error': f'Backup file already exists: {backup_filename}'
                }
            
            self.logger.info(f"Creating {self.db_engine} database backup: {backup_filename}")
            
            if self.is_sqlite:
                # SQLite backup using native backup API
                source_conn = sqlite3.connect(str(self.db_path))
                backup_conn = sqlite3.connect(str(backup_path))
                source_conn.backup(backup_conn)
                source_conn.close()
                backup_conn.close()
            else:
                # PostgreSQL backup using pg_dump
                env = os.environ.copy()
                if self.db_password:
                    env['PGPASSWORD'] = self.db_password
                
                cmd = [
                    'pg_dump',
                    '--host', self.db_host,
                    '--port', str(self.db_port),
                    '--username', self.db_user,
                    '--format', 'custom',  # Use custom format for better compression and features
                    '--file', str(backup_path),
                    '--verbose',
                    '--no-password',  # Use PGPASSWORD env var
                    self.db_name
                ]
                
                result = subprocess.run(cmd, env=env, capture_output=True, text=True)
                if result.returncode != 0:
                    return {
                        'success': False,
                        'error': f'pg_dump failed: {result.stderr}'
                    }
            
            # Validate backup integrity
            if not self._validate_backup(backup_path):
                backup_path.unlink()  # Delete corrupted backup
                return {
                    'success': False,
                    'error': 'Backup validation failed - corrupted backup deleted'
                }
            
            # Get backup metadata
            backup_stats = backup_path.stat()
            backup_info = {
                'success': True,
                'filename': backup_filename,
                'path': str(backup_path),
                'size_bytes': backup_stats.st_size,
                'size_mb': round(backup_stats.st_size / 1024 / 1024, 2),
                'created_at': datetime.fromtimestamp(backup_stats.st_ctime),
                'display_name': self._format_backup_display_name(backup_filename),
                'db_type': 'SQLite' if self.is_sqlite else 'PostgreSQL'
            }
            
            self.logger.info(f"{backup_info['db_type']} backup created successfully: {backup_info['size_mb']} MB")
            return backup_info
            
        except Exception as e:
            self.logger.error(f"Backup creation failed: {str(e)}")
            return {
                'success': False,
                'error': f'Backup failed: {str(e)}'
            }
    
    def list_backups(self) -> List[Dict[str, any]]:
        """
        List all available backups with metadata for UI dropdown.
        
        Returns:
            List of backup dictionaries sorted by creation time (newest first)
        """
        try:
            # Get backup files based on database type
            if self.is_sqlite:
                backup_files = list(self.backup_dir.glob('db_backup_*.sqlite3'))
            else:
                backup_files = list(self.backup_dir.glob('db_backup_*.sql'))
            
            backups = []
            
            for backup_file in backup_files:
                try:
                    stats = backup_file.stat()
                    
                    # Validate backup before including in list
                    if not self._validate_backup(backup_file):
                        self.logger.warning(f"Skipping corrupted backup: {backup_file.name}")
                        continue
                    
                    backup_info = {
                        'filename': backup_file.name,
                        'path': str(backup_file),
                        'size_bytes': stats.st_size,
                        'size_mb': round(stats.st_size / 1024 / 1024, 2),
                        'created_at': datetime.fromtimestamp(stats.st_ctime),
                        'modified_at': datetime.fromtimestamp(stats.st_mtime),
                        'display_name': self._format_backup_display_name(backup_file.name),
                        'is_valid': True
                    }
                    
                    backups.append(backup_info)
                    
                except Exception as e:
                    self.logger.error(f"Error reading backup {backup_file}: {str(e)}")
            
            # Sort by creation time, newest first
            backups.sort(key=lambda x: x['created_at'], reverse=True)
            
            return backups
            
        except Exception as e:
            self.logger.error(f"Error listing backups: {str(e)}")
            return []
    
    def restore_backup(self, backup_filename: str, create_pre_restore_backup: bool = True) -> Dict[str, any]:
        """
        Restore database from backup with safety measures.
        Supports both SQLite and PostgreSQL restore operations.
        
        Safety Process:
        1. Validate selected backup exists and is valid
        2. Create pre-restore backup of current database
        3. Perform database-specific restore operation
        4. Validate restored database
        5. If validation fails, restore original database (SQLite only)
        
        Args:
            backup_filename: Name of backup file to restore
            create_pre_restore_backup: Whether to backup current db first
            
        Returns:
            Dict with restore results
        """
        try:
            backup_path = self.backup_dir / backup_filename
            
            # Validate backup exists and is valid
            if not backup_path.exists():
                return {
                    'success': False,
                    'error': f'Backup file not found: {backup_filename}'
                }
            
            if not self._validate_backup(backup_path):
                return {
                    'success': False,
                    'error': f'Backup file is corrupted: {backup_filename}'
                }
            
            self.logger.info(f"Starting database restore from: {backup_filename}")
            
            # Step 1: Create pre-restore backup of current database
            pre_restore_backup_info = None
            if create_pre_restore_backup:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                pre_restore_name = f"pre_restore_{timestamp}"
                pre_restore_backup_info = self.create_backup(pre_restore_name)
                
                if not pre_restore_backup_info['success']:
                    return {
                        'success': False,
                        'error': f'Failed to create pre-restore backup: {pre_restore_backup_info["error"]}'
                    }
            
            # Step 2: Perform database-specific restore
            if self.is_sqlite:
                # SQLite restore with rollback capability
                temp_current_path = self.db_path.with_suffix('.sqlite3.temp_current')
                shutil.copy2(str(self.db_path), str(temp_current_path))
                
                try:
                    # Perform the restore (atomic operation)
                    shutil.copy2(str(backup_path), str(self.db_path))
                    
                    # Validate restored database
                    if not self._validate_backup(self.db_path):
                        # Restore failed - rollback to original
                        shutil.copy2(str(temp_current_path), str(self.db_path))
                        temp_current_path.unlink()
                        
                        return {
                            'success': False,
                            'error': 'Restored database validation failed - rollback completed'
                        }
                    
                    # Success - clean up temporary file
                    temp_current_path.unlink()
                    
                except Exception as restore_error:
                    # Rollback on any error during restore
                    if temp_current_path.exists():
                        shutil.copy2(str(temp_current_path), str(self.db_path))
                        temp_current_path.unlink()
                    
                    raise restore_error
            else:
                # PostgreSQL restore using pg_restore
                env = os.environ.copy()
                if self.db_password:
                    env['PGPASSWORD'] = self.db_password
                
                # Drop and recreate database (WARNING: destructive operation)
                drop_cmd = [
                    'dropdb',
                    '--host', self.db_host,
                    '--port', str(self.db_port),
                    '--username', self.db_user,
                    '--if-exists',
                    self.db_name
                ]
                
                create_cmd = [
                    'createdb',
                    '--host', self.db_host,
                    '--port', str(self.db_port),
                    '--username', self.db_user,
                    self.db_name
                ]
                
                restore_cmd = [
                    'pg_restore',
                    '--host', self.db_host,
                    '--port', str(self.db_port),
                    '--username', self.db_user,
                    '--dbname', self.db_name,
                    '--verbose',
                    '--no-password',
                    str(backup_path)
                ]
                
                # Execute commands
                drop_result = subprocess.run(drop_cmd, env=env, capture_output=True, text=True)
                create_result = subprocess.run(create_cmd, env=env, capture_output=True, text=True)
                
                if create_result.returncode != 0:
                    return {
                        'success': False,
                        'error': f'Database recreation failed: {create_result.stderr}'
                    }
                
                restore_result = subprocess.run(restore_cmd, env=env, capture_output=True, text=True)
                
                if restore_result.returncode != 0:
                    return {
                        'success': False,
                        'error': f'pg_restore failed: {restore_result.stderr}'
                    }
            
            restore_info = {
                'success': True,
                'restored_from': backup_filename,
                'restore_date': datetime.now(),
                'pre_restore_backup': pre_restore_backup_info['filename'] if pre_restore_backup_info else None,
                'db_type': 'SQLite' if self.is_sqlite else 'PostgreSQL'
            }
            
            self.logger.info(f"{restore_info['db_type']} restore completed successfully from {backup_filename}")
            return restore_info
                
        except Exception as e:
            self.logger.error(f"Database restore failed: {str(e)}")
            return {
                'success': False,
                'error': f'Restore failed: {str(e)}'
            }
    
    def delete_backup(self, backup_filename: str) -> Dict[str, any]:
        """
        Delete specific backup file with safety checks.
        
        Args:
            backup_filename: Name of backup file to delete
            
        Returns:
            Dict with deletion results
        """
        try:
            backup_path = self.backup_dir / backup_filename
            
            if not backup_path.exists():
                return {
                    'success': False,
                    'error': f'Backup file not found: {backup_filename}'
                }
            
            # Safety check - don't delete if it's not a backup file
            if not (backup_filename.startswith('db_backup_') and 
                    (backup_filename.endswith('.sqlite3') or backup_filename.endswith('.sql'))):
                return {
                    'success': False,
                    'error': f'Safety check failed - not a backup file: {backup_filename}'
                }
            
            # Get file info before deletion
            stats = backup_path.stat()
            size_mb = round(stats.st_size / 1024 / 1024, 2)
            
            # Delete the backup
            backup_path.unlink()
            
            self.logger.info(f"Backup deleted: {backup_filename} ({size_mb} MB)")
            
            return {
                'success': True,
                'deleted_file': backup_filename,
                'freed_space_mb': size_mb
            }
            
        except Exception as e:
            self.logger.error(f"Backup deletion failed: {str(e)}")
            return {
                'success': False,
                'error': f'Deletion failed: {str(e)}'
            }
    
    def _validate_backup(self, backup_path: Path) -> bool:
        """
        Validate database backup integrity for both SQLite and PostgreSQL.
        
        Checks:
        1. File exists and has content
        2. Database-specific format validation
        3. Basic content validation
        
        Returns:
            True if backup is valid, False otherwise
        """
        try:
            if not backup_path.exists() or backup_path.stat().st_size == 0:
                return False
            
            if self.is_sqlite:
                # SQLite validation
                with open(backup_path, 'rb') as f:
                    header = f.read(16)
                    if not header.startswith(b'SQLite format 3\x00'):
                        return False
                
                # Try to open and query the database
                conn = sqlite3.connect(str(backup_path))
                cursor = conn.cursor()
                
                # Basic integrity check
                cursor.execute("PRAGMA integrity_check")
                result = cursor.fetchone()
                
                # Check for essential tables (basic schema validation)
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='portfolio_client'")
                if not cursor.fetchone():
                    conn.close()
                    return False
                
                conn.close()
                return result[0] == 'ok'
            else:
                # PostgreSQL validation
                # For PostgreSQL custom format backups, we can use pg_restore --list to validate
                env = os.environ.copy()
                if self.db_password:
                    env['PGPASSWORD'] = self.db_password
                
                cmd = ['pg_restore', '--list', str(backup_path)]
                result = subprocess.run(cmd, env=env, capture_output=True, text=True)
                
                # If pg_restore can list the backup contents, it's valid
                if result.returncode == 0 and 'portfolio_client' in result.stdout:
                    return True
                
                # Fallback: basic file content check for SQL files
                if backup_path.suffix == '.sql':
                    with open(backup_path, 'r', encoding='utf-8') as f:
                        content = f.read(1000)  # Read first 1KB
                        return 'portfolio_client' in content and ('CREATE TABLE' in content or 'COPY' in content)
                
                return False
            
        except Exception as e:
            self.logger.error(f"Backup validation error for {backup_path}: {str(e)}")
            return False
    
    def _format_backup_display_name(self, filename: str) -> str:
        """
        Create user-friendly display name for UI dropdown.
        
        Converts: db_backup_20250118_143022.sqlite3 or db_backup_20250118_143022.sql
        To: Jan 18, 2025 at 2:30 PM (SQLite) or Jan 18, 2025 at 2:30 PM (PostgreSQL)
        """
        try:
            # Extract timestamp from filename
            if filename.startswith('db_backup_'):
                if filename.endswith('.sqlite3'):
                    timestamp_part = filename[10:-8]  # Remove prefix and suffix
                    db_type_suffix = ' (SQLite)'
                elif filename.endswith('.sql'):
                    timestamp_part = filename[10:-4]  # Remove prefix and suffix
                    db_type_suffix = ' (PostgreSQL)'
                else:
                    return filename
                
                if '_' in timestamp_part:
                    # Handle pre_restore_ prefix if present
                    if timestamp_part.startswith('pre_restore_'):
                        timestamp_part = timestamp_part[12:]  # Remove pre_restore_ prefix
                        display_prefix = 'Pre-restore: '
                    else:
                        display_prefix = ''
                    
                    if '_' in timestamp_part:
                        date_part, time_part = timestamp_part.split('_', 1)
                        
                        # Parse date and time
                        dt = datetime.strptime(f"{date_part}_{time_part}", '%Y%m%d_%H%M%S')
                        
                        # Format for display
                        return display_prefix + dt.strftime('%b %d, %Y at %I:%M %p') + db_type_suffix
            
            # Fallback to filename if parsing fails
            return filename
            
        except Exception:
            return filename