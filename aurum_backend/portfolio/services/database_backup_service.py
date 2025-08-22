"""
SQLite Database Backup and Restore Service for Aurum Finance
Provides on-demand backup creation and UI-driven restore functionality.
"""

import os
import shutil
import sqlite3
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from django.conf import settings

logger = logging.getLogger(__name__)

class DatabaseBackupService:
    """
    SQLite-specific backup and restore operations with safety features.
    
    Key Safety Features:
    1. Uses SQLite .backup() API for consistent snapshots
    2. Validates backup integrity before operations
    3. Creates pre-restore backups automatically
    4. Atomic operations (all-or-nothing)
    """
    
    def __init__(self):
        self.db_path = Path(settings.DATABASES['default']['NAME'])
        self.backup_dir = self.db_path.parent  # Same directory as database
        self.logger = logger
        
        # Ensure we're working with SQLite
        if not str(self.db_path).endswith('.sqlite3'):
            raise ValueError("This service only works with SQLite databases")
    
    def create_backup(self, backup_name: Optional[str] = None) -> Dict[str, any]:
        """
        Create a consistent SQLite backup using native backup API.
        
        Args:
            backup_name: Optional custom name (defaults to timestamp)
            
        Returns:
            Dict with backup results and metadata
        """
        try:
            # Generate backup filename
            if backup_name:
                backup_filename = f"db_backup_{backup_name}.sqlite3"
            else:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                backup_filename = f"db_backup_{timestamp}.sqlite3"
            
            backup_path = self.backup_dir / backup_filename
            
            # Ensure backup doesn't already exist
            if backup_path.exists():
                return {
                    'success': False,
                    'error': f'Backup file already exists: {backup_filename}'
                }
            
            self.logger.info(f"Creating database backup: {backup_filename}")
            
            # Create backup using SQLite backup API
            # This method ensures consistency even with active connections
            source_conn = sqlite3.connect(str(self.db_path))
            backup_conn = sqlite3.connect(str(backup_path))
            
            # Perform the backup
            source_conn.backup(backup_conn)
            
            # Close connections
            source_conn.close()
            backup_conn.close()
            
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
                'display_name': self._format_backup_display_name(backup_filename)
            }
            
            self.logger.info(f"Backup created successfully: {backup_info['size_mb']} MB")
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
            backup_files = list(self.backup_dir.glob('db_backup_*.sqlite3'))
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
        
        Safety Process:
        1. Validate selected backup exists and is valid
        2. Create pre-restore backup of current database
        3. Perform atomic restore operation
        4. Validate restored database
        5. If validation fails, restore original database
        
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
            
            # Step 2: Create temporary backup of current database for rollback
            temp_current_path = self.db_path.with_suffix('.sqlite3.temp_current')
            shutil.copy2(str(self.db_path), str(temp_current_path))
            
            try:
                # Step 3: Perform the restore (atomic operation)
                shutil.copy2(str(backup_path), str(self.db_path))
                
                # Step 4: Validate restored database
                if not self._validate_backup(self.db_path):
                    # Restore failed - rollback to original
                    shutil.copy2(str(temp_current_path), str(self.db_path))
                    temp_current_path.unlink()
                    
                    return {
                        'success': False,
                        'error': 'Restored database validation failed - rollback completed'
                    }
                
                # Step 5: Success - clean up temporary file
                temp_current_path.unlink()
                
                restore_info = {
                    'success': True,
                    'restored_from': backup_filename,
                    'restore_date': datetime.now(),
                    'pre_restore_backup': pre_restore_backup_info['filename'] if pre_restore_backup_info else None
                }
                
                self.logger.info(f"Database restore completed successfully from {backup_filename}")
                return restore_info
                
            except Exception as restore_error:
                # Rollback on any error during restore
                if temp_current_path.exists():
                    shutil.copy2(str(temp_current_path), str(self.db_path))
                    temp_current_path.unlink()
                
                raise restore_error
                
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
            if not backup_filename.startswith('db_backup_'):
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
        Validate SQLite database integrity.
        
        Checks:
        1. File exists and has content
        2. SQLite header is valid
        3. Database can be opened and queried
        4. Basic schema validation
        
        Returns:
            True if backup is valid, False otherwise
        """
        try:
            if not backup_path.exists() or backup_path.stat().st_size == 0:
                return False
            
            # Check SQLite file signature
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
            
        except Exception as e:
            self.logger.error(f"Backup validation error for {backup_path}: {str(e)}")
            return False
    
    def _format_backup_display_name(self, filename: str) -> str:
        """
        Create user-friendly display name for UI dropdown.
        
        Converts: db_backup_20250118_143022.sqlite3
        To: Jan 18, 2025 at 2:30 PM
        """
        try:
            # Extract timestamp from filename
            if filename.startswith('db_backup_') and filename.endswith('.sqlite3'):
                timestamp_part = filename[10:-8]  # Remove prefix and suffix
                
                if '_' in timestamp_part:
                    date_part, time_part = timestamp_part.split('_', 1)
                    
                    # Parse date and time
                    dt = datetime.strptime(f"{date_part}_{time_part}", '%Y%m%d_%H%M%S')
                    
                    # Format for display
                    return dt.strftime('%b %d, %Y at %I:%M %p')
            
            # Fallback to filename if parsing fails
            return filename
            
        except Exception:
            return filename