#!/usr/bin/env python3
"""
Backup Scheduler for Aurum Finance
Automated scheduling and execution of database backups with different frequencies.
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Optional dependency handling
try:
    import schedule
    SCHEDULE_AVAILABLE = True
except ImportError:
    SCHEDULE_AVAILABLE = False
    schedule = None

# Add Django project to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'aurum_backend.settings')

import django
django.setup()

from django.conf import settings
from .backup_database import DatabaseBackup


class BackupScheduler:
    """
    Automated backup scheduler with configurable frequencies.
    """
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.backup = DatabaseBackup()
        self.schedule_settings = getattr(settings, 'BACKUP_SCHEDULE', {})
        self._set_defaults()
        self._setup_schedules()
    
    def _set_defaults(self):
        """Set default scheduling configuration."""
        defaults = {
            'DAILY_BACKUP_TIME': '02:00',      # 2 AM daily
            'WEEKLY_BACKUP_DAY': 'sunday',     # Sunday weekly
            'WEEKLY_BACKUP_TIME': '03:00',     # 3 AM on Sunday
            'MONTHLY_BACKUP_DAY': 1,           # 1st of month
            'MONTHLY_BACKUP_TIME': '04:00',    # 4 AM on 1st
            'ENABLE_DAILY': True,
            'ENABLE_WEEKLY': True,
            'ENABLE_MONTHLY': True,
            'AUTO_CLEANUP': True,
            'CLEANUP_FREQUENCY': 'daily',      # Run cleanup daily
        }
        
        for key, value in defaults.items():
            if key not in self.schedule_settings:
                self.schedule_settings[key] = value
    
    def _setup_logging(self) -> logging.Logger:
        """Configure logging for scheduler."""
        logger = logging.getLogger('aurum_scheduler')
        logger.setLevel(logging.INFO)
        
        # Create logs directory if it doesn't exist
        log_dir = settings.BASE_DIR / 'logs'
        log_dir.mkdir(exist_ok=True)
        
        # File handler with rotation
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            log_dir / 'scheduler.log',
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
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
    
    def _setup_schedules(self):
        """Set up backup schedules based on configuration."""
        if not SCHEDULE_AVAILABLE:
            self.logger.warning("Schedule library not available. Install with: pip install schedule")
            return
        
        # Daily backups
        if self.schedule_settings['ENABLE_DAILY']:
            schedule.every().day.at(self.schedule_settings['DAILY_BACKUP_TIME']).do(
                self._run_daily_backup
            )
            self.logger.info(f"Daily backup scheduled at {self.schedule_settings['DAILY_BACKUP_TIME']}")
        
        # Weekly backups
        if self.schedule_settings['ENABLE_WEEKLY']:
            day = self.schedule_settings['WEEKLY_BACKUP_DAY'].lower()
            time_str = self.schedule_settings['WEEKLY_BACKUP_TIME']
            
            getattr(schedule.every(), day).at(time_str).do(self._run_weekly_backup)
            self.logger.info(f"Weekly backup scheduled on {day} at {time_str}")
        
        # Monthly backups (simplified - runs on 1st of month if it's the scheduled day)
        if self.schedule_settings['ENABLE_MONTHLY']:
            schedule.every().day.at(self.schedule_settings['MONTHLY_BACKUP_TIME']).do(
                self._check_monthly_backup
            )
            self.logger.info(f"Monthly backup check scheduled daily at {self.schedule_settings['MONTHLY_BACKUP_TIME']}")
        
        # Cleanup schedule
        if self.schedule_settings['AUTO_CLEANUP']:
            if self.schedule_settings['CLEANUP_FREQUENCY'] == 'daily':
                schedule.every().day.at("05:00").do(self._run_cleanup)
            elif self.schedule_settings['CLEANUP_FREQUENCY'] == 'weekly':
                schedule.every().sunday.at("05:00").do(self._run_cleanup)
            
            self.logger.info(f"Cleanup scheduled {self.schedule_settings['CLEANUP_FREQUENCY']}")
    
    def _run_daily_backup(self):
        """Execute daily backup."""
        try:
            self.logger.info("Starting scheduled daily backup")
            result = self.backup.create_backup('daily')
            
            if result['success']:
                self.logger.info(f"Daily backup completed: {result['backup_size_mb']:.2f} MB")
            else:
                self.logger.error(f"Daily backup failed: {result['error']}")
                self._send_alert('Daily backup failed', result['error'])
        
        except Exception as e:
            self.logger.error(f"Daily backup error: {str(e)}")
            self._send_alert('Daily backup error', str(e))
    
    def _run_weekly_backup(self):
        """Execute weekly backup."""
        try:
            self.logger.info("Starting scheduled weekly backup")
            result = self.backup.create_backup('weekly')
            
            if result['success']:
                self.logger.info(f"Weekly backup completed: {result['backup_size_mb']:.2f} MB")
            else:
                self.logger.error(f"Weekly backup failed: {result['error']}")
                self._send_alert('Weekly backup failed', result['error'])
        
        except Exception as e:
            self.logger.error(f"Weekly backup error: {str(e)}")
            self._send_alert('Weekly backup error', str(e))
    
    def _check_monthly_backup(self):
        """Check if monthly backup should run (on 1st of month)."""
        today = datetime.now()
        if today.day == self.schedule_settings['MONTHLY_BACKUP_DAY']:
            self._run_monthly_backup()
    
    def _run_monthly_backup(self):
        """Execute monthly backup."""
        try:
            self.logger.info("Starting scheduled monthly backup")
            result = self.backup.create_backup('monthly')
            
            if result['success']:
                self.logger.info(f"Monthly backup completed: {result['backup_size_mb']:.2f} MB")
            else:
                self.logger.error(f"Monthly backup failed: {result['error']}")
                self._send_alert('Monthly backup failed', result['error'])
        
        except Exception as e:
            self.logger.error(f"Monthly backup error: {str(e)}")
            self._send_alert('Monthly backup error', str(e))
    
    def _run_cleanup(self):
        """Execute backup cleanup."""
        try:
            self.logger.info("Starting scheduled backup cleanup")
            result = self.backup.cleanup_old_backups()
            
            if result['success']:
                self.logger.info(f"Cleanup completed: {result['deleted_count']} files deleted")
            else:
                self.logger.error(f"Cleanup failed: {result['error']}")
        
        except Exception as e:
            self.logger.error(f"Cleanup error: {str(e)}")
    
    def _send_alert(self, subject: str, message: str):
        """Send alert notification for backup failures."""
        try:
            # This can be extended to send emails, Slack notifications, etc.
            alert_message = f"BACKUP ALERT: {subject}\n\nDetails: {message}\n\nTime: {datetime.now()}"
            
            # Log the alert
            self.logger.critical(alert_message)
            
            # TODO: Implement actual alerting (email, Slack, etc.)
            # For now, just ensure it's logged prominently
            
        except Exception as e:
            self.logger.error(f"Failed to send alert: {str(e)}")
    
    def run_scheduler(self):
        """Run the backup scheduler continuously."""
        if not SCHEDULE_AVAILABLE:
            self.logger.error("Cannot run scheduler: schedule library not available")
            print("Error: schedule library not installed. Install with: pip install schedule")
            return
            
        self.logger.info("Starting Aurum Finance Backup Scheduler")
        self.logger.info(f"Scheduled jobs: {len(schedule.jobs)}")
        
        # Log next scheduled runs
        for job in schedule.jobs:
            self.logger.info(f"Next run: {job.next_run} - {job.job_func.__name__}")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        
        except KeyboardInterrupt:
            self.logger.info("Backup scheduler stopped by user")
        except Exception as e:
            self.logger.error(f"Scheduler error: {str(e)}")
            raise
    
    def run_once(self, backup_type: str = 'manual'):
        """Run a single backup immediately."""
        self.logger.info(f"Running manual {backup_type} backup")
        
        if backup_type == 'daily':
            self._run_daily_backup()
        elif backup_type == 'weekly':
            self._run_weekly_backup()
        elif backup_type == 'monthly':
            self._run_monthly_backup()
        elif backup_type == 'cleanup':
            self._run_cleanup()
        else:
            # Manual backup
            result = self.backup.create_backup('manual')
            if result['success']:
                self.logger.info(f"Manual backup completed: {result['backup_size_mb']:.2f} MB")
                print(f"Backup created: {result['backup_path']}")
            else:
                self.logger.error(f"Manual backup failed: {result['error']}")
                print(f"Backup failed: {result['error']}")
    
    def status(self):
        """Show scheduler status and next scheduled runs."""
        print(f"\nAurum Finance Backup Scheduler Status")
        print(f"=====================================")
        print(f"Total scheduled jobs: {len(schedule.jobs)}")
        print()
        
        if not schedule.jobs:
            print("No jobs scheduled.")
            return
        
        for job in schedule.jobs:
            print(f"Job: {job.job_func.__name__}")
            print(f"Next run: {job.next_run}")
            print(f"Interval: {job.interval} {job.unit}")
            print()


def main():
    """Main scheduler execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Aurum Finance Backup Scheduler')
    parser.add_argument('--daemon', action='store_true', 
                       help='Run scheduler as daemon')
    parser.add_argument('--once', choices=['daily', 'weekly', 'monthly', 'manual', 'cleanup'],
                       help='Run single backup operation')
    parser.add_argument('--status', action='store_true',
                       help='Show scheduler status')
    
    args = parser.parse_args()
    
    scheduler = BackupScheduler()
    
    if args.status:
        scheduler.status()
        return
    
    if args.once:
        scheduler.run_once(args.once)
        return
    
    if args.daemon:
        scheduler.run_scheduler()
    else:
        print("Use --daemon to run scheduler, --once for single backup, or --status for info")


if __name__ == '__main__':
    main()