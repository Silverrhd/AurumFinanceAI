"""
Health Check System for Aurum Finance
Comprehensive system health monitoring and status checks.
"""

import os
import time
import logging
from pathlib import Path
from typing import Dict, List, Optional
from django.conf import settings
from django.db import connection
from django.core.cache import cache
from django.utils import timezone
from datetime import datetime, timedelta

# Optional dependency handling
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    psutil = None

logger = logging.getLogger(__name__)


class HealthChecker:
    """
    Comprehensive health check system for all application components.
    """
    
    def __init__(self):
        self.checks = {
            'database': self._check_database,
            'storage': self._check_storage,
            'services': self._check_services,
            'system': self._check_system,
            'cache': self._check_cache,
        }
    
    def run_all_checks(self) -> Dict:
        """Run all health checks and return comprehensive status."""
        start_time = time.time()
        results = {
            'status': 'healthy',
            'timestamp': timezone.now().isoformat(),
            'checks': {},
            'summary': {
                'total_checks': len(self.checks),
                'passed': 0,
                'failed': 0,
                'warnings': 0
            }
        }
        
        overall_healthy = True
        
        for check_name, check_func in self.checks.items():
            try:
                check_result = check_func()
                results['checks'][check_name] = check_result
                
                if check_result['status'] == 'healthy':
                    results['summary']['passed'] += 1
                elif check_result['status'] == 'warning':
                    results['summary']['warnings'] += 1
                else:
                    results['summary']['failed'] += 1
                    overall_healthy = False
                    
            except Exception as e:
                logger.error(f"Health check {check_name} failed: {str(e)}")
                results['checks'][check_name] = {
                    'status': 'error',
                    'message': f'Check failed: {str(e)}',
                    'timestamp': timezone.now().isoformat()
                }
                results['summary']['failed'] += 1
                overall_healthy = False
        
        results['status'] = 'healthy' if overall_healthy else 'unhealthy'
        results['response_time'] = round((time.time() - start_time) * 1000, 2)  # ms
        
        return results
    
    def run_single_check(self, check_name: str) -> Dict:
        """Run a single health check."""
        if check_name not in self.checks:
            return {
                'status': 'error',
                'message': f'Unknown check: {check_name}',
                'timestamp': timezone.now().isoformat()
            }
        
        try:
            return self.checks[check_name]()
        except Exception as e:
            logger.error(f"Health check {check_name} failed: {str(e)}")
            return {
                'status': 'error',
                'message': f'Check failed: {str(e)}',
                'timestamp': timezone.now().isoformat()
            }
    
    def _check_database(self) -> Dict:
        """Check database connectivity and performance."""
        start_time = time.time()
        
        try:
            # Test basic connectivity
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            
            # Test query performance
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM django_migrations")
                migration_count = cursor.fetchone()[0]
            
            # Check database size (PostgreSQL specific)
            db_size = None
            try:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT pg_size_pretty(pg_database_size(current_database()))
                    """)
                    db_size = cursor.fetchone()[0]
            except Exception:
                pass  # Not PostgreSQL or insufficient permissions
            
            query_time = round((time.time() - start_time) * 1000, 2)
            
            # Determine status based on query time
            if query_time > 1000:  # > 1 second
                status = 'warning'
                message = f'Database responding slowly ({query_time}ms)'
            else:
                status = 'healthy'
                message = 'Database connectivity OK'
            
            return {
                'status': status,
                'message': message,
                'details': {
                    'query_time_ms': query_time,
                    'migrations_count': migration_count,
                    'database_size': db_size,
                    'engine': settings.DATABASES['default']['ENGINE']
                },
                'timestamp': timezone.now().isoformat()
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Database connection failed: {str(e)}',
                'timestamp': timezone.now().isoformat()
            }
    
    def _check_storage(self) -> Dict:
        """Check file system storage and directory access."""
        try:
            issues = []
            storage_info = {}
            
            # Check critical directories
            critical_dirs = [
                settings.AURUM_SETTINGS['DATA_DIR'],
                settings.AURUM_SETTINGS['REPORTS_DIR'],
                settings.BACKUP_SETTINGS['BACKUP_LOCATION'],
                settings.BASE_DIR / 'logs',
                settings.MEDIA_ROOT,
            ]
            
            for directory in critical_dirs:
                dir_path = Path(directory)
                dir_name = dir_path.name
                
                if not dir_path.exists():
                    issues.append(f'Directory {dir_name} does not exist')
                    continue
                
                if not os.access(dir_path, os.R_OK):
                    issues.append(f'Directory {dir_name} is not readable')
                
                if not os.access(dir_path, os.W_OK):
                    issues.append(f'Directory {dir_name} is not writable')
                
                # Get directory size and file count
                try:
                    total_size = sum(f.stat().st_size for f in dir_path.rglob('*') if f.is_file())
                    file_count = len(list(dir_path.rglob('*')))
                    
                    storage_info[dir_name] = {
                        'size_mb': round(total_size / (1024 * 1024), 2),
                        'file_count': file_count,
                        'path': str(dir_path)
                    }
                except Exception as e:
                    storage_info[dir_name] = {'error': str(e)}
            
            # Check disk space (if psutil is available)
            if PSUTIL_AVAILABLE:
                disk_usage = psutil.disk_usage(str(settings.BASE_DIR))
                disk_free_gb = disk_usage.free / (1024**3)
                disk_used_percent = (disk_usage.used / disk_usage.total) * 100
                
                if disk_used_percent > 90:
                    issues.append(f'Disk usage critical: {disk_used_percent:.1f}%')
                    status = 'error'
                elif disk_used_percent > 80:
                    issues.append(f'Disk usage high: {disk_used_percent:.1f}%')
                    status = 'warning'
                else:
                    status = 'healthy'
            else:
                disk_free_gb = 0
                disk_used_percent = 0
                status = 'healthy'
            
            if issues and status == 'healthy':
                status = 'warning'
            
            message = 'Storage OK' if not issues else f'{len(issues)} storage issues found'
            
            return {
                'status': status,
                'message': message,
                'details': {
                    'directories': storage_info,
                    'disk_usage_percent': round(disk_used_percent, 1),
                    'disk_free_gb': round(disk_free_gb, 2),
                    'issues': issues
                },
                'timestamp': timezone.now().isoformat()
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Storage check failed: {str(e)}',
                'timestamp': timezone.now().isoformat()
            }
    
    def _check_services(self) -> Dict:
        """Check business logic services functionality."""
        try:
            from .services.processing_service import ProcessingService
            from .services.database_update_service import DatabaseUpdateService
            from .services.report_generation_service import ReportGenerationService
            
            services_status = {}
            issues = []
            
            # Test ProcessingService
            try:
                processing_service = ProcessingService()
                supported_banks = processing_service.get_supported_banks()
                services_status['processing'] = {
                    'status': 'healthy',
                    'supported_banks': len(supported_banks),
                    'banks': supported_banks[:5]  # First 5 banks
                }
            except Exception as e:
                services_status['processing'] = {'status': 'error', 'error': str(e)}
                issues.append(f'ProcessingService: {str(e)}')
            
            # Test DatabaseUpdateService
            try:
                db_service = DatabaseUpdateService()
                db_status = db_service.get_database_status()
                services_status['database_update'] = {
                    'status': 'healthy' if db_status.get('status') == 'healthy' else 'warning',
                    'details': db_status
                }
            except Exception as e:
                services_status['database_update'] = {'status': 'error', 'error': str(e)}
                issues.append(f'DatabaseUpdateService: {str(e)}')
            
            # Test ReportGenerationService
            try:
                report_service = ReportGenerationService()
                # Just test initialization
                services_status['report_generation'] = {'status': 'healthy'}
            except Exception as e:
                services_status['report_generation'] = {'status': 'error', 'error': str(e)}
                issues.append(f'ReportGenerationService: {str(e)}')
            
            # Overall status
            if issues:
                status = 'error' if any('error' in s.get('status', '') for s in services_status.values()) else 'warning'
                message = f'{len(issues)} service issues found'
            else:
                status = 'healthy'
                message = 'All services operational'
            
            return {
                'status': status,
                'message': message,
                'details': {
                    'services': services_status,
                    'issues': issues
                },
                'timestamp': timezone.now().isoformat()
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Services check failed: {str(e)}',
                'timestamp': timezone.now().isoformat()
            }
    
    def _check_system(self) -> Dict:
        """Check system resources (CPU, memory, etc.)."""
        try:
            if not PSUTIL_AVAILABLE:
                return {
                    'status': 'warning',
                    'message': 'System monitoring unavailable (psutil not installed)',
                    'details': {
                        'psutil_available': False,
                        'recommendation': 'Install psutil for system monitoring: pip install psutil'
                    },
                    'timestamp': timezone.now().isoformat()
                }
            
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            memory_available_gb = memory.available / (1024**3)
            
            # Load average (Unix systems)
            load_avg = None
            try:
                load_avg = os.getloadavg()
            except (OSError, AttributeError):
                pass  # Not available on Windows
            
            # Process count
            process_count = len(psutil.pids())
            
            # Determine status
            issues = []
            if cpu_percent > 90:
                issues.append(f'High CPU usage: {cpu_percent}%')
            if memory_percent > 90:
                issues.append(f'High memory usage: {memory_percent}%')
            if memory_available_gb < 0.5:  # Less than 500MB available
                issues.append(f'Low available memory: {memory_available_gb:.1f}GB')
            
            if any('High' in issue for issue in issues):
                status = 'error'
            elif issues:
                status = 'warning'
            else:
                status = 'healthy'
            
            message = 'System resources OK' if not issues else f'{len(issues)} resource issues'
            
            return {
                'status': status,
                'message': message,
                'details': {
                    'cpu_percent': cpu_percent,
                    'memory_percent': memory_percent,
                    'memory_available_gb': round(memory_available_gb, 2),
                    'load_average': load_avg,
                    'process_count': process_count,
                    'issues': issues
                },
                'timestamp': timezone.now().isoformat()
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f'System check failed: {str(e)}',
                'timestamp': timezone.now().isoformat()
            }
    
    def _check_cache(self) -> Dict:
        """Check cache system functionality."""
        try:
            # Test cache write/read
            test_key = 'health_check_test'
            test_value = f'test_{int(time.time())}'
            
            cache.set(test_key, test_value, 60)
            retrieved_value = cache.get(test_key)
            
            if retrieved_value == test_value:
                status = 'healthy'
                message = 'Cache system operational'
            else:
                status = 'error'
                message = 'Cache read/write test failed'
            
            # Clean up test key
            cache.delete(test_key)
            
            return {
                'status': status,
                'message': message,
                'details': {
                    'backend': settings.CACHES['default']['BACKEND'],
                    'test_successful': retrieved_value == test_value
                },
                'timestamp': timezone.now().isoformat()
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Cache check failed: {str(e)}',
                'timestamp': timezone.now().isoformat()
            }


def get_basic_health() -> Dict:
    """Get basic health status quickly."""
    try:
        # Quick database check
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        
        return {
            'status': 'healthy',
            'message': 'System operational',
            'timestamp': timezone.now().isoformat()
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': f'System unhealthy: {str(e)}',
            'timestamp': timezone.now().isoformat()
        }


def get_detailed_health() -> Dict:
    """Get comprehensive health status."""
    checker = HealthChecker()
    return checker.run_all_checks()


def get_single_check(check_name: str) -> Dict:
    """Get status for a single health check."""
    checker = HealthChecker()
    return checker.run_single_check(check_name)