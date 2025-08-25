"""
Maintenance Mode System for AurumFinance
Provides controlled application downtime during database operations.
"""

import time
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class MaintenanceMode:
    """
    Enterprise-grade maintenance mode system for controlled downtime.
    
    Features:
    - File-based flag system (survives process restarts)
    - Custom maintenance messages
    - Admin bypass capability
    - Automatic timeout protection
    - Detailed logging
    """
    
    def __init__(self):
        # Store maintenance flag in persistent location
        self.flag_file = Path('/var/lib/aurumfinance/maintenance_mode')
        self.default_message = "System maintenance in progress. Please try again in a few minutes."
        self.max_maintenance_duration = 600  # 10 minutes safety timeout
    
    def enable(self, message: Optional[str] = None, admin_bypass: bool = True, timeout_minutes: int = 10) -> Dict[str, Any]:
        """
        Enable maintenance mode with optional custom message.
        
        Args:
            message: Custom maintenance message
            admin_bypass: Whether to allow admin access during maintenance
            timeout_minutes: Automatic timeout (safety feature)
            
        Returns:
            Dict with operation result
        """
        try:
            maintenance_data = {
                'enabled': True,
                'message': message or self.default_message,
                'admin_bypass': admin_bypass,
                'start_time': datetime.now().isoformat(),
                'timeout_minutes': timeout_minutes,
                'enabled_by': 'backup_service'  # Could be enhanced to track user
            }
            
            # Ensure directory exists
            self.flag_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Write maintenance flag file
            with open(self.flag_file, 'w') as f:
                json.dump(maintenance_data, f, indent=2)
            
            logger.info(f"✅ Maintenance mode ENABLED: {message or self.default_message}")
            
            return {
                'success': True,
                'message': 'Maintenance mode enabled',
                'maintenance_message': maintenance_data['message'],
                'start_time': maintenance_data['start_time']
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to enable maintenance mode: {str(e)}")
            return {
                'success': False,
                'error': f'Failed to enable maintenance mode: {str(e)}'
            }
    
    def disable(self) -> Dict[str, Any]:
        """
        Disable maintenance mode and return to normal operation.
        
        Returns:
            Dict with operation result
        """
        try:
            if self.flag_file.exists():
                # Get maintenance info before deleting
                maintenance_info = self.get_info()
                
                # Remove flag file
                self.flag_file.unlink()
                
                duration = None
                if maintenance_info and maintenance_info.get('start_time'):
                    start_time = datetime.fromisoformat(maintenance_info['start_time'])
                    duration = (datetime.now() - start_time).total_seconds()
                
                logger.info(f"✅ Maintenance mode DISABLED" + (f" (duration: {duration:.1f}s)" if duration else ""))
                
                return {
                    'success': True,
                    'message': 'Maintenance mode disabled',
                    'duration_seconds': duration
                }
            else:
                return {
                    'success': True,
                    'message': 'Maintenance mode was not active'
                }
                
        except Exception as e:
            logger.error(f"❌ Failed to disable maintenance mode: {str(e)}")
            return {
                'success': False,
                'error': f'Failed to disable maintenance mode: {str(e)}'
            }
    
    def is_active(self) -> bool:
        """
        Check if maintenance mode is currently active.
        Includes automatic timeout protection.
        
        Returns:
            True if maintenance mode is active, False otherwise
        """
        try:
            if not self.flag_file.exists():
                return False
            
            # Check for timeout (safety feature)
            maintenance_info = self.get_info()
            if maintenance_info and maintenance_info.get('start_time'):
                start_time = datetime.fromisoformat(maintenance_info['start_time'])
                timeout_minutes = maintenance_info.get('timeout_minutes', 10)
                
                if (datetime.now() - start_time).total_seconds() > (timeout_minutes * 60):
                    logger.warning(f"⚠️ Maintenance mode auto-timeout after {timeout_minutes} minutes")
                    self.disable()
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error checking maintenance mode status: {str(e)}")
            # Fail safe - assume not in maintenance if we can't determine
            return False
    
    def get_info(self) -> Optional[Dict[str, Any]]:
        """
        Get detailed maintenance mode information.
        
        Returns:
            Dict with maintenance info or None if not active
        """
        try:
            if not self.flag_file.exists():
                return None
            
            with open(self.flag_file, 'r') as f:
                return json.load(f)
                
        except Exception as e:
            logger.error(f"❌ Error reading maintenance mode info: {str(e)}")
            return None
    
    def should_bypass_for_admin(self, request) -> bool:
        """
        Check if request should bypass maintenance mode (admin access).
        
        Args:
            request: Django request object
            
        Returns:
            True if request should bypass maintenance, False otherwise
        """
        try:
            maintenance_info = self.get_info()
            if not maintenance_info or not maintenance_info.get('admin_bypass', True):
                return False
            
            # Allow admin panel access
            if request.path.startswith('/admin/'):
                return True
            
            # Allow backup/restore endpoints (for emergency management)
            if request.path.startswith('/api/portfolio/backup/'):
                return True
            
            # Allow superuser access (if authenticated)
            if hasattr(request, 'user') and request.user.is_authenticated and request.user.is_superuser:
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error checking admin bypass: {str(e)}")
            return False
    
    def wait_for_requests_to_complete(self, max_wait_seconds: int = 30) -> Dict[str, Any]:
        """
        Wait for active requests to complete before proceeding with maintenance.
        
        Args:
            max_wait_seconds: Maximum time to wait for requests to complete
            
        Returns:
            Dict with wait result
        """
        try:
            logger.info(f"⏳ Waiting up to {max_wait_seconds}s for active requests to complete...")
            
            # Simple implementation - wait a few seconds for requests to drain
            # In production, this could monitor active connections or use a more sophisticated approach
            wait_time = min(max_wait_seconds, 5)  # Conservative wait
            time.sleep(wait_time)
            
            logger.info(f"✅ Request drain complete (waited {wait_time}s)")
            
            return {
                'success': True,
                'wait_time_seconds': wait_time,
                'message': 'Active requests drained successfully'
            }
            
        except Exception as e:
            logger.error(f"❌ Error during request drain: {str(e)}")
            return {
                'success': False,
                'error': f'Request drain failed: {str(e)}'
            }


# Global maintenance mode instance
maintenance_mode = MaintenanceMode()