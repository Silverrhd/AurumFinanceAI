"""
Configuration helpers that provide AurumFinance configuration functionality.
Provides client management and configuration functions using Django models.
"""
import logging
from django.contrib.auth import get_user_model
from ..models import User

logger = logging.getLogger(__name__)

def get_active_clients():
    """
    Get all active client codes from the database.
    
    Returns:
        QuerySet: Client codes for all active clients
    """
    try:
        client_codes = User.objects.filter(role='client').values_list('client_code', flat=True)
        logger.info(f"Found {len(client_codes)} active clients")
        return list(client_codes)
    except Exception as e:
        logger.error(f"Error getting active clients: {str(e)}")
        return []

def get_client_name(client_code):
    """
    Get client display name from client_code.
    
    Args:
        client_code (str): The client code to lookup
        
    Returns:
        str: Client display name or client_code if not found
    """
    try:
        user = User.objects.filter(client_code=client_code, role='client').first()
        if user:
            full_name = f"{user.first_name} {user.last_name}".strip()
            return full_name if full_name else user.username
        return client_code
    except Exception as e:
        logger.error(f"Error getting client name for {client_code}: {str(e)}")
        return client_code

def validate_client_code(client_code):
    """
    Validate that client_code exists in the system.
    
    Args:
        client_code (str): The client code to validate
        
    Returns:
        bool: True if client exists and is active, False otherwise
    """
    try:
        exists = User.objects.filter(client_code=client_code, role='client').exists()
        logger.debug(f"Client code {client_code} validation: {exists}")
        return exists
    except Exception as e:
        logger.error(f"Error validating client code {client_code}: {str(e)}")
        return False

def get_all_client_codes():
    """
    Get all client codes (alias for get_active_clients for compatibility).
    
    Returns:
        list: List of all active client codes
    """
    return get_active_clients()