"""
User Management Service for Aurum Finance Client Users
Provides reusable business logic for creating, managing, and deleting client users.
Used by both Django management commands and future API endpoints.
"""

import logging
import secrets
import string
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from django.contrib.auth import get_user_model
from django.db import transaction

from portfolio.models import Client

User = get_user_model()
logger = logging.getLogger(__name__)


class ClientUserManagementService:
    """Service class for managing client user accounts."""
    
    @staticmethod
    def generate_secure_password(length: int = 8) -> str:
        """Generate a secure random password."""
        alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
        # Ensure at least one of each character type
        password = [
            secrets.choice(string.ascii_uppercase),
            secrets.choice(string.ascii_lowercase), 
            secrets.choice(string.digits),
            secrets.choice("!@#$%^&*")
        ]
        # Fill remaining length with random choices
        for _ in range(length - 4):
            password.append(secrets.choice(alphabet))
        
        # Shuffle the password list
        secrets.SystemRandom().shuffle(password)
        return ''.join(password)
    
    @staticmethod
    def create_client_user(client_code: str, password: Optional[str] = None) -> Dict:
        """
        Create a single client user account.
        
        Args:
            client_code: The client code (e.g., 'JAV', 'BK')
            password: Optional custom password, generates secure one if not provided
            
        Returns:
            Dict with status, username, password, and message
        """
        try:
            # Get client instance
            try:
                client = Client.objects.get(code=client_code)
            except Client.DoesNotExist:
                return {
                    'status': 'error',
                    'message': f'Client with code {client_code} not found',
                    'client_code': client_code
                }
            
            username = f"{client_code}_user"
            
            # Check if user already exists
            if User.objects.filter(username=username).exists():
                return {
                    'status': 'exists',
                    'message': f'User {username} already exists',
                    'username': username,
                    'client_code': client_code
                }
            
            # Generate password if not provided
            if not password:
                password = ClientUserManagementService.generate_secure_password()
            
            # Create user
            with transaction.atomic():
                user = User.objects.create_user(
                    username=username,
                    password=password,
                    email=f"{username}@aurumfinance.com",  # Placeholder email
                    role='client',
                    client_code=client_code,
                    first_name=f"Client {client_code}",
                    last_name="User"
                )
            
            logger.info(f"Created client user: {username} for client {client_code}")
            
            return {
                'status': 'created',
                'message': f'Successfully created user {username}',
                'username': username,
                'password': password,
                'client_code': client_code,
                'user_id': user.id
            }
            
        except Exception as e:
            logger.error(f"Error creating client user {client_code}: {str(e)}")
            return {
                'status': 'error',
                'message': f'Failed to create user for {client_code}: {str(e)}',
                'client_code': client_code
            }
    
    @staticmethod
    def delete_client_user(client_code: str) -> Dict:
        """
        Delete a client user account.
        
        Args:
            client_code: The client code (e.g., 'JAV', 'BK')
            
        Returns:
            Dict with status and message
        """
        try:
            username = f"{client_code}_user"
            
            try:
                user = User.objects.get(username=username, role='client', client_code=client_code)
            except User.DoesNotExist:
                return {
                    'status': 'not_found',
                    'message': f'Client user {username} not found',
                    'username': username,
                    'client_code': client_code
                }
            
            # Delete user
            with transaction.atomic():
                user.delete()
            
            logger.info(f"Deleted client user: {username} for client {client_code}")
            
            return {
                'status': 'deleted',
                'message': f'Successfully deleted user {username}',
                'username': username,
                'client_code': client_code
            }
            
        except Exception as e:
            logger.error(f"Error deleting client user {client_code}: {str(e)}")
            return {
                'status': 'error',
                'message': f'Failed to delete user for {client_code}: {str(e)}',
                'client_code': client_code
            }
    
    @staticmethod
    def list_client_users() -> Dict:
        """
        List all client users and their status.
        
        Returns:
            Dict with users list and summary
        """
        try:
            # Get all clients
            clients = Client.objects.all().order_by('code')
            client_users = []
            
            for client in clients:
                username = f"{client.code}_user"
                
                try:
                    user = User.objects.get(username=username, role='client', client_code=client.code)
                    status = 'exists'
                    user_id = user.id
                    last_login = user.last_login.isoformat() if user.last_login else None
                    date_joined = user.date_joined.isoformat()
                except User.DoesNotExist:
                    status = 'missing'
                    user_id = None
                    last_login = None
                    date_joined = None
                
                client_users.append({
                    'client_code': client.code,
                    'client_name': client.name,
                    'username': username,
                    'status': status,
                    'user_id': user_id,
                    'last_login': last_login,
                    'date_joined': date_joined
                })
            
            total_clients = len(client_users)
            existing_users = len([u for u in client_users if u['status'] == 'exists'])
            missing_users = total_clients - existing_users
            
            return {
                'status': 'success',
                'users': client_users,
                'summary': {
                    'total_clients': total_clients,
                    'existing_users': existing_users,
                    'missing_users': missing_users
                }
            }
            
        except Exception as e:
            logger.error(f"Error listing client users: {str(e)}")
            return {
                'status': 'error',
                'message': f'Failed to list client users: {str(e)}',
                'users': [],
                'summary': {}
            }
    
    @staticmethod
    def reset_password(client_code: str) -> Dict:
        """
        Reset password for a client user.
        
        Args:
            client_code: The client code (e.g., 'JAV', 'BK')
            
        Returns:
            Dict with status, new password, and message
        """
        try:
            username = f"{client_code}_user"
            
            try:
                user = User.objects.get(username=username, role='client', client_code=client_code)
            except User.DoesNotExist:
                return {
                    'status': 'not_found',
                    'message': f'Client user {username} not found',
                    'username': username,
                    'client_code': client_code
                }
            
            # Generate new password
            new_password = ClientUserManagementService.generate_secure_password()
            
            # Update password
            with transaction.atomic():
                user.set_password(new_password)
                user.save()
            
            logger.info(f"Reset password for client user: {username}")
            
            return {
                'status': 'reset',
                'message': f'Successfully reset password for {username}',
                'username': username,
                'password': new_password,
                'client_code': client_code
            }
            
        except Exception as e:
            logger.error(f"Error resetting password for {client_code}: {str(e)}")
            return {
                'status': 'error',
                'message': f'Failed to reset password for {client_code}: {str(e)}',
                'client_code': client_code
            }
    
    @staticmethod
    def cleanup_old_users() -> Dict:
        """
        Clean up inconsistent or old client users (like 'jav', 'jav_client').
        
        Returns:
            Dict with cleanup results
        """
        try:
            # Find users that don't match the {CLIENT_CODE}_user pattern
            client_users = User.objects.filter(role='client')
            deleted_users = []
            
            for user in client_users:
                # Check if username matches expected pattern
                expected_username = f"{user.client_code}_user"
                
                if user.username != expected_username:
                    deleted_users.append({
                        'username': user.username,
                        'client_code': user.client_code,
                        'reason': f'Expected {expected_username}'
                    })
                    user.delete()
                    logger.info(f"Cleaned up inconsistent user: {user.username}")
            
            return {
                'status': 'success',
                'message': f'Cleaned up {len(deleted_users)} inconsistent users',
                'deleted_users': deleted_users
            }
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
            return {
                'status': 'error',
                'message': f'Cleanup failed: {str(e)}',
                'deleted_users': []
            }
    
    @staticmethod
    def bulk_create_all_client_users(clean_slate: bool = False, rotate_passwords: bool = False) -> Dict:
        """
        Create users for all clients in bulk.
        
        Args:
            clean_slate: Whether to cleanup old users first
            rotate_passwords: Whether to reset existing user passwords
            
        Returns:
            Dict with bulk operation results
        """
        try:
            results = {
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'cleanup': None,
                'users': [],
                'summary': {
                    'total_clients': 0,
                    'created': 0,
                    'existed': 0,
                    'reset': 0,
                    'errors': 0
                }
            }
            
            # Clean up old users if requested
            if clean_slate:
                results['cleanup'] = ClientUserManagementService.cleanup_old_users()
            
            # Get all clients
            clients = Client.objects.all().order_by('code')
            results['summary']['total_clients'] = clients.count()
            
            for client in clients:
                if rotate_passwords:
                    # Try to reset password for existing user, create if doesn't exist
                    user_result = ClientUserManagementService.reset_password(client.code)
                    if user_result['status'] == 'not_found':
                        user_result = ClientUserManagementService.create_client_user(client.code)
                    elif user_result['status'] == 'reset':
                        results['summary']['reset'] += 1
                else:
                    # Create user (will skip if exists)
                    user_result = ClientUserManagementService.create_client_user(client.code)
                
                results['users'].append(user_result)
                
                # Update summary
                if user_result['status'] == 'created':
                    results['summary']['created'] += 1
                elif user_result['status'] == 'exists':
                    results['summary']['existed'] += 1
                elif user_result['status'] == 'error':
                    results['summary']['errors'] += 1
            
            return results
            
        except Exception as e:
            logger.error(f"Error in bulk create: {str(e)}")
            return {
                'status': 'error',
                'message': f'Bulk creation failed: {str(e)}',
                'users': [],
                'summary': {}
            }