"""
Custom permission classes for Aurum Finance authentication system.
Implements role-based access control for admin and client users.
"""

from rest_framework.permissions import BasePermission
import logging

logger = logging.getLogger(__name__)


class IsAdminUser(BasePermission):
    """
    Permission class that allows access only to admin users.
    Admin users can see all clients' data and manage the system.
    """
    
    def has_permission(self, request, view):
        """Check if user is authenticated and has admin role"""
        if not request.user or not request.user.is_authenticated:
            logger.warning(f"Unauthenticated access attempt to admin endpoint: {request.path}")
            return False
        
        if not request.user.is_admin:
            logger.warning(f"Non-admin user {request.user.username} attempted to access admin endpoint: {request.path}")
            return False
        
        return True


class IsClientUser(BasePermission):
    """
    Permission class that allows access only to client users.
    Client users can only see their own data.
    """
    
    def has_permission(self, request, view):
        """Check if user is authenticated and has client role"""
        if not request.user or not request.user.is_authenticated:
            logger.warning(f"Unauthenticated access attempt to client endpoint: {request.path}")
            return False
        
        if not request.user.is_client:
            logger.warning(f"Non-client user {request.user.username} attempted to access client endpoint: {request.path}")
            return False
        
        if not request.user.client_code:
            logger.error(f"Client user {request.user.username} has no client_code")
            return False
        
        return True


class IsAdminOrOwnClient(BasePermission):
    """
    Permission class that allows access to admin users or client users accessing their own data.
    Used for endpoints that should be accessible to both admin and client users.
    """
    
    def has_permission(self, request, view):
        """Check if user is authenticated"""
        if not request.user or not request.user.is_authenticated:
            logger.warning(f"Unauthenticated access attempt: {request.path}")
            return False
        
        return True
    
    def has_object_permission(self, request, view, obj):
        """Check if user can access this specific object"""
        if not request.user or not request.user.is_authenticated:
            return False
        
        # Admin users can access everything
        if request.user.is_admin:
            return True
        
        # Client users can only access their own data
        if request.user.is_client:
            # Check if object has client field and matches user's client_code
            if hasattr(obj, 'client') and obj.client == request.user.client_code:
                return True
            
            logger.warning(f"Client user {request.user.username} attempted to access data for client {getattr(obj, 'client', 'unknown')}")
            return False
        
        return False


class ClientDataFilter:
    """
    Utility class to filter querysets based on user permissions.
    Ensures client users only see their own data.
    """
    
    @staticmethod
    def filter_queryset(queryset, user):
        """Filter queryset based on user role and client_code"""
        if not user or not user.is_authenticated:
            return queryset.none()
        
        # Admin users see all data
        if user.is_admin:
            return queryset
        
        # Client users only see their own data
        if user.is_client and user.client_code:
            return queryset.filter(client=user.client_code)
        
        # If user has no proper role or client_code, return empty queryset
        return queryset.none()