"""
URL configuration for aurum_backend project.
"""
from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from rest_framework_simplejwt.views import (
    TokenObtainPairView,
    TokenRefreshView,
)
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
from rest_framework_simplejwt.views import TokenObtainPairView
from rest_framework.response import Response
from rest_framework import serializers
from django.contrib.auth import get_user_model

User = get_user_model()

class AdminTokenObtainPairSerializer(TokenObtainPairSerializer):
    """Serializer for admin-only authentication"""
    def validate(self, attrs):
        data = super().validate(attrs)
        
        # Strict validation: Only admin users can login through admin endpoint
        if not self.user.is_admin:
            raise serializers.ValidationError({
                'non_field_errors': ['Not Valid Admin Credentials']
            })
            
        # Return admin-specific data
        data['user'] = {
            'id': self.user.id,
            'username': self.user.username,
            'email': self.user.email,
            'role': self.user.role,
            'client_code': None,  # Admin users don't have client codes
            'is_admin': True,
            'is_client': False,
        }
        return data

class ClientTokenObtainPairSerializer(TokenObtainPairSerializer):
    """Serializer for client-only authentication"""
    def validate(self, attrs):
        data = super().validate(attrs)
        
        # Strict validation: Only client users can login through client endpoint
        if not self.user.is_client:
            raise serializers.ValidationError({
                'non_field_errors': ['Not Valid Client Credentials']
            })
            
        # Return client-specific data
        data['user'] = {
            'id': self.user.id,
            'username': self.user.username,
            'email': self.user.email,
            'role': self.user.role,
            'client_code': self.user.client_code,
            'is_admin': False,
            'is_client': True,
        }
        return data

# Keep original for backward compatibility if needed
class CustomTokenObtainPairSerializer(TokenObtainPairSerializer):
    """Legacy serializer - can be removed after frontend migration"""
    def validate(self, attrs):
        data = super().validate(attrs)
        data['user'] = {
            'id': self.user.id,
            'username': self.user.username,
            'email': self.user.email,
            'role': self.user.role,
            'client_code': self.user.client_code,
            'is_admin': self.user.is_admin,
            'is_client': self.user.is_client,
        }
        return data

class AdminTokenObtainPairView(TokenObtainPairView):
    """Admin-specific login endpoint"""
    serializer_class = AdminTokenObtainPairSerializer

class ClientTokenObtainPairView(TokenObtainPairView):
    """Client-specific login endpoint"""
    serializer_class = ClientTokenObtainPairSerializer

class CustomTokenObtainPairView(TokenObtainPairView):
    """Legacy view - can be removed after frontend migration"""
    serializer_class = CustomTokenObtainPairSerializer

urlpatterns = [
    path('admin/', admin.site.urls),
    
    # JWT Authentication endpoints
    path('api/auth/admin/login/', AdminTokenObtainPairView.as_view(), name='admin_login'),
    path('api/auth/client/login/', ClientTokenObtainPairView.as_view(), name='client_login'),
    path('api/auth/refresh/', TokenRefreshView.as_view(), name='token_refresh'),
    
    # Legacy endpoint (keep for backward compatibility during migration)
    path('api/auth/login/', CustomTokenObtainPairView.as_view(), name='token_obtain_pair'),
    
    # Portfolio API endpoints (includes authentication)
    path('api/portfolio/', include('portfolio.urls')),
    
    # Django REST Framework browsable API (development only)
    path('api-auth/', include('rest_framework.urls')),
]

# Serve media files in development
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
