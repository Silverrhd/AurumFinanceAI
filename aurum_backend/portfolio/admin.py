"""
Django admin configuration for AurumFinance portfolio models.
Updated for clean Django-only architecture.
"""

from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from .models import User, Client, Asset, Position, Transaction, PortfolioSnapshot, Report


@admin.register(User)
class UserAdmin(BaseUserAdmin):
    """Custom User admin with role and client_code fields"""
    
    # Fields to display in the user list
    list_display = ['username', 'email', 'role', 'client_code', 'is_active', 'is_staff', 'date_joined']
    list_filter = ['role', 'is_active', 'is_staff', 'is_superuser', 'client_code']
    search_fields = ['username', 'email', 'client_code']
    ordering = ['role', 'username']
    
    # Add custom fields to the user form
    fieldsets = BaseUserAdmin.fieldsets + (
        ('Aurum Finance', {
            'fields': ('role', 'client_code'),
        }),
    )
    
    # Add custom fields to the add user form
    add_fieldsets = BaseUserAdmin.add_fieldsets + (
        ('Aurum Finance', {
            'fields': ('role', 'client_code'),
        }),
    )
    
    def get_queryset(self, request):
        """Filter users based on current user's role"""
        qs = super().get_queryset(request)
        
        # If current user is not a superuser, they can only see users from their client
        if not request.user.is_superuser and request.user.is_client:
            return qs.filter(client_code=request.user.client_code)
        
        return qs


@admin.register(Client)
class ClientAdmin(admin.ModelAdmin):
    list_display = ['code', 'name', 'created_at', 'updated_at']
    search_fields = ['code', 'name']
    ordering = ['code']
    readonly_fields = ['created_at', 'updated_at']


@admin.register(Asset)
class AssetAdmin(admin.ModelAdmin):
    list_display = ['ticker', 'name', 'asset_type', 'currency', 'active', 'created_at']
    list_filter = ['asset_type', 'currency', 'active']
    search_fields = ['ticker', 'name', 'isin', 'cusip']
    ordering = ['ticker']
    readonly_fields = ['created_at', 'updated_at']


@admin.register(PortfolioSnapshot)
class PortfolioSnapshotAdmin(admin.ModelAdmin):
    list_display = ['client', 'snapshot_date', 'get_total_value', 'created_at', 'updated_at']
    list_filter = ['client', 'snapshot_date']
    search_fields = ['client__code', 'client__name']
    ordering = ['-snapshot_date', 'client__code']
    readonly_fields = ['created_at', 'updated_at']
    
    def get_total_value(self, obj):
        """Display total value from portfolio metrics"""
        return f"${obj.portfolio_metrics.get('total_value', 0):,.2f}"
    get_total_value.short_description = 'Total Value'


@admin.register(Position)
class PositionAdmin(admin.ModelAdmin):
    list_display = ['asset', 'get_client', 'get_snapshot_date', 'quantity', 'market_value', 'cost_basis', 'bank', 'account']
    list_filter = ['snapshot__client', 'asset__asset_type', 'bank']
    search_fields = ['asset__ticker', 'asset__name']
    ordering = ['-snapshot__snapshot_date', 'asset__ticker']
    readonly_fields = ['created_at']
    
    def get_client(self, obj):
        return obj.snapshot.client.code
    get_client.short_description = 'Client'
    
    def get_snapshot_date(self, obj):
        return obj.snapshot.snapshot_date
    get_snapshot_date.short_description = 'Date'


@admin.register(Transaction)
class TransactionAdmin(admin.ModelAdmin):
    list_display = ['asset', 'client', 'date', 'transaction_type', 'quantity', 'price', 'amount']
    list_filter = ['client', 'transaction_type', 'date']
    search_fields = ['asset__ticker', 'asset__name', 'transaction_id']
    ordering = ['client', '-date']
    readonly_fields = ['created_at']


@admin.register(Report)
class ReportAdmin(admin.ModelAdmin):
    list_display = ['client', 'report_type', 'report_date', 'file_path', 'created_at']
    list_filter = ['client', 'report_type', 'report_date']
    ordering = ['client', '-report_date', 'report_type']
    readonly_fields = ['created_at', 'file_size', 'generation_time']


