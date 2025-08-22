"""
API Serializers for Aurum Finance Portfolio Management System.
Defines request/response schemas matching the current FastAPI system.
"""

from rest_framework import serializers
from .models import User, Asset, Position, Transaction, Report, AssetSnapshot
from datetime import datetime
from typing import Dict, List, Any


# ============================================================================
# AUTHENTICATION SERIALIZERS
# ============================================================================

class UserSerializer(serializers.ModelSerializer):
    """User serializer for API responses"""
    is_admin = serializers.ReadOnlyField()
    is_client = serializers.ReadOnlyField()
    
    class Meta:
        model = User
        fields = ['id', 'username', 'email', 'first_name', 'last_name', 
                 'role', 'client_code', 'is_admin', 'is_client', 'date_joined', 'last_login']
        read_only_fields = ['id', 'is_admin', 'is_client', 'date_joined', 'last_login']


class LoginRequestSerializer(serializers.Serializer):
    """Login request schema matching your current FastAPI endpoint"""
    username = serializers.CharField(
        max_length=150, 
        help_text="Username or email address"
    )
    password = serializers.CharField(
        max_length=128, 
        write_only=True, 
        help_text="User password"
    )


class LoginResponseSerializer(serializers.Serializer):
    """Login response with JWT tokens and user info"""
    access = serializers.CharField(help_text="JWT access token (1 hour expiry)")
    refresh = serializers.CharField(help_text="JWT refresh token (7 days expiry)")
    user = UserSerializer(help_text="User profile information")


class TokenInfoSerializer(serializers.Serializer):
    """Token validation response - matches /api/client/token-info"""
    valid = serializers.BooleanField()
    user_id = serializers.IntegerField()
    username = serializers.CharField()
    role = serializers.CharField()
    client_code = serializers.CharField(allow_null=True)
    expires_at = serializers.DateTimeField()


# ============================================================================
# FILE PROCESSING SERIALIZERS  
# ============================================================================

class FileUploadResultSerializer(serializers.Serializer):
    """Individual file upload result"""
    filename = serializers.CharField()
    bank_detected = serializers.CharField(allow_null=True)
    file_size = serializers.IntegerField()
    status = serializers.ChoiceField(choices=['success', 'error', 'warning'])
    message = serializers.CharField()
    file_path = serializers.CharField(required=False)


class FileUploadResponseSerializer(serializers.Serializer):
    """Response for file upload endpoint - matches /api/admin/upload-files"""
    success = serializers.BooleanField()
    files_processed = serializers.IntegerField()
    results = FileUploadResultSerializer(many=True)
    total_size = serializers.IntegerField(help_text="Total size in bytes")
    processing_time = serializers.FloatField(help_text="Processing time in seconds")
    errors = serializers.ListField(
        child=serializers.CharField(),
        required=False,
        help_text="List of error messages"
    )


class BankStatusSerializer(serializers.Serializer):
    """Bank processing status - matches /api/admin/bank-status"""
    bank_code = serializers.CharField()
    bank_name = serializers.CharField()
    files_count = serializers.IntegerField()
    last_processed = serializers.DateTimeField(allow_null=True)
    status = serializers.ChoiceField(
        choices=['ready', 'processing', 'completed', 'error', 'no_files']
    )
    next_action = serializers.CharField(allow_null=True)
    file_types = serializers.ListField(child=serializers.CharField())


class PreprocessingRequestSerializer(serializers.Serializer):
    """Request for run-preprocessing endpoint"""
    date = serializers.CharField(
        help_text="Date in DD_MM_YYYY format (e.g., '15_07_2025')"
    )
    force_reprocess = serializers.BooleanField(
        default=False,
        help_text="Force reprocessing even if files already exist"
    )


class PreprocessingResponseSerializer(serializers.Serializer):
    """Response for preprocessing pipeline"""
    success = serializers.BooleanField()
    message = serializers.CharField()
    processing_time = serializers.FloatField()
    files_generated = serializers.ListField(child=serializers.CharField())
    banks_processed = serializers.ListField(child=serializers.CharField())
    errors = serializers.ListField(child=serializers.CharField(), required=False)


# ============================================================================
# REPORT GENERATION SERIALIZERS (Your Key Buttons!)
# ============================================================================

class GenerateReportsRequestSerializer(serializers.Serializer):
    """Request for generate-missing-reports endpoint - your main button!"""
    date = serializers.CharField(
        help_text="Date in DD_MM_YYYY format (e.g., '15_07_2025')"
    )


class ReportGenerationProgressSerializer(serializers.Serializer):
    """Progress tracking for report generation - matches your current system"""
    status = serializers.ChoiceField(
        choices=['idle', 'started', 'processing', 'completed', 'error']
    )
    current_client = serializers.CharField(
        allow_null=True,
        help_text="Currently processing client code"
    )
    completed_count = serializers.IntegerField()
    total_count = serializers.IntegerField()
    percentage = serializers.FloatField()
    message = serializers.CharField()
    failed_clients = serializers.ListField(child=serializers.CharField())
    last_error = serializers.CharField(allow_null=True)
    generated_reports = serializers.ListField(
        child=serializers.CharField(),
        required=False
    )


class ReportAnalysisSerializer(serializers.Serializer):
    """Report analysis response - matches /api/admin/report-analysis/{date}"""
    date = serializers.CharField()
    formatted_date = serializers.CharField()
    total_clients = serializers.IntegerField()
    clients_with_reports = serializers.IntegerField()
    missing_clients = serializers.ListField(child=serializers.CharField())
    existing_reports = serializers.ListField(child=serializers.CharField())
    securities_file = serializers.CharField(allow_null=True)
    transactions_file = serializers.CharField(allow_null=True)
    files_exist = serializers.BooleanField()


class ReportDateSerializer(serializers.Serializer):
    """Available report dates"""
    date = serializers.CharField()
    formatted_date = serializers.CharField()
    client_count = serializers.IntegerField()
    report_types = serializers.ListField(child=serializers.CharField())


# ============================================================================
# DASHBOARD DATA SERIALIZERS
# ============================================================================

class AdminDashboardMetricsSerializer(serializers.Serializer):
    """Admin dashboard metrics - matches /api/admin/dashboard/metrics"""
    total_clients = serializers.IntegerField()
    total_assets = serializers.IntegerField()
    total_positions = serializers.IntegerField()
    total_market_value = serializers.FloatField()
    monthly_change = serializers.FloatField()
    ytd_return = serializers.FloatField()
    last_updated = serializers.DateTimeField()
    active_clients = serializers.IntegerField()


class ClientDashboardMetricsSerializer(serializers.Serializer):
    """Client dashboard metrics - matches /api/client/dashboard/metrics"""
    client_code = serializers.CharField()
    total_assets = serializers.IntegerField()
    total_market_value = serializers.FloatField()
    monthly_change = serializers.FloatField()
    ytd_return = serializers.FloatField()
    last_updated = serializers.DateTimeField()
    portfolio_allocation = serializers.DictField()


class ChartDataSerializer(serializers.Serializer):
    """Chart data structure for ApexCharts - matches your current format"""
    series = serializers.ListField(help_text="Chart data series")
    labels = serializers.ListField(required=False, help_text="Chart labels")
    colors = serializers.ListField(required=False, help_text="Chart colors")
    categories = serializers.ListField(required=False, help_text="X-axis categories")
    title = serializers.CharField(required=False)


class AssetAllocationChartSerializer(serializers.Serializer):
    """Asset allocation donut chart data"""
    chart_type = serializers.CharField(default="donut")
    series = serializers.ListField(child=serializers.FloatField())
    labels = serializers.ListField(child=serializers.CharField())
    colors = serializers.ListField(child=serializers.CharField())
    total_value = serializers.FloatField()


class PortfolioHistoryChartSerializer(serializers.Serializer):
    """Portfolio history line chart data"""
    chart_type = serializers.CharField(default="line")
    series = serializers.ListField()
    categories = serializers.ListField(child=serializers.CharField())
    colors = serializers.ListField(child=serializers.CharField())


class AllChartsResponseSerializer(serializers.Serializer):
    """All charts data response - matches /api/admin/dashboard/all-charts"""
    asset_allocation = AssetAllocationChartSerializer()
    portfolio_history = PortfolioHistoryChartSerializer()
    portfolio_comparison = ChartDataSerializer()
    cumulative_return = ChartDataSerializer()
    last_updated = serializers.DateTimeField()


# ============================================================================
# SYSTEM MONITORING SERIALIZERS
# ============================================================================

class SystemHealthSerializer(serializers.Serializer):
    """System health check response"""
    status = serializers.ChoiceField(choices=['healthy', 'warning', 'error'])
    timestamp = serializers.DateTimeField()
    database_status = serializers.CharField()
    file_system_status = serializers.CharField()
    memory_usage = serializers.FloatField()
    disk_usage = serializers.FloatField()
    active_processes = serializers.IntegerField()


class LogFileSerializer(serializers.Serializer):
    """Log file information"""
    filename = serializers.CharField()
    size = serializers.IntegerField()
    modified = serializers.DateTimeField()
    lines = serializers.IntegerField()


class LogEntrySerializer(serializers.Serializer):
    """Individual log entry"""
    timestamp = serializers.DateTimeField()
    level = serializers.CharField()
    message = serializers.CharField()
    module = serializers.CharField(required=False)
    line_number = serializers.IntegerField(required=False)


# ============================================================================
# CLIENT MANAGEMENT SERIALIZERS
# ============================================================================

class ClientInfoSerializer(serializers.Serializer):
    """Client information - matches /api/client/info"""
    client_code = serializers.CharField()
    client_name = serializers.CharField()
    total_assets = serializers.IntegerField()
    total_market_value = serializers.FloatField()
    last_report_date = serializers.DateField(allow_null=True)
    account_status = serializers.CharField()


class ClientListSerializer(serializers.Serializer):
    """List of all clients - matches /api/clients"""
    client_code = serializers.CharField()
    client_name = serializers.CharField()
    asset_count = serializers.IntegerField()
    market_value = serializers.FloatField()
    last_updated = serializers.DateTimeField()
    status = serializers.CharField()


# ============================================================================
# ERROR RESPONSE SERIALIZERS
# ============================================================================

class ErrorResponseSerializer(serializers.Serializer):
    """Standard error response format"""
    error = serializers.CharField(help_text="Error type or code")
    message = serializers.CharField(help_text="Human-readable error message")
    details = serializers.DictField(
        required=False,
        help_text="Additional error details"
    )
    timestamp = serializers.DateTimeField(help_text="Error timestamp")
    request_id = serializers.CharField(
        required=False,
        help_text="Request ID for tracking"
    )


class ValidationErrorSerializer(serializers.Serializer):
    """Validation error response"""
    error = serializers.CharField(default="validation_error")
    message = serializers.CharField()
    field_errors = serializers.DictField(
        help_text="Field-specific validation errors"
    )
    timestamp = serializers.DateTimeField()


# ============================================================================
# PORTFOLIO DATA SERIALIZERS
# ============================================================================

class AssetSerializer(serializers.ModelSerializer):
    """Asset model serializer"""
    class Meta:
        model = Asset
        fields = '__all__'


class PositionSerializer(serializers.ModelSerializer):
    """Position model serializer"""
    asset_ticker = serializers.CharField(source='asset.ticker', read_only=True)
    asset_name = serializers.CharField(source='asset.name', read_only=True)
    
    class Meta:
        model = Position
        fields = '__all__'


class TransactionSerializer(serializers.ModelSerializer):
    """Transaction model serializer"""
    asset_ticker = serializers.CharField(source='asset.ticker', read_only=True)
    asset_name = serializers.CharField(source='asset.name', read_only=True)
    
    class Meta:
        model = Transaction
        fields = '__all__'


class ReportSerializer(serializers.ModelSerializer):
    """Report model serializer"""
    class Meta:
        model = Report
        fields = '__all__'


class AssetSnapshotSerializer(serializers.ModelSerializer):
    """Asset snapshot model serializer"""
    asset_ticker = serializers.CharField(source='asset.ticker', read_only=True)
    asset_name = serializers.CharField(source='asset.name', read_only=True)
    
    class Meta:
        model = AssetSnapshot
        fields = '__all__'