"""
Django models for Aurum Finance portfolio management system.
Clean snapshot-based architecture with proper client isolation.
"""

from django.db import models
from django.contrib.auth.models import AbstractUser
from django.core.exceptions import ValidationError
from decimal import Decimal


class User(AbstractUser):
    """
    Custom User model extending Django's AbstractUser.
    Supports admin and client user types with client isolation.
    """
    ROLE_CHOICES = [
        ('admin', 'Admin'),
        ('client', 'Client'),
    ]
    
    role = models.CharField(
        max_length=20, 
        choices=ROLE_CHOICES, 
        default='client',
        help_text="User role: admin can see all clients, client can only see their own data"
    )
    client_code = models.CharField(
        max_length=10, 
        null=True, 
        blank=True,
        help_text="Client code for client users (required for client role)"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'auth_user_custom'
        indexes = [
            models.Index(fields=['role']),
            models.Index(fields=['client_code']),
            models.Index(fields=['role', 'client_code']),
        ]
    
    def clean(self):
        """Validate that client users have a client_code"""
        super().clean()
        if self.role == 'client' and not self.client_code:
            raise ValidationError("Client users must have a client_code")
        if self.role == 'admin' and self.client_code:
            # Admin users shouldn't have client_code, clear it
            self.client_code = None
    
    def save(self, *args, **kwargs):
        self.clean()
        super().save(*args, **kwargs)
    
    def __str__(self):
        if self.role == 'admin':
            return f"{self.username} (Admin)"
        else:
            return f"{self.username} (Client: {self.client_code})"
    
    @property
    def is_admin(self):
        """Check if user is an admin"""
        return self.role == 'admin'
    
    @property
    def is_client(self):
        """Check if user is a client"""
        return self.role == 'client'


class Client(models.Model):
    """
    Client model for multi-tenant architecture.
    Represents individual investment advisory clients.
    """
    code = models.CharField(max_length=10, unique=True, help_text="Unique client identifier code")
    name = models.CharField(max_length=100, help_text="Client display name")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'portfolio_client'
        indexes = [
            models.Index(fields=['code']),
        ]
    
    def __str__(self):
        return f"{self.code} - {self.name}"


class Asset(models.Model):
    """
    Asset model representing financial instruments with custody-based uniqueness.
    Assets are unique per custody location (bank + account + client) to prevent deduplication issues.
    """
    ticker = models.CharField(max_length=20, db_index=True)
    name = models.CharField(max_length=200)
    asset_type = models.CharField(max_length=50)
    currency = models.CharField(max_length=3, default="USD")
    bank = models.CharField(max_length=20, default="", help_text="Custody bank (empty string instead of UNKNOWN)")
    account = models.CharField(max_length=50, default="", help_text="Custody account (empty string instead of UNKNOWN)")
    client = models.CharField(max_length=20, db_index=True, help_text="Client code for asset isolation")
    isin = models.CharField(max_length=12, null=True, blank=True, db_index=True)
    cusip = models.CharField(max_length=50, null=True, blank=True, db_index=True)
    coupon_rate = models.DecimalField(max_digits=8, decimal_places=4, null=True, blank=True)
    maturity_date = models.DateField(null=True, blank=True)
    dividend_yield = models.DecimalField(
        max_digits=8, decimal_places=4, null=True, blank=True,
        help_text="Annual dividend yield percentage for equities"
    )
    active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'portfolio_asset'
        indexes = [
            models.Index(fields=['ticker']),
            models.Index(fields=['asset_type']),
            models.Index(fields=['isin']),
            models.Index(fields=['cusip']),
            models.Index(fields=['client', 'ticker']),
            models.Index(fields=['client', 'bank', 'account']),
            models.Index(fields=['cusip', 'bank', 'account']),  # For transaction linking
        ]
        # Custody-based uniqueness: same ticker can exist in different custody locations
        unique_together = [['ticker', 'cusip', 'name', 'bank', 'account', 'client']]
    
    @classmethod
    def find_by_cusip_and_custody(cls, cusip, bank, account):
        """Find asset by CUSIP + custody for transaction linking"""
        if cusip and cusip.strip():
            return cls.objects.filter(
                cusip=cusip,
                bank=bank,
                account=account
            ).first()
        return None
    
    def __str__(self):
        return f"{self.ticker} - {self.name} ({self.bank}/{self.account})"


class PortfolioSnapshot(models.Model):
    """
    PortfolioSnapshot model for storing portfolio states at specific dates.
    Central model that provides client isolation and links to positions.
    """
    client = models.ForeignKey(Client, on_delete=models.CASCADE, related_name='snapshots')
    snapshot_date = models.DateField(db_index=True)
    portfolio_metrics = models.JSONField(default=dict, help_text="Calculated portfolio metrics and chart data")
    
    # NEW ROLLOVER TRACKING FIELDS
    has_rolled_accounts = models.BooleanField(
        default=False,
        help_text="True if this snapshot contains rolled-over account data"
    )
    rollover_summary = models.JSONField(
        null=True, blank=True,
        help_text="Summary: {'CS_SP': '2025-07-24', 'LO_SP': '2025-07-24'}"
    )
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'portfolio_snapshot'
        unique_together = [['client', 'snapshot_date']]
        indexes = [
            models.Index(fields=['client', 'snapshot_date']),
            models.Index(fields=['snapshot_date']),
        ]
    
    def __str__(self):
        return f"Snapshot for {self.client.code} on {self.snapshot_date}"


class Position(models.Model):
    """
    Position model representing asset holdings at specific dates.
    Client isolation inherited through PortfolioSnapshot relationship.
    """
    snapshot = models.ForeignKey(PortfolioSnapshot, on_delete=models.CASCADE, related_name='positions')
    asset = models.ForeignKey(Asset, on_delete=models.CASCADE, related_name='positions')
    quantity = models.DecimalField(max_digits=15, decimal_places=4, default=0)
    market_value = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    cost_basis = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    price = models.DecimalField(max_digits=15, decimal_places=4, default=0)
    bank = models.CharField(max_length=20, default="", help_text="Bank name (empty string instead of UNKNOWN)")
    account = models.CharField(max_length=50, default="", help_text="Account identifier (empty string instead of UNKNOWN)")
    yield_pct = models.FloatField(null=True, blank=True)
    coupon_rate = models.FloatField(null=True, blank=True)
    maturity_date = models.DateField(null=True, blank=True)
    estimated_annual_income = models.DecimalField(
        max_digits=15, decimal_places=2, null=True, blank=True,
        help_text="Calculated estimated annual income from position"
    )
    face_value = models.DecimalField(
        max_digits=15, decimal_places=2, null=True, blank=True,
        help_text="Face value for bonds (defaults to market_value if not specified)"
    )
    
    # NEW ROLLOVER TRACKING FIELDS
    is_rolled_over = models.BooleanField(
        default=False,
        help_text="True if this position was rolled over from previous date"
    )
    rolled_from_date = models.DateField(
        null=True, blank=True,
        help_text="Original date this position was copied from"
    )
    
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'portfolio_position'
        indexes = [
            models.Index(fields=['snapshot']),
            models.Index(fields=['asset']),
            models.Index(fields=['snapshot', 'asset']),
        ]
        unique_together = [['snapshot', 'asset']]
    
    def save(self, *args, **kwargs):
        """Calculate estimated_annual_income before saving."""
        if self.coupon_rate and self.quantity:
            # Convert percentage to decimal - all coupon rates are stored as percentages
            coupon_rate = float(self.coupon_rate) / 100
            
            # Calculate estimated annual income: coupon_rate * quantity
            self.estimated_annual_income = Decimal(str(coupon_rate)) * self.quantity
        else:
            self.estimated_annual_income = None
        
        super().save(*args, **kwargs)
    
    def __str__(self):
        return f"{self.asset.ticker} - {self.quantity} shares in {self.snapshot}"


class Transaction(models.Model):
    """
    Transaction model representing buy/sell activities.
    Has direct client relationship for transaction history.
    """
    TRANSACTION_TYPES = [
        ('BUY', 'Buy'),
        ('SELL', 'Sell'),
        ('DIVIDEND', 'Dividend'),
        ('INTEREST', 'Interest'),
        ('FEE', 'Fee'),
        ('TRANSFER_IN', 'Transfer In'),
        ('TRANSFER_OUT', 'Transfer Out'),
    ]
    
    client = models.ForeignKey(Client, on_delete=models.CASCADE, related_name='transactions')
    asset = models.ForeignKey(Asset, on_delete=models.CASCADE, related_name='transactions')
    date = models.DateField(db_index=True)
    transaction_type = models.CharField(max_length=150)
    quantity = models.DecimalField(max_digits=15, decimal_places=4, null=True, blank=True)
    price = models.DecimalField(max_digits=15, decimal_places=4, null=True, blank=True)
    amount = models.DecimalField(max_digits=15, decimal_places=2)
    bank = models.CharField(max_length=20, default="", help_text="Bank name (empty string instead of UNKNOWN)")
    account = models.CharField(max_length=100, default="", help_text="Account identifier (empty string instead of UNKNOWN)")
    transaction_id = models.CharField(max_length=150, default="", help_text="Transaction identifier (empty string instead of UNKNOWN)")
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'portfolio_transaction'
        unique_together = [['transaction_id', 'client']]
        indexes = [
            models.Index(fields=['client', 'date']),
            models.Index(fields=['client', 'asset', 'date']),
            models.Index(fields=['client', 'transaction_type']),
            models.Index(fields=['transaction_id']),
        ]
    
    def __str__(self):
        return f"{self.transaction_type} {self.quantity} {self.asset.ticker} on {self.date} ({self.client.code})"


class Report(models.Model):
    """
    Report model for storing generated report metadata.
    Uses direct client foreign key relationship.
    """
    REPORT_TYPES = [
        ('WEEKLY', 'Weekly Report'),
        ('MONTHLY', 'Monthly Report'),
        ('QUARTERLY', 'Quarterly Report'),
        ('ANNUAL', 'Annual Report'),
        ('BOND_ISSUER', 'Bond Issuer Report'),
        ('BOND_MATURITY', 'Bond Maturity Report'),
        ('EQUITY_BREAKDOWN', 'Equity Breakdown Report'),
        ('CASH_POSITION', 'Cash Position Report'),
        ('MONTHLY_RETURNS', 'Monthly Returns by Custody'),
        ('TOTAL_POSITIONS', 'Total Positions Report'),
    ]
    
    client = models.ForeignKey(Client, on_delete=models.CASCADE, related_name='reports')
    report_type = models.CharField(max_length=25, choices=REPORT_TYPES)
    report_date = models.DateField(db_index=True)
    file_path = models.CharField(max_length=255)
    file_size = models.BigIntegerField(null=True, blank=True)
    generation_time = models.FloatField(null=True, blank=True, help_text="Generation time in seconds")
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'portfolio_report'
        indexes = [
            models.Index(fields=['client', 'report_date']),
            models.Index(fields=['client', 'report_type']),
        ]
        unique_together = [['client', 'report_type', 'report_date']]
    
    def __str__(self):
        return f"{self.report_type} for {self.report_date} ({self.client.code})"


class ProcessingStatus(models.Model):
    """
    Simple model to track database processing status for UX improvements.
    Prevents multiple concurrent processing requests.
    """
    STATUS_CHOICES = [
        ('IDLE', 'Idle'),
        ('PROCESSING', 'Processing'),
        ('ERROR', 'Error'),
    ]
    
    process_type = models.CharField(max_length=50, default='database_update')
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='IDLE')
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    progress_message = models.CharField(max_length=200, blank=True)
    error_message = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'portfolio_processing_status'
        unique_together = [['process_type']]  # Only one status record per process type
    
    def __str__(self):
        return f"{self.process_type}: {self.status}"


class DateAggregatedMetrics(models.Model):
    """
    Stores pre-aggregated dashboard data per snapshot date.
    Key insight: Each date has independent aggregated data, NOT cumulative!
    This replaces expensive N+1 queries with single cache lookups.
    """
    snapshot_date = models.DateField(db_index=True)
    client_filter = models.CharField(max_length=10, default='ALL', db_index=True)
    
    # Summary metrics (aggregated for THIS DATE only)
    total_aum = models.DecimalField(max_digits=20, decimal_places=2)
    total_inception_dollar = models.DecimalField(max_digits=20, decimal_places=2) 
    weighted_inception_percent = models.DecimalField(max_digits=10, decimal_places=4)
    total_annual_income = models.DecimalField(max_digits=20, decimal_places=2)
    client_count = models.IntegerField()
    
    # Pre-computed chart data (to eliminate expensive chart generation)
    asset_allocation_data = models.JSONField(help_text="Aggregated asset allocation for this date")
    bank_allocation_data = models.JSONField(default=dict, help_text='Aggregated bank allocation for this date')
    
    # Metadata
    last_updated = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'portfolio_date_aggregated_metrics'
        unique_together = ['snapshot_date', 'client_filter']
        indexes = [
            models.Index(fields=['-snapshot_date', 'client_filter']),
        ]
    
    def __str__(self):
        return f"Aggregated data for {self.snapshot_date} ({self.client_filter})"