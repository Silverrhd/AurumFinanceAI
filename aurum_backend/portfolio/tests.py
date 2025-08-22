"""
Tests for portfolio models.
"""

from django.test import TestCase
from django.core.exceptions import ValidationError
from datetime import date, datetime
from .models import Asset, Position, Transaction, Report, AssetSnapshot


class AssetModelTest(TestCase):
    """Test Asset model functionality."""
    
    def setUp(self):
        self.asset_data = {
            'client': 'TEST_CLIENT',
            'ticker': 'AAPL',
            'name': 'Apple Inc.',
            'asset_type': 'Stock',
            'currency': 'USD',
            'bank': 'JPM',
            'account': 'ACC123',
            'isin': 'US0378331005',
            'cusip': '037833100'
        }
    
    def test_create_asset(self):
        """Test creating an asset."""
        asset = Asset.objects.create(**self.asset_data)
        self.assertEqual(asset.ticker, 'AAPL')
        self.assertEqual(asset.client, 'TEST_CLIENT')
        self.assertTrue(asset.active)
    
    def test_asset_str_representation(self):
        """Test asset string representation."""
        asset = Asset.objects.create(**self.asset_data)
        expected = "AAPL - Apple Inc. (TEST_CLIENT)"
        self.assertEqual(str(asset), expected)


class PositionModelTest(TestCase):
    """Test Position model functionality."""
    
    def setUp(self):
        self.asset = Asset.objects.create(
            client='TEST_CLIENT',
            ticker='AAPL',
            name='Apple Inc.',
            asset_type='Stock'
        )
        self.position_data = {
            'client': 'TEST_CLIENT',
            'asset': self.asset,
            'date': date.today(),
            'quantity': 100.0,
            'market_value': 15000.00,
            'cost_basis': 12000.00
        }
    
    def test_create_position(self):
        """Test creating a position."""
        position = Position.objects.create(**self.position_data)
        self.assertEqual(position.quantity, 100.0)
        self.assertEqual(position.client, 'TEST_CLIENT')


class TransactionModelTest(TestCase):
    """Test Transaction model functionality."""
    
    def setUp(self):
        self.asset = Asset.objects.create(
            client='TEST_CLIENT',
            ticker='AAPL',
            name='Apple Inc.',
            asset_type='Stock'
        )
    
    def test_create_transaction(self):
        """Test creating a transaction."""
        transaction = Transaction.objects.create(
            client='TEST_CLIENT',
            asset=self.asset,
            transaction_date=date.today(),
            transaction_type='BUY',
            quantity=100.0,
            price=150.00,
            total_amount=15000.00
        )
        self.assertEqual(transaction.transaction_type, 'BUY')
        self.assertEqual(transaction.quantity, 100.0)


class ReportModelTest(TestCase):
    """Test Report model functionality."""
    
    def test_create_report(self):
        """Test creating a report."""
        report = Report.objects.create(
            client='TEST_CLIENT',
            report_type='WEEKLY',
            report_date=date.today(),
            file_path='/reports/weekly_report.html'
        )
        self.assertEqual(report.report_type, 'WEEKLY')
        self.assertEqual(report.client, 'TEST_CLIENT')


class AssetSnapshotModelTest(TestCase):
    """Test AssetSnapshot model functionality."""
    
    def setUp(self):
        self.asset = Asset.objects.create(
            client='TEST_CLIENT',
            ticker='AAPL',
            name='Apple Inc.',
            asset_type='Stock'
        )
    
    def test_create_asset_snapshot(self):
        """Test creating an asset snapshot."""
        snapshot = AssetSnapshot.objects.create(
            client='TEST_CLIENT',
            asset=self.asset,
            snapshot_date=date.today(),
            quantity=100.0,
            market_value=15000.00,
            price_per_share=150.00,
            total_market_value=15000.00,
            allocation_percentage=10.0,
            modified_dietz_return=0.25,
            estimated_annual_income=1200.00
        )
        self.assertEqual(snapshot.total_market_value, 15000.00)
        self.assertEqual(snapshot.client, 'TEST_CLIENT')
        self.assertEqual(snapshot.quantity, 100.0)


class UserModelTest(TestCase):
    """Test custom User model functionality."""
    
    def test_create_admin_user(self):
        """Test creating an admin user."""
        from .models import User
        
        admin = User.objects.create_user(
            username='admin_test',
            email='admin@test.com',
            password='testpass123',
            role='admin'
        )
        
        self.assertEqual(admin.role, 'admin')
        self.assertTrue(admin.is_admin)
        self.assertFalse(admin.is_client)
        self.assertIsNone(admin.client_code)
    
    def test_create_client_user(self):
        """Test creating a client user."""
        from .models import User
        
        client = User.objects.create_user(
            username='client_test',
            email='client@test.com',
            password='testpass123',
            role='client',
            client_code='CLIENT_A'
        )
        
        self.assertEqual(client.role, 'client')
        self.assertFalse(client.is_admin)
        self.assertTrue(client.is_client)
        self.assertEqual(client.client_code, 'CLIENT_A')
    
    def test_client_without_client_code_validation(self):
        """Test that client users must have a client_code."""
        from .models import User
        
        with self.assertRaises(ValidationError):
            client = User(
                username='invalid_client',
                role='client'
                # Missing client_code
            )
            client.clean()
    
    def test_admin_client_code_cleared(self):
        """Test that admin users have client_code cleared."""
        from .models import User
        
        admin = User.objects.create_user(
            username='admin_clear_test',
            password='testpass123',
            role='admin',
            client_code='SHOULD_BE_CLEARED'
        )
        
        self.assertIsNone(admin.client_code)


class AuthenticationTest(TestCase):
    """Test authentication functionality."""
    
    def setUp(self):
        from .models import User
        
        self.admin_user = User.objects.create_user(
            username='admin_auth',
            email='admin@auth.com',
            password='testpass123',
            role='admin'
        )
        
        self.client_user = User.objects.create_user(
            username='client_auth',
            email='client@auth.com',
            password='testpass123',
            role='client',
            client_code='CLIENT_AUTH'
        )
    
    def test_admin_authentication(self):
        """Test admin user authentication."""
        from django.contrib.auth import authenticate
        
        user = authenticate(username='admin_auth', password='testpass123')
        self.assertIsNotNone(user)
        self.assertTrue(user.is_admin)
    
    def test_client_authentication(self):
        """Test client user authentication."""
        from django.contrib.auth import authenticate
        
        user = authenticate(username='client_auth', password='testpass123')
        self.assertIsNotNone(user)
        self.assertTrue(user.is_client)
        self.assertEqual(user.client_code, 'CLIENT_AUTH')
    
    def test_invalid_authentication(self):
        """Test invalid authentication."""
        from django.contrib.auth import authenticate
        
        user = authenticate(username='admin_auth', password='wrongpass')
        self.assertIsNone(user)


class PermissionTest(TestCase):
    """Test permission classes."""
    
    def setUp(self):
        from .models import User
        from django.test import RequestFactory
        
        self.factory = RequestFactory()
        
        self.admin_user = User.objects.create_user(
            username='admin_perm',
            password='testpass123',
            role='admin'
        )
        
        self.client_user = User.objects.create_user(
            username='client_perm',
            password='testpass123',
            role='client',
            client_code='CLIENT_PERM'
        )
    
    def test_admin_permission(self):
        """Test IsAdminUser permission."""
        from .permissions import IsAdminUser
        
        # Admin user should have admin permission
        request = self.factory.get('/admin/')
        request.user = self.admin_user
        
        permission = IsAdminUser()
        self.assertTrue(permission.has_permission(request, None))
        
        # Client user should not have admin permission
        request.user = self.client_user
        self.assertFalse(permission.has_permission(request, None))
    
    def test_client_permission(self):
        """Test IsClientUser permission."""
        from .permissions import IsClientUser
        
        # Client user should have client permission
        request = self.factory.get('/client/')
        request.user = self.client_user
        
        permission = IsClientUser()
        self.assertTrue(permission.has_permission(request, None))
        
        # Admin user should not have client permission
        request.user = self.admin_user
        self.assertFalse(permission.has_permission(request, None))
    
    def test_data_filtering(self):
        """Test ClientDataFilter functionality."""
        from .permissions import ClientDataFilter
        from .models import Asset
        
        # Create assets for different clients
        Asset.objects.create(
            client='CLIENT_PERM',
            ticker='AAPL',
            name='Apple Inc.',
            asset_type='Stock'
        )
        Asset.objects.create(
            client='OTHER_CLIENT',
            ticker='MSFT',
            name='Microsoft Corp.',
            asset_type='Stock'
        )
        
        all_assets = Asset.objects.all()
        
        # Admin should see all assets
        admin_filtered = ClientDataFilter.filter_queryset(all_assets, self.admin_user)
        self.assertEqual(admin_filtered.count(), 2)
        
        # Client should only see their own assets
        client_filtered = ClientDataFilter.filter_queryset(all_assets, self.client_user)
        self.assertEqual(client_filtered.count(), 1)
        self.assertEqual(client_filtered.first().client, 'CLIENT_PERM')