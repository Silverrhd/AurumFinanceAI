"""
Test suite for AurumFinance API clients.
Tests OpenFIGI and Mindicador API client functionality.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from django.test import TestCase, override_settings

# Import API clients
from ..preprocessing.utils.openfigi_client import OpenFIGIClient
from ..preprocessing.utils.mindicador_client import MindicadorClient


class TestOpenFIGIClient(TestCase):
    """Test OpenFIGI API client functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock the API key to avoid requiring real credentials
        self.test_api_key = 'test-api-key-12345'
        self.client = OpenFIGIClient(api_key=self.test_api_key)
    
    def test_client_initialization(self):
        """Test client initialization."""
        self.assertEqual(self.client.api_key, self.test_api_key)
        self.assertEqual(self.client.base_url, "https://api.openfigi.com/v3")
        self.assertIsNotNone(self.client.session)
        self.assertEqual(self.client.stats['api_calls'], 0)
    
    def test_invalid_cusip_handling(self):
        """Test handling of invalid CUSIPs."""
        invalid_cusips = ['', '0', None, '   ']
        
        for cusip in invalid_cusips:
            with self.subTest(cusip=cusip):
                result = self.client.lookup_by_cusip(cusip)
                self.assertIn('error', result)
    
    def test_empty_batch_lookup(self):
        """Test batch lookup with empty input."""
        result = self.client.batch_lookup([])
        self.assertEqual(result, {})
    
    def test_cache_functionality(self):
        """Test caching mechanism."""
        # Add item to cache
        test_result = {'security_type': 'Equity', 'ticker': 'AAPL'}
        self.client._add_to_cache('test_key', test_result)
        
        # Retrieve from cache
        cached_result = self.client._get_from_cache('test_key')
        self.assertEqual(cached_result, test_result)
        
        # Test cache miss
        missing_result = self.client._get_from_cache('nonexistent_key')
        self.assertIsNone(missing_result)
    
    def test_coupon_parsing(self):
        """Test coupon rate parsing."""
        # Numeric values
        self.assertEqual(self.client._parse_coupon(2.5), 2.5)
        self.assertEqual(self.client._parse_coupon(0), 0.0)
        
        # String values
        self.assertEqual(self.client._parse_coupon('3.75'), 3.75)
        self.assertEqual(self.client._parse_coupon('2.5%'), 2.5)
        
        # Invalid values
        self.assertIsNone(self.client._parse_coupon(None))
        self.assertIsNone(self.client._parse_coupon('invalid'))
        self.assertIsNone(self.client._parse_coupon(''))
    
    def test_api_result_transformation(self):
        """Test transformation of API results."""
        mock_api_data = {
            'securityType': 'Common Stock',
            'securityType2': 'Equity',
            'ticker': 'AAPL',
            'name': 'Apple Inc',
            'coupon': '2.5',
            'maturity': '2025-12-31',
            'compositeFIGI': 'BBG000B9XRY4',
            'figi': 'BBG000B9XRY4'
        }
        
        result = self.client._transform_api_result(mock_api_data)
        
        self.assertEqual(result['security_type'], 'Common Stock')
        self.assertEqual(result['security_type2'], 'Equity')
        self.assertEqual(result['ticker'], 'AAPL')
        self.assertEqual(result['name'], 'Apple Inc')
        self.assertEqual(result['coupon'], 2.5)
        self.assertEqual(result['maturity'], '2025-12-31')
    
    @patch('portfolio.preprocessing.utils.openfigi_client.requests.Session.post')
    def test_successful_batch_lookup(self, mock_post):
        """Test successful batch lookup with mocked API response."""
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                'data': [{
                    'securityType': 'Common Stock',
                    'ticker': 'AAPL',
                    'name': 'Apple Inc',
                    'compositeFIGI': 'BBG000B9XRY4'
                }]
            }
        ]
        mock_post.return_value = mock_response
        
        result = self.client.batch_lookup(['037833100'])  # Apple CUSIP
        
        self.assertIn('037833100', result)
        self.assertEqual(result['037833100']['security_type'], 'Common Stock')
        self.assertEqual(result['037833100']['ticker'], 'AAPL')
        self.assertEqual(self.client.stats['api_calls'], 1)
        self.assertEqual(self.client.stats['successful_lookups'], 1)
    
    @patch('portfolio.preprocessing.utils.openfigi_client.requests.Session.post')
    def test_failed_api_request(self, mock_post):
        """Test handling of failed API requests."""
        # Mock failed API response
        mock_response = Mock()
        mock_response.status_code = 404
        mock_post.return_value = mock_response
        
        result = self.client.batch_lookup(['invalid_cusip'])
        
        self.assertIn('invalid_cusip', result)
        self.assertIn('error', result['invalid_cusip'])
        self.assertEqual(self.client.stats['api_calls'], 1)
    
    def test_client_stats(self):
        """Test client statistics tracking."""
        # Initial stats
        stats = self.client.get_client_stats()
        self.assertEqual(stats['api_calls'], 0)
        self.assertEqual(stats['cache_hits'], 0)
        
        # Update stats manually for testing
        self.client.stats['api_calls'] = 5
        self.client.stats['cache_hits'] = 3
        self.client.stats['successful_lookups'] = 4
        
        updated_stats = self.client.get_client_stats()
        self.assertEqual(updated_stats['api_calls'], 5)
        self.assertEqual(updated_stats['cache_hits'], 3)
        self.assertEqual(updated_stats['successful_lookups'], 4)
        self.assertGreater(updated_stats['hit_rate'], 0)
    
    def test_cache_management(self):
        """Test cache management functions."""
        # Add some test data
        self.client._add_to_cache('key1', {'data': 'test1'})
        self.client._add_to_cache('key2', {'data': 'test2'})
        
        # Check cache size
        self.assertEqual(self.client.get_cache_size(), 2)
        
        # Clear cache
        self.client.clear_cache()
        self.assertEqual(self.client.get_cache_size(), 0)


@override_settings(DEBUG=True)
class TestMindicadorClient(TestCase):
    """Test Mindicador API client functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.client = MindicadorClient()
    
    def test_client_initialization(self):
        """Test client initialization."""
        self.assertEqual(self.client.base_url, "https://mindicador.cl/api")
        self.assertIsNotNone(self.client.session)
        self.assertEqual(self.client.stats['api_calls'], 0)
    
    def test_usd_passthrough(self):
        """Test USD currency passthrough."""
        result = self.client.convert_to_usd(100.0, 'USD')
        self.assertEqual(result, 100.0)
        self.assertEqual(self.client.stats['usd_passthrough'], 1)
    
    def test_invalid_input_handling(self):
        """Test handling of invalid inputs."""
        # Zero value
        result = self.client.convert_to_usd(0, 'CLP')
        self.assertEqual(result, 0.0)
        
        # None value
        result = self.client.convert_to_usd(None, 'CLP')
        self.assertEqual(result, 0.0)
        
        # Invalid currency (should return original value)
        result = self.client.convert_to_usd(100.0, 'EUR')
        self.assertEqual(result, 100.0)
    
    def test_supported_currencies(self):
        """Test supported currency methods."""
        supported = self.client.get_supported_currencies()
        self.assertIn('CLP', supported)
        self.assertIn('UF', supported)
        self.assertIn('USD', supported)
        
        self.assertTrue(self.client.is_currency_supported('CLP'))
        self.assertTrue(self.client.is_currency_supported('uf'))  # Case insensitive
        self.assertFalse(self.client.is_currency_supported('EUR'))
    
    def test_cache_functionality(self):
        """Test caching mechanism."""
        # Add item to cache
        test_rate = 850.0
        self.client._add_to_cache('rate_CLP', test_rate)
        
        # Retrieve from cache
        cached_rate = self.client._get_from_cache('rate_CLP')
        self.assertEqual(cached_rate, test_rate)
        
        # Test cache miss
        missing_rate = self.client._get_from_cache('rate_EUR')
        self.assertIsNone(missing_rate)
    
    @patch('portfolio.preprocessing.utils.mindicador_client.requests.Session.get')
    def test_successful_clp_conversion(self, mock_get):
        """Test successful CLP to USD conversion with mocked API response."""
        # Mock successful API response for USD/CLP rate
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'serie': [{'valor': 800.0}]  # 800 CLP per USD
        }
        mock_get.return_value = mock_response
        
        result = self.client.convert_to_usd(8000.0, 'CLP')  # 8000 CLP
        
        # Should be 10 USD (8000 / 800)
        self.assertEqual(result, 10.0)
        self.assertEqual(self.client.stats['api_calls'], 1)
        self.assertEqual(self.client.stats['successful_conversions'], 1)
    
    @patch('portfolio.preprocessing.utils.mindicador_client.requests.Session.get')
    def test_failed_api_request(self, mock_get):
        """Test handling of failed API requests."""
        # Mock failed API response
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        result = self.client.convert_to_usd(1000.0, 'CLP')
        
        self.assertEqual(result, 0.0)
        self.assertEqual(self.client.stats['api_calls'], 1)
        self.assertEqual(self.client.stats['failed_conversions'], 1)
    
    def test_client_stats(self):
        """Test client statistics tracking."""
        # Initial stats
        stats = self.client.get_client_stats()
        self.assertEqual(stats['api_calls'], 0)
        self.assertEqual(stats['successful_conversions'], 0)
        
        # Update stats manually for testing
        self.client.stats['api_calls'] = 3
        self.client.stats['successful_conversions'] = 2
        self.client.stats['usd_passthrough'] = 5
        
        updated_stats = self.client.get_client_stats()
        self.assertEqual(updated_stats['api_calls'], 3)
        self.assertEqual(updated_stats['successful_conversions'], 2)
        self.assertEqual(updated_stats['usd_passthrough'], 5)
    
    def test_cache_management(self):
        """Test cache management functions."""
        # Add some test data
        self.client._add_to_cache('rate_CLP', 800.0)
        self.client._add_to_cache('rate_UF', 32000.0)
        
        # Clear cache
        self.client.clear_cache()
        
        # Cache should be empty
        self.assertIsNone(self.client._get_from_cache('rate_CLP'))
        self.assertIsNone(self.client._get_from_cache('rate_UF'))


if __name__ == '__main__':
    unittest.main()