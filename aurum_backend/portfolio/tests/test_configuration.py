"""
Test suite for AurumFinance configuration system.
Tests centralized configuration management and validation.
"""

import os
import unittest
from unittest.mock import patch, Mock
from django.test import TestCase, override_settings

# Import with override for testing
with patch.dict(os.environ, {'DEBUG': 'True'}, clear=False):
    from ..config import (
        APIClientConfig,
        DatabaseConfig,
        FileProcessingConfig,
        CalculationConfig,
        SecurityConfig,
        LoggingConfig,
        AurumConfig,
        get_config
    )


class TestAPIClientConfig(TestCase):
    """Test API client configuration."""
    
    def test_default_configuration(self):
        """Test default API client configuration."""
        config = APIClientConfig()
        
        self.assertEqual(config.openfigi_base_url, "https://api.openfigi.com/v3")
        self.assertEqual(config.openfigi_rate_limit, 0.1)
        self.assertEqual(config.openfigi_cache_timeout_hours, 24)
        self.assertEqual(config.mindicador_base_url, "https://mindicador.cl/api")
        self.assertEqual(config.mindicador_rate_limit, 1.0)
        self.assertEqual(config.mindicador_cache_timeout_hours, 6)
    
    @patch.dict(os.environ, {'OPENFIGI_API_KEY': 'test-key-123'})
    def test_api_key_from_environment(self):
        """Test API key loading from environment variable."""
        config = APIClientConfig()
        self.assertEqual(config.openfigi_api_key, 'test-key-123')
    
    @patch.dict(os.environ, {}, clear=True)
    def test_missing_api_key_warning(self):
        """Test warning when API key is missing."""
        with self.assertLogs(level='WARNING') as cm:
            config = APIClientConfig()
            self.assertIsNone(config.openfigi_api_key)
            self.assertTrue(any('OPENFIGI_API_KEY' in message for message in cm.output))


class TestDatabaseConfig(TestCase):
    """Test database configuration."""
    
    def test_default_configuration(self):
        """Test default database configuration."""
        config = DatabaseConfig()
        
        self.assertFalse(config.use_postgresql)
        self.assertEqual(config.db_name, "aurum_db")
        self.assertEqual(config.db_user, "aurum_user")
        self.assertEqual(config.db_host, "localhost")
        self.assertEqual(config.db_port, 5432)
    
    @patch.dict(os.environ, {
        'USE_POSTGRESQL': 'true',
        'DB_NAME': 'test_db',
        'DB_USER': 'test_user',
        'DB_PASSWORD': 'test_pass',
        'DB_HOST': 'test_host',
        'DB_PORT': '3306'
    })
    def test_environment_configuration(self):
        """Test database configuration from environment."""
        config = DatabaseConfig()
        
        self.assertTrue(config.use_postgresql)
        self.assertEqual(config.db_name, "test_db")
        self.assertEqual(config.db_user, "test_user")
        self.assertEqual(config.db_password, "test_pass")
        self.assertEqual(config.db_host, "test_host")
        self.assertEqual(config.db_port, 3306)


class TestFileProcessingConfig(TestCase):
    """Test file processing configuration."""
    
    def test_default_configuration(self):
        """Test default file processing configuration."""
        config = FileProcessingConfig()
        
        self.assertEqual(config.max_file_size_mb, 50)
        self.assertEqual(config.allowed_file_extensions, ['.xlsx', '.xls'])
        self.assertEqual(config.max_rows_per_file, 10000)
        self.assertEqual(config.batch_size, 1000)
        self.assertTrue(config.require_client_column)
        self.assertTrue(config.validate_cusip_format)
    
    def test_directory_path_resolution(self):
        """Test directory path resolution to absolute paths."""
        config = FileProcessingConfig()
        
        # Paths should be absolute
        self.assertTrue(os.path.isabs(config.upload_directory))
        self.assertTrue(os.path.isabs(config.processed_directory))
        self.assertTrue(os.path.isabs(config.error_directory))


class TestCalculationConfig(TestCase):
    """Test calculation configuration."""
    
    def test_default_configuration(self):
        """Test default calculation configuration."""
        config = CalculationConfig()
        
        self.assertTrue(config.use_time_weighted_returns)
        self.assertEqual(config.cash_flow_timing_precision, 'daily')
        self.assertEqual(config.annualization_factor, 365)
        self.assertEqual(config.risk_free_rate, 0.02)
        self.assertEqual(config.max_chart_points, 365)
    
    def test_asset_type_mappings(self):
        """Test asset type mappings."""
        config = CalculationConfig()
        
        self.assertEqual(config.asset_type_mappings['equity'], 'Equity')
        self.assertEqual(config.asset_type_mappings['bond'], 'Bond')
        self.assertEqual(config.asset_type_mappings['cash'], 'Cash')
        self.assertIn('etf', config.asset_type_mappings)
        self.assertIn('reit', config.asset_type_mappings)
    
    def test_chart_colors(self):
        """Test chart color configuration."""
        config = CalculationConfig()
        
        self.assertGreater(len(config.chart_colors), 0)
        self.assertIn('#5f76a1', config.chart_colors)
        self.assertIn('#072061', config.chart_colors)


class TestSecurityConfig(TestCase):
    """Test security configuration."""
    
    def test_default_configuration(self):
        """Test default security configuration."""
        config = SecurityConfig()
        
        self.assertEqual(config.min_password_length, 8)
        self.assertTrue(config.require_password_complexity)
        self.assertEqual(config.session_timeout_hours, 24)
        self.assertEqual(config.api_rate_limit_per_minute, 1000)
        self.assertTrue(config.require_ssl_in_production)
        self.assertTrue(config.validate_client_codes)
        self.assertFalse(config.log_sensitive_data)


class TestLoggingConfig(TestCase):
    """Test logging configuration."""
    
    def test_default_configuration(self):
        """Test default logging configuration."""
        config = LoggingConfig()
        
        self.assertEqual(config.log_level, "INFO")
        self.assertIn('%(asctime)s', config.log_format)
        self.assertTrue(config.log_to_file)
        self.assertEqual(config.max_log_file_mb, 10)
        self.assertEqual(config.log_retention_days, 30)
    
    @patch.dict(os.environ, {'LOG_LEVEL': 'debug'})
    def test_log_level_from_environment(self):
        """Test log level configuration from environment."""
        config = LoggingConfig()
        self.assertEqual(config.log_level, "DEBUG")  # Should be uppercase
    
    def test_log_directory_path_resolution(self):
        """Test log directory path resolution."""
        config = LoggingConfig()
        self.assertTrue(os.path.isabs(config.log_directory))


class TestAurumConfig(TestCase):
    """Test main AurumConfig class."""
    
    @patch('portfolio.config.os.makedirs')
    def test_configuration_initialization(self, mock_makedirs):
        """Test configuration initialization."""
        config = AurumConfig()
        
        # Check that all sub-configurations are initialized
        self.assertIsInstance(config.api_clients, APIClientConfig)
        self.assertIsInstance(config.database, DatabaseConfig)
        self.assertIsInstance(config.file_processing, FileProcessingConfig)
        self.assertIsInstance(config.calculations, CalculationConfig)
        self.assertIsInstance(config.security, SecurityConfig)
        self.assertIsInstance(config.logging, LoggingConfig)
        
        # Check that directories are created
        self.assertTrue(mock_makedirs.called)
    
    def test_configuration_getters(self):
        """Test configuration getter methods."""
        config = AurumConfig()
        
        # Test OpenFIGI config
        openfigi_config = config.get_openfigi_client_config()
        self.assertIn('api_key', openfigi_config)
        self.assertIn('base_url', openfigi_config)
        self.assertIn('rate_limit', openfigi_config)
        
        # Test Mindicador config
        mindicador_config = config.get_mindicador_client_config()
        self.assertIn('base_url', mindicador_config)
        self.assertIn('rate_limit', mindicador_config)
        
        # Test database config
        db_config = config.get_database_config()
        self.assertIn('use_postgresql', db_config)
        self.assertIn('db_name', db_config)
    
    @override_settings(DEBUG=True)
    def test_development_mode_detection(self):
        """Test development mode detection."""
        config = AurumConfig()
        
        self.assertTrue(config.is_development())
        self.assertFalse(config.is_production())
        self.assertEqual(config.get_environment(), 'development')
    
    @override_settings(DEBUG=False)
    def test_production_mode_detection(self):
        """Test production mode detection."""
        config = AurumConfig()
        
        self.assertFalse(config.is_development())
        self.assertTrue(config.is_production())
        self.assertEqual(config.get_environment(), 'production')
    
    def test_configuration_summary(self):
        """Test configuration summary generation."""
        config = AurumConfig()
        summary = config.get_config_summary()
        
        self.assertIn('environment', summary)
        self.assertIn('debug_mode', summary)
        self.assertIn('database_type', summary)
        self.assertIn('openfigi_configured', summary)
        self.assertIn('log_level', summary)
        self.assertIn('file_processing', summary)
    
    @patch.dict(os.environ, {}, clear=True)
    @override_settings(DEBUG=False)
    def test_production_validation_errors(self):
        """Test configuration validation in production."""
        with self.assertRaises(ValueError) as context:
            config = AurumConfig()
        
        error_message = str(context.exception)
        self.assertIn('OPENFIGI_API_KEY is required in production', error_message)
    
    @patch.dict(os.environ, {}, clear=True)
    @override_settings(DEBUG=True)
    def test_development_validation_warnings(self):
        """Test configuration validation warnings in development."""
        with self.assertLogs(level='WARNING') as cm:
            config = AurumConfig()
            
        # Should log warnings but not raise errors in development
        self.assertTrue(any('Configuration validation failed' in message for message in cm.output))


class TestConfigurationIntegration(TestCase):
    """Test configuration integration with other components."""
    
    def test_global_config_instance(self):
        """Test global configuration instance."""
        config1 = get_config()
        config2 = get_config()
        
        # Should return the same instance
        self.assertIs(config1, config2)
    
    @patch('portfolio.config.AurumConfig')
    def test_configuration_singleton_behavior(self, mock_aurum_config):
        """Test that configuration behaves as singleton."""
        # Import should only initialize once
        from portfolio.config import config
        
        # Multiple imports should use same instance
        from portfolio.config import get_config
        config_instance = get_config()
        
        # Should be the same object
        self.assertIs(config, config_instance)


if __name__ == '__main__':
    unittest.main()