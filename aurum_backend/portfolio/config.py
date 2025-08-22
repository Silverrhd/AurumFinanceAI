"""
Centralized configuration system for AurumFinance portfolio management.
Provides validated environment variable management and configuration settings.
"""

import os
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from django.conf import settings

logger = logging.getLogger(__name__)


@dataclass
class APIClientConfig:
    """Configuration for external API clients."""
    
    # OpenFIGI API Configuration
    openfigi_api_key: Optional[str] = field(default=None)
    openfigi_base_url: str = field(default="https://api.openfigi.com/v3")
    openfigi_rate_limit: float = field(default=0.1)  # 100ms between requests
    openfigi_cache_timeout_hours: int = field(default=24)
    
    # Mindicador API Configuration
    mindicador_base_url: str = field(default="https://mindicador.cl/api")
    mindicador_rate_limit: float = field(default=1.0)  # 1 second between requests
    mindicador_cache_timeout_hours: int = field(default=6)
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        # Get OpenFIGI API key from environment
        self.openfigi_api_key = os.getenv('OPENFIGI_API_KEY')
        
        if not self.openfigi_api_key:
            logger.warning("OPENFIGI_API_KEY environment variable not set. Valley and IDB transformers may fail.")


@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    
    use_postgresql: bool = field(default=False)
    db_name: str = field(default="aurum_db")
    db_user: str = field(default="aurum_user")
    db_password: str = field(default="")
    db_host: str = field(default="localhost")
    db_port: int = field(default=5432)
    
    def __post_init__(self):
        """Load database configuration from environment."""
        self.use_postgresql = os.getenv('USE_POSTGRESQL', 'False').lower() == 'true'
        self.db_name = os.getenv('DB_NAME', self.db_name)
        self.db_user = os.getenv('DB_USER', self.db_user)
        self.db_password = os.getenv('DB_PASSWORD', self.db_password)
        self.db_host = os.getenv('DB_HOST', self.db_host)
        self.db_port = int(os.getenv('DB_PORT', str(self.db_port)))


@dataclass
class FileProcessingConfig:
    """File processing and parsing configuration."""
    
    # Upload limits
    max_file_size_mb: int = field(default=50)
    allowed_file_extensions: list = field(default_factory=lambda: ['.xlsx', '.xls'])
    
    # Parsing settings
    max_rows_per_file: int = field(default=10000)
    batch_size: int = field(default=1000)
    
    # Validation settings
    require_client_column: bool = field(default=True)
    validate_cusip_format: bool = field(default=True)
    
    # Directory settings
    upload_directory: str = field(default="uploads/")
    processed_directory: str = field(default="processed/")
    error_directory: str = field(default="errors/")
    
    def __post_init__(self):
        """Validate file processing configuration."""
        # Ensure upload directories are absolute paths
        if not os.path.isabs(self.upload_directory):
            self.upload_directory = os.path.join(settings.BASE_DIR, self.upload_directory)
        
        if not os.path.isabs(self.processed_directory):
            self.processed_directory = os.path.join(settings.BASE_DIR, self.processed_directory)
        
        if not os.path.isabs(self.error_directory):
            self.error_directory = os.path.join(settings.BASE_DIR, self.error_directory)


@dataclass
class CalculationConfig:
    """Portfolio calculation and performance settings."""
    
    # Modified Dietz calculation settings
    use_time_weighted_returns: bool = field(default=True)
    cash_flow_timing_precision: str = field(default='daily')  # daily, monthly
    
    # Performance calculation settings
    annualization_factor: int = field(default=365)
    risk_free_rate: float = field(default=0.02)  # 2% annual risk-free rate
    
    # Asset allocation settings
    asset_type_mappings: Dict[str, str] = field(default_factory=lambda: {
        'equity': 'Equity',
        'stock': 'Equity',
        'bond': 'Fixed Income',
        'fixed income': 'Fixed Income',
        'cash': 'Cash',
        'money market': 'Cash',
        'mutual fund': 'Mutual Fund',
        'etf': 'ETF',
        'reit': 'REIT',
        'commodity': 'Commodity',
        'derivative': 'Derivative'
    })
    
    # Chart data settings
    chart_colors: list = field(default_factory=lambda: [
        '#5f76a1', '#072061', '#dae1f3', '#b7babe', '#8aa5c6', '#4e6b8f'
    ])
    max_chart_points: int = field(default=365)  # Maximum data points per chart


@dataclass
class SecurityConfig:
    """Security and validation configuration."""
    
    # Password requirements
    min_password_length: int = field(default=8)
    require_password_complexity: bool = field(default=True)
    
    # Session management
    session_timeout_hours: int = field(default=24)
    
    # API rate limiting
    api_rate_limit_per_minute: int = field(default=1000)
    
    # Data validation
    require_ssl_in_production: bool = field(default=True)
    validate_client_codes: bool = field(default=True)
    log_sensitive_data: bool = field(default=False)


@dataclass
class LoggingConfig:
    """Logging configuration settings."""
    
    log_level: str = field(default="INFO")
    log_format: str = field(default="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    log_to_file: bool = field(default=True)
    log_directory: str = field(default="logs/")
    max_log_file_mb: int = field(default=10)
    log_retention_days: int = field(default=30)
    
    def __post_init__(self):
        """Set up logging configuration."""
        # Set log level from environment
        env_log_level = os.getenv('LOG_LEVEL', self.log_level)
        self.log_level = env_log_level.upper()
        
        # Ensure log directory is absolute path
        if not os.path.isabs(self.log_directory):
            self.log_directory = os.path.join(settings.BASE_DIR, self.log_directory)


class AurumConfig:
    """
    Centralized configuration manager for AurumFinance.
    Provides validated configuration settings for all application components.
    """
    
    def __init__(self):
        """Initialize configuration with validation."""
        self.api_clients = APIClientConfig()
        self.database = DatabaseConfig()
        self.file_processing = FileProcessingConfig()
        self.calculations = CalculationConfig()
        self.security = SecurityConfig()
        self.logging = LoggingConfig()
        
        self._validate_configuration()
        
        logger.info("AurumConfig initialized successfully")
    
    def _validate_configuration(self) -> None:
        """Validate critical configuration settings."""
        errors = []
        
        # Validate API keys for production
        if not settings.DEBUG and not self.api_clients.openfigi_api_key:
            errors.append("OPENFIGI_API_KEY is required in production environment")
        
        # Validate database configuration for PostgreSQL
        if self.database.use_postgresql:
            if not self.database.db_password and not settings.DEBUG:
                errors.append("Database password is required for PostgreSQL in production")
        
        # Validate file processing directories
        for directory in [
            self.file_processing.upload_directory,
            self.file_processing.processed_directory,
            self.file_processing.error_directory,
            self.logging.log_directory
        ]:
            try:
                os.makedirs(directory, exist_ok=True)
            except Exception as e:
                errors.append(f"Cannot create directory {directory}: {str(e)}")
        
        # Report validation errors
        if errors:
            error_msg = "Configuration validation failed:\n" + "\n".join(f"  - {error}" for error in errors)
            if settings.DEBUG:
                logger.warning(error_msg)
            else:
                raise ValueError(error_msg)
    
    def get_openfigi_client_config(self) -> Dict[str, Any]:
        """Get OpenFIGI client configuration."""
        return {
            'api_key': self.api_clients.openfigi_api_key,
            'base_url': self.api_clients.openfigi_base_url,
            'rate_limit': self.api_clients.openfigi_rate_limit,
            'cache_timeout_hours': self.api_clients.openfigi_cache_timeout_hours
        }
    
    def get_mindicador_client_config(self) -> Dict[str, Any]:
        """Get Mindicador client configuration."""
        return {
            'base_url': self.api_clients.mindicador_base_url,
            'rate_limit': self.api_clients.mindicador_rate_limit,
            'cache_timeout_hours': self.api_clients.mindicador_cache_timeout_hours
        }
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration."""
        return {
            'use_postgresql': self.database.use_postgresql,
            'db_name': self.database.db_name,
            'db_user': self.database.db_user,
            'db_password': self.database.db_password,
            'db_host': self.database.db_host,
            'db_port': self.database.db_port
        }
    
    def is_development(self) -> bool:
        """Check if running in development mode."""
        return settings.DEBUG
    
    def is_production(self) -> bool:
        """Check if running in production mode."""
        return not settings.DEBUG
    
    def get_environment(self) -> str:
        """Get current environment name."""
        return os.getenv('DJANGO_ENVIRONMENT', 'development' if settings.DEBUG else 'production')
    
    def get_config_summary(self) -> Dict[str, Any]:
        """Get a summary of configuration for logging/debugging."""
        return {
            'environment': self.get_environment(),
            'debug_mode': settings.DEBUG,
            'database_type': 'postgresql' if self.database.use_postgresql else 'sqlite',
            'openfigi_configured': bool(self.api_clients.openfigi_api_key),
            'log_level': self.logging.log_level,
            'file_processing': {
                'max_file_size_mb': self.file_processing.max_file_size_mb,
                'allowed_extensions': self.file_processing.allowed_file_extensions
            }
        }


# Global configuration instance
config = AurumConfig()


def get_config() -> AurumConfig:
    """Get the global configuration instance."""
    return config