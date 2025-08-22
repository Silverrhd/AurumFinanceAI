"""
Django settings for aurum_backend project.
Supports both development and production environments via environment variables.
"""

from pathlib import Path
from datetime import timedelta
import os

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv not installed, will use system environment variables
    pass

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.environ.get('SECRET_KEY', 'django-insecure-42o=inx8sr*sqiyev1^kgqasxn^ni(yhp%r*vrmcz#=b0o_t=s')

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = os.environ.get('DEBUG', 'True').lower() == 'true'

# Environment detection
ENVIRONMENT = os.environ.get('DJANGO_ENVIRONMENT', 'development')
IS_PRODUCTION = ENVIRONMENT == 'production'

# Allowed hosts configuration
ALLOWED_HOSTS = ['localhost', '127.0.0.1', '18.231.106.188']
if IS_PRODUCTION:
    additional_hosts = os.environ.get('ALLOWED_HOSTS', '')
    if additional_hosts:
        ALLOWED_HOSTS = [host.strip() for host in additional_hosts.split(',') if host.strip()]
    else:
        ALLOWED_HOSTS = []  # Must be set in production

# Application definition
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    'rest_framework_simplejwt',
    'rest_framework_simplejwt.token_blacklist',
    'drf_spectacular',  # OpenAPI 3.0 schema generation
    'corsheaders',
    'portfolio',  # Aurum Finance portfolio models
]

# Add debug toolbar in development
if DEBUG and not IS_PRODUCTION:
    try:
        import debug_toolbar
        INSTALLED_APPS.append('debug_toolbar')
    except ImportError:
        pass

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'corsheaders.middleware.CorsMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

# Add debug toolbar middleware in development
if DEBUG and not IS_PRODUCTION and 'debug_toolbar' in INSTALLED_APPS:
    MIDDLEWARE.insert(1, 'debug_toolbar.middleware.DebugToolbarMiddleware')

ROOT_URLCONF = 'aurum_backend.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR / 'templates'],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'aurum_backend.wsgi.application'

# Database configuration
USE_POSTGRESQL = os.environ.get('USE_POSTGRESQL', 'False').lower() == 'true'

if USE_POSTGRESQL:
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql',
            'NAME': os.environ.get('DB_NAME', 'aurum_finance_dev'),
            'USER': os.environ.get('DB_USER', 'postgres'),
            'PASSWORD': os.environ.get('DB_PASSWORD', ''),
            'HOST': os.environ.get('DB_HOST', 'localhost'),
            'PORT': os.environ.get('DB_PORT', '5432'),
        }
    }
    
    # Production database optimizations
    if IS_PRODUCTION:
        DATABASES['default'].update({
            'CONN_MAX_AGE': 600,
            'OPTIONS': {
                'sslmode': 'require',
            },
        })
else:
    # SQLite for development
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': BASE_DIR / 'db.sqlite3',
        }
    }

# Password validation
AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

# Internationalization
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_TZ = True

# Date Input Format Configuration - Support DD/MM/YYYY format from frontend
DATE_INPUT_FORMATS = [
    '%d/%m/%Y',      # 29/05/2025 (primary - matches frontend display)
    '%Y-%m-%d',      # 2025-05-29 (fallback for compatibility)
    '%d-%m-%Y',      # 29-05-2025 (additional format)
    '%d.%m.%Y',      # 29.05.2025 (European format)
]

# Ensure date parsing uses these formats
USE_L10N = True  # Enable localization for date parsing

# Static files (CSS, JavaScript, Images)
STATIC_URL = '/static/'
STATIC_ROOT = BASE_DIR / 'staticfiles'
STATICFILES_DIRS = [
    BASE_DIR / 'static',
]

# Media files
MEDIA_URL = '/media/'
MEDIA_ROOT = BASE_DIR / 'media'

# Default primary key field type
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# Custom User Model
AUTH_USER_MODEL = 'portfolio.User'

# Django REST Framework
REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': [
        'rest_framework_simplejwt.authentication.JWTAuthentication',
        'rest_framework.authentication.SessionAuthentication',
    ],
    'DEFAULT_PERMISSION_CLASSES': [
        'rest_framework.permissions.IsAuthenticated',
    ],
    'DEFAULT_RENDERER_CLASSES': [
        'rest_framework.renderers.JSONRenderer',
    ],
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.PageNumberPagination',
    'PAGE_SIZE': 50,
    'DEFAULT_FILTER_BACKENDS': [
        'rest_framework.filters.SearchFilter',
        'rest_framework.filters.OrderingFilter',
    ],
    'DEFAULT_SCHEMA_CLASS': 'drf_spectacular.openapi.AutoSchema',
}

# API Documentation Settings
SPECTACULAR_SETTINGS = {
    'TITLE': 'Aurum Finance Portfolio Management API',
    'DESCRIPTION': '''
    Complete API for portfolio management, bank file processing, and report generation.
    
    ## Key Features:
    - Multi-tenant client isolation
    - Role-based access control (Admin/Client)
    - Bank file processing (JPM, MS, CS, Valley, Pershing, HSBC, JB, CSC)
    - Automated report generation
    - Real-time dashboard data
    
    ## Authentication:
    Use JWT tokens obtained from the login endpoint. Include in requests as:
    `Authorization: Bearer <your_access_token>`
    
    ## User Roles:
    - **Admin**: Can access all client data and management functions
    - **Client**: Can only access their own portfolio data
    ''',
    'VERSION': '1.0.0',
    'SERVE_INCLUDE_SCHEMA': False,
    'COMPONENT_SPLIT_REQUEST': True,
    'SWAGGER_UI_SETTINGS': {
        'deepLinking': True,
        'persistAuthorization': True,
        'displayOperationId': True,
        'filter': True,
    },
    'TAGS': [
        {'name': 'Authentication', 'description': 'User login, logout, and token management'},
        {'name': 'File Processing', 'description': 'Bank file upload and preprocessing (12 banks supported)'},
        {'name': 'Report Generation', 'description': 'Weekly, bond issuer, bond maturity, and equity breakdown reports'},
        {'name': 'Dashboard - Admin', 'description': 'Admin dashboard data and charts'},
        {'name': 'Dashboard - Client', 'description': 'Client-specific dashboard data'},
        {'name': 'System Monitoring', 'description': 'Health checks and system status'},
    ]
}

# JWT Settings
SIMPLE_JWT = {
    'ACCESS_TOKEN_LIFETIME': timedelta(hours=1),
    'REFRESH_TOKEN_LIFETIME': timedelta(days=7),
    'ROTATE_REFRESH_TOKENS': True,
    'BLACKLIST_AFTER_ROTATION': True,
    'UPDATE_LAST_LOGIN': True,
    'ALGORITHM': 'HS256',
    'SIGNING_KEY': SECRET_KEY,
    'VERIFYING_KEY': None,
    'AUTH_HEADER_TYPES': ('Bearer',),
    'AUTH_HEADER_NAME': 'HTTP_AUTHORIZATION',
    'USER_ID_FIELD': 'id',
    'USER_ID_CLAIM': 'user_id',
    'AUTH_TOKEN_CLASSES': ('rest_framework_simplejwt.tokens.AccessToken',),
    'TOKEN_TYPE_CLAIM': 'token_type',
}

# CORS settings
CORS_ALLOWED_ORIGINS = [
    "http://localhost:3000",  # Next.js default port
    "http://localhost:3001",  # Next.js running on 3001
    "http://127.0.0.1:3000",
    "http://127.0.0.1:3001",
]

# Production CORS settings
if IS_PRODUCTION:
    cors_origins = os.environ.get('CORS_ALLOWED_ORIGINS', '')
    if cors_origins:
        CORS_ALLOWED_ORIGINS = [origin.strip() for origin in cors_origins.split(',') if origin.strip()]

CORS_ALLOW_CREDENTIALS = True

# Security settings for production
if IS_PRODUCTION:
    SECURE_BROWSER_XSS_FILTER = True
    SECURE_CONTENT_TYPE_NOSNIFF = True
    SECURE_HSTS_INCLUDE_SUBDOMAINS = True
    SECURE_HSTS_PRELOAD = True
    SECURE_HSTS_SECONDS = 31536000
    SECURE_SSL_REDIRECT = os.environ.get('SECURE_SSL_REDIRECT', 'True').lower() == 'true'
    SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')
    
    # Session security
    SESSION_COOKIE_SECURE = True
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Strict'
    CSRF_COOKIE_SECURE = True
    CSRF_COOKIE_HTTPONLY = True
    CSRF_COOKIE_SAMESITE = 'Strict'

# Email configuration
if IS_PRODUCTION:
    EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
    EMAIL_HOST = os.environ.get('EMAIL_HOST', '')
    EMAIL_PORT = int(os.environ.get('EMAIL_PORT', '587'))
    EMAIL_USE_TLS = os.environ.get('EMAIL_USE_TLS', 'True').lower() == 'true'
    EMAIL_HOST_USER = os.environ.get('EMAIL_HOST_USER', '')
    EMAIL_HOST_PASSWORD = os.environ.get('EMAIL_HOST_PASSWORD', '')
    DEFAULT_FROM_EMAIL = os.environ.get('DEFAULT_FROM_EMAIL', 'noreply@aurumfinance.com')
else:
    EMAIL_BACKEND = 'django.core.mail.backends.console.EmailBackend'

# Cache configuration
if IS_PRODUCTION:
    CACHES = {
        'default': {
            'BACKEND': 'django.core.cache.backends.redis.RedisCache',
            'LOCATION': os.environ.get('REDIS_URL', 'redis://127.0.0.1:6379/1'),
        }
    }
else:
    CACHES = {
        'default': {
            'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        }
    }

# Enhanced Logging Configuration for Monitoring
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {name} {module} {funcName} {lineno} {message}',
            'style': '{',
        },
        'simple': {
            'format': '{levelname} {asctime} {message}',
            'style': '{',
        },
        'json': {
            'format': '{{"level": "{levelname}", "time": "{asctime}", "name": "{name}", "module": "{module}", "function": "{funcName}", "line": {lineno}, "message": "{message}"}}',
            'style': '{',
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG' if DEBUG else 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'simple',
        },
        'file_info': {
            'level': 'INFO',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': BASE_DIR / 'logs' / 'aurum_info.log',
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5,
            'formatter': 'verbose',
        },
        'file_error': {
            'level': 'ERROR',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': BASE_DIR / 'logs' / 'aurum_error.log',
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5,
            'formatter': 'json',
        },
        'file_security': {
            'level': 'WARNING',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': BASE_DIR / 'logs' / 'security.log',
            'maxBytes': 10485760,  # 10MB
            'backupCount': 10,
            'formatter': 'verbose',
        },
    },
    'root': {
        'handlers': ['console', 'file_info', 'file_error'],
        'level': 'DEBUG' if DEBUG else 'INFO',
    },
    'loggers': {
        'django': {
            'handlers': ['console', 'file_info'],
            'level': 'INFO',
            'propagate': False,
        },
        'django.security': {
            'handlers': ['file_security'],
            'level': 'WARNING',
            'propagate': False,
        },
        'aurum_backend': {
            'handlers': ['console', 'file_info', 'file_error'],
            'level': 'DEBUG' if DEBUG else 'INFO',
            'propagate': False,
        },
        'portfolio.services': {
            'handlers': ['console', 'file_info', 'file_error'],
            'level': 'DEBUG',
            'propagate': False,
        },
        'aurum_backup': {
            'handlers': ['console', 'file_info'],
            'level': 'INFO',
            'propagate': False,
        },
        'aurum_restore': {
            'handlers': ['console', 'file_info'],
            'level': 'INFO',
            'propagate': False,
        },
        'aurum_scheduler': {
            'handlers': ['console', 'file_info'],
            'level': 'INFO',
            'propagate': False,
        },
    },
}

# Add file logging in production
if IS_PRODUCTION:
    # Create logs directory if it doesn't exist
    log_dir = BASE_DIR / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    LOGGING['handlers']['file'] = {
        'level': 'INFO',
        'class': 'logging.FileHandler',
        'filename': log_dir / 'django.log',
        'formatter': 'verbose',
    }
    LOGGING['root']['handlers'].append('file')
    LOGGING['loggers']['django']['handlers'].append('file')
    LOGGING['loggers']['aurum_backend']['handlers'].append('file')

# Debug toolbar settings
if DEBUG and not IS_PRODUCTION:
    INTERNAL_IPS = ['127.0.0.1', 'localhost']

# File Upload Settings
FILE_UPLOAD_MAX_MEMORY_SIZE = 50 * 1024 * 1024  # 50MB
DATA_UPLOAD_MAX_MEMORY_SIZE = 50 * 1024 * 1024  # 50MB
FILE_UPLOAD_PERMISSIONS = 0o644

# Custom settings for Aurum Finance
AURUM_SETTINGS = {
    'SUPPORTED_BANKS': [
        'JPM', 'MS', 'CS', 'Valley', 'Pershing', 
        'HSBC', 'JB', 'CSC', 'Banchile', 'LO', 'IDB', 'Safra'
    ],
    'BANK_PROCESSING_REQUIREMENTS': {
        # Simple processing only
        'JPM': {'enrichment': False, 'combination': False},
        'MS': {'enrichment': False, 'combination': False},
        'IDB': {'enrichment': False, 'combination': False},
        'Safra': {'enrichment': False, 'combination': False},
        
        # Enrichment only
        'HSBC': {'enrichment': True, 'combination': False},
        
        # Combination only
        'CS': {'enrichment': False, 'combination': True},
        'Valley': {'enrichment': False, 'combination': True},
        'JB': {'enrichment': False, 'combination': True},
        'CSC': {'enrichment': False, 'combination': True},
        'Banchile': {'enrichment': False, 'combination': True},
        
        # Both enrichment and combination
        'Pershing': {'enrichment': True, 'combination': True},
        'LO': {'enrichment': True, 'combination': True},  # Lombard
    },
    'ALLOWED_FILE_EXTENSIONS': ['.xlsx', '.xls'],
    'MAX_PROCESSING_TIME_MINUTES': 3,
    'REPORTS_DIR': BASE_DIR / 'reports',
    'DATA_DIR': BASE_DIR / 'data',
    'ENABLE_FILE_CLEANUP': IS_PRODUCTION,  # Only cleanup files in production
    'MOCK_BANK_PROCESSING': False,  # Always use real processing
    'MAPPINGS_FILE': BASE_DIR / 'static' / 'Mappings.xlsx',  # Account mappings
    'BUSINESS_LOGIC_PATH': BASE_DIR / 'portfolio' / 'business_logic',
    'PREPROCESSING_PATH': BASE_DIR / 'portfolio' / 'preprocessing',
}

# Backup and Monitoring Settings
BACKUP_SETTINGS = {
    'RETENTION_DAYS': 30,           # Keep daily backups for 30 days
    'WEEKLY_RETENTION': 12,         # Keep weekly backups for 3 months  
    'MONTHLY_RETENTION': 12,        # Keep monthly backups for 1 year
    'BACKUP_LOCATION': BASE_DIR / 'backups',
    'REMOTE_BACKUP': False,         # Enable for production
    'COMPRESS_BACKUPS': True,
    'VERIFY_BACKUPS': True,
    'MAX_BACKUP_SIZE_GB': 10,
    'CREATE_PRE_RESTORE_BACKUP': True,
    'REQUIRE_CONFIRMATION': True,
}

BACKUP_SCHEDULE = {
    'DAILY_BACKUP_TIME': '02:00',      # 2 AM daily
    'WEEKLY_BACKUP_DAY': 'sunday',     # Sunday weekly
    'WEEKLY_BACKUP_TIME': '03:00',     # 3 AM on Sunday
    'MONTHLY_BACKUP_DAY': 1,           # 1st of month
    'MONTHLY_BACKUP_TIME': '04:00',    # 4 AM on 1st
    'ENABLE_DAILY': True,
    'ENABLE_WEEKLY': True,
    'ENABLE_MONTHLY': True,
    'AUTO_CLEANUP': True,
    'CLEANUP_FREQUENCY': 'daily',      # Run cleanup daily
}

# Ensure required directories exist
for directory in [AURUM_SETTINGS['REPORTS_DIR'], AURUM_SETTINGS['DATA_DIR'], BACKUP_SETTINGS['BACKUP_LOCATION']]:
    directory.mkdir(exist_ok=True)

# Create subdirectories for data processing
(AURUM_SETTINGS['DATA_DIR'] / 'excel' / 'input_files').mkdir(parents=True, exist_ok=True)
(AURUM_SETTINGS['DATA_DIR'] / 'excel').mkdir(exist_ok=True)
# API Keys Configuration
# OpenFIGI API key for Valley and IDB transformers
OPENFIGI_API_KEY = os.environ.get('OPENFIGI_API_KEY', 'bf21060a-0568-489e-8622-efcaf02e52cf')

# Ensure the API key is available as environment variable for transformers
if not os.environ.get('OPENFIGI_API_KEY'):
    os.environ['OPENFIGI_API_KEY'] = OPENFIGI_API_KEY