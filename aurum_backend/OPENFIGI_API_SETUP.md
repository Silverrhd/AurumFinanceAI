# OpenFIGI API Key Setup for Valley and IDB Transformers

## Overview

Valley and IDB bank transformers require the OpenFIGI API for asset type detection and security data enrichment. This document explains how the API key is configured and used.

## Implementation Details

### 1. Django Settings Configuration

The OpenFIGI API key is configured in `aurum_backend/settings.py`:

```python
# API Keys Configuration
# OpenFIGI API key for Valley and IDB transformers
OPENFIGI_API_KEY = os.environ.get('OPENFIGI_API_KEY', 'bf21060a-0568-489e-8622-efcaf02e52cf')

# Ensure the API key is available as environment variable for transformers
if not os.environ.get('OPENFIGI_API_KEY'):
    os.environ['OPENFIGI_API_KEY'] = OPENFIGI_API_KEY
```

### 2. Transformer Loading Enhancement

The `UnifiedPreprocessor.load_transformer()` method has been enhanced to pass the API key to Valley and IDB transformers:

```python
# Instantiate with API key for Valley and IDB transformers
if bank_code in ['Valley', 'IDB']:
    # Get API key from Django settings or environment
    api_key = None
    try:
        from django.conf import settings
        api_key = getattr(settings, 'OPENFIGI_API_KEY', None)
    except ImportError:
        # Fallback to environment variable if Django not available
        import os
        api_key = os.environ.get('OPENFIGI_API_KEY')
    
    if api_key:
        transformer = transformer_class(api_key=api_key)
        logger.info(f"📦 Loaded {bank_code} transformer with API key")
    else:
        transformer = transformer_class()
        logger.warning(f"⚠️ Loaded {bank_code} transformer without API key - may fail during processing")
else:
    transformer = transformer_class()
```

### 3. Environment Variable Configuration

The API key can be set via environment variable:

```bash
export OPENFIGI_API_KEY="bf21060a-0568-489e-8622-efcaf02e52cf"
```

Or added to a `.env` file (see `.env.example`):

```bash
OPENFIGI_API_KEY=bf21060a-0568-489e-8622-efcaf02e52cf
```

## Usage

### Running Preprocessing with Django Environment

To ensure Valley and IDB transformers have access to the API key, use the Django-enabled preprocessing script:

```bash
# Run all banks
python3 run_preprocess_with_django.py --date 10_07_2025

# Run specific banks (Valley and IDB)
python3 run_preprocess_with_django.py --date 10_07_2025 --banks Valley IDB

# Dry run to test configuration
python3 run_preprocess_with_django.py --date 10_07_2025 --dry-run
```

### Using Django APIs

The preprocessing can also be triggered through Django REST API endpoints:

```bash
# Start preprocessing via API
curl -X POST http://localhost:8000/api/portfolio/preprocess/start/ \
  -H "Authorization: Bearer <your_jwt_token>" \
  -H "Content-Type: application/json" \
  -d '{"date": "10_07_2025"}'
```

## Testing

### Test API Key Configuration

Run the test script to verify the API key is properly configured:

```bash
python3 test_openfigi_api.py
```

This will test:
- Django settings configuration
- Environment variable availability
- Transformer loading with API key
- OpenFIGI API functionality

### Expected Output

When properly configured, you should see:

```
✅ OPENFIGI_API_KEY found in Django settings: bf21060a-0...
✅ OPENFIGI_API_KEY found in environment: bf21060a-0...
✅ Valley transformer has API key: bf21060a-0...
✅ IDB transformer has API key: bf21060a-0...
✅ ProcessingService Valley transformer has API key: bf21060a-0...
```

## Troubleshooting

### API Key Not Found

If you see warnings like:
```
⚠️ Loaded Valley transformer without API key - may fail during processing
```

Check:
1. Environment variable is set: `echo $OPENFIGI_API_KEY`
2. Django settings are correct
3. Django environment is properly loaded

### API Lookup Failures

If you see API errors during processing:
1. Verify the API key is valid
2. Check internet connectivity
3. Verify OpenFIGI API service is available

### Django Environment Issues

If transformers can't access Django settings:
1. Ensure Django is properly initialized before importing transformers
2. Use `run_preprocess_with_django.py` instead of direct `preprocess.py`
3. Check that `DJANGO_SETTINGS_MODULE` is set correctly

## Files Modified

- `aurum_backend/aurum_backend/settings.py` - Added API key configuration
- `aurum_backend/.env.example` - Added API key example
- `aurum_backend/portfolio/preprocessing/preprocess.py` - Enhanced transformer loading
- `aurum_backend/portfolio/views.py` - Removed mistaken Valley/IDB export endpoints
- `aurum_backend/portfolio/urls.py` - Removed mistaken export URLs

## Files Created

- `aurum_backend/test_openfigi_api.py` - API key configuration test
- `aurum_backend/run_preprocess_with_django.py` - Django-enabled preprocessing script
- `aurum_backend/OPENFIGI_API_SETUP.md` - This documentation

## API Key Details

- **Current API Key**: `bf21060a-0568-489e-8622-efcaf02e52cf`
- **Used By**: Valley and IDB transformers
- **Purpose**: Asset type detection and security data enrichment via OpenFIGI API
- **Fallback**: Environment variable `OPENFIGI_API_KEY`