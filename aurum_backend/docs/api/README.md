# Aurum Finance Portfolio Management API

## Overview

This API provides comprehensive portfolio management functionality, including bank file processing, report generation, and dashboard data access. The API is designed to match the functionality of the existing FastAPI system while providing enhanced documentation and standardized interfaces.

## Base URL

```
Development: http://localhost:8000/api/
Production: https://your-domain.com/api/
```

## Authentication

The API uses JWT (JSON Web Token) authentication. All endpoints except login require authentication.

### Getting Started

1. **Login** to get JWT tokens:
   ```bash
   POST /api/auth/login/
   {
     "username": "your_username",
     "password": "your_password"
   }
   ```

2. **Use the access token** in subsequent requests:
   ```bash
   Authorization: Bearer <your_access_token>
   ```

3. **Refresh tokens** when they expire:
   ```bash
   POST /api/auth/token/refresh/
   {
     "refresh": "<your_refresh_token>"
   }
   ```

## User Roles

### Admin Users
- Can access all client data
- Can manage file uploads and processing
- Can generate reports for all clients
- Can access system monitoring endpoints

### Client Users
- Can only access their own portfolio data
- Can view their own dashboard and reports
- Cannot access admin functions

## API Documentation

### Interactive Documentation
- **Swagger UI**: [/api/docs/](http://localhost:8000/api/docs/) - Interactive API testing
- **ReDoc**: [/api/redoc/](http://localhost:8000/api/redoc/) - Clean documentation view
- **OpenAPI Schema**: [/api/schema/](http://localhost:8000/api/schema/) - Machine-readable schema

### Endpoint Categories

1. **[Authentication](./authentication.md)** - Login, logout, token management
2. **[File Processing](./file-processing.md)** - Bank file upload and preprocessing
3. **[Report Generation](./report-generation.md)** - Weekly, bond issuer, and equity breakdown reports
4. **[Dashboard APIs](./dashboard-apis.md)** - Chart data and metrics
5. **[System Monitoring](./system-monitoring.md)** - Health checks and system status

## Quick Start Examples

### Admin Workflow
```bash
# 1. Login as admin
curl -X POST http://localhost:8000/api/auth/login/ \
  -H "Content-Type: application/json" \
  -d '{"username": "admin@aurumfinance.com", "password": "your_password"}'

# 2. Upload bank files
curl -X POST http://localhost:8000/api/admin/upload-files/ \
  -H "Authorization: Bearer <access_token>" \
  -F "files=@bank_file.xlsx"

# 3. Generate reports
curl -X POST http://localhost:8000/api/admin/generate-missing-reports/ \
  -H "Authorization: Bearer <access_token>" \
  -H "Content-Type: application/json" \
  -d '{"date": "15_07_2025"}'

# 4. Check progress
curl -X GET http://localhost:8000/api/admin/generation-progress/ \
  -H "Authorization: Bearer <access_token>"
```

### Client Workflow
```bash
# 1. Login as client
curl -X POST http://localhost:8000/api/auth/login/ \
  -H "Content-Type: application/json" \
  -d '{"username": "client@company.com", "password": "your_password"}'

# 2. Get dashboard metrics
curl -X GET http://localhost:8000/api/client/dashboard/metrics/ \
  -H "Authorization: Bearer <access_token>"

# 3. Get client information
curl -X GET http://localhost:8000/api/client/info/ \
  -H "Authorization: Bearer <access_token>"
```

## Error Handling

All API endpoints return consistent error responses:

```json
{
  "error": "error_type",
  "message": "Human-readable error message",
  "details": {
    "field": "Additional error details"
  },
  "timestamp": "2025-07-19T10:30:00Z"
}
```

### Common HTTP Status Codes

- `200` - Success
- `400` - Bad Request (validation errors)
- `401` - Unauthorized (authentication required)
- `403` - Forbidden (insufficient permissions)
- `404` - Not Found
- `413` - Payload Too Large (file upload limits)
- `500` - Internal Server Error

## Rate Limiting

API requests are rate-limited to prevent abuse:
- **Admin users**: 1000 requests per hour
- **Client users**: 500 requests per hour
- **File uploads**: 10 uploads per minute

## Supported Banks

The system supports file processing for the following banks:

- **JPMorgan (JPM)** - Securities and transactions
- **Morgan Stanley (MS)** - Securities and transactions  
- **Credit Suisse (CS)** - Securities and transactions
- **Valley Bank** - Securities and transactions
- **Pershing** - Securities and transactions (with enrichment)
- **HSBC** - Securities and transactions
- **JB Private Bank** - Securities and transactions
- **Credit Suisse Canada (CSC)** - Securities and transactions

## File Requirements

### Upload Specifications
- **Format**: Excel files only (.xlsx, .xls)
- **Size**: Maximum 50MB per file
- **Multiple files**: Supported in single request
- **Naming**: Automatic bank detection based on file content

### Processing Pipeline
1. **Upload** - Files uploaded to secure storage
2. **Detection** - Automatic bank type identification
3. **Enrichment** - Bank-specific data enhancement
4. **Combination** - Multiple files merged if needed
5. **Transformation** - Standardized format conversion
6. **Validation** - Data integrity checks

## Report Types

### Weekly Portfolio Reports
- Complete portfolio analysis
- Performance calculations (Modified Dietz)
- Asset allocation breakdown
- Income analysis

### Bond Issuer Reports
- Bond holdings by issuer
- Credit quality analysis
- Maturity distribution

### Equity Breakdown Reports
- Sector analysis
- Top holdings
- Benchmark comparison (SPY)

## Support

For API support and questions:
- **Documentation**: Check the interactive docs at `/api/docs/`
- **Issues**: Review error messages and status codes
- **Business Logic**: See [Business Logic Documentation](../business-logic/)

## Changelog

### Version 1.0.0
- Initial API implementation
- Complete FastAPI system migration
- Comprehensive documentation
- All existing functionality preserved