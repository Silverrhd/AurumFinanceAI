# Report Generation API

This section documents the report generation endpoints that power your key buttons in the admin interface.

## Overview

The report generation system provides four main types of reports:
1. **Weekly Portfolio Reports** - Complete portfolio analysis
2. **Bond Issuer Reports** - Bond holdings analysis by issuer
3. **Bond Maturity Reports** - Bond maturity timeline and analysis
4. **Equity Breakdown Reports** - Equity sector and holdings analysis

All report generation happens in the background to avoid blocking the UI, with real-time progress tracking.

## Main Report Generation Button

### Generate Missing Reports

**Endpoint:** `POST /api/admin/generate-missing-reports/`

This is your main "Generate Reports" button functionality that generates missing weekly portfolio reports.

#### Request
```json
{
  "date": "15_07_2025"
}
```

#### Response
```json
{
  "status": "started",
  "current_client": null,
  "completed_count": 0,
  "total_count": 5,
  "percentage": 0.0,
  "message": "Started generating 5 missing reports",
  "failed_clients": [],
  "last_error": null
}
```

#### Process Flow
1. **Analysis Phase**: System analyzes which clients need reports for the specified date
2. **File Validation**: Checks that required securities and transactions files exist
3. **Background Generation**: Starts background task using your existing `generate_weekly_report.py` script
4. **Progress Tracking**: Updates progress in real-time as each client report is generated
5. **Error Handling**: Captures and reports any generation failures

#### Business Logic Integration
The endpoint uses your existing Python scripts:
```bash
python3 generate_weekly_report.py securities.xlsx transactions.xlsx --client CLIENT_CODE --disable-open
```

This preserves all your current:
- Modified Dietz return calculations
- Asset allocation logic
- Performance metrics
- HTML report templates

## Progress Tracking

### Get Generation Progress

**Endpoint:** `GET /api/admin/generation-progress/`

Track the real-time progress of report generation.

#### Response
```json
{
  "status": "processing",
  "current_client": "CLIENT_A",
  "completed_count": 3,
  "total_count": 5,
  "percentage": 60.0,
  "message": "Processing CLIENT_A (3/5 completed)",
  "failed_clients": [],
  "last_error": null
}
```

#### Status Values
- `idle` - No generation in progress
- `started` - Generation initiated
- `processing` - Currently generating reports
- `completed` - All reports generated successfully
- `error` - Generation failed

## Bond Issuer Reports

### Get Bond Issuer Progress

**Endpoint:** `GET /api/admin/bond-issuer-progress/`

Track progress of bond issuer report generation.

#### Response
```json
{
  "status": "processing",
  "current_client": "CLIENT_B",
  "completed_count": 2,
  "total_count": 4,
  "percentage": 50.0,
  "message": "Generating bond issuer report for CLIENT_B",
  "failed_clients": [],
  "last_error": null
}
```

#### Business Logic Integration
Uses your existing `weighted_bond_issuer_report.py` script:
```bash
python3 weighted_bond_issuer_report.py --client CLIENT_CODE --output-dir ./reports
```

## Bond Maturity Reports

### Get Bond Maturity Progress

**Endpoint:** `GET /api/admin/bond-maturity-progress/`

**This tracks your "Bond Maturity Report" button progress!**

#### Response
```json
{
  "status": "processing",
  "current_client": "CLIENT_D",
  "completed_count": 2,
  "total_count": 6,
  "percentage": 33.3,
  "message": "Generating bond maturity report for CLIENT_D",
  "failed_clients": [],
  "last_error": null
}
```

#### Business Logic Integration
Uses your existing `maturity_report.py` script:
```bash
python3 maturity_report.py --client CLIENT_CODE --output-dir ./reports
```

Features preserved:
- Bonds sorted by maturity date (closest to furthest)
- Maturity timeline analysis
- Bond portfolio maturity distribution
- Client-specific or all-clients reporting

## Equity Breakdown Reports

### Get Equity Breakdown Progress

**Endpoint:** `GET /api/admin/equity-breakdown-progress/`

**This tracks your "Equity Breakdown Report" button progress!**

#### Response
```json
{
  "status": "processing",
  "current_client": "CLIENT_C",
  "completed_count": 1,
  "total_count": 3,
  "percentage": 33.3,
  "message": "Generating equity breakdown for CLIENT_C",
  "failed_clients": [],
  "last_error": null
}
```

#### Business Logic Integration
Uses your existing `equity_breakdown_report.py` script:
```bash
python3 equity_breakdown_report.py --client CLIENT_CODE --output-dir ./reports
```

Features preserved:
- FMP API integration for sector analysis
- SPY benchmark comparison
- Top holdings analysis
- Sector exposure calculations

## Report Analysis

### Analyze Report Status

**Endpoint:** `GET /api/admin/report-analysis/{date}/`

Get comprehensive analysis of report status for a specific date.

#### Parameters
- `date` - Date in DD_MM_YYYY format (e.g., `15_07_2025`)

#### Response
```json
{
  "date": "15_07_2025",
  "formatted_date": "2025-07-15",
  "total_clients": 8,
  "clients_with_reports": 3,
  "missing_clients": ["CLIENT_D", "CLIENT_E", "CLIENT_F", "CLIENT_G", "CLIENT_H"],
  "existing_reports": ["CLIENT_A", "CLIENT_B", "CLIENT_C"],
  "securities_file": "/data/excel/securities_15_07_2025.xlsx",
  "transactions_file": "/data/excel/transactions_15_07_2025.xlsx",
  "files_exist": true
}
```

This analysis helps determine:
- Which clients already have reports
- Which clients need reports generated
- Whether required data files exist
- Total scope of generation needed

## Error Handling

### Common Errors

#### Missing Data Files
```json
{
  "error": "missing_files",
  "message": "Required securities or transactions files not found for date 15_07_2025",
  "details": {
    "securities_file": "/data/excel/securities_15_07_2025.xlsx",
    "transactions_file": "/data/excel/transactions_15_07_2025.xlsx",
    "files_exist": false
  },
  "timestamp": "2025-07-19T10:30:00Z"
}
```

#### Generation Timeout
```json
{
  "error": "generation_timeout",
  "message": "Report generation timeout for CLIENT_X (5 minutes)",
  "details": {
    "client": "CLIENT_X",
    "timeout_minutes": 5
  },
  "timestamp": "2025-07-19T10:35:00Z"
}
```

#### Script Execution Error
```json
{
  "error": "script_error",
  "message": "Failed to generate report for CLIENT_Y",
  "details": {
    "client": "CLIENT_Y",
    "script_output": "Error: Invalid data format in securities file",
    "return_code": 1
  },
  "timestamp": "2025-07-19T10:32:00Z"
}
```

## Background Processing

All report generation happens in background tasks to prevent blocking the UI:

### Process Architecture
```
User clicks button → API endpoint → Background task → Progress updates
                                        ↓
                              Python script execution
                                        ↓
                              HTML report generation
                                        ↓
                              Progress completion
```

### Monitoring
- Real-time progress updates via progress endpoints
- Error capture and reporting
- Timeout protection (5 minutes per client)
- Graceful failure handling

### File Management
- Reports saved to `/reports/` directory
- Automatic file naming: `report_{client}_{date}.html`
- Existing reports preserved (no overwriting)

## Integration with Current System

The API endpoints are designed to work seamlessly with your existing:

### Python Scripts
- `generate_weekly_report.py` - Main report generation
- `weighted_bond_issuer_report.py` - Bond issuer analysis
- `equity_breakdown_report.py` - Equity sector analysis

### Calculation Logic
- Modified Dietz return calculations
- Asset allocation algorithms
- Performance metrics
- Income analysis

### Report Templates
- HTML report templates
- Chart generation
- Styling and branding

### Data Sources
- Portfolio database
- FMP API for market data
- Bank-specific data transformations

## Usage Examples

### Generate Reports for Specific Date
```bash
curl -X POST http://localhost:8000/api/admin/generate-missing-reports/ \
  -H "Authorization: Bearer <admin_token>" \
  -H "Content-Type: application/json" \
  -d '{"date": "15_07_2025"}'
```

### Monitor Progress
```bash
# Check overall progress
curl -X GET http://localhost:8000/api/admin/generation-progress/ \
  -H "Authorization: Bearer <admin_token>"

# Check equity breakdown progress
curl -X GET http://localhost:8000/api/admin/equity-breakdown-progress/ \
  -H "Authorization: Bearer <admin_token>"
```

### Analyze Report Status
```bash
curl -X GET http://localhost:8000/api/admin/report-analysis/15_07_2025/ \
  -H "Authorization: Bearer <admin_token>"
```

This comprehensive report generation system ensures that all your current functionality is preserved while providing better API documentation and error handling.