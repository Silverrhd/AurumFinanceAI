# Dashboard Testing Guide

## Overview
This guide explains how to test the newly implemented admin and client dashboards.

## Testing the Implementation

### 1. Start the Frontend Development Server

```bash
cd /Users/thomaskemeny/AurumFinance/aurum_frontend
npm run dev
```

The application will be available at `http://localhost:3000`

### 2. Admin Dashboard Testing

**Admin Login Features:**
- Navigate to `/admin/dashboard` (requires admin role)
- **Client Filtering**: Select different clients from dropdown in header
- **File Upload**: Drag & drop Excel files for processing
- **Database Management**: Update database and process banks
- **Report Generation**: Generate reports for specific clients
- **Real-time Monitoring**: View bank processing status

**Admin Capabilities:**
✅ Upload files for any bank
✅ Populate database across all clients
✅ Generate reports for individual clients
✅ Filter dashboard by specific client or view all clients
✅ Access all processing controls
✅ View comprehensive analytics

### 3. Client Dashboard Testing

**Client Login Features:**
- Navigate to `/client/dashboard` (requires client role)
- **Read-Only Access**: Clear visual indicators of view-only mode
- **Personal Data**: Only sees their own portfolio data
- **Report Download**: Can download their own reports only
- **Portfolio Summary**: Key metrics and performance data

**Client Limitations:**
❌ Cannot upload files
❌ Cannot access database controls
❌ Cannot generate reports for other clients
❌ Cannot access processing controls
✅ Can only view their own data
✅ Can download their own reports

### 4. Key Differences Summary

| Feature | Admin | Client |
|---------|--------|---------|
| File Upload | ✅ Full access | ❌ No access |
| Database Updates | ✅ Full control | ❌ No access |
| Client Selection | ✅ All clients | ❌ Own data only |
| Report Generation | ✅ Any client | ✅ Own reports only |
| Processing Controls | ✅ Full access | ❌ No access |
| Dashboard Filtering | ✅ All clients | ❌ Personal only |

### 5. Mock Data for Testing

The implementation includes mock data for development testing:

**Mock Clients:**
- ABC Corporation (ID: ABC_001)
- XYZ Holdings (ID: XYZ_002) 
- Tech Ventures Ltd (ID: TEC_003)

**Mock Bank Data:**
- 12 supported banks (JPM, MS, CS, UBS, etc.)
- Simulated processing status
- Mock portfolio metrics

### 6. API Endpoints Expected

The frontend expects these Django API endpoints:

**Admin Endpoints:**
- `GET /api/portfolio/admin/clients/` - List all clients
- `POST /api/portfolio/admin/generate-report/` - Generate client reports
- `POST /api/portfolio/process-bank/` - Process bank data
- `POST /api/portfolio/update-database/` - Update database

**Client Endpoints:**
- `GET /api/portfolio/client/summary/` - Client portfolio summary
- `POST /api/portfolio/client/report/` - Generate client's own report

**Shared Endpoints:**
- `GET /api/portfolio/analytics/*` - Chart data endpoints
- `GET /api/portfolio/banks/status/` - Bank processing status
- `POST /api/portfolio/files/upload/` - File upload

### 7. Visual Indicators

**Admin Dashboard:**
- Client selector dropdown in header
- "Generate Report" button for selected client
- Processing status badges
- Full access to all tabs and controls

**Client Dashboard:**
- "Read-Only Access" badge in header
- "Download Report" button (own reports only)
- Limited to portfolio viewing
- No processing or admin controls

### 8. Testing Authentication

The implementation uses JWT tokens stored in localStorage:
- Admin role: `requiredRole="admin"`
- Client role: `requiredRole="client"`

### 9. Real-time Features

- Bank status updates every 30 seconds
- Processing status indicators
- Dynamic chart updates based on client selection
- Responsive design for mobile/desktop

## Conclusion

The implementation provides:
1. **Complete admin control** with client filtering and report generation
2. **Restricted client access** with read-only permissions
3. **Modern UI** with the v0 template integration
4. **ApexCharts visualization** for consistency with HTML reports
5. **Comprehensive testing capabilities** with mock data

Both dashboards maintain 100% feature parity with the original ProjectAurum system while providing a modern, intuitive interface.