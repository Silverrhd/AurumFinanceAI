# Dashboard Metrics Enhancement - Investigation Report
## Deep Dive Investigation & Implementation Plan

**Date**: October 8, 2025
**Objective**: Add Monthly Returns and This Period Returns to Dashboard ($ and %)

---

## 1. CURRENT STATE ANALYSIS

### 1.1 Current Dashboard Metrics (4 boxes)

| Metric | Source Field | Calculation Location |
|--------|--------------|---------------------|
| **Total AUM** | `total_value` | `PortfolioCalculationService._calculate_basic_metrics()` |
| **Since Inception $** | `inception_gain_loss_dollar` | `PortfolioCalculationService._calculate_modified_dietz_returns()` |
| **Since Inception %** | `inception_gain_loss_percent` | `PortfolioCalculationService._calculate_modified_dietz_returns()` |
| **Est Annual Income** | `estimated_annual_income` | `PortfolioCalculationService._calculate_annual_income()` |

### 1.2 Data Flow Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  CALCULATION LAYER                                               │
├──────────────────────────────────────────────────────────────────┤
│  PortfolioCalculationService.calculate_portfolio_metrics()       │
│  - Calculates all metrics for a single client + snapshot date    │
│  - Uses ModifiedDietzService for returns                        │
│  - Stores results in PortfolioSnapshot.portfolio_metrics (JSON)  │
└──────────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│  STORAGE LAYER                                                   │
├──────────────────────────────────────────────────────────────────┤
│  PortfolioSnapshot.portfolio_metrics = {                         │
│    'total_value': 1000000.00,                                   │
│    'inception_gain_loss_dollar': 50000.00,                      │
│    'inception_gain_loss_percent': 5.00,                         │
│    'estimated_annual_income': 25000.00,                         │
│    'real_gain_loss_dollar': 2000.00,  ← PERIOD RETURN (exists!) │
│    'real_gain_loss_percent': 0.20,     ← PERIOD RETURN (exists!) │
│    ...                                                           │
│  }                                                               │
└──────────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│  AGGREGATION LAYER (for "ALL" client filter)                    │
├──────────────────────────────────────────────────────────────────┤
│  CorrectDashboardCacheService.aggregate_date_data()              │
│  - Aggregates across all clients for a specific date            │
│  - Stores in DateAggregatedMetrics model                        │
│  - Provides ultra-fast dashboard queries (<500ms)               │
└──────────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│  PRESENTATION LAYER                                              │
├──────────────────────────────────────────────────────────────────┤
│  views.py:                                                       │
│  - admin_dashboard_data() → Admin dashboard API                 │
│  - client_dashboard_data() → Client dashboard API               │
│                                                                  │
│  Returns JSON with 'summary' containing the 4 current metrics    │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2. CRITICAL FINDINGS

### 🔍 Finding #1: "This Period" Returns ALREADY EXIST!

**Location**: `portfolio/services/portfolio_calculation_service.py` lines 234-310

```python
def _calculate_modified_dietz_returns(self, client: Client, snapshot_date: str,
                                    positions, transactions) -> dict:
    # ...existing code...

    return {
        'real_gain_loss_dollar': real_gain_loss,      # ← THIS PERIOD $ (snapshot-to-snapshot)
        'real_gain_loss_percent': real_gain_loss_percent,  # ← THIS PERIOD % (snapshot-to-snapshot)
        'inception_gain_loss_dollar': inception_gain_loss,
        'inception_gain_loss_percent': inception_gain_loss_percent,
        'period_return': real_gain_loss_percent,
        'net_cash_flow': net_cash_flow
    }
```

**Impact**:
- ✅ Already calculated for every snapshot
- ✅ Already stored in `portfolio_metrics` JSON
- ❌ NOT aggregated in `DateAggregatedMetrics`
- ❌ NOT displayed in dashboard frontend

**What is "This Period"?**
- Period = From **previous snapshot date** to **current snapshot date**
- Example: If current snapshot is `2025-10-08` and previous is `2025-10-01`, then period is 7 days
- Uses Modified Dietz methodology (accounts for external cash flows)

### 🔍 Finding #2: Monthly Returns Calculation Logic EXISTS

**Location**: `portfolio/services/custody_returns_service.py`

**Key Methods**:
```python
def _get_month_end_date(self, year: int, month: int) -> date:
    """Get closest available snapshot date to month-end."""
    # Finds latest snapshot in target month

def _get_previous_month_end_date(self, year: int, month: int) -> date:
    """Get closest available snapshot date to previous month-end."""
    # Finds latest snapshot in previous month

def generate_client_monthly_returns(self, client_code: str, year: int, month: int):
    # Calculates monthly return using Modified Dietz
    # Returns both dollar and percentage
```

**How Monthly Returns Work**:
1. Determine current month (from latest snapshot date)
2. Find closest snapshot to end of current month
3. Find closest snapshot to end of previous month
4. Calculate Modified Dietz return between those two dates

**Example**:
- Latest snapshot: 2025-10-08
- Current month: October 2025
- Find closest October snapshot: 2025-10-08 (latest in October)
- Find closest September snapshot: 2025-09-30 (latest in September)
- Monthly return = Modified Dietz(2025-09-30 → 2025-10-08)

---

## 3. REQUIRED METRICS - DETAILED SPECIFICATION

### New Metric #1: Monthly Return $

**Definition**: Dollar gain/loss from previous month-end to current month-end
**Calculation**: Modified Dietz gain_loss between month boundaries
**Formula**: `End Value - Begin Value - Net External Flows`

**Example Calculation**:
```
Latest snapshot date: 2025-10-08
Current month: October 2025
Previous month: September 2025

Month-end dates:
- October end: 2025-10-08 (latest available in October)
- September end: 2025-09-30 (latest available in September)

Portfolio values:
- Oct 8 value: $1,050,000
- Sep 30 value: $1,000,000
- Net external flows: $30,000 (deposits)

Monthly Return $ = $1,050,000 - $1,000,000 - $30,000 = $20,000
```

---

### New Metric #2: Monthly Return %

**Definition**: Percentage return from previous month-end to current month-end
**Calculation**: Modified Dietz return_percentage between month boundaries
**Formula**: `Gain/Loss / (Begin Value + Weighted External Flows) * 100`

**Example Calculation**:
```
Using same data as above:

Gain/Loss = $20,000
Average Capital = $1,000,000 + Weighted Flows
(Simplified: assuming flows at mid-month, weight = 0.5)
Weighted Flows = $30,000 * 0.5 = $15,000
Average Capital = $1,000,000 + $15,000 = $1,015,000

Monthly Return % = ($20,000 / $1,015,000) * 100 = 1.97%
```

---

### New Metric #3: This Period Return $

**Definition**: Dollar gain/loss from previous snapshot to current snapshot
**Calculation**: ALREADY EXISTS as `real_gain_loss_dollar` in portfolio_metrics
**Source**: `portfolio_calculation_service.py` line 276

**Example**:
```
Current snapshot: 2025-10-08
Previous snapshot: 2025-10-01 (7 days ago)

This Period Return $ = portfolio_metrics['real_gain_loss_dollar']
                     = $5,000 (already calculated)
```

---

### New Metric #4: This Period Return %

**Definition**: Percentage return from previous snapshot to current snapshot
**Calculation**: ALREADY EXISTS as `real_gain_loss_percent` in portfolio_metrics
**Source**: `portfolio_calculation_service.py` line 277

**Example**:
```
This Period Return % = portfolio_metrics['real_gain_loss_percent']
                     = 0.50% (already calculated)
```

---

## 4. DATABASE SCHEMA CHANGES

### 4.1 Modify `DateAggregatedMetrics` Model

**File**: `portfolio/models.py` (around line 352)

**Add 4 new fields**:
```python
class DateAggregatedMetrics(models.Model):
    # ... existing fields ...
    total_aum = models.DecimalField(max_digits=20, decimal_places=2)
    total_inception_dollar = models.DecimalField(max_digits=20, decimal_places=2)
    weighted_inception_percent = models.DecimalField(max_digits=10, decimal_places=4)
    total_annual_income = models.DecimalField(max_digits=20, decimal_places=2)
    client_count = models.IntegerField()

    # NEW FIELDS TO ADD:
    total_period_return_dollar = models.DecimalField(
        max_digits=20, decimal_places=2, default=0,
        help_text="Aggregated this-period dollar returns (snapshot-to-snapshot)"
    )
    weighted_period_return_percent = models.DecimalField(
        max_digits=10, decimal_places=4, default=0,
        help_text="Weighted average this-period percentage return"
    )
    total_monthly_return_dollar = models.DecimalField(
        max_digits=20, decimal_places=2, default=0,
        help_text="Aggregated monthly dollar returns (month-end to month-end)"
    )
    weighted_monthly_return_percent = models.DecimalField(
        max_digits=10, decimal_places=4, default=0,
        help_text="Weighted average monthly percentage return"
    )

    # ... existing fields ...
    asset_allocation_data = models.JSONField(...)
    bank_allocation_data = models.JSONField(...)
    bond_maturity_data = models.JSONField(...)
```

**Migration Required**: Yes - will generate migration file to add these 4 fields

---

### 4.2 Modify `PortfolioSnapshot.portfolio_metrics` Structure

**NO CHANGES NEEDED** for period returns - they already exist!

For monthly returns, we'll calculate on-the-fly during aggregation (not stored per-snapshot).

---

## 5. IMPLEMENTATION PLAN - STEP BY STEP

### Phase 1: Backend - Database Layer

#### Step 1.1: Create Django Migration
**File**: Auto-generated migration file
**Action**: Add 4 new fields to `DateAggregatedMetrics`

```bash
python manage.py makemigrations --name add_period_and_monthly_returns
python manage.py migrate
```

**Estimated Time**: 5 minutes
**Risk Level**: Low (adding nullable fields with defaults)

---

### Phase 2: Backend - Calculation Layer (Individual Clients)

#### Step 2.1: Enhance `PortfolioCalculationService` (Optional)

**File**: `portfolio/services/portfolio_calculation_service.py`

**Note**: Period returns ALREADY calculated. Only need to ensure they're always present.

**Verify lines 303-310** return this structure:
```python
return {
    'real_gain_loss_dollar': real_gain_loss,  # ← Must always be present
    'real_gain_loss_percent': real_gain_loss_percent,  # ← Must always be present
    'inception_gain_loss_dollar': inception_gain_loss,
    'inception_gain_loss_percent': inception_gain_loss_percent,
    'period_return': real_gain_loss_percent,
    'net_cash_flow': net_cash_flow
}
```

**Action**: Add fallback defaults if previous snapshot not found (lines 249-255):
```python
if not previous_snapshot:
    # First snapshot - no comparison possible
    return {
        'real_gain_loss_dollar': 0.0,  # ← Already present
        'real_gain_loss_percent': 0.0,  # ← Already present
        'inception_gain_loss_dollar': 0.0,
        'inception_gain_loss_percent': 0.0,
        'period_return': 0.0
    }
```

✅ **No changes needed** - already correct!

**Estimated Time**: 5 minutes (verification only)
**Risk Level**: None (read-only verification)

---

#### Step 2.2: Create Monthly Return Calculation Helper

**File**: `portfolio/services/portfolio_calculation_service.py`

**Add new method** (around line 493):
```python
def calculate_monthly_return(self, client_code: str, snapshot_date: str) -> dict:
    """
    Calculate monthly return for a client at a specific snapshot date.

    Args:
        client_code: Client identifier
        snapshot_date: Current snapshot date (YYYY-MM-DD)

    Returns:
        {
            'monthly_return_dollar': float,
            'monthly_return_percent': float,
            'month_start_date': str,
            'month_end_date': str
        }
    """
    from datetime import datetime, date

    # Convert snapshot_date to date object
    if isinstance(snapshot_date, str):
        current_date = datetime.strptime(snapshot_date, '%Y-%m-%d').date()
    else:
        current_date = snapshot_date

    # Determine current month and year
    current_year = current_date.year
    current_month = current_date.month

    # Get previous month
    if current_month == 1:
        prev_month = 12
        prev_year = current_year - 1
    else:
        prev_month = current_month - 1
        prev_year = current_year

    # Find closest snapshot to current month-end (should be current snapshot)
    month_end_snapshot = PortfolioSnapshot.objects.filter(
        client__code=client_code,
        snapshot_date__year=current_year,
        snapshot_date__month=current_month
    ).order_by('-snapshot_date').first()

    if not month_end_snapshot:
        return {
            'monthly_return_dollar': 0.0,
            'monthly_return_percent': 0.0,
            'month_start_date': None,
            'month_end_date': None
        }

    # Find closest snapshot to previous month-end
    prev_month_snapshot = PortfolioSnapshot.objects.filter(
        client__code=client_code,
        snapshot_date__year=prev_year,
        snapshot_date__month=prev_month
    ).order_by('-snapshot_date').first()

    if not prev_month_snapshot:
        # No previous month data - return zeros
        return {
            'monthly_return_dollar': 0.0,
            'monthly_return_percent': 0.0,
            'month_start_date': None,
            'month_end_date': month_end_snapshot.snapshot_date.isoformat()
        }

    # Calculate monthly return using Modified Dietz
    dietz_service = ModifiedDietzService()
    detailed_result = dietz_service.calculate_portfolio_return_detailed(
        client_code,
        prev_month_snapshot.snapshot_date,
        month_end_snapshot.snapshot_date
    )

    return {
        'monthly_return_dollar': detailed_result.get('gain_loss', 0.0),
        'monthly_return_percent': detailed_result.get('return_percentage', 0.0),
        'month_start_date': prev_month_snapshot.snapshot_date.isoformat(),
        'month_end_date': month_end_snapshot.snapshot_date.isoformat()
    }
```

**Estimated Time**: 30 minutes
**Risk Level**: Low (new method, doesn't modify existing logic)

---

### Phase 3: Backend - Aggregation Layer (All Clients)

#### Step 3.1: Enhance `CorrectDashboardCacheService.aggregate_date_data()`

**File**: `portfolio/services/correct_dashboard_cache_service.py`

**Modify method** (lines 63-217):

**Add aggregation variables** (after line 99):
```python
# Initialize aggregation variables
total_aum = Decimal('0')
total_inception_dollar = Decimal('0')
weighted_inception_percent = Decimal('0')
total_annual_income = Decimal('0')

# NEW: Add period return aggregation
total_period_return_dollar = Decimal('0')
weighted_period_return_percent = Decimal('0')

# NEW: Add monthly return aggregation
total_monthly_return_dollar = Decimal('0')
weighted_monthly_return_percent = Decimal('0')

asset_allocation_aggregated = {}
bank_allocation_aggregated = {}
bond_maturity_aggregated = {}
client_count = 0
```

**Add aggregation logic in loop** (after line 118):
```python
# Aggregate across all clients for this specific date
for snapshot in snapshots:
    metrics = snapshot.portfolio_metrics

    # Extract client metrics
    client_total_value = Decimal(str(metrics.get('total_value', 0)))
    client_inception_dollar = Decimal(str(metrics.get('inception_gain_loss_dollar', 0)))
    client_inception_percent = Decimal(str(metrics.get('inception_gain_loss_percent', 0)))
    client_annual_income = Decimal(str(metrics.get('estimated_annual_income', 0)))

    # NEW: Extract period returns (already in metrics!)
    client_period_dollar = Decimal(str(metrics.get('real_gain_loss_dollar', 0)))
    client_period_percent = Decimal(str(metrics.get('real_gain_loss_percent', 0)))

    # Aggregate totals
    total_aum += client_total_value
    total_inception_dollar += client_inception_dollar
    total_annual_income += client_annual_income

    # NEW: Aggregate period returns
    total_period_return_dollar += client_period_dollar

    # For weighted averages
    if client_total_value > 0:
        weighted_inception_percent += client_inception_percent * client_total_value
        # NEW: Weight period return by AUM
        weighted_period_return_percent += client_period_percent * client_total_value

    # ... rest of existing aggregation logic ...
```

**Add monthly return calculation in loop** (NEW):
```python
    # NEW: Calculate monthly return for this client
    calc_service = PortfolioCalculationService()
    monthly_result = calc_service.calculate_monthly_return(
        snapshot.client.code,
        snapshot.snapshot_date
    )

    client_monthly_dollar = Decimal(str(monthly_result['monthly_return_dollar']))
    client_monthly_percent = Decimal(str(monthly_result['monthly_return_percent']))

    # Aggregate monthly returns
    total_monthly_return_dollar += client_monthly_dollar

    if client_total_value > 0:
        # Weight monthly return by AUM
        weighted_monthly_return_percent += client_monthly_percent * client_total_value
```

**Calculate final weighted percentages** (after line 147):
```python
# Calculate final weighted percentage
final_inception_percent = weighted_inception_percent / total_aum if total_aum > 0 else Decimal('0')

# NEW: Calculate weighted period return percent
final_period_percent = weighted_period_return_percent / total_aum if total_aum > 0 else Decimal('0')

# NEW: Calculate weighted monthly return percent
final_monthly_percent = weighted_monthly_return_percent / total_aum if total_aum > 0 else Decimal('0')
```

**Update database storage** (lines 178-192):
```python
# Store in cache using database transaction for consistency
with transaction.atomic():
    cache_entry, created = DateAggregatedMetrics.objects.update_or_create(
        snapshot_date=date_obj,
        client_filter='ALL',
        defaults={
            'total_aum': total_aum,
            'total_inception_dollar': total_inception_dollar,
            'weighted_inception_percent': final_inception_percent,
            'total_annual_income': total_annual_income,
            'client_count': client_count,

            # NEW: Store period returns
            'total_period_return_dollar': total_period_return_dollar,
            'weighted_period_return_percent': final_period_percent,

            # NEW: Store monthly returns
            'total_monthly_return_dollar': total_monthly_return_dollar,
            'weighted_monthly_return_percent': final_monthly_percent,

            'asset_allocation_data': asset_allocation_json,
            'bank_allocation_data': bank_allocation_json,
            'bond_maturity_data': bond_maturity_json,
        }
    )
```

**Estimated Time**: 45 minutes
**Risk Level**: Medium (modifies aggregation logic, needs testing)

---

#### Step 3.2: Enhance `CorrectDashboardCacheService.get_current_dashboard_data()`

**File**: `portfolio/services/correct_dashboard_cache_service.py`

**Modify method** (lines 219-422):

**Update summary structure** (around line 236):
```python
# Current metrics from latest date
summary = {
    'total_aum': float(latest_cache.total_aum),
    'inception_dollar_performance': float(latest_cache.total_inception_dollar),
    'inception_return_pct': float(latest_cache.weighted_inception_percent),
    'estimated_annual_income': float(latest_cache.total_annual_income),

    # NEW: Add period returns
    'period_return_dollar': float(latest_cache.total_period_return_dollar),
    'period_return_percent': float(latest_cache.weighted_period_return_percent),

    # NEW: Add monthly returns
    'monthly_return_dollar': float(latest_cache.total_monthly_return_dollar),
    'monthly_return_percent': float(latest_cache.weighted_monthly_return_percent),

    'client_count': latest_cache.client_count,
    'filter_applied': 'ALL'
}
```

**Estimated Time**: 10 minutes
**Risk Level**: Low (additive change)

---

### Phase 4: Backend - View/API Layer

#### Step 4.1: Update `admin_dashboard_data_original()` (Fallback)

**File**: `portfolio/views.py`

**Modify fallback calculation** (around line 2120-2195):

**Add aggregation variables** (after line 2122):
```python
# Initialize aggregated metrics
total_aum = 0
total_inception_dollar = 0
weighted_inception_percent = 0
total_annual_income = 0

# NEW: Add period and monthly return aggregation
total_period_return_dollar = 0
weighted_period_return_percent = 0
total_monthly_return_dollar = 0
weighted_monthly_return_percent = 0

clients_data = []
asset_allocation_aggregated = {}
bank_allocation_aggregated = {}
```

**Add aggregation in loop** (after line 2150):
```python
for client in clients:
    latest_snapshot = PortfolioSnapshot.objects.filter(
        client=client
    ).order_by('-snapshot_date').first()

    if latest_snapshot:
        metrics = latest_snapshot.portfolio_metrics

        # Individual client metrics
        client_total_value = float(metrics.get('total_value', 0))
        client_inception_dollar = float(metrics.get('inception_gain_loss_dollar', 0))
        client_inception_percent = float(metrics.get('inception_gain_loss_percent', 0))
        client_annual_income = float(metrics.get('estimated_annual_income', 0))

        # NEW: Period returns (already in metrics)
        client_period_dollar = float(metrics.get('real_gain_loss_dollar', 0))
        client_period_percent = float(metrics.get('real_gain_loss_percent', 0))

        # Aggregate totals
        total_aum += client_total_value
        total_inception_dollar += client_inception_dollar
        total_annual_income += client_annual_income

        # NEW: Aggregate period returns
        total_period_return_dollar += client_period_dollar

        # For weighted average
        if client_total_value > 0:
            weighted_inception_percent += client_inception_percent * client_total_value
            # NEW: Weight period return
            weighted_period_return_percent += client_period_percent * client_total_value

        # NEW: Calculate monthly return for this client
        calc_service = PortfolioCalculationService()
        monthly_result = calc_service.calculate_monthly_return(
            client.code,
            latest_snapshot.snapshot_date
        )

        client_monthly_dollar = float(monthly_result['monthly_return_dollar'])
        client_monthly_percent = float(monthly_result['monthly_return_percent'])

        total_monthly_return_dollar += client_monthly_dollar

        if client_total_value > 0:
            weighted_monthly_return_percent += client_monthly_percent * client_total_value

        # ... rest of existing aggregation logic ...
```

**Calculate weighted percentages** (after line 2181):
```python
# Calculate weighted average inception percentage
final_inception_percent = weighted_inception_percent / total_aum if total_aum > 0 else 0

# NEW: Calculate weighted period return percent
final_period_percent = weighted_period_return_percent / total_aum if total_aum > 0 else 0

# NEW: Calculate weighted monthly return percent
final_monthly_percent = weighted_monthly_return_percent / total_aum if total_aum > 0 else 0
```

**Update summary response** (around line 2187):
```python
# Prepare summary in the format frontend expects
summary = {
    'total_aum': total_aum,
    'inception_dollar_performance': total_inception_dollar,
    'inception_return_pct': final_inception_percent,
    'estimated_annual_income': total_annual_income,

    # NEW: Add period returns
    'period_return_dollar': total_period_return_dollar,
    'period_return_percent': final_period_percent,

    # NEW: Add monthly returns
    'monthly_return_dollar': total_monthly_return_dollar,
    'monthly_return_percent': final_monthly_percent,

    'client_count': len(clients_data),
    'filter_applied': client_filter
}
```

**Estimated Time**: 30 minutes
**Risk Level**: Medium (modifies fallback logic)

---

#### Step 4.2: Update `client_dashboard_data()` (Client View)

**File**: `portfolio/views.py`

**Location**: Around line 2914

**Similar modifications** as admin fallback - add period and monthly returns to client-specific dashboard.

**Note**: Client dashboard uses individual client logic (not aggregated cache).

**Estimated Time**: 20 minutes
**Risk Level**: Low (separate from admin logic)

---

### Phase 5: Frontend - Display Layer

#### Step 5.1: Update Dashboard Component to Display 8 Metrics

**File**: Frontend dashboard component (exact path unknown - likely React/Vue)

**Current Structure** (4 boxes):
```
┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
│   Total AUM     │  Inception $    │  Inception %    │  Annual Income  │
│   $1,000,000    │   $50,000       │    5.00%        │   $25,000       │
└─────────────────┴─────────────────┴─────────────────┴─────────────────┘
```

**New Structure** (8 boxes - 2 rows of 4):
```
┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
│   Total AUM     │  Inception $    │  Inception %    │  Annual Income  │
│   $1,000,000    │   $50,000       │    5.00%        │   $25,000       │
├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
│   Monthly $     │   Monthly %     │  This Period $  │  This Period %  │
│   $2,500        │    0.25%        │   $500          │    0.05%        │
└─────────────────┴─────────────────┴─────────────────┴─────────────────┘
```

**Data Source**: API response `summary` object

**New Fields to Display**:
- `summary.monthly_return_dollar` → Format as currency ($2,500.00)
- `summary.monthly_return_percent` → Format as percentage (0.25%)
- `summary.period_return_dollar` → Format as currency ($500.00)
- `summary.period_return_percent` → Format as percentage (0.05%)

**Color Coding** (optional enhancement):
- Green text for positive returns
- Red text for negative returns
- Gray for zero

**Estimated Time**: 1-2 hours (depends on frontend framework)
**Risk Level**: Low (additive UI change)

---

## 6. TESTING STRATEGY

### 6.1 Unit Tests

**Test File**: `portfolio/tests/test_portfolio_calculation_service.py`

**Test Cases**:
```python
def test_calculate_monthly_return_single_client():
    """Test monthly return calculation for individual client."""
    # Setup: Create 2 snapshots (Sept 30, Oct 8)
    # Assert: Monthly return matches expected value

def test_calculate_monthly_return_no_previous_month():
    """Test monthly return when previous month has no data."""
    # Setup: Only October snapshot exists
    # Assert: Returns zeros gracefully

def test_period_return_already_in_metrics():
    """Verify period returns exist in portfolio_metrics."""
    # Setup: Calculate metrics for snapshot
    # Assert: real_gain_loss_dollar and real_gain_loss_percent present
```

---

### 6.2 Integration Tests

**Test File**: `portfolio/tests/test_dashboard_cache_service.py`

**Test Cases**:
```python
def test_aggregate_date_data_includes_new_metrics():
    """Test aggregation includes period and monthly returns."""
    # Setup: Create multiple client snapshots
    # Action: Run aggregate_date_data()
    # Assert: DateAggregatedMetrics has all 4 new fields populated

def test_get_current_dashboard_data_returns_new_metrics():
    """Test dashboard data includes new metrics."""
    # Setup: Cache data with new metrics
    # Action: Call get_current_dashboard_data()
    # Assert: Response includes all 8 summary fields
```

---

### 6.3 API Tests

**Test File**: `portfolio/tests/test_views.py`

**Test Cases**:
```python
def test_admin_dashboard_api_includes_new_metrics():
    """Test admin dashboard API returns 8 metrics."""
    # Setup: Authenticate as admin
    # Action: GET /api/admin-dashboard/
    # Assert: Response contains all 8 summary fields

def test_client_dashboard_api_includes_new_metrics():
    """Test client dashboard API returns 8 metrics."""
    # Setup: Authenticate as client
    # Action: GET /api/client-dashboard/
    # Assert: Response contains all 8 summary fields
```

---

### 6.4 Manual Testing Checklist

**Scenario 1: Single Client Dashboard**
- [ ] Login as client
- [ ] Verify 8 boxes display correctly
- [ ] Verify monthly returns show correct values
- [ ] Verify this period returns show correct values
- [ ] Verify color coding (positive=green, negative=red)

**Scenario 2: Admin Dashboard - All Clients**
- [ ] Login as admin
- [ ] Select "ALL" client filter
- [ ] Verify 8 boxes display aggregated values
- [ ] Verify monthly returns aggregate correctly
- [ ] Verify this period returns aggregate correctly

**Scenario 3: Admin Dashboard - Single Client Filter**
- [ ] Login as admin
- [ ] Select specific client (e.g., "JAV")
- [ ] Verify 8 boxes display client-specific values
- [ ] Compare with client login - should match

**Scenario 4: Edge Cases**
- [ ] New client with only 1 snapshot (no previous data)
  - Monthly returns should be $0 / 0%
  - This period returns should be $0 / 0%
- [ ] Client with only current month data (no previous month)
  - Monthly returns should be $0 / 0%
- [ ] Negative returns display correctly with red color
- [ ] Very large numbers format correctly (e.g., $10,000,000.00)

---

## 7. DEPLOYMENT PLAN

### 7.1 Local Testing (Development)

**Steps**:
1. Apply database migration
2. Recalculate metrics for test client
3. Regenerate dashboard cache
4. Test admin dashboard API
5. Test client dashboard API
6. Verify all 8 metrics display

**Commands**:
```bash
# 1. Apply migration
python manage.py makemigrations
python manage.py migrate

# 2. Recalculate metrics for test client (e.g., JAV)
python manage.py shell -c "
from portfolio.services.portfolio_calculation_service import PortfolioCalculationService;
from portfolio.models import PortfolioSnapshot;
service = PortfolioCalculationService();
snapshots = PortfolioSnapshot.objects.filter(client__code='JAV').order_by('snapshot_date');
for snapshot in snapshots:
    service.calculate_portfolio_metrics('JAV', snapshot.snapshot_date)
"

# 3. Regenerate dashboard cache
python manage.py shell -c "
from portfolio.services.correct_dashboard_cache_service import CorrectDashboardCacheService;
from portfolio.models import PortfolioSnapshot;
cache_service = CorrectDashboardCacheService();
dates = PortfolioSnapshot.objects.values_list('snapshot_date', flat=True).distinct();
for date in dates:
    date_str = date.strftime('%d_%m_%Y');
    cache_service.aggregate_date_data(date_str)
"

# 4. Test API
curl -H "Authorization: Bearer <token>" http://localhost:8000/api/admin-dashboard/?client_code=ALL
```

---

### 7.2 Production Deployment

**Pre-Deployment**:
1. ✅ All tests passing in development
2. ✅ Code review approved
3. ✅ Database backup created
4. ✅ Migration tested on staging database

**Deployment Steps**:
```bash
# 1. SSH into production server
ssh aurumapp@production-server

# 2. Pull latest code
cd /opt/aurumfinance/source/aurum_backend
git pull origin main

# 3. Activate virtual environment
source /opt/aurumfinance/venv/bin/activate

# 4. Apply migration
python manage.py migrate

# 5. Recalculate ALL client metrics (one-time operation)
# This adds monthly returns to all existing snapshots
python manage.py shell -c "
from portfolio.services.portfolio_calculation_service import PortfolioCalculationService;
from portfolio.models import Client, PortfolioSnapshot;
service = PortfolioCalculationService();
clients = Client.objects.all();
for client in clients:
    print(f'Processing {client.code}...');
    snapshots = PortfolioSnapshot.objects.filter(client=client).order_by('snapshot_date');
    for snapshot in snapshots:
        print(f'  {snapshot.snapshot_date}');
        service.calculate_portfolio_metrics(client.code, snapshot.snapshot_date.isoformat());
    print(f'Completed {client.code}');
"

# 6. Regenerate ALL dashboard cache
python manage.py shell -c "
from portfolio.services.correct_dashboard_cache_service import CorrectDashboardCacheService;
from portfolio.models import PortfolioSnapshot;
cache_service = CorrectDashboardCacheService();
dates = PortfolioSnapshot.objects.values_list('snapshot_date', flat=True).distinct().order_by('snapshot_date');
for date in dates:
    date_str = date.strftime('%d_%m_%Y');
    print(f'Caching {date_str}...');
    result = cache_service.aggregate_date_data(date_str);
    if result['success']:
        print(f'  ✅ Success');
    else:
        print(f'  ❌ Failed: {result.get(\"error\")}');
"

# 7. Restart Django service
sudo systemctl restart aurumfinance

# 8. Verify API
curl -H "Authorization: Bearer <token>" https://production-url/api/admin-dashboard/?client_code=ALL | jq '.summary'
```

**Estimated Downtime**: ~5 minutes
**Rollback Plan**:
- Revert migration: `python manage.py migrate portfolio <previous_migration_number>`
- Revert code: `git checkout <previous_commit>`
- Restart service

---

## 8. PERFORMANCE CONSIDERATIONS

### 8.1 Monthly Return Calculation Performance

**Concern**: Calculating monthly return for each client during aggregation

**Current Aggregation Time**: ~500ms for 10 clients
**Monthly Calculation Overhead**: ~50ms per client (ModifiedDietzService call)
**Projected Time**: ~1000ms for 10 clients

**Mitigation**:
1. Cache monthly returns in `portfolio_metrics` (future optimization)
2. Use database query optimization (select_related, prefetch_related)
3. Consider background task for large client counts (>50 clients)

---

### 8.2 Dashboard Cache Size

**Current Cache Entry Size**: ~5KB per date
**New Fields Addition**: +50 bytes per date
**Impact**: Negligible (<1% increase)

---

## 9. FUTURE ENHANCEMENTS

### 9.1 Cache Monthly Returns in `portfolio_metrics`

**Rationale**: Avoid recalculating monthly returns during aggregation

**Implementation**:
- Modify `PortfolioCalculationService.calculate_portfolio_metrics()`
- Add monthly return calculation
- Store in `portfolio_metrics` JSON
- Use cached value in aggregation

**Benefit**: Reduce aggregation time by 50%

---

### 9.2 Year-to-Date (YTD) Returns

**Definition**: Return from January 1 to current date
**Calculation**: Similar to monthly returns, but use January 1 as start date

**New Metrics**:
- YTD Return $
- YTD Return %

---

### 9.3 Trailing 12-Month Returns

**Definition**: Return over past 12 months (rolling)
**Calculation**: Modified Dietz from (current date - 12 months) to current date

**New Metrics**:
- Trailing 12M Return $
- Trailing 12M Return %

---

## 10. SUMMARY & RECOMMENDATIONS

### Key Findings

✅ **This Period Returns**: Already calculated and stored - just need to display
✅ **Monthly Returns**: Logic exists in `CustodyReturnsService` - adapt for portfolio-level
✅ **Infrastructure**: Dashboard cache and API endpoints ready for extension

### Implementation Complexity

| Component | Complexity | Estimated Time | Risk |
|-----------|------------|----------------|------|
| Database Migration | Low | 10 min | Low |
| Monthly Return Logic | Medium | 1 hour | Low |
| Aggregation Service | Medium | 1 hour | Medium |
| View/API Layer | Medium | 1 hour | Low |
| Frontend Display | Medium | 2 hours | Low |
| Testing | Medium | 2 hours | Low |
| **TOTAL** | **Medium** | **7-8 hours** | **Low-Medium** |

### Recommended Approach

**Option 1: Full Implementation (Recommended)**
- Implement all 4 new metrics
- Complete testing coverage
- Production deployment with cache regeneration
- Timeline: 1-2 days

**Option 2: Phased Approach**
- Phase 1: Add "This Period" returns (1 hour - data already exists!)
- Phase 2: Add Monthly returns (4-6 hours)
- Timeline: Spread over 2 weeks

### Risks & Mitigation

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Cache regeneration takes too long | Medium | Low | Run during off-hours, batch processing |
| Monthly calculation errors | High | Low | Comprehensive unit tests, manual verification |
| Frontend display issues | Low | Medium | Thorough cross-browser testing |
| Performance degradation | Medium | Low | Monitor aggregation time, optimize if needed |

---

## 11. QUESTIONS FOR REVIEW

Before implementation, please confirm:

1. **Metric Definitions**:
   - Is "This Period" defined as snapshot-to-snapshot correct? ✓
   - Is "Monthly" defined as month-end to month-end correct? ✓

2. **Display Format**:
   - Should negative returns be shown in red color? ✓
   - Should we use comma separators for large numbers ($1,000,000.00)? ✓
   - Should percentages show 2 decimal places (5.25%)? ✓

3. **Edge Cases**:
   - For first snapshot (no previous data), show $0/0% - correct? ✓
   - For clients with no previous month data, show $0/0% - correct? ✓

4. **Performance**:
   - Is 1-2 second aggregation time acceptable for 10+ clients? ✓
   - Should we implement background caching for very large client counts? ✓

5. **Deployment**:
   - Acceptable to recalculate all metrics + regenerate cache (one-time ~10 min operation)? ✓
   - Preferred deployment window (evening/weekend)? ✓

---

**END OF INVESTIGATION REPORT**

Generated: October 8, 2025
Author: Claude (Senior Software Engineer - Deep Dive Analysis)
Review Status: ⏳ Pending User Review
