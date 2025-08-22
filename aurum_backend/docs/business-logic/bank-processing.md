# Bank Processing Workflows

This document describes the business logic for processing bank files from different financial institutions.

## Overview

The Aurum Finance system processes bank statement files from 12 different financial institutions, each with their own file formats and data structures. The processing pipeline transforms these diverse formats into a standardized structure for portfolio analysis.

## Supported Banks (12 Total)

### **Simple Processing Only (4 banks)**

#### 1. JPMorgan (JPM)
**Transformer:** `jpm_transformer.py`
**Processing:** Direct transformation only
**Special Features:** 
- Standard securities file format
- Transaction categorization
- Currency normalization

#### 2. Morgan Stanley (MS)
**Transformer:** `ms_transformer.py`
**Processing:** Direct transformation only
**Special Features:**
- Multi-sheet Excel processing
- Account-level aggregation
- Fee extraction

#### 3. IDB (Inter-American Development Bank)
**Transformer:** `idb_transformer.py`
**Processing:** Direct transformation only
**Special Features:**
- International development bank format
- Multi-currency support
- Institutional investment focus

#### 4. Safra
**Transformer:** `safra_transformer.py`
**Processing:** Direct transformation only
**Special Features:**
- Private banking format
- Brazilian bank format
- High-net-worth client focus

### **Enrichment Only (1 bank)**

#### 5. HSBC
**Transformer:** `hsbc_transformer.py`
**Enricher:** `hsbc_enricher.py`
**Processing:** Transformation + Enrichment
**Special Features:**
- **Enrichment Required:** Market data enhancement
- International format
- Multi-currency transactions
- Offshore account handling

### **Combination Only (5 banks)**

#### 6. Credit Suisse (CS)
**Transformer:** `cs_transformer.py`
**Combiner:** `cs_combiner.py`
**Processing:** Transformation + File Combination
**Special Features:**
- **File Combination Required:** Multiple files per client
- European format handling
- Multi-currency support
- Bond-specific data extraction

#### 7. Valley Bank
**Transformer:** `valley_transformer.py`
**Combiner:** `valley_combiner.py`
**Processing:** Transformation + File Combination
**Special Features:**
- **File Combination Required:** Account-level files
- Regional bank format
- Simplified data structure
- Local investment products

#### 8. JB Private Bank
**Transformer:** `jb_transformer.py`
**Combiner:** `jb_combiner.py`
**Processing:** Transformation + File Combination
**Special Features:**
- **File Combination Required:** Multiple account files
- Private banking format
- High-net-worth client data
- Alternative investments

#### 9. Charles Schwab (CSC)
**Transformer:** `csc_transformer.py`
**Combiner:** `csc_combiner.py`
**Processing:** Transformation + File Combination
**Special Features:**
- **File Combination Required:** Multi-file structure
- Canadian format
- CAD currency handling
- Canadian tax considerations

#### 10. Banchile
**Transformer:** `banchile_transformer.py`
**Combiner:** `banchile_combiner.py`
**Processing:** Transformation + File Combination
**Special Features:**
- **File Combination Required:** Multiple account files
- Chilean bank format
- CLP currency handling
- Latin American market focus

### **Both Enrichment + Combination (2 banks)**

#### 11. Pershing
**Transformer:** `pershing_transformer.py`
**Enricher:** `pershing_enricher.py`
**Combiner:** `pershing_combiner.py`
**Processing:** Transformation + Enrichment + File Combination
**Special Features:**
- **Enrichment Required:** Market data enhancement
- **File Combination Required:** Multiple account aggregation
- Custodial platform format
- Complex multi-account structure

#### 12. Lombard (LO)
**Transformer:** `lombard_transformer.py`
**Enricher:** `lombard_enricher.py`
**Combiner:** `lombard_combiner.py`
**Processing:** Transformation + Enrichment + File Combination
**Special Features:**
- **Enrichment Required:** Market data enhancement
- **File Combination Required:** Multi-file aggregation
- Swiss private banking format
- Complex portfolio structures

## Processing Pipeline

### 1. File Upload and Detection
```
File Upload → Bank Detection → File Validation → Storage
```

**Bank Detection Logic:**
- File name pattern matching
- Content analysis
- Header structure recognition
- Automatic bank type assignment

### 2. Preprocessing Pipeline
```
Raw Bank Files → Enrichment → Combination → Transformation → Standardized Output
```

#### Step 1: Enrichment (Bank-Specific)
Banks requiring enrichment use dedicated enricher classes:

**Pershing Enricher (REAL):**
```python
# preprocessing/combiners/pershing_enricher.py
class PershingEnricher:
    def __init__(self):
        self.bank_code = 'Pershing'
    
    def discover_pershing_files(self, input_dir: Path, date: str) -> Dict[str, Dict[str, Optional[Path]]]:
        """
        Discover Pershing files for a specific date and group by client/account.
        Returns: Dict mapping client_account to file paths
        """
    
    def enrich_securities_with_unitcost(self, securities_file: Path, unitcost_file: Path) -> Path:
        """
        Enrich securities file with unit cost data from unitcost file.
        Merges 'Total Cost' column into securities data.
        """
```

**HSBC Enricher (REAL):**
```python
# preprocessing/combiners/hsbc_enricher.py  
class HSBCEnricher:
    def enrich_hsbc_files(self, input_dir: Path, date: str) -> Dict[str, Path]:
        """
        Enrich HSBC files with market data and additional information.
        """
```

#### Step 2: Combination (Multi-File Banks)
Banks requiring file combination use dedicated combiner classes:

**Pershing Combiner (REAL):**
```python
# preprocessing/combiners/pershing_combiner.py
class PershingCombiner:
    def discover_enriched_files(self, input_dir: Path, date: str) -> Dict[str, Dict[str, Optional[Path]]]:
        """
        Discover enriched Pershing files and group by client/account.
        """
    
    def combine_client_files(self, client_files: Dict[str, Path], output_dir: Path, date: str) -> Tuple[Path, Path]:
        """
        Combine individual client files into unified securities and transactions files.
        """
```

#### Step 3: Transformation (Standardization)
All banks use transformer classes for standardization:

**JPMorgan Transformer (REAL):**
```python
# preprocessing/transformers/jpm_transformer.py
class JPMorganTransformer:
    def get_securities_column_mappings(self) -> Dict[str, str]:
        return {
            'account_number': 'Account Number',
            'name': 'Description',
            'ticker': 'Ticker',
            'cusip': 'CUSIP',
            'quantity': 'Quantity',
            'market_value': 'Value',
            'cost_basis': 'Cost',
            'maturity_date': 'Maturity Date',
            'coupon_rate': 'Coupon Rate (%)'
        }
    
    def transform_securities(self, file_path: str) -> pd.DataFrame:
        """Transform JPMorgan securities file to standard format."""
    
    def load_account_mappings(self, mappings_file: str, sheet_name: str = 'JPM') -> Dict:
        """Load account mappings from Mappings.xlsx JPM sheet."""
```

All bank files are transformed to a common format:

```python
# Standard output format
{
    "securities": [
        {
            "ticker": "AAPL",
            "name": "Apple Inc.",
            "quantity": 100,
            "market_value": 15000.00,
            "cost_basis": 12000.00,
            "asset_type": "Stock",
            "currency": "USD",
            "client": "CLIENT_A"
        }
    ],
    "transactions": [
        {
            "ticker": "AAPL",
            "date": "2025-07-15",
            "type": "BUY",
            "quantity": 100,
            "price": 120.00,
            "total_amount": 12000.00,
            "client": "CLIENT_A"
        }
    ]
}
```

## API Integration

### File Upload Endpoint
**Endpoint:** `POST /api/admin/upload-files/`

**Real Implementation:**
```python
# Django service integration
from ..preprocessing.bank_detector import BankDetector

class ProcessingService:
    def detect_bank(self, filename: str) -> Optional[str]:
        """Use real bank detection logic"""
        return BankDetector.detect_bank(filename)
```

### Preprocessing Endpoint  
**Endpoint:** `POST /api/admin/run-preprocessing/`

**Real Implementation:**
```python
# Django service integration
from ..preprocessing.preprocess import UnifiedPreprocessor

class ProcessingService:
    def __init__(self):
        self.preprocessor = UnifiedPreprocessor()
    
    def process_all_banks(self, date: str):
        """Run real preprocessing pipeline"""
        return self.preprocessor.process_all_banks(date)
```

## Real Bank Architecture

### Bank Detection Patterns (REAL)
```python
# preprocessing/bank_detector.py
class BankDetector:
    BANK_PATTERNS = {
        'JPM': r'^JPM_',
        'CS': r'^CS_',
        'CSC': r'^CSC_',  # Charles Schwab
        'JB': r'^JB_',
        'MS': r'^MS_',
        'Valley': r'^Valley_',
        'Pershing': r'^Pershing_',
        'LO': r'^LO_',      # Lombard Odier
        'Safra': r'^Safra_',
        'IDB': r'^IDB_',
        'Banchile': r'^Banchile_'
    }
    
    # Banks that use individual files per client/account
    INDIVIDUAL_FILE_BANKS = {'Valley', 'CS', 'CSC', 'JB', 'Pershing', 'LO', 'IDB'}
```

### Transformer Registry (REAL)
```python
# preprocessing/preprocess.py
class UnifiedPreprocessor:
    def __init__(self):
        self.supported_banks = ['JPM', 'MS', 'CSC', 'Pershing', 'CS', 'JB', 'HSBC', 'Valley', 'Safra', 'LO', 'IDB', 'Banchile']
        self.transformer_registry = {
            'JPM': 'preprocessing.transformers.jpm_transformer.JPMorganTransformer',
            'MS': 'preprocessing.transformers.ms_transformer.MorganStanleyTransformer',
            'CSC': 'preprocessing.transformers.csc_transformer.CSCTransformer',
            'Pershing': 'preprocessing.transformers.pershing_transformer.PershingTransformer',
            'CS': 'preprocessing.transformers.cs_transformer.CSTransformer',
            'JB': 'preprocessing.transformers.jb_transformer.JBTransformer',
            'HSBC': 'preprocessing.transformers.hsbc_transformer.HSBCTransformer',
            'Valley': 'preprocessing.transformers.valley_transformer.ValleyTransformer',
            'Safra': 'preprocessing.transformers.safra_transformer.SafraTransformer',
            'LO': 'preprocessing.transformers.lombard_transformer.LombardTransformer',
            'IDB': 'preprocessing.transformers.idb_transformer.IDBTransformer',
            'Banchile': 'preprocessing.transformers.banchile_transformer.BanchileTransformer'
        }
```

### External Data Dependencies (REAL)
```python
# Mappings.xlsx structure (REAL - from investigation):
# 6 sheets with account/client mappings:
# - JPM: 61 account mappings
# - MS: 20 account mappings  
# - HSBC: 4 account mappings
# - LO: 5 account mappings
# - Safra: 2 account mappings
# - Banchile: 5 account mappings

# Each sheet contains:
# - Account Number: Bank-specific account identifier
# - client: Internal client code (e.g., 'VLP', 'LP', 'JAV')
# - account: Internal account code (e.g., 'TTAdmin', 'MTAdmin')
```

## Error Handling

### Common Processing Errors

#### Invalid File Format
```json
{
  "error": "invalid_format",
  "message": "File does not match expected bank format",
  "details": {
    "bank": "JPM",
    "expected_sheets": ["Securities", "Transactions"],
    "found_sheets": ["Sheet1", "Sheet2"]
  }
}
```

#### Missing Required Data
```json
{
  "error": "missing_data",
  "message": "Required columns missing from bank file",
  "details": {
    "bank": "MS",
    "missing_columns": ["Ticker", "Quantity"],
    "sheet": "Securities"
  }
}
```

#### Enrichment Failure
```json
{
  "error": "enrichment_failed",
  "message": "Failed to enrich Pershing files with market data",
  "details": {
    "bank": "Pershing",
    "failed_securities": ["UNKNOWN_TICKER"],
    "api_errors": ["Market data API timeout"]
  }
}
```

## Data Validation

### Security Validation
- Ticker symbol validation
- ISIN/CUSIP verification
- Market data consistency checks

### Transaction Validation
- Date format validation
- Amount calculation verification
- Currency consistency checks

### Client Data Validation
- Client code validation
- Account number verification
- Data completeness checks

## Performance Considerations

### File Size Limits
- Maximum 50MB per file
- Automatic compression for large files
- Streaming processing for memory efficiency

### Processing Time
- Target: Under 3 minutes for complete pipeline
- Parallel processing where possible
- Progress tracking for long operations

### Caching
- Market data caching for enrichment
- Processed file caching
- Configuration caching

## Integration Points

### Database Integration (REAL)
After preprocessing, standardized data is loaded using:
```python
# From generate_weekly_report.py (REAL)
def process_new_data(securities_file, transactions_file, snapshot_date, client):
    """
    Complete calculation pipeline including:
    - Excel parsing with StatementParser/TransactionParser
    - Modified Dietz calculations
    - Asset allocation analysis  
    - Database population
    """

# From test_excel_pipeline.py (REAL)
def save_data_to_database(securities_df, transactions_df, client):
    """Database population with all relationships"""

def perform_calculations(assets, positions, transactions):
    """All financial calculations using PerformanceCalculator, IncomeAnalyzer, etc."""
```

### Report Generation (REAL)
```python
# From generate_html_report.py (REAL)
def generate_html_report_from_snapshots(date1, date2, output_file, client):
    """Generate HTML reports from database snapshots"""
```

### Calculation Engine (REAL)
Processed data feeds into:
```python
# Real calculation modules from AurumFinance:
from portfolio.business_logic.calculation_helpers import PerformanceCalculator
from portfolio.business_logic.calculation_helpers import ModifiedDietzCalculator
from portfolio.business_logic.calculation_helpers import InvestmentCashFlowCalculator
from portfolio.business_logic.calculation_helpers import CashFlowClassifier

# These handle:
# - Modified Dietz return calculations
# - Asset allocation analysis
# - Performance metrics calculation
# - Income analysis
```

This bank processing system ensures that regardless of the source bank format, all data is transformed into a consistent structure that can be reliably used for portfolio analysis and reporting.