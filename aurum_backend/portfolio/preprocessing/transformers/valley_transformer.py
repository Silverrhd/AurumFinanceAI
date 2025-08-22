"""
Valley Bank Transformer with OpenFIGI API Integration

This transformer processes Valley bank securities and transactions data,
enriching it with OpenFIGI API data for asset types, maturity dates, 
coupon rates, and tickers.

Key Features:
- OpenFIGI API integration for data enrichment
- CUSIP and name-based lookups with caching
- Bond price logic with European formatting
- Date format conversion (DD-MM-YYYY/MM/DD/YYYY to MM/DD/YYYY)
- Empty transaction price handling

Author: Generated for Project Aurum
Date: 2025-01-13
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
import re
import os
import sys

# Add project root to path
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from portfolio.preprocessing.utils.openfigi_client import OpenFIGIClient


class ValleyTransformer:
    def __init__(self, api_key: str = None):
        self.logger = logging.getLogger(__name__)
        self.api_key = api_key or os.getenv('OPENFIGI_API_KEY')
        self.openfigi_client = OpenFIGIClient(self.api_key)
        
        # Enhanced asset type mappings based on OpenFIGI API responses
        self.api_asset_type_mapping = {
            # OpenFIGI types that map to Fixed Income (with bond pricing logic)
            'US GOVERNMENT': 'Fixed Income',
            'GLOBAL': 'Fixed Income', 
            'DOMESTIC MTN': 'Fixed Income',
            'Corporate Bond': 'Fixed Income',
            'Government Bond': 'Fixed Income', 
            'Treasury Bill': 'Fixed Income',
            'Treasury Note': 'Fixed Income',
            'Treasury Bond': 'Fixed Income',
            'Municipal Bond': 'Fixed Income',
            
            # OpenFIGI types that map to Equity (standard pricing logic)
            'ETP': 'Equity',    # Exchange Traded Products
            'Equity': 'Equity',
            'Common Stock': 'Equity',
            'Preferred Stock': 'Equity',
            'ETF': 'Equity',
            'REIT': 'Equity'
        }
        
        # Column mappings for securities
        self.securities_column_mapping = {
            'bank': 'bank',
            'client': 'client', 
            'account': 'account',
            'Quantity': 'quantity',
            'Market Value': 'market_value',
            'Adj Cost Basis': 'cost_basis',
            'Description': 'name',
            'CUSIP': 'cusip',
            'Mkt Price Ccy': 'price'
        }
        
        # Column mappings for transactions
        self.transactions_column_mapping = {
            'bank': 'bank',
            'client': 'client',
            'account': 'account', 
            'CUSIP': 'cusip',
            'Post Date': 'date',       # Convert to MM/DD/YYYY
            'Description': 'transaction_type',
            'Cantidad': 'quantity',
            # Amount will be calculated from Debit/Credit using CS pattern
            # Price will be set to NaN (Valley doesn't provide)
        }

    def _format_european_number(self, value):
        """Convert American number format to European format."""
        if pd.isna(value):
            return None
            
        try:
            # Convert to float first to handle various input formats
            if isinstance(value, str):
                # Remove any existing formatting
                clean_value = value.replace(',', '').replace(' ', '')
                numeric_value = float(clean_value)
            else:
                numeric_value = float(value)
                
            # Format as European (comma as decimal separator)
            # Handle different scales appropriately
            if abs(numeric_value) >= 1000:
                # For thousands, use dot as thousand separator
                formatted = f"{numeric_value:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
            else:
                # For smaller numbers, just use comma as decimal
                formatted = f"{numeric_value:.2f}".replace('.', ',')
                
            return formatted
            
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Could not format number '{value}': {e}")
            return str(value)

    def _apply_bond_price_logic(self, price_str, is_bond):
        """
        Apply Valley-specific bond price formatting based on API asset type detection.
        - If bond and starts with 1: comma after first digit (12345 → 1,2345)  
        - If bond and other: comma before (89765 → 0,89765)
        - If not bond: standard European formatting
        """
        if pd.isna(price_str):
            return None
            
        if not is_bond:
            return self._format_european_number(price_str)
        
        try:
            # Bond-specific logic
            clean_number = str(price_str).replace(',', '').replace('.', '').replace(' ', '')
            
            if clean_number.startswith('1'):
                # Place comma after first digit: 12345 → 1,2345
                if len(clean_number) > 1:
                    return f"1,{clean_number[1:]}"
                else:
                    return "1"
            else:
                # Place comma before: 89765 → 0,89765  
                return f"0,{clean_number}"
                
        except Exception as e:
            self.logger.warning(f"Could not apply bond price logic to '{price_str}': {e}")
            return self._format_european_number(price_str)

    def _convert_date_to_standard(self, date_str):
        """
        Convert various date formats to MM/DD/YYYY standard.
        Handles: DD-MM-YYYY, MM/DD/YYYY, DD/MM/YYYY
        """
        if pd.isna(date_str) or str(date_str).strip() == '':
            return None
            
        try:
            date_str = str(date_str).strip()
            
            # Try different format patterns
            patterns = [
                ('%d-%m-%Y', 'DD-MM-YYYY'),    # Valley format 1
                ('%m/%d/%Y', 'MM/DD/YYYY'),    # Already standard
                ('%d/%m/%Y', 'DD/MM/YYYY'),    # Valley format 2
                ('%d-%m-%y', 'DD-MM-YY'),      # Short year variant
                ('%m/%d/%y', 'MM/DD/YY'),      # Short year standard
                ('%d/%m/%y', 'DD/MM/YY')       # Short year variant
            ]
            
            for pattern, description in patterns:
                try:
                    date_obj = datetime.strptime(date_str, pattern)
                    result = date_obj.strftime('%m/%d/%Y')
                    self.logger.debug(f"Converted date '{date_str}' from {description} to {result}")
                    return result
                except ValueError:
                    continue
                    
            # If no pattern matches, log warning and return original
            self.logger.warning(f"Could not parse date format: '{date_str}'")
            return date_str
            
        except Exception as e:
            self.logger.error(f"Error converting date '{date_str}': {e}")
            return date_str

    def _process_debit_credit(self, debit_value, credit_value):
        """
        Process Debit/Credit columns into single amount column.
        Following CS transformer pattern exactly:
        
        Logic:
        - If Credit populated → positive amount (+value)  
        - If Debit populated → negative amount (-value)
        - Convert final result to European number formatting
        - Only one column should be populated per transaction
        """
        if pd.notna(debit_value) and pd.notna(credit_value):
            self.logger.warning(f"Both Debit ({debit_value}) and Credit ({credit_value}) populated - using Credit")
        
        if pd.notna(debit_value) and debit_value != '':
            # Debit = money going out = negative
            try:
                amount_str = f"-{debit_value}"
                # Convert to European format: -7992.33 → -7992,33
                amount_european = amount_str.replace('.', ',')
                self.logger.debug(f"Debit processed: {debit_value} → {amount_str} → {amount_european}")
                return amount_european
            except:
                self.logger.warning(f"Could not process debit value: {debit_value}")
                return None
        
        elif pd.notna(credit_value) and credit_value != '':
            # Credit = money coming in = positive
            try:
                amount_str = str(credit_value)
                # Convert to European format: 36.60 → 36,60
                amount_european = amount_str.replace('.', ',')
                self.logger.debug(f"Credit processed: {credit_value} → {amount_str} → {amount_european}")
                return amount_european
            except:
                self.logger.warning(f"Could not process credit value: {credit_value}")
                return None
        
        else:
            # Neither populated
            return None

    def _enrich_securities_with_api(self, df):
        """
        Enrich securities DataFrame with OpenFIGI API data.
        
        Adds: asset_type, maturity_date, coupon_rate, ticker
        """
        self.logger.info("Starting OpenFIGI API enrichment for Valley securities")
        
        # Extract unique CUSIPs for batch processing
        unique_cusips = df['cusip'].dropna().unique().tolist()
        total_cusips = len(unique_cusips)
        
        if total_cusips == 0:
            self.logger.warning("No CUSIPs found for API enrichment")
            return df
            
        self.logger.info(f"Found {total_cusips} unique CUSIPs to enrich")
        
        # Perform batch lookup
        api_results = self.openfigi_client.batch_lookup(unique_cusips, 'cusip')
        
        # Calculate success rate
        successful_lookups = sum(1 for result in api_results.values() if result is not None)
        success_rate = (successful_lookups / total_cusips * 100) if total_cusips > 0 else 0
        
        self.logger.info(f"API enrichment results: {successful_lookups}/{total_cusips} successful ({success_rate:.1f}%)")
        
        # Check if we meet minimum success rate (90%)
        if success_rate < 90:
            self.logger.warning(f"API success rate ({success_rate:.1f}%) below 90% threshold")
        
        # Apply enrichment to DataFrame
        enriched_count = 0
        for idx, row in df.iterrows():
            cusip = row.get('cusip')
            if pd.isna(cusip):
                # No CUSIP - skip API lookup but still apply fallback enrichment
                self._apply_fallback_enrichment(df, idx, row)
                continue
                
            api_data = api_results.get(cusip)
            if api_data and 'error' not in api_data:
                # Map API fields to our columns
                
                # Asset type mapping
                api_security_type = api_data.get('security_type') or api_data.get('security_type2')
                if api_security_type:
                    mapped_asset_type = self.api_asset_type_mapping.get(api_security_type, api_security_type)
                    df.at[idx, 'asset_type'] = mapped_asset_type
                
                # Parse ticker (handles both composite bonds and clean ETF tickers)
                if api_data.get('ticker'):
                    ticker_data = self._parse_composite_ticker(api_data['ticker'])
                    
                    if ticker_data['ticker']:
                        df.at[idx, 'ticker'] = ticker_data['ticker']
                    if ticker_data['coupon_rate']:
                        df.at[idx, 'coupon_rate'] = ticker_data['coupon_rate']  
                    if ticker_data['maturity_date']:
                        df.at[idx, 'maturity_date'] = ticker_data['maturity_date']
                    
                    # Clear ticker for bond assets after extraction (bonds are identified by CUSIP, not ticker)
                    # Bond tickers like "T" are misleading ("T" = AT&T Inc., not Treasury bonds)
                    if ticker_data['coupon_rate'] or ticker_data['maturity_date']:
                        df.at[idx, 'ticker'] = None
                        self.logger.debug(f"Cleared ticker for bond {cusip} after extracting coupon/maturity data")
                
                # Direct field mappings (fallback if ticker parsing didn't provide them)
                if not df.at[idx, 'maturity_date'] and api_data.get('maturity'):
                    df.at[idx, 'maturity_date'] = api_data['maturity']
                    
                if not df.at[idx, 'coupon_rate'] and api_data.get('coupon'):
                    # Format coupon as European number
                    coupon_formatted = self._format_european_number(api_data['coupon'])
                    df.at[idx, 'coupon_rate'] = coupon_formatted
                
                enriched_count += 1
            else:
                # Try fallback strategies
                self._apply_fallback_enrichment(df, idx, row)
        
        self.logger.info(f"Successfully enriched {enriched_count} securities with API data")
        
        # Show cache statistics
        stats = self.openfigi_client.get_client_stats()
        self.logger.info(f"API client stats: {stats}")
        
        return df

    def _clean_asset_name_for_lookup(self, full_name):
        """
        Clean asset names for OpenFIGI name-based lookup.
        
        Examples:
        - "ISHARES VII PLC MSCI CANADA B UCITS ETF ACCUM" → "ISHARES MSCI CANADA"
        - "ISHARES V PLC ISHARES S&P 500 INFORMATION" → "ISHARES S&P 500 INFORMATION" 
        - "ISHS CR MSCI EM ACCUM PTG SHS EXCHANGE" → "ISHARES MSCI EM"
        
        Args:
            full_name: Full asset description from Valley data
            
        Returns:
            Cleaned name suitable for OpenFIGI lookup
        """
        if pd.isna(full_name) or not full_name:
            return None
            
        name = str(full_name).strip().upper()
        
        # Note: Don't skip cash assets here as they need hardcoded pattern matching
        # The hardcoded pattern matching will handle CAJA, CASH, MONEY MARKET
        
        # Common cleaning patterns for iShares ETFs
        cleaning_patterns = [
            # Remove company structure terms
            (r'\s+PLC\s+', ' '),
            (r'\s+PUBLIC LIMITED COMPANY\s+', ' '),
            (r'\s+VII\s+', ' '),
            (r'\s+IV\s+', ' '),
            (r'\s+V\s+', ' '),
            
            # Remove ETF structure terms
            (r'\s+ACCUM\s*$', ''),
            (r'\s+ACCUMULATING\s*$', ''),
            (r'\s+UCITS ETF\s*', ' '),
            (r'\s+ETF\s*$', ''),
            (r'\s+EXCHANGE\s*$', ''),
            (r'\s+SHS\s+', ' '),
            (r'\s+PTG\s+', ' '),
            
            # Remove duplicate ISHARES references
            (r'ISHARES\s+.*?\s+ISHARES\s+', 'ISHARES '),
            
            # Standardize abbreviations
            (r'\bISHS\b', 'ISHARES'),
            (r'\bCR\b', 'CORE'),
            (r'\bEM\b', 'EMERGING MARKETS'),
        ]
        
        # Apply cleaning patterns
        cleaned_name = name
        for pattern, replacement in cleaning_patterns:
            cleaned_name = re.sub(pattern, replacement, cleaned_name)
        
        # Remove extra whitespace
        cleaned_name = re.sub(r'\s+', ' ', cleaned_name).strip()
        
        # Truncate very long names (OpenFIGI has limits)
        if len(cleaned_name) > 50:
            # Try to keep the most important parts
            if 'ISHARES' in cleaned_name:
                # Keep ISHARES + next 40 characters
                ishares_pos = cleaned_name.find('ISHARES')
                cleaned_name = cleaned_name[ishares_pos:ishares_pos + 50].strip()
        
        self.logger.debug(f"Cleaned name: '{full_name}' → '{cleaned_name}'")
        return cleaned_name

    def _apply_fallback_enrichment(self, df, idx, row):
        """
        Apply fallback enrichment strategies when API fails.
        
        Strategy hierarchy:
        1. CUSIP-based lookup (already attempted in main enrichment)
        2. Name-based lookup with cleaned names
        3. Hardcoded pattern matching
        """
        cusip = row.get('cusip', '')
        name = row.get('name', '')
        
        self.logger.debug(f"Applying fallback enrichment for CUSIP: {cusip}, Name: {name}")
        
        # Strategy 2: Name-based lookup
        cleaned_name = self._clean_asset_name_for_lookup(name)
        if cleaned_name:
            self.logger.info(f"Attempting name-based lookup for '{cleaned_name}' (original: '{name}')")
            
            try:
                # Try name-based lookup with OpenFIGI API
                api_data = self.openfigi_client.lookup_by_name(cleaned_name)
                
                if api_data and 'error' not in api_data:
                    self.logger.info(f"✅ Name-based lookup successful for '{cleaned_name}'")
                    
                    # Apply the same enrichment logic as main API processing
                    api_security_type = api_data.get('security_type') or api_data.get('security_type2')
                    if api_security_type:
                        mapped_asset_type = self.api_asset_type_mapping.get(api_security_type, api_security_type)
                        df.at[idx, 'asset_type'] = mapped_asset_type
                    
                    # Parse ticker (handles both composite bonds and clean ETF tickers)
                    if api_data.get('ticker'):
                        ticker_data = self._parse_composite_ticker(api_data['ticker'])
                        
                        if ticker_data['ticker']:
                            df.at[idx, 'ticker'] = ticker_data['ticker']
                        if ticker_data['coupon_rate']:
                            df.at[idx, 'coupon_rate'] = ticker_data['coupon_rate']  
                        if ticker_data['maturity_date']:
                            df.at[idx, 'maturity_date'] = ticker_data['maturity_date']
                        
                        # Clear ticker for bond assets after extraction (bonds are identified by CUSIP, not ticker)
                        # Bond tickers like "T" are misleading ("T" = AT&T Inc., not Treasury bonds)
                        if ticker_data['coupon_rate'] or ticker_data['maturity_date']:
                            df.at[idx, 'ticker'] = None
                            self.logger.debug(f"Cleared ticker for bond {cusip} after extracting coupon/maturity data")
                    
                    # Direct field mappings (fallback if ticker parsing didn't provide them)
                    if not df.at[idx, 'maturity_date'] and api_data.get('maturity'):
                        df.at[idx, 'maturity_date'] = api_data['maturity']
                        
                    if not df.at[idx, 'coupon_rate'] and api_data.get('coupon'):
                        coupon_formatted = self._format_european_number(api_data['coupon'])
                        df.at[idx, 'coupon_rate'] = coupon_formatted
                    
                    return  # Success, no need for further fallback
                    
            except Exception as e:
                self.logger.warning(f"Name-based lookup failed for '{cleaned_name}': {e}")
        
        # Strategy 3: Hardcoded pattern matching
        self._apply_hardcoded_patterns(df, idx, row)

    def _apply_hardcoded_patterns(self, df, idx, row):
        """Apply hardcoded pattern matching as final fallback."""
        cusip = str(row.get('cusip', ''))
        name = str(row.get('name', '')).upper()
        
        # Pattern matching for cash/cash equivalents  
        cash_patterns = [
            ('CAJA', 'Cash'),
            ('CASH', 'Cash'),
            ('MONEY MARKET', 'Cash')
        ]
        
        # Pattern matching for common ETF types and specific equities
        etf_patterns = [
            ('ISHARES', 'Equity'),
            ('SPDR', 'Equity'), 
            ('VANGUARD', 'Equity'),
            ('ETF', 'Equity'),
            ('MSCI', 'Equity'),
            ('S&P 500', 'Equity'),
            ('S&P500', 'Equity'),
            # NEW: Specific company patterns for when API fails
            ('APPLE INC', 'Equity'),
            ('MICROSOFT', 'Equity'),
            ('AMAZON', 'Equity'),
            ('GOOGLE', 'Equity'),
            ('TESLA', 'Equity'),
        ]
        
        # Pattern matching for bond types
        bond_patterns = [
            ('TREASURY', 'Fixed Income'),
            ('TREAS', 'Fixed Income'),
            ('BOND', 'Fixed Income'),
            ('NOTE', 'Fixed Income'),
            ('BILL', 'Fixed Income'),
            ('GOVERNMENT', 'Fixed Income'),
            # NEW: Corporate bond patterns for problematic assets
            ('CORP', 'Fixed Income'),           # "BANK AMER CORP"
            ('MEDIUM TERM', 'Fixed Income'),    # "MEDIUM TERM NTS"
            ('MTN', 'Fixed Income'),            # Medium Term Notes  
            ('SR NT', 'Fixed Income'),          # Senior Notes
            ('FINL CO', 'Fixed Income'),        # "GENERAL MTRS FINL CO"
            ('FXD RT', 'Fixed Income'),         # "FXD RT SR NT"
            ('NTS', 'Fixed Income'),            # Notes abbreviation
        ]
        
        # Apply cash patterns first (most specific)
        for pattern, asset_type in cash_patterns:
            if pattern in name:
                df.at[idx, 'asset_type'] = asset_type
                self.logger.info(f"Applied hardcoded cash pattern '{pattern}' → {asset_type} for {cusip}")
                return
        
        # Apply ETF patterns (more specific than bonds)
        for pattern, asset_type in etf_patterns:
            if pattern in name:
                df.at[idx, 'asset_type'] = asset_type
                self.logger.info(f"Applied hardcoded ETF pattern '{pattern}' → {asset_type} for {cusip}")
                return
        
        # Apply bond patterns
        for pattern, asset_type in bond_patterns:
            if pattern in name:
                df.at[idx, 'asset_type'] = asset_type
                self.logger.info(f"Applied hardcoded bond pattern '{pattern}' → {asset_type} for {cusip}")
                return
        
        # G-prefix CUSIPs are typically international securities (often ETFs)
        if cusip.startswith('G'):
            df.at[idx, 'asset_type'] = 'Equity'
            self.logger.info(f"Applied G-prefix pattern → Equity for {cusip}")
            return
        
        # NEW: Structural cash detection as final fallback
        if self._detect_cash_by_structure(row):
            df.at[idx, 'asset_type'] = 'Cash'
            self.logger.info(f"Applied structural cash pattern → Cash for {cusip}")
            return
        
        self.logger.debug(f"No hardcoded patterns matched for {cusip}: {name}")

    def _detect_cash_by_structure(self, row):
        """
        Detect cash assets by data structure patterns.
        
        Cash assets typically have:
        - Market value > 0
        - No CUSIP (empty/null/0)  
        - No quantity or quantity = 0
        - No cost basis or cost basis = 0
        
        This catches cases where name-based patterns fail.
        """
        try:
            # Check if asset has market value
            market_value = row.get('market_value')
            has_market_value = pd.notna(market_value) and float(market_value) > 0
            
            # Check if CUSIP is missing/invalid
            cusip = row.get('cusip')
            has_no_cusip = pd.isna(cusip) or str(cusip).strip() in ['', '0', 'nan', 'None']
            
            # Check if quantity is missing/zero
            quantity = row.get('quantity')
            has_no_quantity = pd.isna(quantity) or float(quantity or 0) == 0
            
            # Check if cost basis is missing/zero
            cost_basis = row.get('cost_basis')  
            has_no_cost_basis = pd.isna(cost_basis) or float(cost_basis or 0) == 0
            
            # Cash pattern: has market value but missing other key fields
            is_cash_structure = has_market_value and has_no_cusip and has_no_quantity and has_no_cost_basis
            
            if is_cash_structure:
                self.logger.debug(f"Detected cash structure: MV={market_value}, CUSIP={cusip}, Qty={quantity}, CB={cost_basis}")
            
            return is_cash_structure
            
        except Exception as e:
            self.logger.warning(f"Error in cash structure detection: {e}")
            return False

    def _detect_bond_from_enrichment(self, row):
        """Detect if security is a bond based on enriched data."""
        # Check asset type first (primary method)
        asset_type = row.get('asset_type', '')
        if 'Fixed Income' in str(asset_type):
            return True
            
        # Check if maturity date exists (bonds have maturity dates)
        maturity = row.get('maturity_date')
        if pd.notna(maturity) and str(maturity).strip():
            return True
            
        # Check if coupon rate exists (bonds have coupon rates)
        coupon = row.get('coupon_rate')
        if pd.notna(coupon) and str(coupon).strip():
            return True
            
        # Check CUSIP patterns (US Treasury typically start with 912)
        cusip = row.get('cusip', '')
        if str(cusip).startswith('912'):
            return True
            
        # Check name for bond keywords
        name = str(row.get('name', '')).upper()
        bond_keywords = ['TREAS', 'BOND', 'NOTE', 'BILL', 'TREASURY']
        if any(keyword in name for keyword in bond_keywords):
            return True
            
        return False

    def _parse_composite_ticker(self, api_ticker):
        """
        Parse composite tickers from OpenFIGI API.
        
        For bonds: "T 0.375 01/31/26" → ticker="T", coupon="0.375", maturity="01/31/26"
        For ETFs: "SLV" → ticker="SLV", coupon=None, maturity=None
        
        Args:
            api_ticker: Ticker string from OpenFIGI API
            
        Returns:
            dict with keys: ticker, coupon_rate, maturity_date
        """
        if pd.isna(api_ticker) or not api_ticker:
            return {'ticker': None, 'coupon_rate': None, 'maturity_date': None}
            
        ticker_str = str(api_ticker).strip()
        
        # Pattern for bond composite ticker: SYMBOL COUPON MM/DD/YY [OPTIONAL_SUFFIX]
        # Examples: "T 0.375 01/31/26", "BAC 3.248 10/21/27 MTN", "BMO 4.7 09/14/27 H"
        bond_pattern = r'^(\w+)\s+([\d.]+)\s+([\d/]+)(?:\s+\w+)*$'
        match = re.match(bond_pattern, ticker_str)
        
        if match:
            # Composite bond ticker
            symbol = match.group(1)
            coupon = match.group(2)
            maturity = match.group(3)
            
            # Format coupon as European number
            coupon_formatted = self._format_european_number(float(coupon))
            
            # Convert date to standard MM/DD/YYYY format
            maturity_formatted = self._convert_date_to_standard(maturity)
            
            self.logger.debug(f"Parsed composite ticker '{ticker_str}' → ticker='{symbol}', coupon='{coupon_formatted}', maturity='{maturity_formatted}'")
            
            return {
                'ticker': symbol,
                'coupon_rate': coupon_formatted, 
                'maturity_date': maturity_formatted
            }
        else:
            # Clean ticker (ETFs, stocks)
            self.logger.debug(f"Clean ticker: '{ticker_str}'")
            return {
                'ticker': ticker_str,
                'coupon_rate': None,
                'maturity_date': None  
            }

    def transform_securities(self, securities_file: str) -> pd.DataFrame:
        """Transform Valley securities data with API enrichment."""
        self.logger.info("Starting Valley securities transformation")
        
        # Load the securities file
        df = pd.read_excel(securities_file)
        self.logger.info(f"Loaded {len(df)} securities records")
        
        # Step 1: Filter and rename columns
        available_columns = [col for col in self.securities_column_mapping.keys() if col in df.columns]
        df_filtered = df[available_columns].copy()
        
        # Rename columns
        rename_mapping = {col: self.securities_column_mapping[col] for col in available_columns}
        df_renamed = df_filtered.rename(columns=rename_mapping)
        
        # Step 2: Add missing columns with None values
        target_columns = ['bank', 'client', 'account', 'asset_type', 'name', 'cusip', 'ticker', 
                         'quantity', 'price', 'maturity_date', 'market_value', 'cost_basis', 'coupon_rate']
        
        for col in target_columns:
            if col not in df_renamed.columns:
                df_renamed[col] = None
        
        # Step 3: Apply OpenFIGI API enrichment
        df_enriched = self._enrich_securities_with_api(df_renamed)
        
        # Step 4: Apply bond price logic
        if 'price' in df_enriched.columns:
            df_enriched['price'] = df_enriched['price'].astype(object)
        
        for idx, row in df_enriched.iterrows():
            is_bond = self._detect_bond_from_enrichment(row)
            
            if 'price' in df_enriched.columns and pd.notna(row['price']):
                formatted_price = self._apply_bond_price_logic(row['price'], is_bond)
                df_enriched.at[idx, 'price'] = formatted_price
        
        # Step 5: Format numeric columns as European
        numeric_columns = ['quantity', 'market_value', 'cost_basis']
        for col in numeric_columns:
            if col in df_enriched.columns:
                df_enriched[col] = df_enriched[col].apply(self._format_european_number)
        
        # Step 6: Final column ordering
        final_columns = ['bank', 'client', 'account', 'asset_type', 'ticker', 'name', 'cusip', 
                        'quantity', 'price', 'maturity_date', 'coupon_rate', 'market_value', 'cost_basis']
        
        for col in final_columns:
            if col not in df_enriched.columns:
                df_enriched[col] = None
                
        df_final = df_enriched[final_columns].copy()
        
        # NEW: Add validation to prevent empty-name mystery assets
        before_count = len(df_final)
        
        # Remove rows with empty names that could cause mystery assets
        df_final = df_final[
            (df_final['name'].notna()) & 
            (df_final['name'].astype(str).str.strip() != '') &
            (df_final['name'].astype(str).str.strip() != 'nan')
        ]
        
        after_count = len(df_final)
        
        if before_count != after_count:
            removed_count = before_count - after_count
            self.logger.warning(f"Removed {removed_count} invalid assets with empty/invalid names to prevent mystery assets")
        
        self.logger.info(f"Completed transformation: {len(df_final)} records")
        return df_final

    def transform_transactions(self, transactions_file: str, mappings_file: str = None) -> pd.DataFrame:
        """
        Transform Valley transactions file to standard format.
        
        Args:
            transactions_file: Path to Valley transactions Excel file
            mappings_file: Path to account mappings (optional for Valley)
            
        Returns:
            Transformed DataFrame in standard format
        """
        self.logger.info(f"🔄 Transforming Valley transactions file: {transactions_file}")
        
        try:
            # Read the transactions file
            df = pd.read_excel(transactions_file)
            self.logger.info(f"📊 Loaded {len(df)} transaction records with {len(df.columns)} columns")
            
            # Initialize output DataFrame
            output_columns = [
                'bank', 'client', 'account', 'date', 'transaction_type', 'cusip',
                'price', 'quantity', 'amount'
            ]
            
            result_df = pd.DataFrame()
            
            # Step 1: Copy basic columns
            self.logger.info("📋 Step 1: Copying basic identification columns...")
            for col in ['bank', 'client', 'account']:
                if col in df.columns:
                    result_df[col] = df[col]
                    self.logger.info(f"  ✅ Copied {col}: {len(df[col].dropna())} non-null values")
                else:
                    self.logger.warning(f"  ⚠️ Missing column: {col}")
                    result_df[col] = None
            
            # Step 2: Date conversion 
            self.logger.info("📋 Step 2: Converting post dates...")
            if 'Post Date' in df.columns:
                result_df['date'] = df['Post Date'].apply(self._convert_date_to_standard)
                
                converted_count = result_df['date'].dropna().shape[0]
                self.logger.info(f"  ✅ Converted {converted_count} dates to MM/DD/YYYY format")
            else:
                self.logger.warning("  ⚠️ Missing Post Date column")
                result_df['date'] = None
            
            # Step 3: Transaction type
            self.logger.info("📋 Step 3: Processing transaction types...")
            if 'Description' in df.columns:
                result_df['transaction_type'] = df['Description'].apply(
                    lambda x: str(x).strip()[:56] if pd.notna(x) else None
                )
                
                non_null_count = result_df['transaction_type'].dropna().shape[0]
                self.logger.info(f"  ✅ Processed {non_null_count} transaction types")
            else:
                self.logger.warning("  ⚠️ Missing Description column")
                result_df['transaction_type'] = None
            
            # Step 4: Simple mappings
            self.logger.info("📋 Step 4: Copying transaction columns...")
            simple_mappings = {
                'cusip': 'CUSIP',
                'quantity': 'Cantidad'
            }
            
            for output_col, input_col in simple_mappings.items():
                if input_col in df.columns:
                    if output_col == 'quantity':
                        # Apply European formatting to quantity
                        result_df[output_col] = df[input_col].apply(self._format_european_number)
                    else:
                        result_df[output_col] = df[input_col]
                    
                    non_null_count = df[input_col].dropna().shape[0]
                    self.logger.info(f"  ✅ Copied {input_col} → {output_col}: {non_null_count} non-null values")
                else:
                    self.logger.warning(f"  ⚠️ Missing source column: {input_col}")
                    result_df[output_col] = None
            
            # Step 5: Set price to NaN (Valley doesn't provide transaction prices)
            self.logger.info("📋 Step 5: Setting transaction prices...")
            result_df['price'] = np.nan
            self.logger.info("  ✅ Set all transaction prices to NaN (Valley doesn't provide this data)")
            
            # Step 6: Debit/Credit consolidation using CS pattern
            self.logger.info("📋 Step 6: Consolidating Debit/Credit into amount...")
            if 'Debit' in df.columns and 'Credit' in df.columns:
                result_df['amount'] = df.apply(
                    lambda row: self._process_debit_credit(row['Debit'], row['Credit']),
                    axis=1
                )
                
                debit_count = df['Debit'].dropna().shape[0]
                credit_count = df['Credit'].dropna().shape[0]
                self.logger.info(f"  ✅ Processed {debit_count} debit transactions (negative amounts)")
                self.logger.info(f"  ✅ Processed {credit_count} credit transactions (positive amounts)")
            else:
                self.logger.warning("  ⚠️ Missing Debit or Credit columns")
                result_df['amount'] = None
            
            # Final validation and ordering
            self.logger.info("📋 Step 7: Final validation and column ordering...")
            
            # Ensure all required columns exist
            for col in output_columns:
                if col not in result_df.columns:
                    result_df[col] = None
                    self.logger.warning(f"  ⚠️ Added missing column: {col}")
            
            # Reorder columns
            result_df = result_df[output_columns]
            
            self.logger.info(f"✅ Valley transactions transformation completed successfully!")
            self.logger.info(f"📊 Output: {len(result_df)} records with {len(result_df.columns)} columns")
            
            # Show sample of transformed data
            self.logger.info("📋 Sample of transformed data:")
            for i, row in result_df.head(3).iterrows():
                transaction_type = str(row['transaction_type'])[:30] if pd.notna(row['transaction_type']) else 'N/A'
                self.logger.info(f"  Row {i}: {row['date']} | {transaction_type}... | {row['amount']}")
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"❌ Error transforming Valley transactions: {e}")
            raise

    def process_files(self, securities_file, transactions_file, output_dir):
        """Process Valley files."""
        self.logger.info("Processing Valley files")
        
        # Transform securities
        securities_df = self.transform_securities(securities_file)
        
        # Save output
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, "Valley_securities_processed.xlsx")
        securities_df.to_excel(output_file, index=False)
        
        self.logger.info(f"Saved: {output_file}")
        
        # Show final API statistics
        stats = self.openfigi_client.get_client_stats()
        self.logger.info(f"API Stats: {stats}")
        
        return output_file


if __name__ == "__main__":
    # Test the transformer
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    transformer = ValleyTransformer()
    
    # Test with actual Valley files
    securities_file = "../../data/excel/input_files/Valley_securities_05_06_2025.xlsx"
    transactions_file = "../../data/excel/input_files/Valley_transactions_05_06_2025.xlsx"
    output_dir = "../../test_output"
    
    try:
        output_file = transformer.process_files(
            securities_file, transactions_file, output_dir
        )
        print(f"\nTransformation completed successfully!")
        print(f"Securities: {output_file}")
    except Exception as e:
        print(f"Error: {e}") 