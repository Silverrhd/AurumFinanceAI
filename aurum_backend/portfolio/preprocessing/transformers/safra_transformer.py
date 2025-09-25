"""Safra Excel file transformer for preprocessing raw bank files."""

import pandas as pd
import logging
from portfolio.services.mappings_encryption_service import MappingsEncryptionService
import os
import re
from typing import Dict, Tuple, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class SafraTransformer:
    """Transformer for Safra Excel files following step-by-step approach."""
    
    def __init__(self):
        self.bank_code = 'Safra'
        logger.info(f"Initialized {self.bank_code} transformer")
    
    def get_securities_column_mappings(self) -> Dict[str, str]:
        """Get column mappings for Safra securities files (Safra → Standard)."""
        return {
            # Standard Column → Safra Column (from actual file)
            'account_number': 'AccountNumber',
            'cusip': 'Cusip',
            'name': 'Full Desc',
            'quantity': 'Quantity',
            'price': 'ClosingPrice USD',
            'maturity_date': 'Maturity',
            'coupon_rate': 'Rate',
            'market_value': 'MarketValueUSD - Settled',
            'asset_class': 'AssetClass',  # For asset type mapping
            'ticker': 'Sedol',
            'avg_cost_usd': 'Avg Cost USD'  # For cost basis calculation
        }
    
    def get_transactions_column_mappings(self) -> Dict[str, str]:
        """Get column mappings for Safra transactions files (Safra → Standard)."""
        return {
            # Standard Column → Safra Column (from actual file)
            'account_number': 'Account Number',
            'date': 'Transaction Date',
            'transaction_type': 'Description',
            'debit': 'Debit',
            'credit': 'Credit',
            'cusip': 'Cusip',
            'price': 'Price',
            'quantity': 'Quantity'
        }
    
    def load_account_mappings(self, mappings_file: str, sheet_name: str = 'Safra') -> Dict[str, Dict[str, str]]:
        """
        Load account mappings from Excel file.
        
        Args:
            mappings_file: Path to Mappings.xlsx
            sheet_name: Sheet name to read from (default: 'Safra')
            
        Returns:
            Dict mapping account numbers to client/account info
        """
        logger.info(f"Loading account mappings from {mappings_file}, sheet: {sheet_name}")
        
        try:
            encryption_service = MappingsEncryptionService()
            df = encryption_service.read_encrypted_excel(mappings_file + '.encrypted', sheet_name=sheet_name)
            
            # Validate required columns (handle case variations)
            column_mapping = {}
            for col in df.columns:
                col_lower = col.lower().strip()
                if col_lower == 'account number':
                    column_mapping['account_number'] = col
                elif col_lower == 'client':
                    column_mapping['client'] = col
                elif col_lower == 'account':
                    column_mapping['account'] = col
            
            required_fields = ['account_number', 'client', 'account']
            missing_fields = [field for field in required_fields if field not in column_mapping]
            if missing_fields:
                available_cols = list(df.columns)
                raise ValueError(f"Mappings file missing required columns. Missing: {missing_fields}, Available: {available_cols}")
            
            # Create mapping dictionary
            mappings = {}
            
            for _, row in df.iterrows():
                account_num = str(row[column_mapping['account_number']]).strip()
                client = row[column_mapping['client']]
                account = row[column_mapping['account']]
                
                # Skip rows with missing client data
                if pd.isna(client) or pd.isna(account):
                    logger.warning(f"Skipping mapping for account {account_num} - missing client or account data")
                    continue
                
                mapping_data = {
                    'client': str(client).strip(),
                    'account': str(account).strip()
                }
                
                mappings[account_num] = mapping_data
                logger.info(f"Added account mapping: {account_num} → {client}, {account}")
            
            logger.info(f"Loaded {len(mappings)} account mappings")
            return mappings
            
        except Exception as e:
            logger.error(f"Error loading account mappings: {e}")
            raise 

    def map_account_to_client(self, account_number: str, mappings: Dict[str, Dict[str, str]]) -> Tuple[Optional[str], Optional[str]]:
        """
        Map Safra account number to client and account using mappings.
        
        Args:
            account_number: Safra account number
            mappings: Account mappings dictionary
            
        Returns:
            Tuple of (client, account) or (None, None) if not found
        """
        if not account_number or pd.isna(account_number):
            return None, None
        
        account_str = str(account_number).strip()
        
        # Try exact match
        if account_str in mappings:
            mapping = mappings[account_str]
            return mapping['client'], mapping['account']
        
        logger.warning(f"No mapping found for account number: {account_str}")
        return None, None
    
    def convert_american_to_european_number(self, value) -> Optional[str]:
        """Convert American number format to European format."""
        if pd.isna(value) or value == '' or value is None:
            return None
        
        try:
            # Convert to string and clean
            value_str = str(value).strip()
            
            # Handle negative numbers
            is_negative = value_str.startswith('-')
            if is_negative:
                value_str = value_str[1:]
            
            # Remove commas and convert to float
            clean_value = value_str.replace(',', '')
            float_value = float(clean_value)
            
            # Format with European notation (comma as decimal separator)
            european_str = f"{float_value:.2f}".replace('.', ',')
            
            # Add negative sign back if needed
            if is_negative:
                european_str = f"-{european_str}"
            
            return european_str
            
        except (ValueError, TypeError):
            logger.warning(f"Could not convert '{value}' to European number format")
            return None
    
    def is_bond_asset(self, asset_class: str) -> bool:
        """
        Check if asset is a bond based on AssetClass.
        
        Args:
            asset_class: Asset class from Safra file
            
        Returns:
            True if asset is a bond, False otherwise
        """
        if pd.isna(asset_class):
            return False
        return str(asset_class).strip() == "Bonds"
    
    def convert_safra_bond_price(self, price, asset_class) -> Optional[str]:
        """
        Convert Safra bond prices with special logic for bond assets.
        
        Logic for bonds (AssetClass = "Bonds"):
        - Remove all commas and periods
        - If starts with "1": place comma after 1 (123456 → 1,23456)
        - If starts with other number: place comma before (89765 → 0,89765)
        
        For non-bonds: standard American → European conversion
        
        Args:
            price: Original price value
            asset_class: Asset class to determine if bond pricing needed
            
        Returns:
            Converted price as string with European decimal format
        """
        if pd.isna(price) or price == '' or price is None:
            return None
        
        try:
            # Convert to string and clean
            price_str = str(price).strip()
            
            # Handle negative numbers
            is_negative = price_str.startswith('-')
            if is_negative:
                price_str = price_str[1:]
            
            # Remove commas and convert to float
            clean_price = price_str.replace(',', '')
            float_price = float(clean_price)
            
            # Format with European notation (comma as decimal separator)
            if self.is_bond_asset(asset_class):
                # Special bond price handling
                logger.debug(f"Applying bond pricing logic to: {float_price}")
                if float_price > 100:
                    # Price starts with 1: place comma after 1
                    formatted_price = f"{float_price - 100:.4f}"
                    european_str = f"1,{formatted_price[2:]}"  # Skip the "0." part
                    logger.debug(f"Bond price (starts with 1): {float_price} → {european_str}")
                else:
                    # Price starts with other number: place comma before
                    formatted_price = f"{float_price:.4f}"
                    european_str = f"0,{formatted_price.replace('0.', '')}"
                    logger.debug(f"Bond price (other number): {float_price} → {european_str}")
            else:
                # Standard conversion for non-bonds
                european_str = f"{float_price:.2f}".replace('.', ',')
            
            # Add negative sign back if needed
            if is_negative:
                european_str = f"-{european_str}"
            
            return european_str
            
        except (ValueError, TypeError):
            logger.warning(f"Could not convert bond price '{price}'")
            return None
    
    def reclassify_safra_asset_type(self, asset_class: str) -> str:
        """
        Reclassify Safra asset types to standard types.
        
        Args:
            asset_class: Asset class from Safra file
            
        Returns:
            Standardized asset type
        """
        if pd.isna(asset_class):
            return "Other"
        
        asset_class_str = str(asset_class).strip()
        
        # Map Safra asset classes to standard types
        safra_mapping = {
            'DDA': 'Cash',
            'Bonds': 'Fixed Income',
            'Equities': 'Equity',
            'Other': 'Alternatives'
        }
        
        return safra_mapping.get(asset_class_str, asset_class_str)
    
    def convert_coupon_rate(self, value) -> Optional[str]:
        """
        Convert coupon rate by adding comma after first number.
        Example: 3.125 → 3,125
        """
        if pd.isna(value) or value == '' or value is None:
            logger.debug(f"Coupon rate is empty or None: {value}")
            return None
        
        try:
            # Convert to string and clean
            input_str = str(value).strip().replace('%', '').replace('.', '').replace(',', '')
            logger.debug(f"Converting coupon rate - RAW INPUT: '{value}' (type: {type(value)})")
            logger.debug(f"After cleaning: '{input_str}'")
            
            # Add comma after first number
            if len(input_str) > 0 and input_str[0].isdigit():
                # If only one digit, return it as is
                if len(input_str) == 1:
                    logger.debug(f"Single digit: '{input_str}'")
                    return input_str
                # Otherwise add comma after first digit
                result = input_str[0] + ',' + input_str[1:]
                logger.debug(f"Final result: '{result}'")
                return result
            else:
                logger.warning(f"No leading digit found in: '{input_str}'")
                return None
            
        except Exception as e:
            logger.warning(f"Error converting coupon rate '{value}': {str(e)}")
            return None
    
    def calculate_safra_cost_basis(self, avg_cost_usd, quantity, market_value, asset_class) -> Optional[str]:
        """Calculate cost basis with Safra-specific logic."""
        if pd.isna(asset_class):
            return None
        
        asset_class_str = str(asset_class).strip()
        logger.debug(f"Calculating cost basis for asset_class='{asset_class_str}'")
        
        try:
            if asset_class_str == "DDA":  # Cash assets
                # For cash: no cost basis
                logger.debug("Cash asset - skipping cost basis calculation")
                return None
            
            elif asset_class_str == "Bonds":  # Bond assets
                if pd.isna(avg_cost_usd) or pd.isna(quantity):
                    logger.debug(f"Missing data for bond cost basis: avg_cost='{avg_cost_usd}', quantity='{quantity}'")
                    return None
                
                # Log input values
                logger.debug(f"Bond cost basis inputs: avg_cost='{avg_cost_usd}', quantity='{quantity}'")
                
                # Step 1: Clean avg_cost string and convert to float
                avg_cost_str = str(avg_cost_usd).replace(',', '')
                avg_cost_float = float(avg_cost_str)
                logger.debug(f"Step 1 - Clean avg_cost: '{avg_cost_usd}' → '{avg_cost_str}' → {avg_cost_float}")
                
                # Step 2: Convert to bond price (99.89 → 0.9989)
                bond_price = avg_cost_float / 100.0
                logger.debug(f"Step 2 - Convert to bond price: {avg_cost_float} → {bond_price}")
                
                # Step 3: Clean quantity and convert to float
                quantity_str = str(quantity).replace(',', '')
                quantity_float = float(quantity_str)
                logger.debug(f"Step 3 - Clean quantity: '{quantity}' → '{quantity_str}' → {quantity_float}")
                
                # Step 4: Multiply bond price by quantity
                american_result = bond_price * quantity_float
                logger.debug(f"Step 4 - Calculate result: {bond_price} * {quantity_float} = {american_result}")
                
                # Step 5: Convert to European format
                european_result = f"{american_result:.2f}".replace('.', ',')
                logger.debug(f"Step 5 - Convert to European: {american_result:.2f} → {european_result}")
                
                # Step 6: Divide by 100 for final bond cost basis
                final_result = f"{float(european_result.replace(',', '.')) / 100:.2f}".replace('.', ',')
                logger.debug(f"Step 6 - Divide by 100 for final bond cost basis: {european_result} → {final_result}")
                
                return final_result
            
            else:  # Other assets (Equities, etc.)
                if pd.isna(avg_cost_usd) or pd.isna(quantity):
                    logger.debug(f"Missing data for standard cost basis: avg_cost='{avg_cost_usd}', quantity='{quantity}'")
                    return None
                
                # Log input values
                logger.debug(f"Standard cost basis inputs: avg_cost='{avg_cost_usd}', quantity='{quantity}'")
                
                # Step 1: Clean values and convert to float
                avg_cost_float = float(str(avg_cost_usd).replace(',', ''))
                quantity_float = float(str(quantity).replace(',', ''))
                logger.debug(f"Step 1 - Clean values: avg_cost='{avg_cost_usd}' → {avg_cost_float}, quantity='{quantity}' → {quantity_float}")
                
                # Step 2: Calculate result
                american_result = avg_cost_float * quantity_float
                logger.debug(f"Step 2 - Calculate result: {avg_cost_float} * {quantity_float} = {american_result}")
                
                # Step 3: Convert to European format
                european_result = f"{american_result:.2f}".replace('.', ',')
                logger.debug(f"Step 3 - Convert to European: {american_result:.2f} → {european_result}")
                
                return european_result
                
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not calculate cost basis for avg_cost='{avg_cost_usd}', quantity='{quantity}', asset_class='{asset_class}': {e}")
            return None
    
    def process_safra_debit_credit(self, debit_value, credit_value) -> Optional[str]:
        """
        Process Safra Debit/Credit columns into single amount column.
        
        Logic:
        - Debit: Already negative, transfer as-is → European format
        - Credit: Positive, transfer as-is → European format
        - Only one column should be populated per transaction
        
        Args:
            debit_value: Debit amount (already negative)
            credit_value: Credit amount (positive)
            
        Returns:
            Amount in European format
        """
        if pd.notna(debit_value) and pd.notna(credit_value):
            logger.warning(f"Both Debit ({debit_value}) and Credit ({credit_value}) populated - using Credit")
        
        if pd.notna(debit_value) and debit_value != '':
            # Debit already negative, just convert to European format
            try:
                european_amount = self.convert_american_to_european_number(debit_value)
                logger.debug(f"Debit processed: {debit_value} → {european_amount}")
                return european_amount
            except:
                logger.warning(f"Could not process debit value: {debit_value}")
                return None
        
        elif pd.notna(credit_value) and credit_value != '':
            # Credit positive, convert to European format
            try:
                european_amount = self.convert_american_to_european_number(credit_value)
                logger.debug(f"Credit processed: {credit_value} → {european_amount}")
                return european_amount
            except:
                logger.warning(f"Could not process credit value: {credit_value}")
                return None
        
        else:
            # Neither populated
            return None
    
    def transform_securities(self, securities_file: str, mappings_file: str) -> pd.DataFrame:
        """
        Transform Safra securities file to standard format.
        
        Args:
            securities_file: Path to Safra securities Excel file
            mappings_file: Path to account mappings file
            
        Returns:
            Transformed DataFrame in standard format
        """
        logger.info(f"🔄 Transforming Safra securities file: {securities_file}")
        
        try:
            # Read the securities file with header=1 (column titles start in row 2)
            df = pd.read_excel(
                securities_file, 
                header=1,
                dtype={'Rate': str}  # Force Rate column to be read as string
            )
            logger.info(f"📊 Loaded {len(df)} securities records with {len(df.columns)} columns")
            logger.info(f"📋 Columns: {list(df.columns)}")
            
            # Get column mappings
            column_map = self.get_securities_column_mappings()
            
            # Validate required columns exist
            required_safra_columns = list(column_map.values())
            missing_cols = [col for col in required_safra_columns if col not in df.columns]
            if missing_cols:
                logger.error(f"Safra securities file missing required columns: {missing_cols}")
                logger.info(f"Available columns: {list(df.columns)}")
                raise ValueError(f"Safra securities file missing required columns: {missing_cols}")
            
            # Step 1: Filter and rename columns
            logger.info("=== STEP 1: COLUMN FILTERING AND RENAMING ===")
            df_filtered = df[required_safra_columns].copy()
            
            # Rename columns to standard format
            reverse_map = {safra_col: std_col for std_col, safra_col in column_map.items()}
            df_renamed = df_filtered.rename(columns=reverse_map)
            logger.info(f"Renamed columns: {list(df_renamed.columns)}")
            
            # Step 2: Add system columns
            logger.info("=== STEP 2: ADD SYSTEM COLUMNS ===")
            
            # Load account mappings
            mappings = self.load_account_mappings(mappings_file, 'Safra')
            
            # Add bank column
            df_renamed['bank'] = self.bank_code
            logger.info(f"Added bank column with value: {self.bank_code}")
            
            # Map account numbers to client and account
            logger.info("Mapping account numbers to client and account...")
            client_list = []
            account_list = []
            unmapped_count = 0
            
            for _, row in df_renamed.iterrows():
                client, account = self.map_account_to_client(
                    row['account_number'], 
                    mappings
                )
                client_list.append(client)
                account_list.append(account)
                
                if client is None or account is None:
                    unmapped_count += 1
            
            df_renamed['client'] = client_list
            df_renamed['account'] = account_list
            
            logger.info(f"Mapped {len(df_renamed) - unmapped_count} accounts successfully")
            if unmapped_count > 0:
                logger.warning(f"{unmapped_count} accounts could not be mapped")
            
            # Remove rows with unmapped accounts
            df_mapped = df_renamed.dropna(subset=['client', 'account'])
            logger.info(f"After removing unmapped accounts: {len(df_mapped)} rows")
            
            # Step 3: Asset type reclassification
            logger.info("=== STEP 3: ASSET TYPE RECLASSIFICATION ===")
            df_mapped['asset_type'] = df_mapped['asset_class'].apply(self.reclassify_safra_asset_type)
            logger.info("Reclassified asset types")
            
            # Step 4: Number format conversions
            logger.info("=== STEP 4: NUMBER FORMAT CONVERSIONS ===")
            
            # Convert basic numeric columns from American to European format
            basic_numeric_columns = ['quantity', 'market_value']  # Remove coupon_rate
            for col in basic_numeric_columns:
                if col in df_mapped.columns:
                    logger.info(f"Converting {col} from American to European format")
                    df_mapped[col] = df_mapped[col].apply(self.convert_american_to_european_number)
            
            # Handle coupon rates separately - just replace period with comma
            if 'coupon_rate' in df_mapped.columns:
                logger.info("Converting coupon rates (period to comma)")
                df_mapped['coupon_rate'] = df_mapped['coupon_rate'].apply(self.convert_coupon_rate)
            
            # Step 5: Bond price handling
            logger.info("=== STEP 5: BOND PRICE HANDLING ===")
            logger.info("Converting prices with Safra bond pricing logic...")
            df_mapped['price'] = df_mapped.apply(
                lambda row: self.convert_safra_bond_price(row['price'], row['asset_class']),
                axis=1
            )
            
            # Count bond vs non-bond conversions
            bond_count = df_mapped[df_mapped['asset_class'] == 'Bonds'].shape[0]
            logger.info(f"  ✅ Processed {bond_count} bond prices with special bond handling")
            logger.info(f"  ✅ Processed {len(df_mapped) - bond_count} non-bond prices with standard conversion")
            
            # Step 6: Cost basis calculation
            logger.info("=== STEP 6: COST BASIS CALCULATION ===")
            logger.info("Calculating cost basis with Safra-specific logic...")
            df_mapped['cost_basis'] = df_mapped.apply(
                lambda row: self.calculate_safra_cost_basis(
                    row['avg_cost_usd'], 
                    row['quantity'], 
                    row['market_value'], 
                    row['asset_class']
                ),
                axis=1
            )
            
            # Count cost basis calculations by type
            cash_count = df_mapped[df_mapped['asset_class'] == 'DDA'].shape[0]
            bond_count = df_mapped[df_mapped['asset_class'] == 'Bonds'].shape[0]
            other_count = len(df_mapped) - cash_count - bond_count
            
            logger.info(f"  ✅ Calculated {cash_count} cash cost basis (market_value = cost_basis)")
            logger.info(f"  ✅ Calculated {bond_count} bond cost basis (bond price * quantity)")
            logger.info(f"  ✅ Calculated {other_count} other cost basis (avg_cost * quantity)")
            
            # Step 7: Final column setup and cleanup
            logger.info("=== STEP 7: FINAL COLUMN SETUP ===")
            
            # Remove temporary columns
            temp_cols_to_remove = ['account_number', 'asset_class', 'avg_cost_usd']
            for col in temp_cols_to_remove:
                if col in df_mapped.columns:
                    df_mapped = df_mapped.drop(columns=[col])
                    logger.info(f"Removed temporary column: {col}")
            
            # Ensure all required output columns exist
            output_columns = [
                'bank', 'client', 'account', 'asset_type', 'name', 'ticker', 'cusip',
                'quantity', 'price', 'market_value', 'cost_basis', 'maturity_date', 'coupon_rate'
            ]
            
            for col in output_columns:
                if col not in df_mapped.columns:
                    df_mapped[col] = None
                    logger.warning(f"Added missing column: {col}")
            
            # Reorder columns
            df_final = df_mapped[output_columns]
            
            logger.info(f"✅ Safra securities transformation completed successfully!")
            logger.info(f"📊 Output: {len(df_final)} records with {len(df_final.columns)} columns")
            
            # Show sample of transformed data
            logger.info("📋 Sample of transformed data:")
            for i, row in df_final.head(3).iterrows():
                logger.info(f"  Row {i}: {row['name'][:50]}... | {row['asset_type']} | {row['quantity']} | {row['price']}")
            
            return df_final
            
        except Exception as e:
            logger.error(f"❌ Error transforming Safra securities: {e}")
            raise
    
    def transform_transactions(self, transactions_file: str, mappings_file: str) -> pd.DataFrame:
        """
        Transform Safra transactions file to standard format.
        
        Args:
            transactions_file: Path to Safra transactions Excel file
            mappings_file: Path to account mappings file
            
        Returns:
            Transformed DataFrame in standard format
        """
        logger.info(f"🔄 Transforming Safra transactions file: {transactions_file}")
        
        try:
            # Read the transactions file with header=1 (column titles start in row 2)
            df = pd.read_excel(transactions_file, header=1)
            logger.info(f"📊 Loaded {len(df)} transaction records with {len(df.columns)} columns")
            logger.info(f"📋 Columns: {list(df.columns)}")
            
            # Get column mappings
            column_map = self.get_transactions_column_mappings()
            
            # Validate required columns exist
            required_safra_columns = list(column_map.values())
            missing_cols = [col for col in required_safra_columns if col not in df.columns]
            if missing_cols:
                logger.error(f"Safra transactions file missing required columns: {missing_cols}")
                logger.info(f"Available columns: {list(df.columns)}")
                raise ValueError(f"Safra transactions file missing required columns: {missing_cols}")
            
            # Step 1: Filter and rename columns
            logger.info("=== STEP 1: COLUMN FILTERING AND RENAMING ===")
            df_filtered = df[required_safra_columns].copy()
            
            # Rename columns to standard format
            reverse_map = {safra_col: std_col for std_col, safra_col in column_map.items()}
            df_renamed = df_filtered.rename(columns=reverse_map)
            logger.info(f"Renamed columns: {list(df_renamed.columns)}")
            
            # Step 2: Add system columns
            logger.info("=== STEP 2: ADD SYSTEM COLUMNS ===")
            
            # Load account mappings
            mappings = self.load_account_mappings(mappings_file, 'Safra')
            
            # Add bank column
            df_renamed['bank'] = self.bank_code
            logger.info(f"Added bank column with value: {self.bank_code}")
            
            # Map account numbers to client and account
            logger.info("Mapping account numbers to client and account...")
            client_list = []
            account_list = []
            unmapped_count = 0
            
            for _, row in df_renamed.iterrows():
                client, account = self.map_account_to_client(
                    row['account_number'], 
                    mappings
                )
                client_list.append(client)
                account_list.append(account)
                
                if client is None or account is None:
                    unmapped_count += 1
            
            df_renamed['client'] = client_list
            df_renamed['account'] = account_list
            
            logger.info(f"Mapped {len(df_renamed) - unmapped_count} accounts successfully")
            if unmapped_count > 0:
                logger.warning(f"{unmapped_count} accounts could not be mapped")
            
            # Remove rows with unmapped accounts
            df_mapped = df_renamed.dropna(subset=['client', 'account'])
            logger.info(f"After removing unmapped accounts: {len(df_mapped)} rows")
            
            # Step 3: Debit/Credit consolidation
            logger.info("=== STEP 3: DEBIT/CREDIT CONSOLIDATION ===")
            logger.info("Consolidating Debit/Credit into amount...")
            df_mapped['amount'] = df_mapped.apply(
                lambda row: self.process_safra_debit_credit(row['debit'], row['credit']),
                axis=1
            )
            
            # Count debit vs credit transactions
            debit_count = sum(1 for amt in df_mapped['amount'] if isinstance(amt, str) and amt.startswith('-'))
            credit_count = len(df_mapped) - debit_count
            
            logger.info(f"  ✅ Processed {debit_count} debit transactions (already negative)")
            logger.info(f"  ✅ Processed {credit_count} credit transactions (positive)")
            
            # Step 4: Number format conversions
            logger.info("=== STEP 4: NUMBER FORMAT CONVERSIONS ===")
            
            # Convert numeric columns from American to European format
            numeric_columns = ['price', 'quantity']
            for col in numeric_columns:
                if col in df_mapped.columns:
                    logger.info(f"Converting {col} from American to European format")
                    df_mapped[col] = df_mapped[col].apply(self.convert_american_to_european_number)
            
            # Step 5: Final column setup and cleanup
            logger.info("=== STEP 5: FINAL COLUMN SETUP ===")
            
            # Remove temporary columns
            temp_cols_to_remove = ['account_number', 'debit', 'credit']
            for col in temp_cols_to_remove:
                if col in df_mapped.columns:
                    df_mapped = df_mapped.drop(columns=[col])
                    logger.info(f"Removed temporary column: {col}")
            
            # Ensure all required output columns exist
            output_columns = [
                'bank', 'client', 'account', 'date', 'transaction_type', 'amount',
                'cusip', 'price', 'quantity'
            ]
            
            for col in output_columns:
                if col not in df_mapped.columns:
                    df_mapped[col] = None
                    logger.warning(f"Added missing column: {col}")
            
            # Reorder columns
            df_final = df_mapped[output_columns]
            
            logger.info(f"✅ Safra transactions transformation completed successfully!")
            logger.info(f"📊 Output: {len(df_final)} records with {len(df_final.columns)} columns")
            
            # Show sample of transformed data
            logger.info("📋 Sample of transformed data:")
            for i, row in df_final.head(3).iterrows():
                trans_desc = str(row['transaction_type'])[:50] + "..." if isinstance(row['transaction_type'], str) and len(str(row['transaction_type'])) > 50 else row['transaction_type']
                logger.info(f"  Row {i}: {row['date']} | {trans_desc} | {row['amount']}")
            
            return df_final
            
        except Exception as e:
            logger.error(f"❌ Error transforming Safra transactions: {e}")
            raise 