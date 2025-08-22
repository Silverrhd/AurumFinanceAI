import pandas as pd
import numpy as np
import logging
from datetime import datetime
import re

class JBTransformer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Asset type mappings (handling spaces)
        self.asset_type_mapping = {
            'Cash and short-term investments': 'Money market',
            'Bonds and similar positions': 'Fixed Income',
            'Equities and similar positions': 'Equity',
            'Alternative investments': 'Alternatives',
            'Alternative instruments': 'Alternatives',
            'Cash': 'Cash',
            'Other funds and investment products': 'Alternatives'
        }
        
        # Column mappings for securities
        self.securities_column_mapping = {
            'bank': 'bank',
            'client': 'client', 
            'account': 'account',
            'Asset Class': 'asset_type',
            'Instrument Name': 'name',
            'Instrument': 'cusip',
            'Quantity': 'quantity',
            'Price': 'price',
            'Maturity Date': 'maturity_date',
            'Market Value': 'market_value',
            'Net Cost Value': 'cost_basis'
        }
        
        # Column mappings for transactions
        self.transactions_column_mapping = {
            'bank': 'bank',
            'client': 'client',
            'account': 'account',
            'Accounting Date': 'date',
            'ISIN': 'cusip',
            'Operation Nature': 'transaction_type',
            'Quantity': 'quantity',
            'Price': 'price',
            'Net Amount': 'amount'
        }

    def _trim_column_names(self, df):
        """Remove leading and trailing spaces from column names"""
        df.columns = df.columns.str.strip()
        return df

    def _is_bond(self, row):
        """Check if asset is a bond based on maturity date presence"""
        maturity_date = row.get('maturity_date') or row.get('Maturity Date')
        return pd.notna(maturity_date) and str(maturity_date).strip() != ''

    def _extract_coupon_rate(self, instrument_name, is_bond):
        """Extract coupon rate from instrument name for bonds"""
        if not is_bond:
            return None
            
        if pd.isna(instrument_name) or str(instrument_name).strip() == '':
            return None
            
        name_str = str(instrument_name).strip()
        
        # Check if name starts with a number
        if not re.match(r'^\d', name_str):
            return None
            
        # Extract number until space (already in European format)
        match = re.match(r'^([0-9,]+)', name_str)
        if match:
            coupon_str = match.group(1)
            # JB coupon rates are already in European format, return as-is
            return coupon_str
        
        return None

    def _reposition_bond_price_comma(self, price_value):
        """Apply comma repositioning logic for bond prices"""
        if pd.isna(price_value):
            return None
            
        try:
            # Step 1: Remove any existing commas or periods to get clean number
            price_str = str(price_value).replace(',', '').replace('.', '')
            
            # Step 2: Check if number starts with 1 or other digit
            if price_str.startswith('1'):
                # If starts with 1, place comma after the 1: 1982 -> "1,982"
                if len(price_str) > 1:
                    repositioned = f"1,{price_str[1:]}"
                else:
                    repositioned = "1"
            else:
                # If starts with any other number, place comma before: 98866 -> "0,98866"
                repositioned = f"0,{price_str}"
                
            return repositioned
                
        except Exception as e:
            self.logger.warning(f"Could not reposition bond price '{price_value}': {e}")
            return str(price_value)

    def _convert_date_format(self, date_str, from_format):
        """Convert date from JB format to MM/DD/YYYY"""
        if pd.isna(date_str) or str(date_str).strip() == '':
            return None
            
        try:
            date_str = str(date_str).strip()
            
            if from_format == 'DD-MM-YYYY':
                # Securities maturity date format
                date_obj = datetime.strptime(date_str, '%d-%m-%Y')
            elif from_format == 'DD/MM/YYYY':
                # Transactions date format  
                date_obj = datetime.strptime(date_str, '%d/%m/%Y')
            else:
                self.logger.warning(f"Unknown date format: {from_format}")
                return date_str
                
            return date_obj.strftime('%m/%d/%Y')
            
        except Exception as e:
            self.logger.warning(f"Could not convert date '{date_str}': {e}")
            return date_str

    def _map_asset_type(self, asset_class):
        """Map JB asset class to standard asset type"""
        if pd.isna(asset_class):
            return None
            
        # Trim spaces and map
        asset_class_clean = str(asset_class).strip()
        return self.asset_type_mapping.get(asset_class_clean, asset_class_clean)

    def transform_securities(self, securities_file: str) -> pd.DataFrame:
        """Transform JB securities data to standard format"""
        self.logger.info("Starting JB securities transformation")
        
        # Load the securities file
        df = pd.read_excel(securities_file)
        self.logger.info(f"Loaded {len(df)} securities records with {len(df.columns)} columns")
        
        # Step 1: Trim column names
        df = self._trim_column_names(df)
        self.logger.info(f"Trimmed column names. Available columns: {list(df.columns)}")
        
        # Step 2: Filter and rename columns
        available_columns = [col for col in self.securities_column_mapping.keys() if col in df.columns]
        df_filtered = df[available_columns].copy()
        
        # Rename columns
        rename_mapping = {col: self.securities_column_mapping[col] for col in available_columns}
        df_renamed = df_filtered.rename(columns=rename_mapping)
        
        self.logger.info(f"Filtered to {len(available_columns)} columns")
        
        # Step 3: Add missing columns with None values
        target_columns = ['bank', 'client', 'account', 'asset_type', 'name', 'cusip', 'ticker', 
                         'quantity', 'price', 'maturity_date', 'market_value', 'cost_basis', 'coupon_rate']
        
        for col in target_columns:
            if col not in df_renamed.columns:
                df_renamed[col] = None
                
        # Step 4: Apply transformations
        self.logger.info("Applying JB-specific transformations")
        
        # Asset type mapping
        if 'asset_type' in df_renamed.columns:
            df_renamed['asset_type'] = df_renamed['asset_type'].apply(self._map_asset_type)
        
        # Date conversion for maturity_date
        if 'maturity_date' in df_renamed.columns:
            df_renamed['maturity_date'] = df_renamed['maturity_date'].apply(
                lambda x: self._convert_date_format(x, 'DD-MM-YYYY')
            )
        
        # Convert price column to object type to handle string formatting for bonds
        if 'price' in df_renamed.columns:
            df_renamed['price'] = df_renamed['price'].astype(object)
        
        # Bond detection and price/coupon processing
        for idx, row in df_renamed.iterrows():
            try:
                is_bond = self._is_bond(row)
                
                # Bond price repositioning - apply same logic as CS
                if is_bond and 'price' in df_renamed.columns and pd.notna(row['price']):
                    repositioned_price = self._reposition_bond_price_comma(row['price'])
                    df_renamed.at[idx, 'price'] = repositioned_price
                
                # Coupon rate extraction
                if 'name' in df_renamed.columns:
                    coupon_rate = self._extract_coupon_rate(row['name'], is_bond)
                    df_renamed.at[idx, 'coupon_rate'] = coupon_rate
                    
            except Exception as e:
                self.logger.error(f"Error processing row {idx}: {e}")
                self.logger.error(f"Row data: {row.to_dict()}")
                raise
        
        # Step 5: Set ticker to empty (no ticker column in JB)
        df_renamed['ticker'] = None
        
        # Step 6: Final column ordering
        df_final = df_renamed[target_columns]
        
        self.logger.info(f"JB securities transformation complete. Output shape: {df_final.shape}")
        return df_final

    def transform_transactions(self, transactions_file: str) -> pd.DataFrame:
        """Transform JB transactions data to standard format"""
        self.logger.info("Starting JB transactions transformation")
        
        # Load the transactions file
        df = pd.read_excel(transactions_file)
        self.logger.info(f"Loaded {len(df)} transaction records with {len(df.columns)} columns")
        
        # Step 1: Trim column names
        df = self._trim_column_names(df)
        self.logger.info(f"Trimmed column names. Available columns: {list(df.columns)}")
        
        # Step 2: Filter and rename columns
        available_columns = [col for col in self.transactions_column_mapping.keys() if col in df.columns] 
        df_filtered = df[available_columns].copy()
        
        # Rename columns
        rename_mapping = {col: self.transactions_column_mapping[col] for col in available_columns}
        df_renamed = df_filtered.rename(columns=rename_mapping)
        
        self.logger.info(f"Filtered to {len(available_columns)} columns")
        
        # Step 3: Add missing columns with None values
        target_columns = ['bank', 'client', 'account', 'date', 'cusip', 'transaction_type', 
                         'quantity', 'price', 'amount']
        
        for col in target_columns:
            if col not in df_renamed.columns:
                df_renamed[col] = None
                
        # Step 4: Date conversion
        if 'date' in df_renamed.columns:
            df_renamed['date'] = df_renamed['date'].apply(
                lambda x: self._convert_date_format(x, 'DD/MM/YYYY')
            )
        
        # Step 5: Final column ordering  
        df_final = df_renamed[target_columns]
        
        self.logger.info(f"JB transactions transformation complete. Output shape: {df_final.shape}")
        return df_final

    def process_files(self, securities_file, transactions_file, output_dir):
        """Process both JB securities and transactions files"""
        self.logger.info("Starting JB file processing")
        
        results = {}
        
        try:
            # Process securities
            if securities_file:
                self.logger.info(f"Processing securities file: {securities_file}")
                securities_df = pd.read_excel(securities_file, header=0)
                securities_transformed = self.transform_securities(securities_df)
                
                securities_output = f"{output_dir}/JB_securities_transformed.xlsx"
                securities_transformed.to_excel(securities_output, index=False)
                results['securities'] = securities_output
                self.logger.info(f"Securities saved to: {securities_output}")
            
            # Process transactions
            if transactions_file:
                self.logger.info(f"Processing transactions file: {transactions_file}")
                transactions_df = pd.read_excel(transactions_file, header=0)
                transactions_transformed = self.transform_transactions(transactions_df)
                
                transactions_output = f"{output_dir}/JB_transactions_transformed.xlsx"
                transactions_transformed.to_excel(transactions_output, index=False)
                results['transactions'] = transactions_output
                self.logger.info(f"Transactions saved to: {transactions_output}")
                
        except Exception as e:
            self.logger.error(f"Error processing JB files: {e}")
            raise
            
        self.logger.info("JB file processing complete")
        return results 