"""
Banchile Transformer

This module transforms combined Banchile files into the standard format,
handling currency conversion (CLP/UF → USD) and column mapping.

Features:
- Currency conversion using MindicadorClient
- European number format preservation
- Asset type classification
- Column mapping and standardization
"""

import logging
import pandas as pd
from typing import Dict, List, Optional, Union
from ..utils.mindicador_client import MindicadorClient

logger = logging.getLogger(__name__)


class BanchileTransformer:
    """
    Transforms combined Banchile files into standard format.
    
    Features:
    - Currency conversion (CLP/UF → USD)
    - European number format preservation
    - Asset type classification
    - Column mapping and standardization
    """
    
    # Asset type classification mappings
    ASSET_TYPE_MAPPINGS = {
        # Fixed Income
        'CFIBCHDECH': 'Fixed Income',
        'CFIBCHBLEN': 'Fixed Income',
        'CFIBCHDEGB': 'Fixed Income',
        'DEUDA_USD': 'Fixed Income',
        
        # Equity
        'CFIBCHMPGB': 'Equity',
        'BCHCAL1A': 'Equity',
        'CFIMCITI': 'Equity',
        
        # Alternatives
        'CFIBGRESTA': 'Alternatives',
        'CRECIMIENT': 'Alternatives',
        'CFIBCHINPC': 'Alternatives',
        'FIDERTARES': 'Alternatives',
        'CFIINF1A-E': 'Alternatives',
        'ESTRATEGIC': 'Alternatives',
        
        # Money Market
        'MM_CAP_EMP': 'Money Market',
        'DISPONIBLE': 'Money Market',
        'M_C_FIN_P1': 'Money Market',
        'CORPUSFUND': 'Money Market'
    }
    
    # Product type mappings
    PRODUCT_TYPE_MAPPINGS = {
        'Acciones': 'Equity',
        'Caja Extranjera': 'Cash',
        'Caja Local': 'Cash'
    }
    
    def __init__(self, mindicador_client: Optional[MindicadorClient] = None):
        """
        Initialize transformer with optional MindicadorClient.
        
        Args:
            mindicador_client: Optional MindicadorClient instance for currency conversion
        """
        self.logger = logging.getLogger(__name__)
        self.currency_client = mindicador_client or MindicadorClient()
        
        # Log initialization
        self.logger.info("🔄 Initialized Banchile transformer")
    
    def _map_asset_type(self, producto: str, instrumento: str) -> str:
        """
        Map asset type based on Producto and Instrumento fields.
        
        Args:
            producto: Product type from source
            instrumento: Instrument identifier
            
        Returns:
            Mapped asset type
        """
        # First try product type mapping
        if producto in self.PRODUCT_TYPE_MAPPINGS:
            mapped_type = self.PRODUCT_TYPE_MAPPINGS[producto]
            self.logger.debug(f"📊 Mapped product '{producto}' → '{mapped_type}'")
            return mapped_type
            
        # Then try instrument mapping
        if instrumento in self.ASSET_TYPE_MAPPINGS:
            mapped_type = self.ASSET_TYPE_MAPPINGS[instrumento]
            self.logger.debug(f"📊 Mapped instrument '{instrumento}' → '{mapped_type}'")
            return mapped_type
            
        # Default to Unknown if no mapping found
        self.logger.warning(f"⚠️ No asset type mapping for producto='{producto}', instrumento='{instrumento}'")
        return 'Unknown'
    
    def _convert_currency_value(self, value: float, currency: str) -> float:
        """
        Convert currency value to USD, maintaining numeric format.
        
        Args:
            value: Numeric value
            currency: Source currency code
            
        Returns:
            Converted value in USD as float
        """
        try:
            # Convert to float if string
            if isinstance(value, str):
                if value == '--':
                    self.logger.debug(f"Found special case '--', returning 0.0")
                    return 0.0  # Handle special case for missing values
                    
                # Remove dots and replace comma with dot
                clean_value = value.replace('.', '').replace(',', '.')
                self.logger.debug(f"Cleaned value string: '{value}' → '{clean_value}'")
                value = float(clean_value)
            
            # Convert to regular number
            regular_number = float(value)
            self.logger.debug(f"Converted to regular number: {regular_number}")
            
            self.logger.debug(f"Converting {value} {currency} to USD")
            
            # Now convert using MindicadorClient
            if currency.upper() == 'USD':
                self.logger.debug(f"Already in USD, returning as is: {regular_number}")
                return regular_number
                
            # Convert to USD
            self.logger.debug(f"About to convert {regular_number} {currency} to USD using MindicadorClient")
            usd_value = self.currency_client.convert_to_usd(regular_number, currency)
            self.logger.debug(f"Converted {regular_number} {currency} → {usd_value:.2f} USD")
            return usd_value
            
        except Exception as e:
            self.logger.error(f"❌ Currency conversion failed for {value} {currency}: {e}")
            return 0.0  # Return 0 on error
    
    def _format_to_european(self, value: float) -> str:
        """
        Format numeric value to European format string.
        
        Args:
            value: Numeric value
            
        Returns:
            String in European format (e.g. '1.234,56')
        """
        try:
            # Split into integer and decimal parts
            integer_part = int(value)
            decimal_part = int(round((value - integer_part) * 100))
            
            # Format integer part with dots for thousands
            formatted_integer = f"{integer_part:,}".replace(',', '.')
            
            # Combine with decimal part
            if decimal_part > 0:
                return f"{formatted_integer},{decimal_part:02d}"
            return formatted_integer
            
        except Exception as e:
            self.logger.error(f"❌ European formatting failed for {value}: {e}")
            return str(value)
    
    def convert_banchile_date(self, date_value) -> Optional[str]:
        """
        Convert Banchile date format (DD/MM/YYYY) to standard format (MM/DD/YYYY).
        
        Args:
            date_value: Date string in DD/MM/YYYY format
            
        Returns:
            Date string in MM/DD/YYYY format or None if invalid
        """
        if pd.isna(date_value):
            return None
            
        try:
            date_str = str(date_value).strip()
            
            # Handle DD/MM/YYYY format
            if '/' in date_str:
                day, month, year = date_str.split('/')
                return f"{month.zfill(2)}/{day.zfill(2)}/{year}"
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Could not convert date '{date_value}': {e}")
            return None
    
    def get_transactions_column_mappings(self) -> Dict[str, str]:
        """Column mappings for Banchile transactions"""
        return {
            'bank': 'bank',
            'client': 'client',
            'account': 'account',
            'date': 'Fecha de movimiento',
            'transaction_type': 'Operación',
            'cusip': 'Instrumento',
            'quantity': 'Cantidad',
            'price': 'Precio / Tasa (%)',
            'amount': 'Monto Transado (MO)',
            'currency': 'Moneda Origen (MO)'
        }
    
    def transform_securities(self, securities_file: str) -> pd.DataFrame:
        """
        Transform securities file to standard format.
        
        Args:
            securities_file: Path to securities Excel file
            
        Returns:
            Transformed dataframe
        """
        self.logger.info(f"🔄 Loading securities file: {securities_file}")
        
        # Load the Excel file
        try:
            df = pd.read_excel(securities_file)
            self.logger.info(f"📂 Loaded {len(df)} securities records from file")
        except Exception as e:
            self.logger.error(f"❌ Failed to load securities file: {e}")
            raise
        
        self.logger.info(f"🔄 Transforming {len(df)} securities records")
        
        # Create copy to avoid modifying original
        result = df.copy()
        
        try:
            # 1. Handle currency conversion for monetary fields
            self.logger.info("💱 Converting monetary values to USD")
            
            # Process each row to handle currency conversion
            for idx, row in result.iterrows():
                currency = row['Moneda Origen (MO)']
                
                # Convert cost basis
                if pd.notna(row['Monto Inicial (MO)']):
                    result.at[idx, 'cost_basis'] = self._convert_currency_value(
                        row['Monto Inicial (MO)'], 
                        currency
                    )
                
                # Convert market value
                if pd.notna(row['Monto Final (MO)']):
                    result.at[idx, 'market_value'] = self._convert_currency_value(
                        row['Monto Final (MO)'], 
                        currency
                    )
                    
                # Convert price
                if pd.notna(row['Precio / Tasa (%)']):
                    result.at[idx, 'price'] = self._convert_currency_value(
                        row['Precio / Tasa (%)'],
                        currency
                    )
            
            # 2. Map asset types
            self.logger.info("📊 Mapping asset types")
            result['asset_type'] = result.apply(
                lambda x: self._map_asset_type(x['Producto'], x['Instrumento']),
                axis=1
            )
            
            # 3. Direct mappings
            self.logger.info("🔄 Applying column mappings")
            
            # Keep these columns as is
            result['bank'] = result['bank']
            result['client'] = result['client']
            result['account'] = result['account']
            result['name'] = result['Nombre']
            result['quantity'] = result['Nominales Final']
            result['ticker'] = result['Instrumento']
            result['cusip'] = result['Instrumento']
            
            # Add empty columns
            result['coupon_rate'] = None
            result['maturity_date'] = None
            
            # 4. Format monetary values to European style
            for col in ['cost_basis', 'market_value', 'price']:
                if col in result.columns:
                    result[col] = result[col].apply(self._format_to_european)
            
            # 5. Select and order final columns
            final_columns = [
                'bank', 'client', 'account', 'asset_type', 'name',
                'cost_basis', 'market_value', 'quantity', 'price',
                'ticker', 'cusip', 'coupon_rate', 'maturity_date'
            ]
            
            result = result[final_columns]
            
            self.logger.info(f"✅ Successfully transformed {len(result)} securities records")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Securities transformation failed: {e}")
            raise
    
    def transform_transactions(self, transactions_file: str) -> pd.DataFrame:
        """
        Transform transactions file to standard format with currency conversion.
        
        Args:
            transactions_file: Path to transactions Excel file
            
        Returns:
            Transformed dataframe
        """
        self.logger.info(f"🔄 Loading transactions file: {transactions_file}")
        
        # Load the Excel file
        try:
            df = pd.read_excel(transactions_file)
            self.logger.info(f"📂 Loaded {len(df)} transaction records from file")
        except Exception as e:
            self.logger.error(f"❌ Failed to load transactions file: {e}")
            raise
        
        self.logger.info(f"🔄 Transforming {len(df)} transaction records")
        
        # Create copy to avoid modifying original
        result = df.copy()
        
        try:
            # Step 1: Apply column mappings
            self.logger.info("📋 Step 1: Applying column mappings")
            mappings = self.get_transactions_column_mappings()
            
            # Create new dataframe with mapped columns
            transformed = pd.DataFrame()
            for std_col, banchile_col in mappings.items():
                if banchile_col in df.columns:
                    transformed[std_col] = df[banchile_col]
                    self.logger.debug(f"  ✓ Mapped {banchile_col} → {std_col}")
            
            # Step 2: Convert dates
            self.logger.info("📅 Step 2: Converting dates")
            if 'date' in transformed.columns:
                transformed['date'] = transformed['date'].apply(self.convert_banchile_date)
                self.logger.debug("  ✓ Converted dates to MM/DD/YYYY format")
            
            # Step 3: Currency conversion for monetary fields
            self.logger.info("💱 Step 3: Converting monetary values to USD")
            conversion_count = 0
            
            # Create a new dataframe for converted values
            converted = transformed.copy()
            
            # Process each row for currency conversion
            for idx, row in transformed.iterrows():
                currency = row['currency']
                
                # Convert amount
                if pd.notna(row['amount']) and pd.notna(currency):
                    converted.at[idx, 'amount'] = self._convert_currency_value(
                        row['amount'],
                        currency
                    )
                    conversion_count += 1
                
                # Convert price if needed
                if pd.notna(row['price']) and pd.notna(currency):
                    converted.at[idx, 'price'] = self._convert_currency_value(
                        row['price'],
                        currency
                    )
                    conversion_count += 1
            
            self.logger.info(f"  ✓ Converted {conversion_count} monetary values to USD")
            
            # Step 4: Format numeric values to European style for display
            self.logger.info("📝 Step 4: Formatting numeric values")
            display_df = converted.copy()
            
            # Format monetary columns
            for col in ['amount', 'price']:
                if col in display_df.columns:
                    display_df[col] = display_df[col].apply(self._format_to_european)
            
            # Step 5: Remove helper columns and order final output
            final_columns = [
                'bank', 'client', 'account', 'date', 'transaction_type',
                'cusip', 'quantity', 'price', 'amount'
            ]
            
            result = display_df[final_columns].copy()
            self.logger.info(f"✅ Successfully transformed {len(result)} transaction records")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Transactions transformation failed: {e}")
            raise
    
    def transform(
        self,
        securities_file: Optional[str] = None,
        transactions_file: Optional[str] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Transform both securities and transactions dataframes.
        
        Args:
            securities_file: Optional path to securities Excel file
            transactions_file: Optional path to transactions Excel file
            
        Returns:
            Dict with transformed dataframes
        """
        result = {}
        
        if securities_file is not None:
            result['securities'] = self.transform_securities(securities_file)
            
        if transactions_file is not None:
            result['transactions'] = self.transform_transactions(transactions_file)
            
        return result


if __name__ == "__main__":
    """Test script for BanchileTransformer."""
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Test transformation
    try:
        print("🧪 Testing Banchile Transformer")
        print("=" * 50)
        
        # Load test data
        securities_file = "data/excel/input_files/Banchile_securities_05_06_2025.xlsx"
        print(f"\n📊 Loading test data from {securities_file}")
        
        df = pd.read_excel(securities_file)
        print(f"Loaded {len(df)} records")
        
        # Initialize transformer
        transformer = BanchileTransformer()
        
        # Transform securities
        print("\n🔄 Testing securities transformation")
        result = transformer.transform_securities(securities_file)
        
        # Show results
        print("\n✅ Transformation complete!")
        print(f"Input records: {len(df)}")
        print(f"Output records: {len(result)}")
        print("\nOutput columns:")
        for col in result.columns:
            print(f"  - {col}")
            
        # Show sample conversions
        print("\n💱 Sample currency conversions:")
        samples = result.head(3)
        for _, row in samples.iterrows():
            print(f"  {row['name']}: {row['market_value']} USD")
            
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        raise 