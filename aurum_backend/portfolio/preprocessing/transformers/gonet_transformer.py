#!/usr/bin/env python3
"""
Gonet Bank Data Transformer

Transforms Gonet bank data files into standardized format for AurumFinance.
Handles securities and transactions data with Gonet-specific logic.
"""

import logging
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)


class GonetTransformer:
    """Transformer for Gonet bank data files."""

    def __init__(self):
        """Initialize Gonet transformer."""
        self.bank_name = 'Gonet'
        logger.info(f"🏦 Initialized {self.bank_name} transformer")

    def _detect_asset_type(self, name: str) -> str:
        """
        Detect asset type from security name/description.

        Logic:
        - Name contains "Current account" (case-insensitive) → "Cash"
        - All other securities → "Alternatives"

        Args:
            name: Security Description/Name

        Returns:
            Asset type string
        """
        if pd.isna(name):
            return 'Alternatives'

        name_str = str(name).strip().lower()

        # Check if this is a current account (cash)
        if 'current account' in name_str:
            logger.debug(f"Asset type detection: '{name}' → Cash (current account)")
            return 'Cash'

        # Default: all other Gonet securities are alternatives
        logger.debug(f"Asset type detection: '{name}' → Alternatives")
        return 'Alternatives'

    def transform_securities(self, securities_file: str) -> pd.DataFrame:
        """
        Transform Gonet securities data to standard format.

        Args:
            securities_file: Path to combined Gonet securities Excel file

        Returns:
            DataFrame with transformed securities data
        """
        logger.info(f"🔄 Transforming Gonet securities file: {securities_file}")

        try:
            # Read the combined securities file
            df = pd.read_excel(securities_file)

            if df.empty:
                logger.warning("⚠️ Securities file is empty")
                return self._create_empty_securities_dataframe()

            logger.info(f"📊 Loaded {len(df)} securities with {len(df.columns)} columns")
            logger.info(f"📋 Available columns: {list(df.columns)}")

            # Define CUSIP sets for special handling
            BOND_CUSIPS = {
                'XS3062252864', 'XS2795014971', 'XS2857433036',
                'XS3124511828', 'XS2516373425', 'USP14008AE91'
            }
            SPECIAL_PRICE_CUSIPS = {'LU2448382197'}

            # Create result DataFrame
            result_df = pd.DataFrame()

            # STEP 1: Direct mappings
            logger.info("📋 Step 1: Direct column mappings")
            result_df['bank'] = df['bank']
            result_df['client'] = df['client']
            result_df['account'] = df['account']
            result_df['cusip'] = df['CUSIP']
            result_df['name'] = df['Security Description']
            result_df['market_value'] = df['Valuation USD']
            result_df['ticker'] = None
            result_df['coupon_rate'] = None
            result_df['maturity_date'] = None

            # STEP 1b: Detect asset types from security descriptions
            logger.info("📋 Step 1b: Detecting asset types from security descriptions")
            result_df['asset_type'] = df['Security Description'].apply(self._detect_asset_type)

            # Log asset type distribution
            asset_counts = result_df['asset_type'].value_counts()
            for asset_type, count in asset_counts.items():
                logger.info(f"  📊 {asset_type}: {count} securities")

            # STEP 2: Clean quantity (remove spaces)
            logger.info("📋 Step 2: Cleaning quantity values (removing spaces)")
            result_df['quantity'] = df['Nominal'].apply(self._clean_quantity)

            # STEP 3: Process prices based on CUSIP
            logger.info("📋 Step 3: Processing prices (bond logic and special logic)")
            prices_processed = []
            avg_prices_processed = []

            for idx, row in df.iterrows():
                cusip = row['CUSIP']

                if cusip in BOND_CUSIPS:
                    # Apply bond price logic
                    market_price = self._apply_bond_price_logic(row['Market price'])
                    avg_price = self._apply_bond_price_logic(row['Average purchase price'])
                    logger.debug(f"  Bond {cusip}: price {row['Market price']} → {market_price}")
                elif cusip in SPECIAL_PRICE_CUSIPS:
                    # Apply special price logic (divide by 1000)
                    market_price = self._apply_special_price_logic(row['Market price'])
                    avg_price = self._apply_special_price_logic(row['Average purchase price'])
                    logger.debug(f"  Special {cusip}: price {row['Market price']} → {market_price}")
                else:
                    # Transfer as-is
                    market_price = row['Market price']
                    avg_price = row['Average purchase price']

                prices_processed.append(market_price)
                avg_prices_processed.append(avg_price)

            result_df['price'] = prices_processed

            # STEP 4: Calculate cost_basis
            logger.info("📋 Step 4: Calculating cost_basis (quantity × average purchase price)")
            cost_basis_list = []
            for idx, row in result_df.iterrows():
                cost_basis = self._calculate_cost_basis(
                    row['quantity'],
                    avg_prices_processed[idx]
                )
                cost_basis_list.append(cost_basis)

            result_df['cost_basis'] = cost_basis_list

            # STEP 5: Ensure column order
            output_columns = [
                'bank', 'client', 'account', 'asset_type', 'name',
                'cost_basis', 'market_value', 'quantity', 'price',
                'ticker', 'cusip', 'coupon_rate', 'maturity_date'
            ]
            result_df = result_df[output_columns]

            logger.info(f"✅ Transformation completed: {len(result_df)} securities")
            logger.info(f"📊 Output columns: {list(result_df.columns)}")

            return result_df

        except Exception as e:
            logger.error(f"❌ Error transforming securities: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return self._create_empty_securities_dataframe()

    def transform_transactions(self, transactions_file: str) -> pd.DataFrame:
        """
        Transform Gonet transactions data to standard format.

        Args:
            transactions_file: Path to combined Gonet transactions Excel file

        Returns:
            DataFrame with transformed transactions data
        """
        logger.info(f"🔄 Transforming Gonet transactions file: {transactions_file}")

        try:
            # Read the combined transactions file
            df = pd.read_excel(transactions_file)

            if df.empty:
                logger.warning("⚠️ Transactions file is empty")
                return self._create_empty_transactions_dataframe()

            logger.info(f"📊 Loaded {len(df)} transactions with {len(df.columns)} columns")
            logger.info(f"📋 Available columns: {list(df.columns)}")

            # Create result DataFrame
            result_df = pd.DataFrame()

            # STEP 1: Direct mappings
            logger.info("📋 Step 1: Direct column mappings")
            result_df['bank'] = df['bank']
            result_df['client'] = df['client']
            result_df['account'] = df['account']
            result_df['cusip'] = df['CUSIP']
            result_df['transaction_type'] = df['Descripción']

            # STEP 2: Empty columns (no data available)
            result_df['price'] = None
            result_df['quantity'] = None

            # STEP 3: Date conversion (DD.MM.YYYY → MM/DD/YYYY)
            logger.info("📋 Step 2: Converting date format (DD.MM.YYYY → MM/DD/YYYY)")
            result_df['date'] = df['Fecha'].apply(self._convert_gonet_date)

            # STEP 4: Amount conversion (Débito + Crédito → amount)
            logger.info("📋 Step 3: Merging Débito/Crédito into amount (American+spaces → European)")
            amounts = []
            for idx, row in df.iterrows():
                amount = self._convert_amount_from_debit_credit(row['Débito'], row['Crédito'])
                amounts.append(amount)

            result_df['amount'] = amounts

            # STEP 5: Ensure column order
            output_columns = [
                'bank', 'client', 'account', 'date', 'transaction_type',
                'cusip', 'price', 'quantity', 'amount'
            ]
            result_df = result_df[output_columns]

            logger.info(f"✅ Transformation completed: {len(result_df)} transactions")
            logger.info(f"📊 Output columns: {list(result_df.columns)}")

            return result_df

        except Exception as e:
            logger.error(f"❌ Error transforming transactions: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return self._create_empty_transactions_dataframe()

    def _clean_quantity(self, quantity_value):
        """
        Remove spaces from quantity values.

        Examples:
            "250 000" → "250000"
            21000 → "21000"
            844.34 → "844.34"
        """
        if pd.isna(quantity_value):
            return None

        # Convert to string and remove all spaces
        quantity_str = str(quantity_value).replace(' ', '')

        return quantity_str

    def _apply_bond_price_logic(self, price_value):
        """
        Bond price transformation for Gonet bonds.

        Examples:
            100 → 1 or 1,000
            102,34 → 1,0234
            99,81 → 0,9981
            89,32 → 0,8932

        Algorithm:
        1. Clean: remove all commas, periods, spaces
        2. If starts with '1': place comma after first digit (10234 → 1,0234)
        3. Else: place comma before (9981 → 0,9981)
        """
        if pd.isna(price_value):
            return None

        try:
            # Clean all formatting
            clean_price = str(price_value).replace(',', '').replace('.', '').replace(' ', '')

            # Handle negative (shouldn't happen for bonds but defensive)
            is_negative = clean_price.startswith('-')
            if is_negative:
                clean_price = clean_price[1:]

            # Apply bond logic
            if clean_price.startswith('1'):
                # 10234 → 1,0234
                if len(clean_price) > 1:
                    result = f"1,{clean_price[1:]}"
                else:
                    result = "1"
            else:
                # 9981 → 0,9981
                result = f"0,{clean_price}"

            return f"-{result}" if is_negative else result

        except Exception as e:
            logger.warning(f"Could not apply bond price logic to '{price_value}': {e}")
            return None

    def _apply_special_price_logic(self, price_value):
        """
        Special transformation for LU2448382197.

        Examples:
            232,87 → 0,23287
            257,64 → 0,25764

        Algorithm:
        1. Parse as float (handle European format)
        2. Divide by 1000
        3. Format with comma as decimal separator
        """
        if pd.isna(price_value):
            return None

        try:
            # Convert to float (handle European format with comma)
            price_str = str(price_value).replace(',', '.')
            price_float = float(price_str)

            # Divide by 1000
            adjusted_price = price_float / 1000

            # Format as European decimal (5 decimal places)
            result = f"{adjusted_price:.5f}".replace('.', ',')

            return result

        except Exception as e:
            logger.warning(f"Could not apply special price logic to '{price_value}': {e}")
            return None

    def _calculate_cost_basis(self, quantity_str, avg_purchase_price_str):
        """
        Calculate cost_basis = quantity × average_purchase_price.

        Both values already have price logic applied.
        Need to convert European format to float for calculation.
        """
        if pd.isna(quantity_str) or pd.isna(avg_purchase_price_str):
            return None

        try:
            # Convert European format to float
            quantity = float(str(quantity_str).replace(',', '.'))
            avg_price = float(str(avg_purchase_price_str).replace(',', '.'))

            cost_basis = quantity * avg_price

            # Format back to European with comma (2 decimal places)
            result = f"{cost_basis:.2f}".replace('.', ',')

            return result

        except Exception as e:
            logger.warning(f"Could not calculate cost_basis for qty={quantity_str}, price={avg_purchase_price_str}: {e}")
            return None

    def _create_empty_securities_dataframe(self) -> pd.DataFrame:
        """Create empty DataFrame with standard securities columns."""
        return pd.DataFrame(columns=[
            'bank', 'client', 'account', 'asset_type', 'name',
            'cost_basis', 'market_value', 'quantity', 'price',
            'ticker', 'cusip', 'coupon_rate', 'maturity_date'
        ])

    def _convert_gonet_date(self, date_value):
        """
        Convert Gonet date format from DD.MM.YYYY to MM/DD/YYYY.

        Examples:
            03.01.2024 → 01/03/2024
            17.04.2024 → 04/17/2024
            25.12.2024 → 12/25/2024
        """
        if pd.isna(date_value):
            return None

        try:
            # Parse DD.MM.YYYY format
            date_str = str(date_value).strip()
            date_obj = datetime.strptime(date_str, '%d.%m.%Y')

            # Format to MM/DD/YYYY
            result = date_obj.strftime('%m/%d/%Y')

            return result

        except Exception as e:
            logger.warning(f"Could not convert date '{date_value}': {e}")
            return None

    def _convert_amount_from_debit_credit(self, debito_value, credito_value):
        """
        Merge Débito and Crédito columns into single amount.
        Convert American format with spaces to European format.

        Logic:
        - If Crédito exists: use Crédito (positive)
        - Else if Débito exists: use Débito (negative)
        - Remove spaces, convert to European format

        Examples:
            Crédito: "4 382.05" → "4382,05"
            Crédito: "1 141.78 1" → "1141,78" (strip extra text)
            Débito: "-6 416.94" → "-6416,94"
            Débito: "-203 200.00" → "-203200,00"
        """
        # Determine which value to use
        if pd.notna(credito_value):
            amount_value = credito_value
        elif pd.notna(debito_value):
            amount_value = debito_value
        else:
            return None

        try:
            # Convert to string
            amount_str = str(amount_value).strip()

            # Handle negative sign
            is_negative = amount_str.startswith('-')
            if is_negative:
                amount_str = amount_str[1:].strip()

            # Step 1: Remove ALL spaces (thousands separators)
            amount_str = amount_str.replace(' ', '')

            # Step 2: Remove any trailing text (like "1" in "1 141.78 1")
            # Extract only the numeric part with period/comma
            import re
            numeric_match = re.match(r'^([\d,\.]+)', amount_str)
            if numeric_match:
                amount_str = numeric_match.group(1)

            # Step 3: Convert American to European format
            # American: 7,802.30 → European: 7.802,30
            if '.' in amount_str:
                # Has decimal point
                if ',' in amount_str:
                    # Has both comma and period: "7,802.30"
                    # Replace comma with period, then final period with comma
                    parts = amount_str.split('.')
                    if len(parts) == 2:
                        integer_part = parts[0].replace(',', '.')
                        decimal_part = parts[1]
                        amount_str = f"{integer_part},{decimal_part}"
                else:
                    # Only period: "7802.30"
                    # Replace period with comma
                    amount_str = amount_str.replace('.', ',')

            # Re-add negative sign if needed
            result = f"-{amount_str}" if is_negative else amount_str

            return result

        except Exception as e:
            logger.warning(f"Could not convert amount from Débito={debito_value}, Crédito={credito_value}: {e}")
            return None

    def _create_empty_transactions_dataframe(self) -> pd.DataFrame:
        """Create empty DataFrame with standard transactions columns."""
        return pd.DataFrame(columns=[
            'bank', 'client', 'account', 'date', 'transaction_type',
            'cusip', 'price', 'quantity', 'amount'
        ])
