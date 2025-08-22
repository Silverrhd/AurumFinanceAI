"""
ALT Combination Service for AurumFinance.
Combines alternative asset files with final output Excel files during preprocessing.

Treats ALT as just another bank - simple file combination approach.
"""

import logging
import os
import pandas as pd
from typing import Optional

logger = logging.getLogger(__name__)


class AltCombinationService:
    """
    Service to combine ALT files with final output Excel files.
    
    ALT files have identical structure to output files - simple concatenation.
    No transformation needed, just combination at final pipeline stage.
    """
    
    def __init__(self):
        self.logger = logger
        self.input_alt_dir = 'data/excel/input_files/alternatives'
        self.output_dir = 'data/excel'
    
    def combine_alt_with_output_files(self, snapshot_date: str) -> dict:
        """
        Combine ALT files with final output Excel files.
        
        Args:
            snapshot_date: Date in format 'DD_MM_YYYY' (e.g., '29_05_2025')
            
        Returns:
            Dict with combination results and statistics
        """
        self.logger.info(f"🔄 Starting ALT files combination for {snapshot_date}")
        
        results = {
            'snapshot_date': snapshot_date,
            'securities_combined': False,
            'transactions_combined': False,
            'alt_securities_added': 0,
            'alt_transactions_added': 0,
            'total_securities_after': 0,
            'total_transactions_after': 0,
            'errors': []
        }
        
        try:
            # 1. Combine ALT securities with output securities
            securities_result = self._combine_securities(snapshot_date)
            results.update(securities_result)
            
            # 2. Combine ALT transactions with output transactions (if exists)
            transactions_result = self._combine_transactions(snapshot_date)
            results.update(transactions_result)
            
            # 3. Log final summary
            self.logger.info(f"✅ ALT combination completed for {snapshot_date}")
            self.logger.info(f"📊 Securities: +{results['alt_securities_added']} ALT positions")
            if results['transactions_combined']:
                self.logger.info(f"📊 Transactions: +{results['alt_transactions_added']} ALT transactions")
                
            return results
            
        except Exception as e:
            error_msg = f"❌ ALT combination failed for {snapshot_date}: {str(e)}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
            return results
    
    def _combine_securities(self, snapshot_date: str) -> dict:
        """Combine ALT securities with output securities file."""
        result = {
            'securities_combined': False,
            'alt_securities_added': 0,
            'total_securities_after': 0
        }
        
        # File paths
        output_securities_path = os.path.join(self.output_dir, f'securities_{snapshot_date}.xlsx')
        alt_securities_path = os.path.join(self.input_alt_dir, f'alt_securities_{snapshot_date}.xlsx')
        
        # Check if output securities file exists
        if not os.path.exists(output_securities_path):
            self.logger.warning(f"⚠️ Output securities file not found: {output_securities_path}")
            return result
        
        # Read existing output securities
        self.logger.info(f"📄 Reading output securities: {output_securities_path}")
        output_securities_df = pd.read_excel(output_securities_path)
        original_count = len(output_securities_df)
        self.logger.info(f"📊 Original securities count: {original_count}")
        
        # Check if ALT securities file exists
        if not os.path.exists(alt_securities_path):
            self.logger.info(f"ℹ️ No ALT securities file found for {snapshot_date}: {alt_securities_path}")
            result['total_securities_after'] = original_count
            return result
        
        # Read ALT securities
        self.logger.info(f"📄 Reading ALT securities: {alt_securities_path}")
        alt_securities_df = pd.read_excel(alt_securities_path)
        alt_count = len(alt_securities_df)
        self.logger.info(f"📊 ALT securities count: {alt_count}")
        
        # Validate ALT securities structure
        if not self._validate_securities_structure(alt_securities_df, output_securities_df):
            raise ValueError("ALT securities file structure doesn't match output format")
        
        # Combine dataframes
        combined_df = pd.concat([output_securities_df, alt_securities_df], ignore_index=True)
        final_count = len(combined_df)
        
        # Save combined file
        combined_df.to_excel(output_securities_path, index=False)
        self.logger.info(f"💾 Saved combined securities: {final_count} total positions")
        
        # Verify ALT bank is present
        alt_positions = combined_df[combined_df['bank'] == 'ALT']
        verified_alt_count = len(alt_positions)
        if verified_alt_count != alt_count:
            self.logger.warning(f"⚠️ ALT position count mismatch: expected {alt_count}, got {verified_alt_count}")
        
        result.update({
            'securities_combined': True,
            'alt_securities_added': alt_count,
            'total_securities_after': final_count
        })
        
        return result
    
    def _combine_transactions(self, snapshot_date: str) -> dict:
        """Combine ALT transactions with output transactions file."""
        result = {
            'transactions_combined': False,
            'alt_transactions_added': 0,
            'total_transactions_after': 0
        }
        
        # File paths
        output_transactions_path = os.path.join(self.output_dir, f'transactions_{snapshot_date}.xlsx')
        alt_transactions_path = os.path.join(self.input_alt_dir, f'alt_transactions_{snapshot_date}.xlsx')
        
        # Check if output transactions file exists
        if not os.path.exists(output_transactions_path):
            self.logger.info(f"ℹ️ No output transactions file found for {snapshot_date}")
            return result
        
        # Check if ALT transactions file exists
        if not os.path.exists(alt_transactions_path):
            self.logger.info(f"ℹ️ No ALT transactions file found for {snapshot_date}")
            # Still count existing transactions for statistics
            try:
                existing_transactions_df = pd.read_excel(output_transactions_path)
                result['total_transactions_after'] = len(existing_transactions_df)
            except Exception:
                pass
            return result
        
        # Read existing output transactions
        self.logger.info(f"📄 Reading output transactions: {output_transactions_path}")
        output_transactions_df = pd.read_excel(output_transactions_path)
        original_count = len(output_transactions_df)
        self.logger.info(f"📊 Original transactions count: {original_count}")
        
        # Read ALT transactions
        self.logger.info(f"📄 Reading ALT transactions: {alt_transactions_path}")
        alt_transactions_df = pd.read_excel(alt_transactions_path)
        alt_count = len(alt_transactions_df)
        self.logger.info(f"📊 ALT transactions count: {alt_count}")
        
        # Validate ALT transactions structure
        if not self._validate_transactions_structure(alt_transactions_df, output_transactions_df):
            raise ValueError("ALT transactions file structure doesn't match output format")
        
        # Combine dataframes
        combined_df = pd.concat([output_transactions_df, alt_transactions_df], ignore_index=True)
        final_count = len(combined_df)
        
        # Save combined file
        combined_df.to_excel(output_transactions_path, index=False)
        self.logger.info(f"💾 Saved combined transactions: {final_count} total transactions")
        
        result.update({
            'transactions_combined': True,
            'alt_transactions_added': alt_count,
            'total_transactions_after': final_count
        })
        
        return result
    
    def _validate_securities_structure(self, alt_df: pd.DataFrame, output_df: pd.DataFrame) -> bool:
        """Validate that ALT securities structure matches output format."""
        alt_columns = set(alt_df.columns)
        output_columns = set(output_df.columns)
        
        if alt_columns != output_columns:
            missing_in_alt = output_columns - alt_columns
            extra_in_alt = alt_columns - output_columns
            
            self.logger.error(f"❌ ALT securities structure mismatch:")
            if missing_in_alt:
                self.logger.error(f"  Missing columns in ALT: {missing_in_alt}")
            if extra_in_alt:
                self.logger.error(f"  Extra columns in ALT: {extra_in_alt}")
            return False
        
        # Check that ALT bank is present
        if 'bank' in alt_df.columns:
            alt_banks = alt_df['bank'].unique()
            if 'ALT' not in alt_banks:
                self.logger.warning(f"⚠️ Expected 'ALT' bank, found: {alt_banks}")
        
        self.logger.info("✅ ALT securities structure validation passed")
        return True
    
    def _validate_transactions_structure(self, alt_df: pd.DataFrame, output_df: pd.DataFrame) -> bool:
        """Validate that ALT transactions structure matches output format."""
        alt_columns = set(alt_df.columns)
        output_columns = set(output_df.columns)
        
        if alt_columns != output_columns:
            missing_in_alt = output_columns - alt_columns
            extra_in_alt = alt_columns - output_columns
            
            self.logger.error(f"❌ ALT transactions structure mismatch:")
            if missing_in_alt:
                self.logger.error(f"  Missing columns in ALT: {missing_in_alt}")
            if extra_in_alt:
                self.logger.error(f"  Extra columns in ALT: {extra_in_alt}")
            return False
        
        self.logger.info("✅ ALT transactions structure validation passed")
        return True
    
    def check_alt_files_available(self, snapshot_date: str) -> dict:
        """
        Check which ALT files are available for a given date.
        
        Args:
            snapshot_date: Date in format 'DD_MM_YYYY'
            
        Returns:
            Dict with file availability status
        """
        alt_securities_path = os.path.join(self.input_alt_dir, f'alt_securities_{snapshot_date}.xlsx')
        alt_transactions_path = os.path.join(self.input_alt_dir, f'alt_transactions_{snapshot_date}.xlsx')
        
        return {
            'snapshot_date': snapshot_date,
            'alt_securities_available': os.path.exists(alt_securities_path),
            'alt_transactions_available': os.path.exists(alt_transactions_path),
            'alt_securities_path': alt_securities_path,
            'alt_transactions_path': alt_transactions_path
        }