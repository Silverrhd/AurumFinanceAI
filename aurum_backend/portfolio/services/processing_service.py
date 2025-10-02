"""
Processing Service for AurumFinance Portfolio Management.
Handles file preprocessing, bank detection, and routing to appropriate transformers.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from ..preprocessing.bank_detector import BankDetector
from ..preprocessing.preprocess import UnifiedPreprocessor
from .alt_combination_service import AltCombinationService

logger = logging.getLogger(__name__)


class ProcessingService:
    """
    Service for handling file processing pipeline including bank detection,
    file transformation, and routing to appropriate processors.
    """
    
    def __init__(self):
        """Initialize the processing service with preprocessor."""
        self.preprocessor = UnifiedPreprocessor()
        self.supported_banks = [
            'JPM', 'MS', 'CSC', 'Pershing', 'CS', 'JB', 
            'HSBC', 'Valley', 'Safra', 'LO', 'IDB', 'Banchile', 'ALT', 'Citi', 'Pictet'
        ]
    
    def detect_bank(self, filename: str) -> Optional[str]:
        """
        Detect bank from filename using existing bank detection logic.
        
        Args:
            filename: Name of the file to analyze
            
        Returns:
            Bank code or None if not detected
        """
        try:
            bank_code = BankDetector.detect_bank(filename)
            logger.info(f"Detected bank '{bank_code}' from filename: {filename}")
            return bank_code
        except Exception as e:
            logger.error(f"Error detecting bank from filename {filename}: {e}")
            return None
    
    def extract_date_from_filename(self, filename: str) -> Optional[str]:
        """
        Extract date from filename using existing logic.
        
        Args:
            filename: Name of the file to analyze
            
        Returns:
            Date string in DD_MM_YYYY format or None if not found
        """
        try:
            date_str = BankDetector.extract_date_from_filename(filename)
            logger.info(f"Extracted date '{date_str}' from filename: {filename}")
            return date_str
        except Exception as e:
            logger.error(f"Error extracting date from filename {filename}: {e}")
            return None
    
    def get_supported_banks(self) -> List[str]:
        """
        Get list of supported bank codes.
        
        Returns:
            List of supported bank codes
        """
        return self.supported_banks.copy()
    
    def is_bank_supported(self, bank_code: str) -> bool:
        """
        Check if a bank code is supported.
        
        Args:
            bank_code: Bank code to check
            
        Returns:
            True if bank is supported, False otherwise
        """
        return bank_code in self.supported_banks
    
    def load_transformer(self, bank_code: str):
        """
        Load transformer for specific bank using existing logic.
        
        Args:
            bank_code: Bank code to load transformer for
            
        Returns:
            Transformer instance or None if not found
        """
        try:
            if not self.is_bank_supported(bank_code):
                logger.error(f"Bank code '{bank_code}' is not supported")
                return None
                
            transformer = self.preprocessor.load_transformer(bank_code)
            logger.info(f"Loaded transformer for bank: {bank_code}")
            return transformer
        except Exception as e:
            logger.error(f"Error loading transformer for bank {bank_code}: {e}")
            return None
    
    def process_bank_files(self, bank_code: str, date: str) -> Dict[str, Any]:
        """
        Process files for a specific bank and date using existing logic.
        
        Args:
            bank_code: Bank code to process
            date: Date string in DD_MM_YYYY format
            
        Returns:
            Dictionary with processing results
        """
        try:
            if not self.is_bank_supported(bank_code):
                return {
                    'success': False,
                    'error': f"Bank code '{bank_code}' is not supported",
                    'bank_code': bank_code
                }
            
            # Load transformer for the bank
            transformer = self.load_transformer(bank_code)
            if not transformer:
                return {
                    'success': False,
                    'error': f"Could not load transformer for bank '{bank_code}'",
                    'bank_code': bank_code
                }
            
            # Process files using the transformer
            result = transformer.process_files(date)
            
            return {
                'success': True,
                'bank_code': bank_code,
                'date': date,
                'files_processed': result.get('files_processed', 0),
                'processing_time': result.get('processing_time', 0),
                'message': f"Successfully processed {bank_code} files for {date}"
            }
            
        except Exception as e:
            logger.error(f"Error processing files for bank {bank_code} on date {date}: {e}")
            return {
                'success': False,
                'error': str(e),
                'bank_code': bank_code,
                'date': date
            }
    
    def process_all_banks(self, date: str) -> Dict[str, Any]:
        """
        Process files for all supported banks on a given date.
        
        Args:
            date: Date string in DD_MM_YYYY format
            
        Returns:
            Dictionary with processing results for all banks
        """
        try:
            logger.info(f"Starting processing for all banks on date: {date}")
            
            # Use existing preprocessor logic with proper directories
            input_dir = Path("data/excel/input_files")
            output_dir = Path("data/excel")
            result = self.preprocessor.process_all_banks(input_dir, output_dir, target_date=date)
            
            return {
                'success': True,
                'date': date,
                'banks_processed': len(self.supported_banks),
                'total_files_processed': result.get('total_files_processed', 0),
                'total_processing_time': result.get('total_processing_time', 0),
                'bank_results': result.get('bank_results', {}),
                'message': f"Successfully processed all banks for {date}"
            }
            
        except Exception as e:
            logger.error(f"Error processing all banks for date {date}: {e}")
            return {
                'success': False,
                'error': str(e),
                'date': date
            }
    
    def run_complete_pipeline(self, date: str) -> Dict[str, Any]:
        """
        Run the complete preprocessing pipeline for a given date.
        This creates the standardized Excel files that can be used for database updates.
        
        Args:
            date: Date string in DD_MM_YYYY format
            
        Returns:
            Dictionary with pipeline results
        """
        try:
            logger.info(f"Running complete preprocessing pipeline for date: {date}")
            
            # Process all banks to create standardized files
            result = self.process_all_banks(date)
            
            # Step: Combine ALT files with output files if available (run regardless of process_all_banks success)
            logger.info("🔄 Checking for ALT files to combine...")
            alt_result = self._combine_alt_files(date)
            
            if result['success']:
                # Additional pipeline steps could be added here
                return {
                    'success': True,
                    'date': date,
                    'pipeline_stage': 'preprocessing_complete',
                    'files_generated': [
                        f"data/excel/securities_{date.replace('_', '_')}.xlsx",
                        f"data/excel/transactions_{date.replace('_', '_')}.xlsx"
                    ],
                    'processing_time': result.get('total_processing_time', 0),
                    'banks_processed': result.get('banks_processed', 0),
                    'alt_combination_result': alt_result,
                    'message': f"Complete preprocessing pipeline finished for {date}{' (with ALT data)' if alt_result.get('securities_combined') else ''}"
                }
            else:
                # Even if main processing failed, return success if ALT combination worked
                if alt_result.get('securities_combined') or alt_result.get('transactions_combined'):
                    return {
                        'success': True,
                        'date': date,
                        'pipeline_stage': 'preprocessing_partial',
                        'files_generated': [
                            f"data/excel/securities_{date.replace('_', '_')}.xlsx",
                            f"data/excel/transactions_{date.replace('_', '_')}.xlsx"
                        ],
                        'processing_time': 0,
                        'banks_processed': 0,
                        'alt_combination_result': alt_result,
                        'message': f"Preprocessing completed with ALT data (main processing had issues: {result.get('error', 'unknown error')})"
                    }
                else:
                    return result
                
        except Exception as e:
            logger.error(f"Error running complete pipeline for date {date}: {e}")
            return {
                'success': False,
                'error': str(e),
                'date': date,
                'pipeline_stage': 'preprocessing_failed'
            }
    
    def _combine_alt_files(self, date: str) -> Dict[str, Any]:
        """
        Combine ALT files with output files after bank processing.
        
        Args:
            date: Date string in DD_MM_YYYY format
            
        Returns:
            Dictionary with ALT combination results
        """
        logger.info(f"🔄 Starting ALT combination for {date}")
        try:
            alt_service = AltCombinationService()
            alt_result = alt_service.combine_alt_with_output_files(date)
            
            if alt_result.get('securities_combined') or alt_result.get('transactions_combined'):
                logger.info(f"✅ ALT files combined successfully for {date}")
                if alt_result.get('alt_securities_added', 0) > 0:
                    logger.info(f"📊 Added {alt_result['alt_securities_added']} ALT securities")
                if alt_result.get('alt_transactions_added', 0) > 0:
                    logger.info(f"📊 Added {alt_result['alt_transactions_added']} ALT transactions")
            else:
                logger.info(f"ℹ️ No ALT files found for {date} - continuing with regular processing")
            
            return alt_result
            
        except Exception as e:
            logger.error(f"❌ ALT combination failed for {date}: {e}")
            import traceback
            logger.error(f"ALT combination error details: {traceback.format_exc()}")
            # Return empty result so main processing continues
            return {
                'snapshot_date': date,
                'securities_combined': False,
                'transactions_combined': False,
                'alt_securities_added': 0,
                'alt_transactions_added': 0,
                'errors': [str(e)]
            }
    
    def get_bank_status(self, date: Optional[str] = None) -> Dict[str, Any]:
        """
        Get processing status for all supported banks.
        
        Args:
            date: Optional date to check status for
            
        Returns:
            Dictionary with bank status information
        """
        try:
            bank_status = {}
            
            for bank_code in self.supported_banks:
                try:
                    # Check if transformer can be loaded
                    transformer = self.load_transformer(bank_code)
                    
                    status = {
                        'bank_code': bank_code,
                        'supported': True,
                        'transformer_available': transformer is not None,
                        'status': 'ready' if transformer else 'transformer_error'
                    }
                    
                    if date:
                        # Could add date-specific status checks here
                        status['date'] = date
                        status['files_available'] = True  # Placeholder
                    
                    bank_status[bank_code] = status
                    
                except Exception as e:
                    bank_status[bank_code] = {
                        'bank_code': bank_code,
                        'supported': True,
                        'transformer_available': False,
                        'status': 'error',
                        'error': str(e)
                    }
            
            return {
                'success': True,
                'banks': bank_status,
                'total_banks': len(self.supported_banks),
                'date': date
            }
            
        except Exception as e:
            logger.error(f"Error getting bank status: {e}")
            return {
                'success': False,
                'error': str(e),
                'date': date
            }
    
    def validate_file_format(self, filename: str, file_content: bytes = None) -> Dict[str, Any]:
        """
        Validate that a file is in the correct format for processing.
        
        Args:
            filename: Name of the file to validate
            file_content: Optional file content for deeper validation
            
        Returns:
            Dictionary with validation results
        """
        try:
            # Basic filename validation
            if not filename.lower().endswith(('.xlsx', '.xls')):
                return {
                    'valid': False,
                    'error': 'File must be an Excel file (.xlsx or .xls)',
                    'filename': filename
                }
            
            # Bank detection validation
            bank_code = self.detect_bank(filename)
            if not bank_code:
                return {
                    'valid': False,
                    'error': 'Could not detect bank from filename',
                    'filename': filename
                }
            
            # Date extraction validation
            date_str = self.extract_date_from_filename(filename)
            if not date_str:
                return {
                    'valid': False,
                    'error': 'Could not extract date from filename',
                    'filename': filename,
                    'bank_code': bank_code
                }
            
            return {
                'valid': True,
                'filename': filename,
                'bank_code': bank_code,
                'date': date_str,
                'message': f'File is valid for {bank_code} processing'
            }
            
        except Exception as e:
            logger.error(f"Error validating file {filename}: {e}")
            return {
                'valid': False,
                'error': str(e),
                'filename': filename
            }