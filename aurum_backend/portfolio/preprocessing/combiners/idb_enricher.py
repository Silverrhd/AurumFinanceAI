"""
IDB Data Enricher

Enriches IDB securities files with OpenFIGI API data for enhanced asset type detection.
Handles IDB-specific file processing and API integration.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Optional
import os
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from preprocessing.transformers.idb_transformer import IDBTransformer

logger = logging.getLogger(__name__)


class IDBEnricher:
    """Enricher for IDB securities files with OpenFIGI API integration."""
    
    def __init__(self, api_key: str = None):
        """
        Initialize IDB enricher.
        
        Args:
            api_key: OpenFIGI API key (optional, will use environment variable)
        """
        self.api_key = api_key or os.getenv('OPENFIGI_API_KEY')
        self.transformer = IDBTransformer(api_key=self.api_key)
        logger.info("IDB enricher initialized with API integration")
    
    def enrich_idb_files(self, input_dir: Path, output_dir: Path, date: str, dry_run: bool = False) -> bool:
        """
        Enrich IDB files with OpenFIGI API data.
        
        Args:
            input_dir: Directory containing raw IDB files
            output_dir: Directory for enriched output files
            date: Date string in DD_MM_YYYY format
            dry_run: If True, show what would be processed without processing
            
        Returns:
            True if enrichment successful, False otherwise
        """
        try:
            logger.info(f"🔍 Scanning for IDB files in {input_dir}")
            
            # Find IDB files for the specified date
            securities_files = list(input_dir.glob(f"*securities*{date}*.xlsx"))
            transactions_files = list(input_dir.glob(f"*transactions*{date}*.xlsx"))
            
            if not securities_files and not transactions_files:
                logger.warning(f"⚠️ No IDB files found for date {date}")
                return True  # Not an error, just no files to process
            
            logger.info(f"📋 Found {len(securities_files)} securities files and {len(transactions_files)} transactions files")
            
            if dry_run:
                logger.info("🧪 DRY RUN - Would process:")
                for file in securities_files:
                    logger.info(f"  📄 Securities: {file.name}")
                for file in transactions_files:
                    logger.info(f"  📄 Transactions: {file.name}")
                return True
            
            # Process securities files
            enriched_securities = []
            for securities_file in securities_files:
                logger.info(f"🔄 Enriching securities file: {securities_file.name}")
                
                try:
                    # Use transformer to process and enrich the file
                    enriched_df = self.transformer.transform_securities(str(securities_file))
                    
                    if not enriched_df.empty:
                        enriched_securities.append(enriched_df)
                        logger.info(f"✅ Enriched {len(enriched_df)} securities records")
                    else:
                        logger.warning(f"⚠️ No data extracted from {securities_file.name}")
                        
                except Exception as e:
                    logger.error(f"❌ Error enriching {securities_file.name}: {str(e)}")
                    continue
            
            # Process transactions files
            enriched_transactions = []
            for transactions_file in transactions_files:
                logger.info(f"🔄 Enriching transactions file: {transactions_file.name}")
                
                try:
                    # Use transformer to process the file
                    enriched_df = self.transformer.transform_transactions(str(transactions_file))
                    
                    if not enriched_df.empty:
                        enriched_transactions.append(enriched_df)
                        logger.info(f"✅ Enriched {len(enriched_df)} transaction records")
                    else:
                        logger.warning(f"⚠️ No data extracted from {transactions_file.name}")
                        
                except Exception as e:
                    logger.error(f"❌ Error enriching {transactions_file.name}: {str(e)}")
                    continue
            
            # Save enriched files
            success = True
            
            if enriched_securities:
                combined_securities = pd.concat(enriched_securities, ignore_index=True)
                securities_output = output_dir / f"IDB_securities_{date}.xlsx"
                
                try:
                    combined_securities.to_excel(securities_output, index=False)
                    logger.info(f"💾 Saved enriched securities to: {securities_output}")
                except Exception as e:
                    logger.error(f"❌ Error saving securities file: {str(e)}")
                    success = False
            
            if enriched_transactions:
                combined_transactions = pd.concat(enriched_transactions, ignore_index=True)
                transactions_output = output_dir / f"IDB_transactions_{date}.xlsx"
                
                try:
                    combined_transactions.to_excel(transactions_output, index=False)
                    logger.info(f"💾 Saved enriched transactions to: {transactions_output}")
                except Exception as e:
                    logger.error(f"❌ Error saving transactions file: {str(e)}")
                    success = False
            
            if success:
                logger.info("🎉 IDB enrichment completed successfully")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Error during IDB enrichment: {str(e)}")
            return False
    
    def get_api_stats(self) -> Dict:
        """Get OpenFIGI API usage statistics."""
        if hasattr(self.transformer, 'openfigi_client'):
            return self.transformer.openfigi_client.get_client_stats()
        return {}