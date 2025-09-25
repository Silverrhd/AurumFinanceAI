#!/usr/bin/env python3
"""
HSBC Data Enricher CLI

Enriches HSBC securities files with unit cost data and adds bank/client/account columns.
This script is a CLI wrapper around the HSBCEnricher class.
"""

import argparse
import sys
import logging
from pathlib import Path

# Load environment variables from .env file
from dotenv import load_dotenv
aurum_backend_root = Path(__file__).parent.parent.parent  # Gets to aurum_backend
load_dotenv(aurum_backend_root / '.env')

# Add the project root to Python path  
sys.path.insert(0, str(aurum_backend_root))

from portfolio.preprocessing.combiners.hsbc_enricher import HSBCEnricher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def main():
    """Main CLI entry point for HSBC enrichment."""
    parser = argparse.ArgumentParser(
        description='Enrich HSBC securities files with unit cost data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 enrich_hsbc.py --date 10_07_2025 --input-dir data/excel_files/hsbc --output-dir data/excel_files
  python3 enrich_hsbc.py --date 10_07_2025 --input-dir hsbc --output-dir . --dry-run
        """
    )
    
    parser.add_argument(
        '--date', 
        required=True, 
        help='Date in DD_MM_YYYY format (e.g., 10_07_2025)'
    )
    parser.add_argument(
        '--input-dir', 
        required=True, 
        help='Input directory containing raw HSBC files'
    )
    parser.add_argument(
        '--output-dir', 
        required=True, 
        help='Output directory for enriched files'
    )
    parser.add_argument(
        '--dry-run', 
        action='store_true', 
        help='Show what would be processed without actually processing'
    )
    
    args = parser.parse_args()
    
    try:
        # Convert paths
        input_dir = Path(args.input_dir)
        output_dir = Path(args.output_dir)
        
        # Find Mappings.xlsx or Mappings.xlsx.encrypted file
        mappings_file = None
        for search_path in [input_dir.parent, input_dir, input_dir.parent.parent]:
            # Check for encrypted version first
            potential_encrypted = search_path / "Mappings.xlsx.encrypted" 
            potential_regular = search_path / "Mappings.xlsx"
            
            if potential_encrypted.exists():
                mappings_file = str(search_path / "Mappings.xlsx")  # Keep original path for compatibility
                break
            elif potential_regular.exists():
                mappings_file = str(potential_regular)
                break
        
        if not mappings_file:
            logger.error("❌ Could not find Mappings.xlsx or Mappings.xlsx.encrypted file")
            logger.error("   Searched in:")
            logger.error(f"   - {input_dir.parent / 'Mappings.xlsx'}")
            logger.error(f"   - {input_dir / 'Mappings.xlsx'}")
            logger.error(f"   - {input_dir.parent.parent / 'Mappings.xlsx'}")
            logger.error(f"   - {input_dir.parent / 'Mappings.xlsx.encrypted'}")
            logger.error(f"   - {input_dir / 'Mappings.xlsx.encrypted'}")
            logger.error(f"   - {input_dir.parent.parent / 'Mappings.xlsx.encrypted'}")
            return 1
        
        logger.info(f"🚀 Starting HSBC enrichment")
        logger.info(f"📅 Date: {args.date}")
        logger.info(f"📁 Input: {input_dir}")
        logger.info(f"📁 Output: {output_dir}")
        logger.info(f"🗂️ Mappings: {mappings_file}")
        
        if args.dry_run:
            logger.info("🧪 DRY RUN MODE - No files will be processed")
        
        # Create and run enricher
        enricher = HSBCEnricher()
        success = enricher.enrich_hsbc_files(
            input_dir=input_dir,
            output_dir=output_dir,
            date=args.date,
            mappings_file=mappings_file,
            dry_run=args.dry_run
        )
        
        if success:
            logger.info("🎉 HSBC enrichment completed successfully!")
            return 0
        else:
            logger.error("❌ HSBC enrichment failed")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Error during HSBC enrichment: {str(e)}")
        return 1


if __name__ == '__main__':
    sys.exit(main())