#!/usr/bin/env python3
"""
Pershing Data Enricher CLI

Enriches Pershing securities files with unit cost data.
This script is a CLI wrapper around the PershingEnricher class.
"""

import argparse
import sys
import logging
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from preprocessing.combiners.pershing_enricher import PershingEnricher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def main():
    """Main CLI entry point for Pershing enrichment."""
    parser = argparse.ArgumentParser(
        description='Enrich Pershing securities files with unit cost data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 enrich_pershing.py --date 10_07_2025 --input-dir pershing/nonenriched_pershing --output-dir pershing
  python3 enrich_pershing.py --date 10_07_2025 --input-dir nonenriched_pershing --output-dir . --dry-run
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
        help='Input directory containing raw Pershing files'
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
        
        logger.info(f"🚀 Starting Pershing enrichment")
        logger.info(f"📅 Date: {args.date}")
        logger.info(f"📁 Input: {input_dir}")
        logger.info(f"📁 Output: {output_dir}")
        
        if args.dry_run:
            logger.info("🧪 DRY RUN MODE - No files will be processed")
        
        # Create and run enricher
        enricher = PershingEnricher()
        success = enricher.enrich_all_clients(
            input_dir=input_dir,
            output_dir=output_dir,
            date=args.date,
            dry_run=args.dry_run
        )
        
        if success:
            logger.info("🎉 Pershing enrichment completed successfully!")
            return 0
        else:
            logger.error("❌ Pershing enrichment failed")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Error during Pershing enrichment: {str(e)}")
        return 1


if __name__ == '__main__':
    sys.exit(main())