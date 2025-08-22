#!/usr/bin/env python3
"""
Lombard File Combiner CLI

Combines enriched Lombard files into unified bank files.
This script is a CLI wrapper around the LombardCombiner class.
"""

import argparse
import sys
import logging
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from preprocessing.combiners.lombard_combiner import LombardCombiner

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def main():
    """Main CLI entry point for Lombard file combination."""
    parser = argparse.ArgumentParser(
        description='Combine enriched Lombard files into unified bank files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 combine_lombard.py --date 10_07_2025 --input-dir data/excel_files/lombard --output-dir data/excel_files
  python3 combine_lombard.py --date 10_07_2025 --input-dir lombard --output-dir . --dry-run
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
        help='Input directory containing enriched Lombard files'
    )
    parser.add_argument(
        '--output-dir', 
        required=True, 
        help='Output directory for combined files'
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
        
        # Find Mappings.xlsx file (should be in parent of output directory or output directory itself)
        mappings_file = None
        for search_path in [output_dir.parent, output_dir, input_dir.parent, input_dir]:
            potential_mappings = search_path / "Mappings.xlsx"
            if potential_mappings.exists():
                mappings_file = str(potential_mappings)
                break
        
        if not mappings_file:
            logger.error("❌ Could not find Mappings.xlsx file")
            logger.error("   Searched in:")
            logger.error(f"   - {output_dir.parent / 'Mappings.xlsx'}")
            logger.error(f"   - {output_dir / 'Mappings.xlsx'}")
            logger.error(f"   - {input_dir.parent / 'Mappings.xlsx'}")
            logger.error(f"   - {input_dir / 'Mappings.xlsx'}")
            return 1
        
        logger.info(f"🚀 Starting Lombard file combination")
        logger.info(f"📅 Date: {args.date}")
        logger.info(f"📁 Input: {input_dir}")
        logger.info(f"📁 Output: {output_dir}")
        logger.info(f"🗂️ Mappings: {mappings_file}")
        
        if args.dry_run:
            logger.info("🧪 DRY RUN MODE - No files will be combined")
        
        # Create and run combiner
        combiner = LombardCombiner()
        success = combiner.combine_all_files(
            lombard_dir=input_dir,
            output_dir=output_dir,
            date=args.date,
            mappings_file=mappings_file,
            dry_run=args.dry_run
        )
        
        if success:
            logger.info("🎉 Lombard file combination completed successfully!")
            return 0
        else:
            logger.error("❌ Lombard file combination failed")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Error during Lombard file combination: {str(e)}")
        return 1


if __name__ == '__main__':
    sys.exit(main())