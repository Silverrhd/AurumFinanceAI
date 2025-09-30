#!/usr/bin/env python3
"""
Pictet Bank File Combiner CLI

Combines individual Pictet Bank client files into unified bank files.
This script is a CLI wrapper around the PictetCombiner class.
"""

import argparse
import sys
import logging
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from preprocessing.combiners.pictet_combiner import PictetCombiner

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def main():
    """Main CLI entry point for Pictet file combination."""
    parser = argparse.ArgumentParser(
        description='Combine individual Pictet Bank client files into unified bank files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 combine_pictet.py --date 25_09_2025 --input-dir data/excel_files/pictet --output-dir data/excel_files
  python3 combine_pictet.py --date 25_09_2025 --input-dir pictet --output-dir . --dry-run
        """
    )
    
    parser.add_argument(
        '--date', 
        required=True, 
        help='Date in DD_MM_YYYY format (e.g., 25_09_2025)'
    )
    parser.add_argument(
        '--input-dir', 
        required=True, 
        help='Input directory containing individual Pictet files'
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
    
    # Convert to Path objects
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    
    # Validate input directory
    if not input_dir.exists():
        logger.error(f"❌ Input directory does not exist: {input_dir}")
        sys.exit(1)
    
    # Create output directory if needed
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info("🏦 Starting Pictet Bank file combination")
    logger.info(f"📅 Date: {args.date}")
    logger.info(f"📁 Input directory: {input_dir.absolute()}")
    logger.info(f"📁 Output directory: {output_dir.absolute()}")
    
    if args.dry_run:
        logger.info("🧪 DRY RUN MODE - No files will be combined")
    
    # Find mappings file
    mappings_file = None
    search_paths = [
        output_dir.parent / "Mappings.xlsx",
        output_dir / "Mappings.xlsx", 
        input_dir.parent / "Mappings.xlsx",
        input_dir / "Mappings.xlsx"
    ]
    
    for search_path in search_paths:
        potential_encrypted = search_path.parent / (search_path.name + '.encrypted')
        potential_regular = search_path
        
        if potential_encrypted.exists():
            mappings_file = str(search_path)  # Keep original path for compatibility
            break
        elif potential_regular.exists():
            mappings_file = str(potential_regular)
            break
    
    if not mappings_file:
        logger.error("❌ Could not find Mappings.xlsx or Mappings.xlsx.encrypted file")
        logger.error("   Searched in:")
        for search_path in search_paths:
            logger.error(f"   - {search_path}")
            logger.error(f"   - {search_path.parent / (search_path.name + '.encrypted')}")
        sys.exit(1)
    
    logger.info(f"🗺️ Found mappings file: {mappings_file}")
    
    # Initialize combiner and run
    combiner = PictetCombiner()
    
    try:
        success = combiner.combine_all_files(
            pictet_dir=input_dir,
            output_dir=output_dir,
            date=args.date,
            mappings_file=mappings_file,
            dry_run=args.dry_run
        )
        
        if success:
            if args.dry_run:
                logger.info("✅ Dry run completed successfully")
            else:
                logger.info("✅ Pictet file combination completed successfully")
            sys.exit(0)
        else:
            logger.error("❌ Pictet file combination failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Error during Pictet file combination: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()