#!/usr/bin/env python3
"""
JB Private Bank File Combiner CLI

Combines individual JB client files into unified bank files.
This script is a CLI wrapper around the JBCombiner class.
Note: Uses --jb-dir instead of --input-dir for compatibility with preprocessing pipeline.
"""

import argparse
import sys
import logging
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from preprocessing.combiners.jb_combiner import JBCombiner

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def main():
    """Main CLI entry point for JB file combination."""
    parser = argparse.ArgumentParser(
        description='Combine individual JB Private Bank client files into unified bank files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 combine_jb.py --date 10_07_2025 --jb-dir data/excel_files/jb --output-dir data/excel_files
  python3 combine_jb.py --date 10_07_2025 --jb-dir jb --output-dir . --dry-run
        """
    )
    
    parser.add_argument(
        '--date', 
        required=True, 
        help='Date in DD_MM_YYYY format (e.g., 10_07_2025)'
    )
    parser.add_argument(
        '--jb-dir', 
        required=True, 
        help='JB directory containing individual JB files (Note: uses --jb-dir not --input-dir)'
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
        jb_dir = Path(args.jb_dir)
        output_dir = Path(args.output_dir)
        
        logger.info(f"🚀 Starting JB file combination")
        logger.info(f"📅 Date: {args.date}")
        logger.info(f"📁 JB Directory: {jb_dir}")
        logger.info(f"📁 Output: {output_dir}")
        
        if args.dry_run:
            logger.info("🧪 DRY RUN MODE - No files will be combined")
        
        # Create and run combiner
        combiner = JBCombiner()
        success = combiner.combine_all_files(
            jb_dir=jb_dir,
            output_dir=output_dir,
            date=args.date,
            dry_run=args.dry_run
        )
        
        if success:
            logger.info("🎉 JB file combination completed successfully!")
            return 0
        else:
            logger.error("❌ JB file combination failed")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Error during JB file combination: {str(e)}")
        return 1


if __name__ == '__main__':
    sys.exit(main())