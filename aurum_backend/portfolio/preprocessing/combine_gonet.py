#!/usr/bin/env python3
"""
Gonet Bank File Combiner CLI

Combines individual Gonet Bank client files into unified bank files.
This script is a CLI wrapper around the GonetCombiner class.
"""

import argparse
import sys
import logging
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from preprocessing.combiners.gonet_combiner import GonetCombiner

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def main():
    """Main CLI entry point for Gonet file combination."""
    parser = argparse.ArgumentParser(
        description='Combine individual Gonet Bank client files into unified bank files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 combine_gonet.py --date 25_09_2025 --input-dir data/excel/input_files/gonet --output-dir data/excel/input_files
  python3 combine_gonet.py --date 25_09_2025 --input-dir gonet --output-dir . --dry-run
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
        help='Input directory containing individual Gonet files'
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

        logger.info(f"🚀 Starting Gonet file combination")
        logger.info(f"📅 Date: {args.date}")
        logger.info(f"📁 Input: {input_dir}")
        logger.info(f"📁 Output: {output_dir}")

        if args.dry_run:
            logger.info("🧪 DRY RUN MODE - No files will be combined")

        # Create and run combiner
        combiner = GonetCombiner()
        success = combiner.combine_all_files(
            gonet_dir=input_dir,
            output_dir=output_dir,
            date=args.date,
            dry_run=args.dry_run
        )

        if success:
            logger.info("🎉 Gonet file combination completed successfully!")
            return 0
        else:
            logger.error("❌ Gonet file combination failed")
            return 1

    except Exception as e:
        logger.error(f"❌ Error during Gonet file combination: {str(e)}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
