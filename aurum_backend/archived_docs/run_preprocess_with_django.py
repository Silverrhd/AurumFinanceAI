#!/usr/bin/env python3
"""
Script to run preprocessing with Django environment loaded.
This ensures that Valley and IDB transformers have access to the OpenFIGI API key.
"""

import os
import sys
import django
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Setup Django environment BEFORE importing preprocessing modules
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'aurum_backend.settings')
django.setup()

# Now import and run preprocessing
from portfolio.preprocessing.preprocess import UnifiedPreprocessor

def main():
    """Run preprocessing with Django environment."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Run bank file preprocessing with Django environment',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 run_preprocess_with_django.py --date 10_07_2025
  python3 run_preprocess_with_django.py --date 10_07_2025 --banks Valley IDB
  python3 run_preprocess_with_django.py --date 10_07_2025 --dry-run
        """
    )
    
    parser.add_argument(
        '--date',
        required=True,
        help='Date to process in DD_MM_YYYY format'
    )
    
    parser.add_argument(
        '--banks',
        nargs='*',
        help='Specific banks to process (default: all banks)'
    )
    
    parser.add_argument(
        '--input-dir',
        default='data/excel/input_files',
        help='Input directory containing bank files'
    )
    
    parser.add_argument(
        '--output-dir', 
        default='data/excel',
        help='Output directory for processed files'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be processed without actually processing'
    )
    
    args = parser.parse_args()
    
    # Convert paths to absolute
    input_dir = Path(args.input_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    
    print("🚀 Starting preprocessing with Django environment")
    print(f"📅 Date: {args.date}")
    print(f"📂 Input: {input_dir}")
    print(f"📁 Output: {output_dir}")
    
    if args.banks:
        print(f"🏦 Banks: {', '.join(args.banks)}")
    if args.dry_run:
        print("🔍 DRY RUN MODE")
    
    # Verify API key is available
    from django.conf import settings
    api_key = getattr(settings, 'OPENFIGI_API_KEY', None)
    if api_key:
        print(f"🔑 OpenFIGI API key available: {api_key[:10]}...")
    else:
        print("⚠️ OpenFIGI API key not found - Valley and IDB may fail")
    
    print("=" * 60)
    
    try:
        # Create and run preprocessor
        preprocessor = UnifiedPreprocessor()
        success = preprocessor.process_all_banks(
            input_dir=input_dir,
            output_dir=output_dir,
            target_date=args.date,
            banks_filter=args.banks,
            dry_run=args.dry_run
        )
        
        if success:
            print("🎉 Preprocessing completed successfully!")
            return 0
        else:
            print("❌ Preprocessing failed!")
            return 1
    
    except KeyboardInterrupt:
        print("🛑 Preprocessing interrupted by user")
        return 1
    except Exception as e:
        print(f"💥 Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())