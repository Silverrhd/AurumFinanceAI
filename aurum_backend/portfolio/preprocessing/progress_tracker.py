#!/usr/bin/env python3
"""
Progress tracking utility for bank data preprocessing.
Provides detailed box-style progress indicators with emoji status indicators.
"""

import time
from typing import Optional, Dict, Any
from tqdm import tqdm
import sys


class ProgressTracker:
    """
    Manages progress bars and status indicators for bank preprocessing operations.
    Uses detailed box-style progress indicators with emoji status indicators.
    """
    
    def __init__(self):
        self.start_time = time.time()
        self.current_operation = None
        self.stats = {
            'banks_discovered': 0,
            'files_processed': 0,
            'records_processed': 0,
            'cells_converted': 0,
            'files_saved': 0
        }
    
    def start_operation(self, operation_name: str, emoji: str = "🔄"):
        """Start a new operation with status indicator."""
        self.current_operation = operation_name
        print(f"\n{emoji} {operation_name}...")
    
    def create_progress_bar(self, total: int, desc: str, unit: str = "it") -> tqdm:
        """Create a progress bar with consistent styling."""
        return tqdm(
            total=total,
            desc=desc,
            unit=unit,
            bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
            ncols=80,
            leave=True
        )
    
    def create_bank_progress_box(self, bank_name: str, date: str, total_files: int) -> 'BankProgressBox':
        """Create a detailed box-style progress indicator for bank processing."""
        return BankProgressBox(bank_name, date, total_files, self)
    
    def update_stats(self, **kwargs):
        """Update processing statistics."""
        for key, value in kwargs.items():
            if key in self.stats:
                self.stats[key] += value
    
    def show_success(self, message: str):
        """Show success message with green checkmark."""
        print(f"✅ {message}")
    
    def show_warning(self, message: str):
        """Show warning message with yellow warning sign."""
        print(f"⚠️ {message}")
    
    def show_error(self, message: str):
        """Show error message with red X."""
        print(f"❌ {message}")
    
    def show_final_summary(self):
        """Show final processing summary."""
        elapsed_time = time.time() - self.start_time
        minutes, seconds = divmod(elapsed_time, 60)
        
        print(f"\n🎉 Processing complete! Total time: {int(minutes):02d}:{int(seconds):02d}")
        print(f"📊 Summary:")
        print(f"   • Banks processed: {self.stats['banks_discovered']}")
        print(f"   • Files processed: {self.stats['files_processed']}")
        print(f"   • Records processed: {self.stats['records_processed']:,}")
        print(f"   • Cells converted: {self.stats['cells_converted']:,}")
        print(f"   • Files saved: {self.stats['files_saved']}")


class BankProgressBox:
    """
    Detailed box-style progress indicator for individual bank processing.
    """
    
    def __init__(self, bank_name: str, date: str, total_files: int, tracker: ProgressTracker):
        self.bank_name = bank_name
        self.date = date
        self.total_files = total_files
        self.tracker = tracker
        self.files_completed = 0
        self.securities_records = 0
        self.transactions_records = 0
        self.start_time = time.time()
        
        # Print box header
        header = f" {bank_name} ({date}) "
        box_width = max(50, len(header) + 10)
        border = "─" * (box_width - 2)
        
        print(f"┌─{header}{'─' * (box_width - len(header) - 3)}┐")
        
        # Create progress bar for files
        self.file_progress = tqdm(
            total=total_files,
            desc="│ Files",
            unit="file",
            bar_format='│ Files: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}]   │',
            ncols=box_width,
            leave=False,
            file=sys.stdout
        )
    
    def update_file_progress(self, securities_count: int = 0, transactions_count: int = 0):
        """Update progress for a completed file."""
        self.files_completed += 1
        self.securities_records += securities_count
        self.transactions_records += transactions_count
        
        self.file_progress.update(1)
        
        # Update tracker stats
        self.tracker.update_stats(
            files_processed=1,
            records_processed=securities_count + transactions_count
        )
    
    def complete(self):
        """Complete the bank processing and show final status."""
        self.file_progress.close()
        
        # Show completion status
        if self.securities_records > 0:
            print(f"│ ✅ Securities: {self.securities_records:,} records{' ' * 20}│")
        if self.transactions_records > 0:
            print(f"│ ✅ Transactions: {self.transactions_records:,} records{' ' * 18}│")
        
        # Calculate processing time
        elapsed = time.time() - self.start_time
        print(f"│ ⏱️ Completed in {elapsed:.1f}s{' ' * 25}│")
        
        # Close box
        print("└" + "─" * 48 + "┘")


class ConversionProgressTracker:
    """
    Simple progress tracker for number conversion operations.
    """
    
    def __init__(self, total_columns: int, tracker: ProgressTracker):
        self.tracker = tracker
        self.total_converted = 0
        
        self.progress_bar = tqdm(
            total=total_columns,
            desc="Columns",
            unit="col",
            bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}, {rate_fmt}]',
            ncols=80,
            leave=True
        )
    
    def update(self, converted_count: int):
        """Update progress with number of cells converted."""
        self.total_converted += converted_count
        self.progress_bar.update(1)
        
        # Update tracker stats
        self.tracker.update_stats(cells_converted=converted_count)
    
    def complete(self):
        """Complete the conversion tracking."""
        self.progress_bar.close()
        if self.total_converted > 0:
            print(f"✅ Converted {self.total_converted:,} cells across all columns")


class FileProgressTracker:
    """
    Simple progress tracker for file operations.
    """
    
    def __init__(self, total_files: int, operation: str, tracker: ProgressTracker):
        self.tracker = tracker
        self.operation = operation
        
        self.progress_bar = tqdm(
            total=total_files,
            desc=operation,
            unit="file",
            bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}, {rate_fmt}]',
            ncols=80,
            leave=True
        )
    
    def update(self, filename: str, record_count: int = 0):
        """Update progress for a completed file."""
        self.progress_bar.update(1)
        
        # Update tracker stats
        if "saving" in self.operation.lower():
            self.tracker.update_stats(files_saved=1)
            print(f"✅ {filename}: {record_count:,} records")
    
    def complete(self):
        """Complete the file tracking."""
        self.progress_bar.close() 