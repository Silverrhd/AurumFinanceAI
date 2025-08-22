"""
Report utility functions for organized file management.
"""

import os
import logging
from pathlib import Path
from django.conf import settings
from typing import Optional

logger = logging.getLogger(__name__)


def get_report_directory(client_code: Optional[str], report_type: str) -> Path:
    """
    Get the organized directory path for storing reports.
    
    Args:
        client_code: Client code (e.g., 'JAV', 'JN') or None for all clients
        report_type: Report type (e.g., 'weekly', 'equity_breakdown')
    
    Returns:
        Path: Absolute path to the report directory
        
    Example:
        get_report_directory('JAV', 'weekly') -> /reports/JAV/weekly_reports/
        get_report_directory(None, 'weekly') -> /reports/ALL_CLIENTS/weekly_reports/
    """
    base_dir = settings.AURUM_SETTINGS['REPORTS_DIR']
    client_folder = client_code if client_code else 'ALL_CLIENTS'
    report_folder = f"{report_type}_reports"
    
    full_path = base_dir / client_folder / report_folder
    full_path.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Report directory created/verified: {full_path}")
    return full_path


def get_report_filename(report_type: str, report_date: str) -> str:
    """
    Generate standardized report filename.
    
    Args:
        report_type: Report type (e.g., 'weekly', 'equity_breakdown')
        report_date: Report date in YYYY-MM-DD format
    
    Returns:
        str: Standardized filename
        
    Example:
        get_report_filename('weekly', '2025-07-10') -> 'weekly_report_2025-07-10.html'
    """
    return f"{report_type}_report_{report_date}.html"


def get_report_relative_path(client_code: Optional[str], report_type: str, report_date: str) -> str:
    """
    Get the relative path for storing in database.
    
    Args:
        client_code: Client code or None for all clients
        report_type: Report type
        report_date: Report date in YYYY-MM-DD format
    
    Returns:
        str: Relative path for database storage
        
    Example:
        get_report_relative_path('JAV', 'weekly', '2025-07-10') 
        -> 'JAV/weekly_reports/weekly_report_2025-07-10.html'
    """
    client_folder = client_code if client_code else 'ALL_CLIENTS'
    report_folder = f"{report_type}_reports"
    filename = get_report_filename(report_type, report_date)
    
    return f"{client_folder}/{report_folder}/{filename}"


def get_absolute_report_path(relative_path: str) -> Path:
    """
    Convert relative path to absolute path for file operations.
    
    Args:
        relative_path: Relative path from database
    
    Returns:
        Path: Absolute path to the report file
    """
    base_dir = settings.AURUM_SETTINGS['REPORTS_DIR']
    return base_dir / relative_path


def report_exists(client_code: Optional[str], report_type: str, report_date: str) -> bool:
    """
    Check if a report file already exists.
    
    Args:
        client_code: Client code or None for all clients
        report_type: Report type
        report_date: Report date in YYYY-MM-DD format
    
    Returns:
        bool: True if report file exists
    """
    directory = get_report_directory(client_code, report_type)
    filename = get_report_filename(report_type, report_date)
    file_path = directory / filename
    
    exists = file_path.exists()
    logger.info(f"Report existence check: {file_path} -> {exists}")
    return exists


def save_report_html(client_code: Optional[str], report_type: str, report_date: str, html_content: str) -> tuple[str, int]:
    """
    Save HTML report content to organized file structure.
    
    Args:
        client_code: Client code or None for all clients
        report_type: Report type
        report_date: Report date in YYYY-MM-DD format
        html_content: HTML content to save
    
    Returns:
        tuple: (relative_path, file_size_bytes)
    """
    # Get directory and filename
    directory = get_report_directory(client_code, report_type)
    filename = get_report_filename(report_type, report_date)
    file_path = directory / filename
    
    # Save HTML content
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    # Get file size
    file_size = file_path.stat().st_size
    
    # Return relative path for database storage
    relative_path = get_report_relative_path(client_code, report_type, report_date)
    
    logger.info(f"Report saved: {file_path} ({file_size} bytes)")
    return relative_path, file_size


def load_report_html(relative_path: str) -> str:
    """
    Load HTML content from saved report file.
    
    Args:
        relative_path: Relative path from database
    
    Returns:
        str: HTML content
        
    Raises:
        FileNotFoundError: If report file doesn't exist
    """
    file_path = get_absolute_report_path(relative_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"Report file not found: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    logger.info(f"Report loaded: {file_path}")
    return html_content