#!/usr/bin/env python
"""
Weekly Portfolio Report Generator

This script generates weekly portfolio reports using only the latest data files.
It leverages the incremental data update system to:
1. Process new securities and transactions files
2. Integrate with existing historical data
3. Generate comparison reports (weekly, monthly, yearly)

Usage:
    python generate_weekly_report.py <securities_file.xlsx> <transactions_file.xlsx> [options]

Options:
    --date YYYY-MM-DD       Specify the date for this snapshot (default: extract from filename or use today)
    --output-dir DIR        Directory to save reports (default: ./reports)
    --report-type TYPE      Report type: html, pdf, simple, or all (default: all)
    --timeframe TIMEFRAME   Comparison timeframe: week, month, year (default: week)
    --disable-open          Don't automatically open reports after generation
    --client CLIENT         Process only this client's data
    --all-clients           Process all active clients (default: process all data without client separation)

Example:
    python3 generate_weekly_report.py data/portfolio-2023-07-15.xlsx data/transactions-2023-07-15.xlsx
    python3 generate_weekly_report.py data/portfolio.xlsx data/transactions.xlsx --date 2023-07-15
    python3 generate_weekly_report.py data/portfolio.xlsx data/transactions.xlsx --report-type html --timeframe month
    python3 generate_weekly_report.py data/excel/ACCOUNTNAME_DD_MM_YYYY.xlsx data/excel/ACCOUNTNAME_transactions_DD_MM_YYYY.xlsx
    python3 generate_weekly_report.py data/excel/securities_10_04_2025.xlsx data/excel/transactions_10_04_2025.xlsx --client JAV
    python3 generate_weekly_report.py data/excel/securities_10_04_2025.xlsx data/excel/transactions_10_04_2025.xlsx --all-clients
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import required modules
from . import portfolio_database as pdb
from .config_helper import get_active_clients, get_client_name
from .account_rollover import apply_missing_account_rollover

# Parser classes - these need to be created or exist in preprocessing
# For now, we'll create stub classes to prevent import errors

def ensure_dir_exists(directory):
    """Ensure the specified directory exists."""
    if not os.path.isabs(directory):
        # Convert relative path to absolute path based on script location
        script_dir = os.path.dirname(os.path.abspath(__file__))
        directory = os.path.join(script_dir, directory)
    os.makedirs(directory, exist_ok=True)
    return directory

def get_comparison_date(current_date, timeframe="week", client=None):
    """
    Get the comparison date based on the specified timeframe.
    
    Args:
        current_date (str): Current date in YYYY-MM-DD format
        timeframe (str): Comparison timeframe (week, month, year)
        client (str, optional): Client to filter snapshot dates by
    
    Returns:
        str: The most appropriate comparison date
    """
    # Convert string date to datetime
    date_obj = datetime.strptime(current_date, "%Y-%m-%d")
    
    # Calculate target date based on timeframe
    if timeframe == "week":
        target_date = date_obj - timedelta(days=7)
    elif timeframe == "month":
        # Go back approximately one month
        if date_obj.month == 1:
            target_date = date_obj.replace(year=date_obj.year - 1, month=12)
        else:
            target_date = date_obj.replace(month=date_obj.month - 1)
    elif timeframe == "year":
        target_date = date_obj.replace(year=date_obj.year - 1)
    else:
        # Default to one week
        target_date = date_obj - timedelta(days=7)
    
    target_date_str = target_date.strftime("%Y-%m-%d")
    
    # Find the closest snapshot date to the target date
    # First, get all available snapshot dates
    available_dates = pdb.get_available_dates(client)
    
    if not available_dates:
        logger.warning(f"No historical snapshots found for comparison" + 
                      (f" for client {client}" if client else ""))
        return None
    
    # Find the closest date to the target date
    closest_date = min(available_dates, key=lambda d: abs(datetime.strptime(d, "%Y-%m-%d") - target_date))
    
    logger.info(f"Selected {closest_date} as the comparison date (requested {timeframe} timeframe)" +
               (f" for client {client}" if client else ""))
    return closest_date

def process_new_data(securities_file, transactions_file, snapshot_date=None, client=None):
    """
    Process new data files and save to the database.
    
    Args:
        securities_file (str): Path to the securities Excel file
        transactions_file (str): Path to the transactions Excel file
        snapshot_date (str, optional): Date for this snapshot in YYYY-MM-DD format
        client (str, optional): Client code to filter data by
    
    Returns:
        str: The date of the saved snapshot
    """
    if client:
        logger.info(f"Processing new data files for client {client}")
        
        # Parse securities file for specific client
        securities_parser = StatementParser(securities_file)
        securities_by_client = securities_parser.parse()
        securities = securities_by_client.get(client, [])
        
        # Parse transactions file for specific client
        transactions_parser = TransactionParser(transactions_file)
        transactions_by_client = transactions_parser.parse()
        transactions = transactions_by_client.get(client, [])
        
        # ENHANCED LOGIC: Handle zero current data scenario for rollover
        if not securities and not transactions:
            logger.warning(f"No current data found for client {client} in the provided files")
            
            # Check if client is registered
            # get_active_clients is already imported at the top
            active_clients = get_active_clients()
            
            if client not in active_clients:
                logger.error(f"Client {client} is not registered in the system")
                return None
            
            # Check for previous data for rollover
            previous_date = pdb.get_snapshot_before_date(snapshot_date, client)
            if not previous_date:
                logger.info(f"No previous data found for client {client} - appears to be a new client with no data")
                return None
            
            logger.info(f"🔄 ZERO-DATA SCENARIO: Client {client} has previous data from {previous_date} - proceeding with rollover-only processing")
            # Continue with empty current data to trigger rollover logic
        
        # Save to database for this client
        from test_excel_pipeline import save_data_to_database, perform_calculations
        
        assets_data, positions_data, transactions_data = save_data_to_database(securities, transactions, client)
        
        # Apply missing account rollover logic
        assets_data, positions_data, rollover_log = apply_missing_account_rollover(
            assets_data, positions_data, client, snapshot_date
        )
        
        results = perform_calculations(assets_data, positions_data, transactions_data)
        
        # Add rollover info to results for report generation
        results['rollover_accounts'] = rollover_log
        
        # Save snapshot
        pdb.save_snapshot(
            snapshot_date=snapshot_date,
            assets_data=assets_data,
            positions_data=positions_data,
            transactions_data=transactions_data,
            portfolio_metrics=results,
            client=client
        )
        
        logger.info(f"New data processed and saved for client {client} with date: {snapshot_date}")
        return snapshot_date
    else:
        # Use the database integration function
        snapshot_date = pdb.integrate_new_data(securities_file, transactions_file, snapshot_date)
        
        logger.info(f"New data processed and saved with date: {snapshot_date}")
        return snapshot_date

def generate_reports(current_date, comparison_date, output_dir, report_types=None, client=None):
    """
    Generate reports comparing two snapshot dates.
    
    Args:
        current_date (str): Current snapshot date in YYYY-MM-DD format
        comparison_date (str): Comparison snapshot date in YYYY-MM-DD format
        output_dir (str): Directory to save the reports
        report_types (list, optional): List of report types to generate (html, pdf, simple)
        client (str, optional): Client code to filter data by
    
    Returns:
        list: Paths to the generated reports
    """
    # Check if this is the first report (comparison_date is same as current_date)
    is_first_report = comparison_date == current_date
    
    client_desc = f" for client {client}" if client else ""
    
    if is_first_report:
        logger.info(f"Generating initial portfolio report for {current_date}{client_desc} (no comparison)")
    else:
        logger.info(f"Generating reports comparing {comparison_date} to {current_date}{client_desc}")
    
    # Ensure output directory exists and convert to absolute path
    output_dir = ensure_dir_exists(output_dir)
    
    # If client is specified, create client-specific directory
    if client:
        output_dir = os.path.join(output_dir, "clients", client)
        ensure_dir_exists(output_dir)
    
    # Default to all report types if none specified
    if not report_types:
        report_types = ["html", "pdf", "simple"]
    
    # Generate reports
    generated_reports = []
    
    # Format for report filenames
    timeframe = "weekly"
    if not is_first_report and (datetime.strptime(current_date, "%Y-%m-%d") - datetime.strptime(comparison_date, "%Y-%m-%d")).days > 60:
        timeframe = "yearly" if (datetime.strptime(current_date, "%Y-%m-%d") - datetime.strptime(comparison_date, "%Y-%m-%d")).days > 300 else "monthly"
    
    # Include client in report filename if specified
    report_prefix = f"{timeframe}_report_{current_date}"
    if client:
        report_prefix = f"{client}_{report_prefix}"
    
    # Generate HTML report
    if "html" in report_types:
        try:
            from generate_html_report import generate_html_report_from_snapshots
            
            # Get the snapshots from the database
            current_snapshot = pdb.get_snapshot(current_date, client)
            comparison_snapshot = pdb.get_snapshot(comparison_date, client)
            
            # Calculate total annual income for both snapshots using stored values
            def calculate_total_annual_income(snapshot):
                total_income = 0
                if snapshot and 'positions' in snapshot:
                    for position in snapshot['positions']:
                        # Use the stored annual income value
                        position_income = position.get('annual_income', 0)
                        total_income += position_income
                return total_income
            
            # Process both snapshots
            def process_snapshot_positions(snapshot):
                if snapshot and 'positions' in snapshot:
                    for position in snapshot['positions']:
                        # Get the coupon rate directly from the position data
                        if 'coupon_rate' not in position or position['coupon_rate'] is None:
                            asset_id = position.get('asset_id')
                            if asset_id and 'assets' in snapshot:
                                matching_asset = next((asset for asset in snapshot['assets'] if asset.get('id') == asset_id), None)
                                if matching_asset and 'coupon_rate' in matching_asset:
                                    position['coupon_rate'] = matching_asset['coupon_rate']
                # Add client name if available
                if snapshot and client:
                    snapshot['client_name'] = get_client_name(client)
            
            # Process both snapshots
            process_snapshot_positions(current_snapshot)
            process_snapshot_positions(comparison_snapshot)
            
            output_file = os.path.join(output_dir, f"{report_prefix}.html")
            generate_html_report_from_snapshots(comparison_date, current_date, output_file, is_first_report, client)
            
            logger.info(f"HTML report generated: {output_file}")
            generated_reports.append(output_file)
        except Exception as e:
            logger.error(f"Error generating HTML report: {str(e)}")
    
    # Generate PDF report
    if "pdf" in report_types:
        try:
            # PDF generation not yet implemented in Django version
            # PDF report generation not currently implemented
            
            output_file = os.path.join(output_dir, f"{report_prefix}.pdf")
            generate_pdf_report_from_snapshots(comparison_date, current_date, output_file, is_first_report, client)
            
            logger.info(f"PDF report generated: {output_file}")
            generated_reports.append(output_file)
        except Exception as e:
            logger.error(f"Error generating PDF report: {str(e)}")
            import traceback
            traceback.print_exc()
    
    # Generate simple report
    if "simple" in report_types and not is_first_report:
        try:
            from generate_comparison_report import generate_comparison_report
            
            output_file = os.path.join(output_dir, f"{report_prefix}_comparison.pdf")
            generate_comparison_report(comparison_date, current_date, output_file)
            
            logger.info(f"Simple comparison report generated: {output_file}")
            generated_reports.append(output_file)
        except Exception as e:
            logger.error(f"Error generating simple report: {str(e)}")
    
    return generated_reports

def open_reports(report_files):
    """
    Open generated reports with the default application.
    
    Args:
        report_files (list): List of report file paths
    """
    logger.info("Opening generated reports")
    
    import platform
    import subprocess
    
    for report in report_files:
        try:
            if platform.system() == 'Darwin':  # macOS
                subprocess.run(['open', report], check=True)
            elif platform.system() == 'Windows':
                os.startfile(report)
            else:  # Linux
                subprocess.run(['xdg-open', report], check=True)
                
            logger.info(f"Opened report: {report}")
        except Exception as e:
            logger.error(f"Failed to open report {report}: {str(e)}")

def extract_date_from_filename(filename):
    """
    Extract date from filename.
    
    Args:
        filename (str): Filename to extract date from
    
    Returns:
        str: Date in YYYY-MM-DD format if found, None otherwise
    """
    import re
    
      # Try to match any CLIENT_DD_MM_YYYY pattern (e.g. JAV_03_04_2025, BRUMA_03_04_2025)
    client_match = re.search(r'([A-Za-z]+)_(\d{2})_(\d{2})_(\d{4})', filename)
    if client_match:
        _, day, month, year = client_match.groups()
        return f"{year}-{month}-{day}"
    
    # Try to match YYYY-MM-DD pattern
    date_match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
    if date_match:
        return date_match.group(1)
    
    # Try to match YYYYMMDD pattern
    date_match = re.search(r'(\d{4})(\d{2})(\d{2})', filename)
    if date_match:
        year, month, day = date_match.groups() 
        return f"{year}-{month}-{day}"
    
    return None

def process_all_clients(securities_file, transactions_file, snapshot_date, output_dir, report_types, timeframe, disable_open):
    """
    Process data for all active clients.
    
    Args:
        securities_file (str): Path to securities Excel file
        transactions_file (str): Path to transactions Excel file
        snapshot_date (str): Date for this snapshot
        output_dir (str): Directory to save reports
        report_types (list): List of report types to generate
        timeframe (str): Comparison timeframe
        disable_open (bool): Whether to disable automatic opening of reports
        
    Returns:
        int: 0 for success, 1 for error
    """
    logger.info("Processing all active clients")
    
    # Get list of active clients
    active_clients = get_active_clients()
    logger.info(f"Found {len(active_clients)} active clients: {', '.join(active_clients)}")
    
    all_generated_reports = []
    
    # Process each client
    for client in active_clients:
        logger.info(f"Processing client: {client}")
        
        # Process data for this client
        processed_date = process_new_data(securities_file, transactions_file, snapshot_date, client)
        
        if not processed_date:
            logger.warning(f"No data was processed for client {client}, skipping report generation")
            continue
        
        # Determine comparison date
        comparison_date = get_comparison_date(processed_date, timeframe, client)
        
        if not comparison_date:
            logger.warning(f"No comparison data available for client {client}. Using current date for comparison.")
            comparison_date = processed_date
        
        # Generate reports
        generated_reports = generate_reports(
            processed_date, 
            comparison_date, 
            output_dir, 
            report_types, 
            client
        )
        
        all_generated_reports.extend(generated_reports)
    
    # Automatically open reports unless disabled
    if all_generated_reports and not disable_open:
        open_reports(all_generated_reports)
    
    return 0

def main():
    """Main function."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Generate weekly portfolio reports')
    parser.add_argument('securities_file', help='Path to securities Excel file')
    parser.add_argument('transactions_file', help='Path to transactions Excel file')
    parser.add_argument('--date', help='Date for this snapshot (YYYY-MM-DD)')
    parser.add_argument('--output-dir', default='./reports', help='Directory to save reports')
    parser.add_argument('--report-type', choices=['html', 'pdf', 'simple', 'all'], default='all',
                        help='Type of report to generate')
    parser.add_argument('--timeframe', choices=['week', 'month', 'year'], default='week',
                        help='Comparison timeframe')
    parser.add_argument('--disable-open', action='store_true', help="Don't automatically open reports")
    parser.add_argument('--client', help='Process only this client\'s data')
    parser.add_argument('--all-clients', action='store_true', help='Process all active clients')
    
    args = parser.parse_args()
    
    # Validate input files
    for file_path in [args.securities_file, args.transactions_file]:
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return 1
    
    try:
        # Convert output directory to absolute path
        if not os.path.isabs(args.output_dir):
            args.output_dir = os.path.abspath(args.output_dir)
        
        # Create output directory if it doesn't exist
        os.makedirs(args.output_dir, exist_ok=True)
        
        # Determine the snapshot date
        snapshot_date = args.date
        
        if not snapshot_date:
            # Try to extract date from filename
            snapshot_date = extract_date_from_filename(args.securities_file)
            if not snapshot_date:
                snapshot_date = extract_date_from_filename(args.transactions_file)
            
            # If still no date, use today's date
            if not snapshot_date:
                snapshot_date = datetime.now().strftime("%Y-%m-%d")
        
        logger.info(f"Using snapshot date: {snapshot_date}")
        
        # Determine report types to generate
        report_types = ['html', 'pdf', 'simple'] if args.report_type == 'all' else [args.report_type]
        
        # Check if we should process all clients
        if args.all_clients:
            return process_all_clients(
                args.securities_file, 
                args.transactions_file, 
                snapshot_date, 
                args.output_dir, 
                report_types, 
                args.timeframe, 
                args.disable_open
            )
        
        # For single client or standard processing
        # Process new data
        snapshot_date = process_new_data(args.securities_file, args.transactions_file, snapshot_date, args.client)
        
        if not snapshot_date:
            logger.error("Failed to process data. Please check logs for details.")
            return 1
        
        # Determine the comparison date based on timeframe
        comparison_date = get_comparison_date(snapshot_date, args.timeframe, args.client)
        
        if not comparison_date:
            logger.warning("No comparison data available. Generating single-date report only.")
            comparison_date = snapshot_date
        
        # Generate reports
        generated_reports = generate_reports(
            snapshot_date, 
            comparison_date, 
            args.output_dir, 
            report_types, 
            args.client
        )
        
        # Automatically open reports unless disabled
        if generated_reports and not args.disable_open:
            open_reports(generated_reports)
        
        logger.info("Report generation completed successfully")
        print(f"\nReport generation completed successfully!")
        print(f"Reports generated in: {args.output_dir}{'/clients/' + args.client if args.client else ''}")
        
        for report in generated_reports:
            print(f"  - {os.path.basename(report)}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error generating reports: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main()) 