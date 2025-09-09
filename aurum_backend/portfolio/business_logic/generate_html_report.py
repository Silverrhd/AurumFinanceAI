#!/usr/bin/env python

"""
Generate HTML Portfolio Report

This script generates an HTML portfolio report from the data in the securities and transactions Excel files.
"""

import os
import sys
import json
import base64
import logging
import tempfile
from datetime import datetime, timedelta
from io import BytesIO

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Use Agg backend for non-interactive environments
import matplotlib.pyplot as plt
import io
from jinja2 import Environment, FileSystemLoader
import matplotlib.ticker as ticker
import seaborn as sns

from .config_helper import get_client_name
from . import portfolio_database as pdb
from .calculation_helpers import (
    normalize_asset_type,
    calculate_bond_maturity_timeline,
    ModifiedDietzCalculator,
    CashFlowClassifier,
    calculate_safe_unrealized_gain_loss,
    calculate_safe_gain_loss_percentage,
    InvestmentCashFlowCalculator
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Reuse the same functions from simplified_report.py
def parse_securities_file(file_path):
    """Parse the securities Excel file."""
    logger.info(f"Parsing securities file: {file_path}")
    try:
        # StatementParser functionality will be handled by preprocessing transformers
        # For now, we'll create a simple parser stub
        parser = StatementParser(file_path)
        securities = parser.parse()
        logger.info(f"Successfully parsed {len(securities)} securities")
        return securities
    except Exception as e:
        logger.error(f"Error parsing securities file: {e}")
        raise

def parse_transactions_file(file_path):
    """Parse the transactions Excel file."""
    logger.info(f"Parsing transactions file: {file_path}")
    try:
        df = pd.read_excel(file_path)
        logger.info(f"Successfully parsed {len(df)} transactions")
        return df.to_dict('records')
    except Exception as e:
        logger.error(f"Error parsing transactions file: {e}")
        raise

def create_asset_allocation_pie_chart(summary_data):
    """
    Create a pie chart for asset allocation.
    
    Args:
        summary_data (dict): Summary data containing asset allocation information
        
    Returns:
        str: Base64-encoded PNG image data
    """
    # Initialize matplotlib with non-interactive backend for server environments
    import matplotlib
    matplotlib.use('Agg')  # Use Agg backend for non-interactive environments
    import matplotlib.pyplot as plt
    import io
    import base64
    
    logger.info("Creating asset allocation pie chart")
    
    # Extract asset allocation data
    asset_allocation = summary_data.get('asset_allocation', {})
    if not asset_allocation and 'portfolio_metrics' in summary_data:
        asset_allocation = summary_data['portfolio_metrics'].get('asset_allocation', {})
    
    # Prepare data for plotting
    labels = []
    sizes = []
    colors = ['#5f76a1', '#072061', '#b7babe', '#dae1f3']
    
    # Sort by market value
    sorted_allocation = sorted(
        asset_allocation.items(), 
        key=lambda x: x[1].get('market_value', 0), 
        reverse=True
    )
    
    for asset_type, data in sorted_allocation:
        if data.get('market_value', 0) > 0:  # Only include non-zero values
            labels.append(asset_type)
            sizes.append(data.get('market_value', 0))
    
    # Create figure
    plt.figure(figsize=(8, 6))
    plt.pie(
        sizes, 
        labels=labels, 
        colors=colors[:len(labels)], 
        autopct='%1.1f%%',
        startangle=90,
        shadow=False,
    )
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
    plt.title('Asset Allocation', pad=20, fontsize=16)
    
    # Convert plot to PNG image
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    plt.close()
    buf.seek(0)
    
    # Encode PNG image to base64 string
    image_base64 = base64.b64encode(buf.getvalue()).decode('utf-8')
    buf.close()
    
    return image_base64

def create_asset_allocation_polar_chart_data(summary_data):
    """
    Create JSON data for ApexCharts polar area chart for asset allocation.
    
    Args:
        summary_data (dict): Summary data containing asset allocation information
        
    Returns:
        dict: JSON data structure for ApexCharts polar area chart
    """
    logger.info("Creating asset allocation polar chart data")
    
    # Extract asset allocation data
    asset_allocation = summary_data.get('asset_allocation', {})
    if not asset_allocation and 'portfolio_metrics' in summary_data:
        asset_allocation = summary_data['portfolio_metrics'].get('asset_allocation', {})
    
    # Check if we have data
    if not asset_allocation:
        return {
            'hasData': False,
            'message': 'No asset allocation data available'
        }
    
    # Prepare data for polar chart
    labels = []
    series = []  # Will contain percentage values
    monetary_values = []  # Will contain dollar amounts for tooltips
    total_value = 0
    
    # Sort by market value (largest to smallest)
    sorted_allocation = sorted(
        asset_allocation.items(), 
        key=lambda x: x[1].get('market_value', 0), 
        reverse=True
    )
    
    # First pass: calculate total value
    for asset_type, data in sorted_allocation:
        market_value = data.get('market_value', 0)
        if market_value > 0:
            total_value += market_value
    
    # Second pass: calculate percentages and collect data
    for asset_type, data in sorted_allocation:
        market_value = data.get('market_value', 0)
        if market_value > 0:  # Only include non-zero values
            percentage = (market_value / total_value) * 100 if total_value > 0 else 0
            labels.append(asset_type)
            series.append(percentage)  # Use percentage for chart display
            monetary_values.append(market_value)  # Keep monetary values for tooltips
    
    # If no valid data
    if not series:
        return {
            'hasData': False,
            'message': 'No asset allocation data with positive values'
        }
    
    return {
        'series': series,  # Percentage values for chart display
        'labels': labels,
        'monetaryValues': monetary_values,  # Dollar amounts for tooltips
        'colors': ['#5f76a1', '#072061', '#b7babe', '#dae1f3'],
        'totalValue': total_value,
        'hasData': True
    }

def create_custody_allocation_pie_chart(custody_allocation):
    """
    Create a pie chart for custody allocation.
    
    Args:
        custody_allocation (dict): Dictionary containing custody allocation information
        
    Returns:
        str: Base64-encoded PNG image data
    """
    # Initialize matplotlib with non-interactive backend for server environments
    import matplotlib
    matplotlib.use('Agg')  # Use Agg backend for non-interactive environments
    import matplotlib.pyplot as plt
    import io
    import base64
    
    logger.info("Creating custody allocation pie chart")
    
    # Prepare data for plotting
    labels = []
    sizes = []
    colors = ['#5f76a1', '#072061', '#b7babe', '#dae1f3']
    
    # Sort by market value
    sorted_allocation = sorted(
        custody_allocation.items(), 
        key=lambda x: x[1].get('market_value', 0), 
        reverse=True
    )
    
    for custody, data in sorted_allocation:
        if data.get('market_value', 0) > 0:  # Only include non-zero values
            labels.append(custody)
            sizes.append(data.get('market_value', 0))
    
    # Create figure
    plt.figure(figsize=(8, 6))
    plt.pie(
        sizes, 
        labels=labels, 
        colors=colors[:len(labels)], 
        autopct='%1.1f%%',
        startangle=90,
        shadow=False,
    )
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
    plt.title('Custody Allocation', pad=20, fontsize=16)
    
    # Convert plot to PNG image
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    plt.close()
    buf.seek(0)
    
    # Encode PNG image to base64 string
    image_base64 = base64.b64encode(buf.getvalue()).decode('utf-8')
    buf.close()
    
    return image_base64

def create_custody_allocation_polar_chart_data(custody_allocation):
    """
    Create JSON data for ApexCharts polar area chart for custody allocation.
    
    Args:
        custody_allocation (dict): Dictionary containing custody allocation information
        
    Returns:
        dict: JSON data structure for ApexCharts polar area chart
    """
    logger.info("Creating custody allocation polar chart data")
    
    # Check if we have data
    if not custody_allocation:
        return {
            'hasData': False,
            'message': 'No custody allocation data available'
        }
    
    # Prepare data for polar chart
    labels = []
    series = []  # Will contain percentage values
    monetary_values = []  # Will contain dollar amounts for tooltips
    total_value = 0
    
    # Sort by market value (largest to smallest)
    sorted_allocation = sorted(
        custody_allocation.items(), 
        key=lambda x: x[1].get('market_value', 0), 
        reverse=True
    )
    
    # First pass: calculate total value
    for custody, data in sorted_allocation:
        market_value = data.get('market_value', 0)
        if market_value > 0:
            total_value += market_value
    
    # Second pass: calculate percentages and collect data
    for custody, data in sorted_allocation:
        market_value = data.get('market_value', 0)
        if market_value > 0:  # Only include non-zero values
            percentage = (market_value / total_value) * 100 if total_value > 0 else 0
            labels.append(custody)
            series.append(percentage)  # Use percentage for chart display
            monetary_values.append(market_value)  # Keep monetary values for tooltips
    
    # If no valid data
    if not series:
        return {
            'hasData': False,
            'message': 'No custody allocation data with positive values'
        }
    
    return {
        'series': series,  # Percentage values for chart display
        'labels': labels,
        'monetaryValues': monetary_values,  # Dollar amounts for tooltips
        'colors': ['#5f76a1', '#072061', '#b7babe', '#dae1f3'],
        'totalValue': total_value,
        'hasData': True
    }

def create_portfolio_comparison_chart(week1_summary, week2_summary):
    """
    Create a vertical bar chart comparing key metrics between two weeks.
    
    Args:
        week1_summary (dict): Summary data for week 1
        week2_summary (dict): Summary data for week 2
        
    Returns:
        str: Base64 encoded PNG image data
    """
    logger.info("Creating portfolio comparison chart")
    
    # Calculate the metrics we want to display
    total_value_change = week2_summary['total_portfolio_value'] - week1_summary['total_portfolio_value']
    real_gain_loss = week2_summary.get('weekly_dollar_performance', 0)
    
    # NET CASH FLOW = sum of both weeks (week1 + week2)
    net_cash_flow = week2_summary.get('cash_flow', 0) + week1_summary.get('cash_flow', 0)
    
    income_change = week2_summary['total_annual_income'] - week1_summary['total_annual_income']
    
    # Prepare data for plotting
    metrics = ['Total Value Change', 'Real Gain/Loss', 'Net Cash Flow', 'Est. Annual Income Change']
    values = [total_value_change, real_gain_loss, net_cash_flow, income_change]
    
    # Create colors based on values
    colors = ['#4CAF50' if v >= 0 else '#F44336' for v in values]
    
    # Create figure and axis
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Create vertical bars
    x_pos = np.arange(len(metrics))
    ax.bar(x_pos, values, align='center', color=colors)
    
    # Add horizontal zero line
    ax.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    
    # Customize the plot
    ax.set_xticks(x_pos)
    ax.set_xticklabels(metrics, rotation=45, ha='right')
    
    # Format y-axis with dollar values
    formatter = ticker.FuncFormatter(lambda x, p: f'${x:,.0f}')
    ax.yaxis.set_major_formatter(formatter)
    
    # Calculate max absolute value for label positioning
    max_abs_value = max(abs(max(values)), abs(min(values)))
    
    # Add value labels on the bars
    for i, v in enumerate(values):
        # Position the label based on whether the value is positive or negative
        if v >= 0:
            label_pos = v + max_abs_value * 0.02  # Slightly above for positive values
            va = 'bottom'
        else:
            label_pos = v - max_abs_value * 0.02  # Slightly below for negative values
            va = 'top'
            
        ax.text(i, label_pos, f'${abs(v):,.2f}', 
                ha='center', va=va,
                color='black')
    
    # Remove right spine
    ax.spines['right'].set_visible(False)
    
    # Add grid lines
    ax.yaxis.grid(True, linestyle='--', alpha=0.3)
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    
    # Convert plot to base64 encoded string
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight')
    buffer.seek(0)
    image_png = buffer.getvalue()
    buffer.close()
    plt.close(fig)
    
    # Encode the bytes to base64
    image_base64 = base64.b64encode(image_png).decode('utf-8')
    return image_base64

def create_portfolio_comparison_chart_data(week1_summary, week2_summary):
    """
    Create JSON data for ApexCharts column chart comparing key metrics between two weeks.
    
    Args:
        week1_summary (dict): Summary data for week 1
        week2_summary (dict): Summary data for week 2
        
    Returns:
        dict: JSON data structure for ApexCharts column chart
    """
    logger.info("Creating portfolio comparison chart data")
    
    # Calculate the metrics we want to display (same logic as original)
    total_value_change = week2_summary['total_portfolio_value'] - week1_summary['total_portfolio_value']
    real_gain_loss = week2_summary.get('weekly_dollar_performance', 0)
    
    # NET CASH FLOW = sum of both weeks (week1 + week2)
    net_cash_flow = week2_summary.get('cash_flow', 0) + week1_summary.get('cash_flow', 0)
    
    income_change = week2_summary['total_annual_income'] - week1_summary['total_annual_income']
    
    # Prepare data for column chart
    categories = ['Total Value Change', 'Real Gain/Loss', 'Net Cash Flow', 'Est. Annual Income Change']
    values = [total_value_change, real_gain_loss, net_cash_flow, income_change]
    
    # Calculate y-axis range for better scaling
    max_abs_value = max(abs(max(values)), abs(min(values))) if values else 1000
    y_axis_buffer = max_abs_value * 0.1  # 10% buffer
    
    logger.info(f"Portfolio comparison chart values:")
    logger.info(f"  Total Value Change: ${total_value_change:,.2f}")
    logger.info(f"  Real Gain/Loss: ${real_gain_loss:,.2f}")
    logger.info(f"  Net Cash Flow (week1 + week2): ${net_cash_flow:,.2f}")
    logger.info(f"  Income Change: ${income_change:,.2f}")
    
    return {
        'series': [{
            'name': 'Weekly Change',
            'data': values
        }],
        'categories': categories,
        'colors': ['#5f76a1', '#b7babe'],  # Blue for positive, gray for negative
        'yAxisMin': min(values) - y_axis_buffer if values else -1000,
        'yAxisMax': max(values) + y_axis_buffer if values else 1000,
        'hasData': True
    }

def create_portfolio_history_chart(client=None):
    """
    Create a line chart showing portfolio value evolution across all available snapshots.
    
    Args:
        client (str, optional): Client identifier code to filter snapshots by.
    
    Returns:
        str: Base64 encoded PNG image data
    """
    client_log = f" for client {client}" if client else ""
    logger.info(f"Creating portfolio history chart{client_log}")
    
    # Get all available snapshot dates from the database for this client
    dates = pdb.get_available_dates(client)
    
    if not dates or len(dates) < 2:
        logger.warning(f"Not enough data to create portfolio history chart{client_log}")
        # Create a simple placeholder chart
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.text(0.5, 0.5, 'Not enough historical data to display portfolio value evolution',
                horizontalalignment='center', verticalalignment='center', fontsize=14)
        ax.axis('off')
    else:
        # Sort dates to ensure chronological order
        dates = sorted(dates)
        
        # Get portfolio value for each date, filtered by client
        values = []
        for date in dates:
            snapshot = pdb.get_snapshot(date, client)
            if snapshot and 'portfolio_metrics' in snapshot:
                portfolio_metrics = snapshot.get('portfolio_metrics', {})
                # Check if portfolio_metrics is nested
                if 'portfolio_metrics' in portfolio_metrics:
                    total_value = portfolio_metrics['portfolio_metrics'].get('total_value', 0)
                else:
                    total_value = portfolio_metrics.get('total_value', 0)
                values.append(total_value)
            else:
                values.append(0)
        
        # Get significant cash flow events (>$100,000)
        first_date = min(dates)
        last_date = max(dates)
        significant_cash_flows = []
        
        all_transactions = pdb.get_transactions_by_date_range(first_date, last_date, client)
        
        # Use the CashFlowClassifier to identify external cash flows
        classifier = CashFlowClassifier()
        for tx in all_transactions:
            if classifier.is_external_cash_flow(tx):
                amount = classifier.get_cash_flow_amount(tx)
                if abs(amount) >= 100000:  # Only include significant cash flows (>$100,000)
                    tx_date = tx.get('date')
                    if isinstance(tx_date, str):
                        # Convert string to datetime
                        tx_date = datetime.strptime(tx_date, '%Y-%m-%d')
                    elif isinstance(tx_date, datetime):
                        # Already datetime
                        pass
                    else:
                        # Skip if date is invalid
                        continue
                    
                    significant_cash_flows.append({
                        'date': tx_date.strftime('%Y-%m-%d'),
                        'amount': amount
                    })
        
        # Create the line chart
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Plot the main line
        ax.plot(dates, values, marker='o', linestyle='-', color='#007bff', linewidth=2, markersize=6, label='Portfolio Value')
        
        # Calculate and plot moving average (if we have enough data points)
        if len(values) >= 3:
            window_size = min(3, len(values))
            moving_avg = np.convolve(values, np.ones(window_size)/window_size, mode='valid')
            # We need to adjust the dates for the moving average line
            ma_dates = dates[window_size-1:]
            ax.plot(ma_dates, moving_avg, linestyle='--', color='#ff9800', linewidth=1.5, label='Moving Average')
        
        # Mark significant cash flows on the chart
        cash_flow_dates = [cf['date'] for cf in significant_cash_flows]
        cash_flow_amounts = [cf['amount'] for cf in significant_cash_flows]
        
        # Find the closest portfolio value for each cash flow date
        cash_flow_values = []
        for cf_date in cash_flow_dates:
            if cf_date in dates:
                idx = dates.index(cf_date)
                cash_flow_values.append(values[idx])
            else:
                # Find closest date
                closest_date = min(dates, key=lambda d: abs(datetime.strptime(d, '%Y-%m-%d') - datetime.strptime(cf_date, '%Y-%m-%d')))
                idx = dates.index(closest_date)
                cash_flow_values.append(values[idx])
        
        # Plot cash flow markers with different colors for deposits/withdrawals
        for i, (cf_date, cf_amount, cf_value) in enumerate(zip(cash_flow_dates, cash_flow_amounts, cash_flow_values)):
            if cf_amount > 0:
                # Deposit - use triangle up
                ax.plot(cf_date, cf_value, marker='^', markersize=10, color='#28a745', label='Deposit' if i == 0 else "")
            else:
                # Withdrawal - use triangle down
                ax.plot(cf_date, cf_value, marker='v', markersize=10, color='#dc3545', label='Withdrawal' if i == 0 else "")
        
        # Add labels and title
        ax.set_xlabel('Date', fontsize=12, labelpad=15)
        ax.set_ylabel('Portfolio Value ($)', fontsize=12, labelpad=15)
        ax.set_title('Portfolio Value Evolution', fontsize=14, pad=20)
        
        # Add grid for better readability
        ax.grid(True, linestyle='--', alpha=0.7)
        
        # Format the y-axis to show dollar values
        import matplotlib.ticker as ticker
        formatter = ticker.StrMethodFormatter('${x:,.0f}')
        ax.yaxis.set_major_formatter(formatter)
        
        # Add value annotations for portfolio values
        for i, (x, y) in enumerate(zip(dates, values)):
            # Only annotate every other point if we have many data points
            if len(dates) > 8 and i % 2 != 0:
                continue
            ax.annotate(f'${y:,.0f}', 
                       (x, y),
                       textcoords="offset points", 
                       xytext=(0, 10), 
                       ha='center',
                       fontsize=9)

        # Rotate x labels for better readability if there are many dates
        if len(dates) > 3:
            plt.xticks(rotation=45)
        
        # Add legend if we have moving average or cash flows
        if len(values) >= 3 or significant_cash_flows:
            ax.legend(loc='upper left')
        
        plt.tight_layout()
    
    # Convert plot to base64 encoded string
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight')
    buffer.seek(0)
    image_png = buffer.getvalue()
    buffer.close()
    plt.close(fig)
    
    # Encode the bytes to base64
    image_base64 = base64.b64encode(image_png).decode('utf-8')
    return image_base64

def create_portfolio_history_chart_data(client=None):
    """
    Create JSON data for ApexCharts portfolio value evolution chart.
    
    Args:
        client (str, optional): Client identifier code to filter snapshots by.
        
    Returns:
        dict: JSON data structure for ApexCharts
    """
    client_log = f" for client {client}" if client else ""
    logger.info(f"Creating portfolio history chart data{client_log}")
    
    # Get all available snapshot dates from the database for this client
    dates = pdb.get_available_dates(client)
    
    if not dates or len(dates) < 2:
        logger.warning(f"Not enough data to create portfolio history chart{client_log}")
        return {
            'series': [{
                'name': 'Portfolio Value',
                'data': []
            }],
            'colors': ['#5f76a1'],
            'gradient': {
                'from': '#5f76a1',
                'to': '#dae1f3'
            },
            'hasData': False,
            'message': 'Not enough historical data to display portfolio value evolution'
        }
    
    # Sort dates to ensure chronological order
    dates = sorted(dates)
    
    # Get portfolio value for each date, filtered by client
    values = []
    for date in dates:
        snapshot = pdb.get_snapshot(date, client)
        if snapshot and 'portfolio_metrics' in snapshot:
            portfolio_metrics = snapshot.get('portfolio_metrics', {})
            # Check if portfolio_metrics is nested
            if 'portfolio_metrics' in portfolio_metrics:
                total_value = portfolio_metrics['portfolio_metrics'].get('total_value', 0)
            else:
                total_value = portfolio_metrics.get('total_value', 0)
            values.append(total_value)
        else:
            values.append(0)
    
    # Format data for ApexCharts
    chart_data = []
    for i, (date, value) in enumerate(zip(dates, values)):
        # Convert date string to timestamp for ApexCharts
        try:
            from datetime import datetime
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            timestamp = int(date_obj.timestamp() * 1000)  # ApexCharts expects milliseconds
            chart_data.append({'x': timestamp, 'y': value})
        except ValueError:
            logger.warning(f"Invalid date format: {date}")
            continue
    
    # Get significant cash flow events for annotations (>$100,000)
    first_date = min(dates)
    last_date = max(dates)
    cash_flow_annotations = []
    
    try:
        all_transactions = pdb.get_transactions_by_date_range(first_date, last_date, client)
        
        # Use the CashFlowClassifier to identify external cash flows
        classifier = CashFlowClassifier()
        for tx in all_transactions:
            if classifier.is_external_cash_flow(tx):
                amount = classifier.get_cash_flow_amount(tx)
                if abs(amount) >= 100000:  # Only include significant cash flows (>$100,000)
                    tx_date = tx.get('date')
                    if isinstance(tx_date, str):
                        # Convert string to datetime
                        tx_date = datetime.strptime(tx_date, '%Y-%m-%d')
                    elif isinstance(tx_date, datetime):
                        # Already datetime
                        pass
                    else:
                        # Skip if date is invalid
                        continue
                    
                    # Find closest portfolio value for this date
                    cf_date_str = tx_date.strftime('%Y-%m-%d')
                    cf_value = 0
                    if cf_date_str in dates:
                        idx = dates.index(cf_date_str)
                        cf_value = values[idx]
                    else:
                        # Find closest date
                        closest_date = min(dates, key=lambda d: abs(datetime.strptime(d, '%Y-%m-%d') - tx_date))
                        idx = dates.index(closest_date)
                        cf_value = values[idx]
                    
                    cash_flow_annotations.append({
                        'x': int(tx_date.timestamp() * 1000),
                        'y': cf_value,
                        'amount': amount,
                        'type': 'deposit' if amount > 0 else 'withdrawal'
                    })
    except Exception as e:
        logger.warning(f"Could not retrieve cash flow data: {e}")
    
    return {
        'series': [{
            'name': 'Portfolio Value',
            'data': chart_data
        }],
        'colors': ['#5f76a1'],
        'gradient': {
            'from': '#5f76a1',
            'to': '#dae1f3'
        },
        'hasData': True,
        'yAxisMin': min(values) * 0.95 if values else 0,
        'yAxisMax': max(values) * 1.05 if values else 100000,
        'currentValue': f"${values[-1]:,.2f}" if values else "$0.00",
        'currentDate': dates[-1] if dates else "N/A",
        'cashFlowAnnotations': cash_flow_annotations
    }

def calculate_cumulative_returns(dates, returns):
    """
    Calculate cumulative returns from a series of periodic returns.
    
    Args:
        dates (list): List of dates in chronological order
        returns (list): List of periodic returns as percentages
        
    Returns:
        tuple: (dates, cumulative_returns)
    """
    logger.info(f"Calculating cumulative returns for {len(dates)} dates")
    
    if len(dates) <= 1:
        # Handle case with only one data point
        logger.warning("Only one data point available for cumulative returns, returning flat line")
        return dates, [1000.0] * len(dates)
    
    cumulative_returns = [1000.0]  # Start with 1000.0 (base value)
    
    for i in range(1, len(returns)):
        # Apply the chaining formula
        prev_cumulative = cumulative_returns[i-1]
        period_return = returns[i] / 100.0  # Convert percentage to decimal
        new_cumulative = prev_cumulative * (1 + period_return)
        cumulative_returns.append(new_cumulative)
        logger.debug(f"Date: {dates[i]}, Return: {returns[i]}%, Cumulative: {new_cumulative:.2f}")
    
    return dates, cumulative_returns

def create_cumulative_return_chart(dates, returns, significant_cash_flows=None, client=None):
    """
    Create a line chart showing cumulative returns over time.
    
    Args:
        dates (list): List of dates
        returns (list): List of periodic returns as percentages
        significant_cash_flows (list, optional): List of dictionaries with date and amount
        client (str, optional): Client identifier code
        
    Returns:
        str: Base64-encoded PNG image
    """
    client_log = f" for client {client}" if client else ""
    logger.info(f"Creating cumulative return chart{client_log}")
    
    if not dates or len(dates) < 2 or not returns or len(returns) < 2:
        logger.warning(f"Not enough data to create cumulative return chart{client_log}")
        # Create a simple placeholder chart
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.text(0.5, 0.5, 'Not enough historical data to display cumulative returns',
                horizontalalignment='center', verticalalignment='center', fontsize=14)
        ax.axis('off')
    else:
        # Calculate cumulative returns
        cum_dates, cum_returns = calculate_cumulative_returns(dates, returns)
        
        # Create the line chart
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Plot the main line
        ax.plot(cum_dates, cum_returns, marker='o', linestyle='-', color='#5f76a1', linewidth=2, markersize=6, label='Cumulative Return')
        
        # Mark significant cash flows on the chart if provided
        if significant_cash_flows:
            cash_flow_dates = [cf['date'] for cf in significant_cash_flows]
            cash_flow_amounts = [cf['amount'] for cf in significant_cash_flows]
            
            # Find the closest cumulative return value for each cash flow date
            cash_flow_values = []
            for cf_date in cash_flow_dates:
                if cf_date in cum_dates:
                    idx = cum_dates.index(cf_date)
                    cash_flow_values.append(cum_returns[idx])
                else:
                    # Find closest date
                    closest_date = min(cum_dates, key=lambda d: abs(datetime.strptime(d, '%Y-%m-%d') - datetime.strptime(cf_date, '%Y-%m-%d')))
                    idx = cum_dates.index(closest_date)
                    cash_flow_values.append(cum_returns[idx])
            
            # Plot cash flow markers with different colors for deposits/withdrawals
            for i, (cf_date, cf_amount, cf_value) in enumerate(zip(cash_flow_dates, cash_flow_amounts, cash_flow_values)):
                if cf_amount > 0:
                    # Deposit - use triangle up
                    ax.plot(cf_date, cf_value, marker='^', markersize=10, color='#28a745', label='Deposit' if i == 0 else "")
                else:
                    # Withdrawal - use triangle down
                    ax.plot(cf_date, cf_value, marker='v', markersize=10, color='#dc3545', label='Withdrawal' if i == 0 else "")
        
        # Add labels and title
        ax.set_xlabel('Date', fontsize=12, labelpad=15)
        ax.set_ylabel('Cumulative Return (Base: 1000)', fontsize=12, labelpad=15)
        ax.set_title('Cumulative Return Over Time', fontsize=14, pad=20)
        
        # Add grid for better readability
        ax.grid(True, linestyle='--', alpha=0.7)
        
        # Format the y-axis to show absolute values
        import matplotlib.ticker as ticker
        formatter = ticker.StrMethodFormatter('{x:,.2f}')
        ax.yaxis.set_major_formatter(formatter)
        
        # Set y-axis to start at an appropriate level
        min_return = min(cum_returns)
        y_min = 950 if min_return >= 950 else min_return - 50
        y_max = max(cum_returns) + 50
        ax.set_ylim(y_min, y_max)
        
        # Add value annotations for important points
        # Starting point
        ax.annotate(f'Start: {cum_returns[0]:.2f}', 
                   (cum_dates[0], cum_returns[0]),
                   textcoords="offset points", 
                   xytext=(0, 10), 
                   ha='center',
                   fontsize=9)
        
        # Current value
        ax.annotate(f'Current: {cum_returns[-1]:.2f}', 
                   (cum_dates[-1], cum_returns[-1]),
                   textcoords="offset points", 
                   xytext=(0, 10), 
                   ha='center',
                   fontsize=9)
        
        # Rotate x labels for better readability if there are many dates
        if len(cum_dates) > 3:
            plt.xticks(rotation=45)
        
        # Add legend if we have cash flows
        if significant_cash_flows:
            ax.legend(loc='upper left')
        
        plt.tight_layout()
    
    # Convert plot to base64 encoded string
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight')
    buffer.seek(0)
    image_png = buffer.getvalue()
    buffer.close()
    plt.close(fig)
    
    # Encode the bytes to base64
    image_base64 = base64.b64encode(image_png).decode('utf-8')
    return image_base64

def create_cumulative_return_chart_data(dates, returns, significant_cash_flows=None, client=None):
    """
    Create JSON data for ApexCharts cumulative returns chart.
    
    Args:
        dates (list): List of dates
        returns (list): List of periodic returns as percentages
        significant_cash_flows (list, optional): List of dictionaries with date and amount (IGNORED for now)
        client (str, optional): Client identifier code
        
    Returns:
        dict: JSON data structure for ApexCharts
    """
    client_log = f" for client {client}" if client else ""
    logger.info(f"Creating cumulative return chart data{client_log}")
    
    if not dates or len(dates) < 2 or not returns or len(returns) < 2:
        logger.warning(f"Not enough data to create cumulative return chart{client_log}")
        # Return placeholder data
        return {
            'series': [{
                'name': 'Cumulative Return (Base: 1000)',
                'data': []
            }],
            'colors': ['#5f76a1'],
            'gradient': {
                'from': '#5f76a1',
                'to': '#dae1f3'
            },
            'hasData': False,
            'message': 'Not enough historical data to display cumulative returns'
        }
    
    # Calculate cumulative returns using existing logic
    cum_dates, cum_returns = calculate_cumulative_returns(dates, returns)
    
    # Format data for ApexCharts
    chart_data = []
    for i, (date, value) in enumerate(zip(cum_dates, cum_returns)):
        # Convert date string to timestamp for ApexCharts
        try:
            from datetime import datetime
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            timestamp = int(date_obj.timestamp() * 1000)  # ApexCharts expects milliseconds
            chart_data.append({'x': timestamp, 'y': round(value, 2)})
        except ValueError:
            logger.warning(f"Invalid date format: {date}")
            continue
    
    return {
        'series': [{
            'name': 'Cumulative Return (Base: 1000)',
            'data': chart_data
        }],
        'colors': ['#5f76a1'],
        'gradient': {
            'from': '#5f76a1',
            'to': '#dae1f3'
        },
        'hasData': True,
        'yAxisMin': min(cum_returns) - 50 if cum_returns else 950,
        'yAxisMax': max(cum_returns) + 50 if cum_returns else 1050,
        'startValue': round(cum_returns[0], 2) if cum_returns else 1000,
        'endValue': round(cum_returns[-1], 2) if cum_returns else 1000,
        'startDataPoint': {'x': chart_data[0]['x'], 'y': chart_data[0]['y']} if chart_data else None,
        'endDataPoint': {'x': chart_data[-1]['x'], 'y': chart_data[-1]['y']} if chart_data else None,
        'currentValue': f"{round(cum_returns[-1], 2):,.2f}" if cum_returns else "1,000.00",
        'currentDate': cum_dates[-1] if cum_dates else "N/A"
    }

def generate_html_report(securities_file, output_file="portfolio_report.html"):
    """Generate an HTML report from the securities data."""
    logger.info("Generating HTML report")
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Parse the securities file
    parser = StatementParser(securities_file)
    securities = parser.parse()
    
    # Set up Jinja2 environment
    env = Environment(loader=FileSystemLoader('templates'))
    env.filters['format_percentage'] = format_percentage
    env.filters['format_currency'] = format_currency
    env.filters['format_number'] = format_number
    
    # Load the template
    template = env.get_template('report_template.html')
    
    # Calculate total portfolio value and estimated annual income
    total_value = 0
    total_est_annual_income = 0
    
    # Process each security
    for security in securities:
        # Calculate market value and add to total
        market_value = security.get('market_value', 0)
        total_value += market_value
        
        # Calculate position weight
        security['weight_pct'] = (market_value / total_value * 100) if total_value > 0 else 0
        
        # Use safe calculation functions to handle zero cost basis gracefully
        asset_name = security.get('name', security.get('ticker', 'Unknown'))
        security['gain_loss'] = calculate_safe_unrealized_gain_loss(market_value, security.get('cost_basis', 0), asset_name)
        security['gain_loss_pct'] = calculate_safe_gain_loss_percentage(market_value, security.get('cost_basis', 0), asset_name)
        
        # Handle coupon rate and calculate estimated annual income
        coupon_rate = security.get('coupon_rate')
        quantity = security.get('quantity', 0)
        
        if coupon_rate is not None:
            # Convert European format to American format, then to decimal
            coupon_str = str(coupon_rate).replace('%', '').replace(',', '.')
            coupon_decimal = float(coupon_str) / 100  # Always divide by 100 - all values are percentages
            security['coupon_rate'] = coupon_decimal * 100  # Store as percentage for display
            
            # Calculate estimated annual income
            security['est_annual_income'] = quantity * coupon_decimal
            total_est_annual_income += security['est_annual_income']
        else:
            security['est_annual_income'] = 0
    
    # Generate the HTML
    html_content = template.render(
        positions=securities,
        total_value=total_value,
        total_est_annual_income=total_est_annual_income,
        report_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    
    # Write the HTML content to file
    with open(output_file, 'w') as f:
        f.write(html_content)
    
    logger.info(f"HTML report generated successfully: {output_file}")
    return output_file

def generate_html_report_from_snapshots(date1, date2, output_file, is_first_report=False, client=None):
    """
    Generate an HTML report comparing two snapshots from the database.
    
    Args:
        date1 (str): Date of the first snapshot in YYYY-MM-DD format
        date2 (str): Date of the second snapshot in YYYY-MM-DD format
        output_file (str): Path to save the HTML report
        is_first_report (bool): Whether this is the first report (no comparison)
        client (str, optional): Client identifier code to filter data by
    
    Returns:
        str: Path to the generated HTML report
    """
    client_log = f" for client {client}" if client else ""
    logger.info(f"Generating HTML report from snapshots: {date1} and {date2}{client_log}")
    
    # Retrieve snapshots from database
    snapshot1 = pdb.get_snapshot(date1, client)
    snapshot2 = pdb.get_snapshot(date2, client)
    
    if not snapshot1 or not snapshot2:
        logger.error(f"Could not retrieve snapshots for dates {date1} and {date2}{client_log}")
        raise ValueError(f"Missing snapshot data for dates {date1} and/or {date2}{client_log}")
    
    # Extract data from snapshots
    week1_data = {
        'portfolio_metrics': snapshot1['portfolio_metrics'],
        'assets': snapshot1['assets'],
        'positions': snapshot1['positions']
    }
    
    week2_data = {
        'portfolio_metrics': snapshot2['portfolio_metrics'],
        'assets': snapshot2['assets'],
        'positions': snapshot2['positions'],
        'rollover_accounts': snapshot2['portfolio_metrics'].get('rollover_accounts', [])
    }
    
    # Get transactions for week 2 (current week)
    # For first report, include transactions on the report date
    # For comparison reports, use transactions between dates
    if is_first_report:
        transactions = pdb.get_transactions(date2, date2, client)
    else:
        from datetime import datetime, timedelta
        start_date = datetime.strptime(date1, "%Y-%m-%d") + timedelta(days=1)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = date2
        transactions = pdb.get_transactions(start_date_str, end_date_str, client)
    
    week2_data['transactions'] = transactions
    
    # Create mapping of asset IDs to names
    asset_name_map_week1 = {asset['id']: asset.get('name', 'Unknown') for asset in snapshot1['assets']}
    asset_name_map_week2 = {asset['id']: asset.get('name', 'Unknown') for asset in snapshot2['assets']}
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    # Generate the report content
    generate_html_report_content(
        week1_data,
        week2_data,
        asset_name_map_week1,
        asset_name_map_week2,
        date1,
        date2,
        output_file,
        is_first_report,
        client
    )
    
    logger.info(f"HTML report generated at: {output_file}{client_log}")
    return output_file

def create_html_table(data_list, title, columns=None, sort_by=None, classes="data-table", display_title=None, has_subtotal=False):
    if not data_list:
        return f"<h3>{display_title or title}</h3><p>No data available</p>"
    
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(data_list)
    
    # Apply sorting if specified
    if sort_by and sort_by in df.columns:
        df = df.sort_values(by=sort_by, ascending=False)
    
    # Select only specified columns if given
    if columns:
        # Only keep columns that exist in the dataframe
        columns = [col for col in columns if col in df.columns]
        if columns:
            df = df[columns]
    
    # Format numeric columns and add CSS classes
    currency_columns = ['price', 'market_value', 'cost_basis', 'unrealized_gain', 'annual_income']
    numeric_columns = ['quantity']
    percentage_columns = ['unrealized_gain_pct', 'coupon_rate']
    
    for col in df.columns:
        if col in currency_columns and col in df.columns:
            df[col] = df[col].apply(lambda x: f"${x:,.2f}" if pd.notna(x) and x is not None and isinstance(x, (int, float)) else "-")
        elif col in numeric_columns and col in df.columns:
            df[col] = df[col].apply(lambda x: f"{x:,.2f}" if pd.notna(x) and x is not None and isinstance(x, (int, float)) else "-")
        elif col in percentage_columns and col in df.columns:
            df[col] = df[col].apply(lambda x: f"{float(x):.2f}%" if pd.notna(x) and x is not None and isinstance(x, (int, float)) and x != 0 else "0.00%" if pd.notna(x) and x is not None and isinstance(x, (int, float)) and x == 0 else "-")
    
    # Convert DataFrame to HTML table with position-specific classes if it's a positions table
    if "Current" in title and "Positions" in title:
        classes = "data-table position-table"
        # Add column-specific classes
        table_html = df.to_html(classes=classes, border=0, index=False)
        
        # Add column classes for width control and format headers
        for col in df.columns:
            # Format the header text - replace underscores with spaces and capitalize
            header_text = col.replace('_', ' ').title()
            if col == 'unrealized_gain_pct':
                header_text = 'Unrealized Gain %'
            elif col == 'annual_income':
                header_text = 'Annual Income'
            elif col == 'market_value':
                header_text = 'Market Value'
            elif col == 'cost_basis':
                header_text = 'Cost Basis'
            elif col == 'asset_type':
                header_text = 'Asset Type'
            elif col == 'custody':
                header_text = 'Custody'
            
            col_class = f"col-{col.lower().replace('_', '-')}"
            if col in numeric_columns + currency_columns + percentage_columns:
                table_html = table_html.replace(f'<th>{col}</th>', f'<th class="{col_class} numeric">{header_text}</th>')
                table_html = table_html.replace(f'<td>{col}</td>', f'<td class="{col_class} numeric">{col}</td>')
            else:
                table_html = table_html.replace(f'<th>{col}</th>', f'<th class="{col_class}">{header_text}</th>')
                table_html = table_html.replace(f'<td>{col}</td>', f'<td class="{col_class}">{col}</td>')
    else:
        table_html = df.to_html(classes=classes, border=0, index=False)
    
    # Add subtotal row class if this table has subtotal
    if has_subtotal:
        # Simple approach: replace the last <tr> before </tbody> with subtotal class
        import re
        # Find the last row in tbody and add subtotal-row class
        table_html = re.sub(r'<tr>(?=(?:(?!<tr).)*</tbody>)', '<tr class="subtotal-row">', table_html)
    
    # Use display_title if provided, otherwise use the original title
    displayed_title = display_title or title
    return f"<h3>{displayed_title}</h3>{table_html}"

def create_positions_tables(positions, columns, sort_by='market_value'):
    """Create separate tables for each asset class."""
    # Group positions by asset class
    asset_classes = {
        'Cash / Money Market': [],
        'Fixed Income': [],
        'Equities': [],
        'Alternatives': []
    }
    
    # Group positions by normalized asset type
    for position in positions:
        asset_type = normalize_asset_type(position.get('asset_type', 'Unknown'))
        if asset_type == 'Cash':
            asset_classes['Cash / Money Market'].append(position)
        else:
            asset_classes[asset_type].append(position)
    
    # Generate tables for each asset class
    tables_html = []
    for asset_class, positions in asset_classes.items():
        if positions:  # Only create table if there are positions
            # Sort positions before adding subtotal
            if sort_by and sort_by in positions[0]:
                positions.sort(key=lambda x: x.get(sort_by, 0), reverse=True)
            
            # Calculate subtotals
            subtotal = {
                'custody': '',
                'name': 'Subtotal',
                'ticker': '',
                'asset_type': '',
                'quantity': None,
                'price': None,
                'market_value': sum(p.get('market_value', 0) for p in positions),
                'cost_basis': sum(p.get('cost_basis', 0) for p in positions),
                'unrealized_gain': sum(calculate_safe_unrealized_gain_loss(p.get('market_value', 0), p.get('cost_basis', 0), p.get('name', 'Unknown')) for p in positions),
                'unrealized_gain_pct': None,
                'coupon_rate': None,
                'annual_income': sum(p.get('annual_income', 0) for p in positions)
            }
            
            # Add subtotal row to positions
            positions_with_subtotal = positions + [subtotal]
            
            table_html = create_html_table(
                positions_with_subtotal,
                f"Current Positions - {asset_class}",  # Keep original title for formatting check
                columns,
                None,  # Set sort_by to None since we already sorted
                display_title=f"{asset_class}",  # Only display the asset class name
                has_subtotal=True  # Flag to indicate this table has a subtotal row
            )
            tables_html.append(table_html)
    
    return '\n'.join(tables_html)

def get_first_snapshot_date(client=None):
    """
    Get the earliest snapshot date available in the database.
    
    Args:
        client (str, optional): Client identifier code to filter dates by.
        
    Returns:
        str: The earliest snapshot date in YYYY-MM-DD format, or None if no dates exist.
    """
    available_dates = pdb.get_available_dates(client)
    if available_dates:
        return min(available_dates)
    return None

def generate_html_report_content(week1_data, week2_data, asset_name_map_week1, asset_name_map_week2, date1, date2, output_file, is_first_report=False, client=None):
    """
    Generate the HTML report content from the provided data.
    
    Args:
        week1_data (dict): Data for week 1
        week2_data (dict): Data for week 2
        asset_name_map_week1 (dict): Mapping of asset IDs to names for week 1
        asset_name_map_week2 (dict): Mapping of asset IDs to names for week 2
        date1 (str): Date of week 1 in YYYY-MM-DD format
        date2 (str): Date of week 2 in YYYY-MM-DD format
        output_file (str): Path to output HTML file
        is_first_report (bool): Whether this is the first report (no comparison)
        client (str, optional): Client identifier code to filter data by
        
    Returns:
        str: Path to the generated HTML file
    """
    client_log = f" for client {client}" if client else ""
    logger.info(f"Generating HTML report content comparing {date1} to {date2}{client_log}")
    logger.info(f"Output file path: {output_file}")
    logger.info(f"Absolute output file path: {os.path.abspath(output_file)}")
    
    # Set up Jinja2 environment with custom filters
    from django.conf import settings
    template_dir = settings.BASE_DIR / 'templates'
    env = Environment(loader=FileSystemLoader(template_dir))
    env.filters['format_currency'] = format_currency
    env.filters['format_percentage'] = format_percentage
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(os.path.abspath(output_file))
    logger.info(f"Creating output directory: {output_dir}")
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    template = env.get_template('report_template.html')
    
    # Extract portfolio metrics
    portfolio_metrics1 = week1_data.get('portfolio_metrics', {})
    portfolio_metrics2 = week2_data.get('portfolio_metrics', {})
    
    # Get portfolio metrics directly from the portfolio_metrics structure
    # Check if portfolio_metrics is in the new nested structure
    core_metrics1 = portfolio_metrics1.get('portfolio_metrics', portfolio_metrics1)  # Fall back to old structure if not nested
    total_value1 = core_metrics1.get('total_value', 0)
    cost_basis1 = core_metrics1.get('cost_basis', 0)
    asset_allocation1 = core_metrics1.get('asset_allocation', {})

    core_metrics2 = portfolio_metrics2.get('portfolio_metrics', portfolio_metrics2)  # Fall back to old structure if not nested
    total_value2 = core_metrics2.get('total_value', 0)
    cost_basis2 = core_metrics2.get('cost_basis', 0)
    asset_allocation2 = core_metrics2.get('asset_allocation', {})
    
    # Add debug logging for asset allocation structure
    logger.info("Template Context Asset Allocation:")
    logger.info(f"Asset Allocation Data: {asset_allocation2}")
    logger.info(f"Total Portfolio Value: {total_value2}")
    
    # Create simple portfolio data dictionaries for both weeks
    week1_summary = {
        'total_portfolio_value': total_value1,
        'total_cost_basis': cost_basis1,
        'count_of_assets': len(week1_data.get('assets', [])),
        'count_of_transactions': len(week1_data.get('transactions', [])),
        'positions': week1_data.get('positions', []),
        'transactions': week1_data.get('transactions', []),
        'asset_allocation': asset_allocation1
    }
    
    week2_summary = {
        'total_portfolio_value': total_value2,
        'total_cost_basis': cost_basis2,
        'count_of_assets': len(week2_data.get('assets', [])),
        'count_of_transactions': len(week2_data.get('transactions', [])),
        'positions': week2_data.get('positions', []),
        'transactions': week2_data.get('transactions', []),
        'asset_allocation': asset_allocation2,
        'rollover_accounts': week2_data.get('rollover_accounts', [])
    }
    
    # Filter ALT accounts from rollover alert display (ALTs still get rolled over normally)
    if week2_summary.get('rollover_accounts'):
        week2_summary['rollover_accounts'] = [
            account for account in week2_summary['rollover_accounts'] 
            if not account.startswith('ALT_')
        ]
    
    # Calculate derived metrics
    week1_summary['total_gain_loss'] = week1_summary['total_portfolio_value'] - week1_summary['total_cost_basis']
    week1_summary['total_gain_loss_percentage'] = (week1_summary['total_gain_loss'] / week1_summary['total_cost_basis'] * 100) if week1_summary['total_cost_basis'] != 0 else 0
    
    week2_summary['total_gain_loss'] = week2_summary['total_portfolio_value'] - week2_summary['total_cost_basis']
    week2_summary['total_gain_loss_percentage'] = (week2_summary['total_gain_loss'] / week2_summary['total_cost_basis'] * 100) if week2_summary['total_cost_basis'] != 0 else 0
    
    # Calculate total annual income based on stored values
    def calculate_total_annual_income(positions):
        total_income = 0
        for position in positions:
            # Use the stored annual income value
            position_income = position.get('annual_income', 0)
            total_income += position_income
            # Keep the annual income in the position for display in the table
            position['annual_income'] = position_income
        return total_income

    # Calculate total annual income for both weeks
    week1_summary['total_annual_income'] = calculate_total_annual_income(week1_summary['positions'])
    week2_summary['total_annual_income'] = calculate_total_annual_income(week2_summary['positions'])

    # Calculate annual yield for both weeks
    week1_summary['total_annual_yield'] = (week1_summary['total_annual_income'] / week1_summary['total_portfolio_value'] * 100) if week1_summary['total_portfolio_value'] != 0 else 0
    week2_summary['total_annual_yield'] = (week2_summary['total_annual_income'] / week2_summary['total_portfolio_value'] * 100) if week2_summary['total_portfolio_value'] != 0 else 0
    
    # Calculate investment cash flows for each week using InvestmentCashFlowCalculator
    investment_calculator = InvestmentCashFlowCalculator()
    
    # Calculate investment cash flows for week 1 (period-specific transactions only)
    week1_cash_flow = investment_calculator.calculate_investment_cash_flows(
        week1_summary.get('transactions', [])
    )
    
    # Calculate investment cash flows for week 2 (period-specific transactions only)
    week2_cash_flow = investment_calculator.calculate_investment_cash_flows(
        week2_summary.get('transactions', [])
    )
    
    logger.info(f"PERIOD-SPECIFIC cash flow calculation:")
    logger.info(f"Week 1 investment cash flow: ${week1_cash_flow:.2f}")
    logger.info(f"Week 2 investment cash flow: ${week2_cash_flow:.2f}")
    
    # Add cash flows to summaries
    week1_summary['cash_flow'] = week1_cash_flow
    week2_summary['cash_flow'] = week2_cash_flow
    
    # Calculate NET cash flow = sum of both weeks (NOT perpetual)
    combined_cash_flow = week1_cash_flow + week2_cash_flow
    
    # Create comparison data structure
    comparison = {
        'date1': date1,
        'date2': date2,
        'week1_summary': week1_summary,
        'week2_summary': week2_summary,
        'portfolio_value_change': week2_summary['total_portfolio_value'] - week1_summary['total_portfolio_value'],
        'combined_cash_flow': combined_cash_flow,
        'income_change': week2_summary['total_annual_income'] - week1_summary['total_annual_income']
    }
    
    # Only calculate asset allocation from positions if not already provided in metrics
    if not week1_summary['asset_allocation']:
        week1_asset_types = {
            'Cash': {'market_value': 0.0, 'percentage': 0.0},
            'Fixed Income': {'market_value': 0.0, 'percentage': 0.0},
            'Equities': {'market_value': 0.0, 'percentage': 0.0},
            'Alternatives': {'market_value': 0.0, 'percentage': 0.0}
        }
        total_value = sum(position.get('market_value', 0) for position in week1_summary['positions'])
        
        for position in week1_summary['positions']:
            asset_type = normalize_asset_type(position.get('asset_type', 'Unknown'))
            market_value = position.get('market_value', 0)
            week1_asset_types[asset_type]['market_value'] += market_value
        
        # Calculate percentages
        if total_value > 0:
            for asset_type in week1_asset_types:
                week1_asset_types[asset_type]['percentage'] = (week1_asset_types[asset_type]['market_value'] / total_value) * 100
        
        week1_summary['asset_allocation'] = week1_asset_types
    
    if not week2_summary['asset_allocation']:
        week2_asset_types = {
            'Cash': {'market_value': 0.0, 'percentage': 0.0},
            'Fixed Income': {'market_value': 0.0, 'percentage': 0.0},
            'Equities': {'market_value': 0.0, 'percentage': 0.0},
            'Alternatives': {'market_value': 0.0, 'percentage': 0.0}
        }
        total_value = sum(position.get('market_value', 0) for position in week2_summary['positions'])
        
        for position in week2_summary['positions']:
            asset_type = normalize_asset_type(position.get('asset_type', 'Unknown'))
            market_value = position.get('market_value', 0)
            week2_asset_types[asset_type]['market_value'] += market_value
        
        # Calculate percentages
        if total_value > 0:
            for asset_type in week2_asset_types:
                week2_asset_types[asset_type]['percentage'] = (week2_asset_types[asset_type]['market_value'] / total_value) * 100
        
        week2_summary['asset_allocation'] = week2_asset_types

    # Calculate change metrics (only if not first report)
    portfolio_value_change = 0
    portfolio_value_pct = 0
    gain_loss_change = 0
    gain_loss_pct = 0
    income_change = 0
    income_pct = 0
    
    if not is_first_report:
        # Get all transactions between date1 and date2
        transactions_for_period = pdb.get_transactions_by_date_range(date1, date2, client)
        
        # Use Modified Dietz to calculate portfolio return
        dietz_calculator = ModifiedDietzCalculator()
        modified_dietz_return = dietz_calculator.calculate_return(
            week1_summary['total_portfolio_value'],
            week2_summary['total_portfolio_value'],
            transactions_for_period,
            date1,
            date2
        )
        
        # Calculate the net cash flow for the weekly period
        net_cash_flow = 0
        for tx in transactions_for_period:
            if dietz_calculator.classifier.is_external_cash_flow(tx):
                net_cash_flow += dietz_calculator.classifier.get_cash_flow_amount(tx)
        
        # Calculate weekly $ performance (numerator of Modified Dietz)
        weekly_dollar_performance = week2_summary['total_portfolio_value'] - week1_summary['total_portfolio_value'] - net_cash_flow
        week2_summary['weekly_dollar_performance'] = weekly_dollar_performance
        
        # Use Modified Dietz return as the portfolio_value_pct
        portfolio_value_change = week2_summary['total_portfolio_value'] - week1_summary['total_portfolio_value']
        portfolio_value_pct = modified_dietz_return
        
        # For gain/loss and income, we're still using simple calculations since they're not affected by cash flows
        gain_loss_change = week2_summary['total_gain_loss'] - week1_summary['total_gain_loss']
        gain_loss_pct = (gain_loss_change / abs(week1_summary['total_gain_loss']) * 100) if week1_summary['total_gain_loss'] != 0 else 0
        
        income_change = week2_summary['total_annual_income'] - week1_summary['total_annual_income']
        income_pct = (income_change / week1_summary['total_annual_income'] * 100) if week1_summary['total_annual_income'] != 0 else 0
        
        # Store the Modified Dietz return for display in the template
        week2_summary['total_return_pct'] = modified_dietz_return
    else:
        # For first report, we don't have a previous week to compare to
        portfolio_value_change = 0
        portfolio_value_pct = 0
        gain_loss_change = 0
        gain_loss_pct = 0
        income_change = 0
        income_pct = 0
        week2_summary['total_return_pct'] = 0
        week2_summary['weekly_dollar_performance'] = 0

    # Calculate Return % Since Inception (first upload)
    first_date = get_first_snapshot_date(client)
    if first_date and first_date != date2:  # Make sure we're not on the first upload
        first_snapshot = pdb.get_snapshot(first_date, client)
        if first_snapshot:
            # Get first portfolio value from the first snapshot
            first_portfolio_metrics = first_snapshot.get('portfolio_metrics', {})
            # Check if portfolio_metrics is in the new nested structure
            if 'portfolio_metrics' in first_portfolio_metrics:
                first_value = first_portfolio_metrics['portfolio_metrics'].get('total_value', 0)
            else:
                first_value = first_portfolio_metrics.get('total_value', 0)
                
            current_value = week2_summary['total_portfolio_value']
            
            # Get all transactions from first snapshot date up to current date for this client
            # Use first_date (not None) for proper SINCE INCEPTION calculation from when data was first uploaded
            all_transactions = pdb.get_transactions_by_date_range(start_date=first_date, end_date=date2, client=client)
            
            # Initialize dietz_calculator if not already done
            if not is_first_report:
                # We already initialized dietz_calculator above
                pass
            else:
                dietz_calculator = ModifiedDietzCalculator()
            
            # Calculate Modified Dietz return since inception
            inception_return = dietz_calculator.calculate_return(
                first_value,
                current_value,
                all_transactions,
                first_date,
                date2
            )
            
            # Calculate the net cash flow since inception
            inception_net_cash_flow = 0
            for tx in all_transactions:
                if dietz_calculator.classifier.is_external_cash_flow(tx):
                    inception_net_cash_flow += dietz_calculator.classifier.get_cash_flow_amount(tx)
            
            # Calculate inception $ performance (numerator of Modified Dietz)
            inception_dollar_performance = current_value - first_value - inception_net_cash_flow
            week2_summary['inception_dollar_performance'] = inception_dollar_performance
            
            # Store in week2_summary for the template
            week2_summary['inception_return_pct'] = inception_return
            
            logger.info(f"Calculated inception return: {inception_return}% (from {first_date} to {date2}){client_log}")
            logger.info(f"Calculated inception $ performance: ${inception_dollar_performance:.2f}{client_log}")
    else:
        # If we're on the first upload or no previous data, set to 0
        week2_summary['inception_return_pct'] = 0
        week2_summary['inception_dollar_performance'] = 0
        logger.info(f"No inception return calculated (first upload or no previous data){client_log}")

    # Calculate bond maturity timeline
    bond_maturity_timeline = calculate_bond_maturity_timeline(week2_summary.get('positions', []), date2)
    
    # Generate charts as base64 encoded data
    asset_allocation_chart = create_asset_allocation_polar_chart_data(week2_summary)
    
    comparison_chart_data = create_portfolio_comparison_chart_data(week1_summary, week2_summary) if not is_first_report else None
    
    portfolio_history_chart = create_portfolio_history_chart_data(client)
    
    # Create position and transaction tables
    # We'll create a function to convert a list of dictionaries to an HTML table
    def create_html_table(data_list, title, columns=None, sort_by=None, classes="data-table"):
        if not data_list:
            return f"<h3>{title}</h3><p>No data available</p>"
        
        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(data_list)
        
        # Apply sorting if specified
        if sort_by and sort_by in df.columns:
            df = df.sort_values(by=sort_by, ascending=False)
        
        # Select only specified columns if given
        if columns:
            # Only keep columns that exist in the dataframe
            columns = [col for col in columns if col in df.columns]
            if columns:
                df = df[columns]
        
        # Format numeric columns and add CSS classes
        currency_columns = ['price', 'market_value', 'cost_basis', 'unrealized_gain', 'annual_income']
        numeric_columns = ['quantity']
        percentage_columns = ['unrealized_gain_pct', 'coupon_rate']
        
        for col in df.columns:
            if col in currency_columns and col in df.columns:
                df[col] = df[col].apply(lambda x: f"${x:,.2f}" if pd.notna(x) and x is not None and isinstance(x, (int, float)) else "-")
            elif col in numeric_columns and col in df.columns:
                df[col] = df[col].apply(lambda x: f"{x:,.2f}" if pd.notna(x) and x is not None and isinstance(x, (int, float)) else "-")
            elif col in percentage_columns and col in df.columns:
                df[col] = df[col].apply(lambda x: f"{float(x):.2f}%" if pd.notna(x) and x is not None and isinstance(x, (int, float)) and x != 0 else "0.00%" if pd.notna(x) and x is not None and isinstance(x, (int, float)) and x == 0 else "-")
        
        # Convert DataFrame to HTML table with position-specific classes if it's a positions table
        if "Current" in title and "Positions" in title:
            classes = "data-table position-table"
            # Add column-specific classes
            table_html = df.to_html(classes=classes, border=0, index=False)
            
            # Add column classes for width control and format headers
            for col in df.columns:
                # Format the header text - replace underscores with spaces and capitalize
                header_text = col.replace('_', ' ').title()
                if col == 'unrealized_gain_pct':
                    header_text = 'Unrealized Gain %'
                elif col == 'annual_income':
                    header_text = 'Annual Income'
                elif col == 'market_value':
                    header_text = 'Market Value'
                elif col == 'cost_basis':
                    header_text = 'Cost Basis'
                elif col == 'asset_type':
                    header_text = 'Asset Type'
                elif col == 'custody':
                    header_text = 'Custody'
            
                col_class = f"col-{col.lower().replace('_', '-')}"
                if col in numeric_columns + currency_columns + percentage_columns:
                    table_html = table_html.replace(f'<th>{col}</th>', f'<th class="{col_class} numeric">{header_text}</th>')
                    table_html = table_html.replace(f'<td>{col}</td>', f'<td class="{col_class} numeric">{col}</td>')
                else:
                    table_html = table_html.replace(f'<th>{col}</th>', f'<th class="{col_class}">{header_text}</th>')
                    table_html = table_html.replace(f'<td>{col}</td>', f'<td class="{col_class}">{col}</td>')
        else:
            table_html = df.to_html(classes=classes, border=0, index=False)
        
        return f"<h3>{title}</h3>{table_html}"
    
    # Select important columns for positions and transactions
    position_columns = ['custody', 'name', 'ticker', 'quantity', 'market_value', 'cost_basis', 'unrealized_gain', 'unrealized_gain_pct', 'coupon_rate', 'annual_income']
    transaction_columns = ['custody', 'date', 'transaction_type', 'cusip', 'price', 'quantity', 'amount']
    
    # Process positions to add unrealized gain/loss and coupon rates
    def process_positions(positions, assets):
        for position in positions:
            market_value = position.get('market_value', 0)
            cost_basis = position.get('cost_basis', 0)
            asset_name = position.get('name', position.get('ticker', 'Unknown'))
            
            # Use safe calculation functions to handle zero cost basis gracefully
            position['unrealized_gain'] = calculate_safe_unrealized_gain_loss(market_value, cost_basis, asset_name)
            position['unrealized_gain_pct'] = calculate_safe_gain_loss_percentage(market_value, cost_basis, asset_name)
            
            # Add custody field (concatenate account and bank)
            account = position.get('account', '')
            bank = position.get('bank', '')
            position['custody'] = f"{account} {bank}".strip()
            
            # Get the coupon rate directly from the position data
            position['coupon_rate'] = position.get('coupon_rate', None)
            
            # If coupon_rate is not in position, try to get it from the matching asset
            if position['coupon_rate'] is None:
                asset_id = position.get('asset_id')
                if asset_id:
                    matching_asset = next((asset for asset in assets if asset.get('id') == asset_id), None)
                    if matching_asset:
                        position['coupon_rate'] = matching_asset.get('coupon_rate')
            
            # Ensure other fields are populated
            position['name'] = position.get('name', 'Unknown')
            position['ticker'] = position.get('ticker', '')
            position['asset_type'] = position.get('asset_type', 'Unknown')

    # Process both weeks' positions
    process_positions(week1_summary['positions'], week1_data.get('assets', []))
    process_positions(week2_summary['positions'], week2_data.get('assets', []))
    
    # Create position and transaction HTML tables
    positions_table = create_positions_tables(
        week2_data['positions'],
        position_columns,
        sort_by='market_value'
    )
    
    # Process transactions
    transactions = week2_summary.get('transactions', [])
    
    # Add custody field to transactions
    for transaction in transactions:
        account = transaction.get('account', '')
        bank = transaction.get('bank', '')
        transaction['custody'] = f"{account} {bank}".strip()
    
    transactions_df = pd.DataFrame(transactions) if transactions else pd.DataFrame()
    
    if not transactions_df.empty and 'date' in transactions_df.columns:
        # Convert date columns to string to avoid sorting issues
        for idx, transaction in enumerate(transactions_df['date']):
            if isinstance(transaction, datetime):
                transactions_df.at[idx, 'date'] = transaction.strftime('%Y-%m-%d')
    
    transactions_table = create_html_table(
        transactions_df.to_dict('records') if not transactions_df.empty else [],
        "Recent Transactions", 
        transaction_columns, 
        sort_by='date'
    )
    
    # Remove old manual HTML section since we use templates now
    
    # Structure asset allocation data for the table and chart
    asset_allocation_data = week2_summary.get('asset_allocation', {})
    if not asset_allocation_data and 'portfolio_metrics' in week2_summary:
        asset_allocation_data = week2_summary['portfolio_metrics'].get('asset_allocation', {})
    
    # Sort asset allocation by market value
    sorted_allocation = dict(sorted(
        asset_allocation_data.items(),
        key=lambda x: x[1].get('market_value', 0),
        reverse=True
    ))
    
    # Calculate total market value for verification
    total_allocation_value = sum(data.get('market_value', 0) for data in asset_allocation_data.values())
    
    # Verify percentages sum to 100 (approximately)
    total_percentage = sum(data.get('percentage', 0) for data in asset_allocation_data.values())
    if abs(total_percentage - 100) > 0.01:  # Allow for small floating point differences
        # Recalculate percentages
        for asset_type in sorted_allocation:
            market_value = sorted_allocation[asset_type].get('market_value', 0)
            sorted_allocation[asset_type]['percentage'] = (market_value / total_allocation_value * 100) if total_allocation_value > 0 else 0
    
    # Calculate custody allocation
    logger.info("Calculating custody allocation for template context")
    custody_allocation, custody_total = calculate_custody_allocation(week2_summary['positions'])
    
    # Generate the custody allocation pie chart
    custody_allocation_chart_data = create_custody_allocation_polar_chart_data(custody_allocation)
    
    # Generate cumulative return chart data
    # Get all available snapshot dates and returns for this client
    all_dates = pdb.get_available_dates(client)
    all_dates = sorted(all_dates)  # Ensure chronological order
    
    # Only proceed if we have at least 2 dates
    cumulative_return_chart_data = ""
    if len(all_dates) >= 2:
        # Get all the weekly returns based on Modified Dietz
        weekly_returns = []
        
        # Define significant cash flows (withdrawals/deposits > $100,000)
        significant_cash_flows = []
        for i in range(len(all_dates) - 1):
            start_date = all_dates[i]
            end_date = all_dates[i + 1]
            
            # Get transactions between these dates
            transactions = pdb.get_transactions_by_date_range(start_date, end_date, client)
            
            # Add significant transactions to the list
            for transaction in transactions:
                amount = transaction.get('amount', 0)
                if abs(amount) > 100000:  # Only add transactions > $100,000
                    significant_cash_flows.append({
                        'date': transaction.get('date', end_date),
                        'amount': amount
                    })
        
        for i in range(len(all_dates) - 1):
            start_date = all_dates[i]
            end_date = all_dates[i + 1]
            
            # Get snapshots for these dates
            start_snapshot = pdb.get_snapshot(start_date, client)
            end_snapshot = pdb.get_snapshot(end_date, client)
            
            if not start_snapshot or not end_snapshot:
                logger.warning(f"Missing snapshot data for dates {start_date} and/or {end_date}")
                # Use a placeholder value of 0 for this period
                weekly_returns.append(0)
                continue
            
            # Get portfolio values
            start_metrics = start_snapshot.get('portfolio_metrics', {})
            end_metrics = end_snapshot.get('portfolio_metrics', {})
            
            # Check if portfolio_metrics is in the new nested structure
            if 'portfolio_metrics' in start_metrics:
                start_value = start_metrics['portfolio_metrics'].get('total_value', 0)
            else:
                start_value = start_metrics.get('total_value', 0)
                
            if 'portfolio_metrics' in end_metrics:
                end_value = end_metrics['portfolio_metrics'].get('total_value', 0)
            else:
                end_value = end_metrics.get('total_value', 0)
            
            # Get transactions between these dates
            transactions_for_period = pdb.get_transactions_by_date_range(start_date, end_date, client)
            
            # Calculate Modified Dietz return
            dietz_calculator = ModifiedDietzCalculator()
            weekly_return = dietz_calculator.calculate_return(
                start_value,
                end_value,
                transactions_for_period,
                start_date,
                end_date
            )
            
            weekly_returns.append(weekly_return)
        
        # Generate the cumulative return chart using all historical data
        # We need to add a 0 return for the first date to have the same number of returns as dates
        returns_data = [0] + weekly_returns  # First return is 0 (start at 1.0)
        cumulative_return_chart_data = create_cumulative_return_chart_data(
            all_dates, 
            returns_data, 
            significant_cash_flows=significant_cash_flows,
            client=client
        )
    else:
        logger.warning(f"Not enough data points to generate cumulative return chart for client {client}")
        # Return placeholder JSON data
        cumulative_return_chart_data = {
            'series': [{
                'name': 'Cumulative Return (Base: 1000)',
                'data': []
            }],
            'colors': ['#5f76a1'],
            'gradient': {
                'from': '#5f76a1',
                'to': '#dae1f3'
            },
            'hasData': False,
            'message': 'Not enough historical data to display cumulative returns'
        }
    
    # Calculate biggest movers (only if not first report)
    biggest_movers = []
    if not is_first_report:
        biggest_movers = calculate_biggest_movers_fixed(
            week1_data.get('positions', []),
            week2_data.get('positions', []),
            limit=5
        )
    
    # Render the template with the updated context
    template_context = {
        'date1': date1,
        'date2': date2,
        'client_name': (week2_data.get('client_name') or (get_client_name(client) if client else None)).replace(' Client', '') if week2_data.get('client_name') or (get_client_name(client) if client else None) else None,
        'week2_data': {
            'total_value': week2_summary['total_portfolio_value'],
            'total_cost_basis': week2_summary['total_cost_basis'],
            'total_return_percentage': week2_summary['total_gain_loss_percentage'],
            'total_annual_income': week2_summary['total_annual_income']
        },
        'week1_data': None if is_first_report else {
            'total_value': week1_summary['total_portfolio_value'],
            'total_cost_basis': week1_summary['total_cost_basis'],
            'total_return_percentage': week1_summary['total_gain_loss_percentage'],
            'total_annual_income': week1_summary['total_annual_income']
        },
        'total_value': week2_summary['total_portfolio_value'],
        'asset_allocation': sorted_allocation,  # Use the sorted and verified allocation data
        'custody_allocation': custody_allocation,  # Add custody allocation data
        'asset_allocation_chart': asset_allocation_chart,
        'custody_allocation_chart': custody_allocation_chart_data,  # Add custody allocation chart
        'portfolio_comparison_chart': comparison_chart_data if not is_first_report else None,
        'portfolio_history_chart': portfolio_history_chart,
        'cumulative_return_chart': cumulative_return_chart_data,  # Add the new cumulative return chart
        'positions_table': positions_table,
        'transactions_table': transactions_table if week2_summary.get('transactions') else None,
        'report_date': date1,
        'comparison': comparison,  # Add the comparison object
        'is_first_report': is_first_report,
        'week2_summary': week2_summary,  # Add week2_summary for rollover access  # Make sure is_first_report is passed
        'comparison_chart_data': comparison_chart_data,
        'bond_maturity_timeline': bond_maturity_timeline,
        'biggest_movers': biggest_movers,  # Add biggest movers data
        'week2_summary': week2_summary,  # Add the entire week2_summary object to the template context
        'week1_summary': None if is_first_report else week1_summary  # Add week1_summary conditionally
    }
    
    # Write the HTML content to file
    try:
        with open(output_file, 'w') as f:
            f.write(template.render(**template_context))
        logger.info(f"Successfully wrote HTML report to: {output_file}")
    except Exception as e:
        logger.error(f"Failed to write HTML report to {output_file}: {str(e)}")
        raise
    
    return output_file

# Helper functions for formatting
def format_currency(value):
    """Format a number as currency."""
    try:
        return f"${value:,.2f}"
    except (ValueError, TypeError):
        return "$0.00"

def format_percentage(value):
    """Format a number as percentage."""
    try:
        return f"{value:.2f}%"
    except (ValueError, TypeError):
        return "0.00%"

def format_number(value):
    """Format a value as a number."""
    return f"{value:,.0f}"

def get_color_class(value):
    """Get color class based on value."""
    return "positive" if value >= 0 else "negative"

def calculate_custody_allocation(positions):
    """
    Calculate allocation by custody (account + bank).
    
    Args:
        positions (list): List of position dictionaries
        
    Returns:
        tuple: (sorted_allocation_dict, total_value)
    """
    logger.info("Calculating custody allocation")
    
    custody_allocation = {}
    total_value = sum(position.get('market_value', 0) for position in positions)
    
    logger.info(f"Total portfolio value for custody allocation: {total_value}")
    
    # Group by custody
    for position in positions:
        # Get custody field or generate from account and bank
        custody = position.get('custody', '')
        if not custody and (position.get('account') or position.get('bank')):
            account = position.get('account', '')
            bank = position.get('bank', '')
            custody = f"{account} {bank}".strip()
        
        # Skip if still empty
        if not custody:
            logger.warning(f"Position has no custody information: {position.get('name')}")
            continue
            
        market_value = position.get('market_value', 0)
        if custody not in custody_allocation:
            custody_allocation[custody] = {'market_value': 0, 'percentage': 0}
        
        custody_allocation[custody]['market_value'] += market_value
        logger.debug(f"Added {market_value} to {custody}, new total: {custody_allocation[custody]['market_value']}")
    
    # Calculate percentages
    if total_value > 0:
        for custody in custody_allocation:
            custody_allocation[custody]['percentage'] = (
                custody_allocation[custody]['market_value'] / total_value * 100
            )
            logger.debug(f"Custody {custody}: {custody_allocation[custody]['market_value']} " 
                         f"({custody_allocation[custody]['percentage']:.2f}%)")
    
    # Sort by market value (descending)
    sorted_allocation = dict(sorted(
        custody_allocation.items(),
        key=lambda x: x[1]['market_value'],
        reverse=True
    ))
    
    # Verification: sum of percentages should be ~100%
    total_percentage = sum(data['percentage'] for data in sorted_allocation.values())
    logger.info(f"Total custody allocation percentage: {total_percentage:.2f}%")
    
    return sorted_allocation, total_value

# Note: The calculate_bond_maturity_timeline function is imported from calculation_helpers

def calculate_biggest_movers(week1_positions, week2_positions, limit=5):
    """
    Calculate the biggest movers (by percentage change) between two periods.
    
    Args:
        week1_positions (list): List of position dictionaries for week 1
        week2_positions (list): List of position dictionaries for week 2
        limit (int): Maximum number of movers to return
        
    Returns:
        list: List of biggest movers, sorted by absolute percentage change
    """
    logger.info(f"Calculating biggest movers (limit: {limit})")
    
    # Create dictionary mapping from position identifier to position details for week 1
    week1_positions_dict = {}
    for position in week1_positions:
        # Use a compound key that uniquely identifies the position
        key = (position.get('cusip', ''), position.get('name', ''), position.get('ticker', ''))
        week1_positions_dict[key] = position
    
    # Calculate percentage changes for positions that exist in both weeks
    movers = []
    for position in week2_positions:
        key = (position.get('cusip', ''), position.get('name', ''), position.get('ticker', ''))
        
        # Skip if position didn't exist in week 1
        if key not in week1_positions_dict:
            continue
        
        week1_position = week1_positions_dict[key]
        week1_value = week1_position.get('market_value', 0)
        week2_value = position.get('market_value', 0)
        
        # Skip if either value is zero to avoid division by zero
        if week1_value == 0 or week2_value == 0:
            continue
        
        # Calculate percentage change
        pct_change = ((week2_value - week1_value) / week1_value) * 100
        dollar_change = week2_value - week1_value
        
        # Create mover entry
        mover = {
            'name': position.get('name', 'Unknown'),
            'asset_type': position.get('asset_type', 'Unknown'),
            'pct_change': pct_change,
            'dollar_change': dollar_change,
            'abs_pct_change': abs(pct_change)  # For sorting
        }
        movers.append(mover)
    
    # Sort by absolute percentage change (descending)
    movers.sort(key=lambda x: x['abs_pct_change'], reverse=True)
    
    # Take top 'limit' movers
    top_movers = movers[:limit]
    logger.info(f"Found {len(top_movers)} biggest movers")
    
    return top_movers

def calculate_biggest_movers_fixed(week1_positions, week2_positions, limit=5):
    """
    Calculate the biggest movers (by price movement only) between two periods.
    
    Filters out:
    - Cash and Money Market positions (transaction noise)
    - Positions with significant quantity changes (buy/sell activity)
    - Small positions (penny stock noise)
    
    Focuses on:
    - Price-driven market value changes for stable holdings
    - Real investment performance movements
    
    Args:
        week1_positions (list): List of position dictionaries for week 1
        week2_positions (list): List of position dictionaries for week 2
        limit (int): Maximum number of movers to return
        
    Returns:
        list: List of biggest movers, sorted by absolute dollar change
    """
    logger.info(f"Calculating biggest movers (fixed algorithm, limit: {limit})")
    
    # Configuration
    excluded_asset_types = ['Cash', 'Money Market']
    max_quantity_change_pct = 5.0  # Maximum allowed quantity change percentage
    min_position_size = 5000  # Minimum position size to avoid noise
    
    # Create dictionary mapping from position identifier to position details for week 1
    week1_positions_dict = {}
    for position in week1_positions:
        # Use a compound key that uniquely identifies the position
        # Handle both string and integer CUSIP values
        cusip_val = position.get('cusip', '')
        cusip_str = str(cusip_val).strip() if cusip_val is not None else ''
        
        key = (
            cusip_str,
            position.get('name', '').strip(), 
            position.get('ticker', '').strip(),
            position.get('asset_type', '').strip()
        )
        week1_positions_dict[key] = position
    
    # Calculate percentage changes for positions that exist in both weeks
    movers = []
    excluded_count = {'cash': 0, 'quantity_change': 0, 'small_size': 0, 'missing': 0}
    
    for position in week2_positions:
        # Handle both string and integer CUSIP values
        cusip_val = position.get('cusip', '')
        cusip_str = str(cusip_val).strip() if cusip_val is not None else ''
        
        key = (
            cusip_str,
            position.get('name', '').strip(), 
            position.get('ticker', '').strip(),
            position.get('asset_type', '').strip()
        )
        
        # Skip if position didn't exist in week 1
        if key not in week1_positions_dict:
            excluded_count['missing'] += 1
            continue
        
        week1_position = week1_positions_dict[key]
        
        # Filter 1: Exclude cash and money market positions
        asset_type = position.get('asset_type', '').strip()
        if asset_type in excluded_asset_types:
            excluded_count['cash'] += 1
            logger.debug(f"Excluded cash/money market: {position.get('name', 'Unknown')} ({asset_type})")
            continue
        
        # Get market values and quantities
        week1_value = week1_position.get('market_value', 0)
        week2_value = position.get('market_value', 0)
        week1_qty = week1_position.get('quantity', 0)
        week2_qty = position.get('quantity', 0)
        
        # Filter 2: Require meaningful position sizes
        if week1_value < min_position_size or week2_value < min_position_size:
            excluded_count['small_size'] += 1
            logger.debug(f"Excluded small position: {position.get('name', 'Unknown')} (${week1_value:,.0f} -> ${week2_value:,.0f})")
            continue
        
        # Filter 3: Exclude positions with significant quantity changes (indicates transactions)
        if week1_qty != 0:
            qty_change_pct = abs(((week2_qty - week1_qty) / week1_qty) * 100)
            if qty_change_pct > max_quantity_change_pct:
                excluded_count['quantity_change'] += 1
                logger.debug(f"Excluded quantity change: {position.get('name', 'Unknown')} ({qty_change_pct:+.1f}% qty change)")
                continue
        elif week2_qty != 0:
            # New position (week1_qty = 0, week2_qty > 0) - this is a purchase
            excluded_count['quantity_change'] += 1
            logger.debug(f"Excluded new position: {position.get('name', 'Unknown')} (new purchase)")
            continue
        
        # Skip if either value is zero to avoid division by zero
        if week1_value == 0 or week2_value == 0:
            continue
        
        # Calculate price-based percentage change
        pct_change = ((week2_value - week1_value) / week1_value) * 100
        dollar_change = week2_value - week1_value
        
        # Create mover entry
        mover = {
            'name': position.get('name', 'Unknown'),
            'asset_type': asset_type,
            'pct_change': pct_change,
            'dollar_change': dollar_change,
            'abs_pct_change': abs(pct_change),  # For sorting
            'movement_type': 'Price Movement',  # Indicator that this is price-driven
            'week1_value': week1_value,
            'week2_value': week2_value
        }
        movers.append(mover)
        logger.debug(f"Included mover: {position.get('name', 'Unknown')} ({pct_change:+.2f}%)")
    
    # Sort by absolute dollar change (descending) - prioritizes financial impact over percentage
    movers.sort(key=lambda x: abs(x['dollar_change']), reverse=True)
    
    # Take top 'limit' movers
    top_movers = movers[:limit]
    
    # Log filtering results
    logger.info(f"Biggest movers filtering results:")
    logger.info(f"  - Total week2 positions: {len(week2_positions)}")
    logger.info(f"  - Excluded cash/money market: {excluded_count['cash']}")
    logger.info(f"  - Excluded quantity changes: {excluded_count['quantity_change']}")
    logger.info(f"  - Excluded small positions: {excluded_count['small_size']}")
    logger.info(f"  - Excluded missing in week1: {excluded_count['missing']}")
    logger.info(f"  - Final movers found: {len(movers)}")
    logger.info(f"  - Returning top {len(top_movers)} movers")
    
    return top_movers

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 5 and len(sys.argv) != 6:
        print("Usage: python generate_html_report.py week1_securities.xlsx week1_transactions.xlsx week2_securities.xlsx week2_transactions.xlsx [output_file.html]")
        sys.exit(1)
    
    week1_securities_file = sys.argv[1]
    week1_transactions_file = sys.argv[2]
    week2_securities_file = sys.argv[3]
    week2_transactions_file = sys.argv[4]
    
    output_html = sys.argv[5] if len(sys.argv) == 6 else "portfolio_report.html"
    
    generate_html_report(
        week1_securities_file,
        week1_transactions_file,
        week2_securities_file,
        week2_transactions_file,
        output_html
    ) 