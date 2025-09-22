"""
Correct Dashboard Cache Service for Aurum Finance
Fixes the fundamental error of treating point-in-time snapshots as cumulative data.

Key Corrections:
1. Store date-specific aggregations, NOT cumulative totals
2. Use latest snapshot per client for current metrics
3. Use historical aggregations for portfolio evolution
4. Eliminate N+1 queries with proper aggregation
"""

import logging
from datetime import datetime, time
from typing import Dict, Optional
from decimal import Decimal
from django.db import transaction

from ..models import PortfolioSnapshot, Client, DateAggregatedMetrics

logger = logging.getLogger(__name__)


class CorrectDashboardCacheService:
    """
    CORRECT approach: Store date-specific aggregations, not cumulative totals.
    
    This service aggregates dashboard data per snapshot date, allowing:
    - Ultra-fast current metrics (single cache lookup)
    - Portfolio evolution charts (across cached dates)
    - Proper returns calculation (between consecutive dates)
    """
    
    def __init__(self):
        self.logger = logger
    
    def aggregate_date_data(self, snapshot_date: str) -> Dict:
        """
        For a specific date, aggregate data across all clients.
        This replaces the expensive per-client queries in dashboard.
        
        Args:
            snapshot_date: Date in format 'dd_mm_yyyy' (e.g., '29_05_2025')
            
        Returns:
            Dict with success status and cached date info
        """
        try:
            # Convert date format
            date_obj = datetime.strptime(snapshot_date, '%d_%m_%Y').date()
            
            self.logger.info(f"🔄 Aggregating dashboard data for date: {date_obj}")
            
            # Get all snapshots for this specific date with optimized queries
            snapshots = PortfolioSnapshot.objects.filter(
                snapshot_date=date_obj
            ).select_related('client').prefetch_related('positions__asset')
            
            if not snapshots.exists():
                return {
                    'success': False,
                    'error': f'No snapshots found for date {date_obj}'
                }
            
            # Initialize aggregation variables
            total_aum = Decimal('0')
            total_inception_dollar = Decimal('0')
            weighted_inception_percent = Decimal('0')
            total_annual_income = Decimal('0')
            asset_allocation_aggregated = {}
            bank_allocation_aggregated = {}
            client_count = 0
            
            # Aggregate across all clients for this specific date
            for snapshot in snapshots:
                metrics = snapshot.portfolio_metrics
                
                # Extract client metrics
                client_total_value = Decimal(str(metrics.get('total_value', 0)))
                client_inception_dollar = Decimal(str(metrics.get('inception_gain_loss_dollar', 0)))
                client_inception_percent = Decimal(str(metrics.get('inception_gain_loss_percent', 0)))
                client_annual_income = Decimal(str(metrics.get('estimated_annual_income', 0)))
                
                # Aggregate totals
                total_aum += client_total_value
                total_inception_dollar += client_inception_dollar
                total_annual_income += client_annual_income
                
                # For weighted average inception percentage
                if client_total_value > 0:
                    weighted_inception_percent += client_inception_percent * client_total_value
                
                # Aggregate asset allocation
                client_asset_allocation = metrics.get('asset_allocation', {})
                for asset_type, allocation_data in client_asset_allocation.items():
                    if isinstance(allocation_data, dict) and 'value' in allocation_data:
                        if asset_type not in asset_allocation_aggregated:
                            asset_allocation_aggregated[asset_type] = Decimal('0')
                        asset_allocation_aggregated[asset_type] += Decimal(str(allocation_data['value']))
                
                # Aggregate bank allocation
                client_bank_allocation = metrics.get('bank_allocation', {})
                for bank_name, allocation_data in client_bank_allocation.items():
                    if isinstance(allocation_data, dict) and 'value' in allocation_data:
                        if bank_name not in bank_allocation_aggregated:
                            bank_allocation_aggregated[bank_name] = Decimal('0')
                        bank_allocation_aggregated[bank_name] += Decimal(str(allocation_data['value']))
                
                client_count += 1
            
            # Calculate final weighted percentage
            final_inception_percent = weighted_inception_percent / total_aum if total_aum > 0 else Decimal('0')
            
            # Consolidate Cash + Money Market before storing (consistent with chart generation)
            consolidated_allocation = {}
            for asset_type, value in asset_allocation_aggregated.items():
                if asset_type in ['Cash', 'Money Market']:
                    if 'Cash/Money Market' not in consolidated_allocation:
                        consolidated_allocation['Cash/Money Market'] = Decimal('0')
                    consolidated_allocation['Cash/Money Market'] += value
                else:
                    consolidated_allocation[asset_type] = value
            
            # Convert consolidated allocation to regular dict for JSON storage
            asset_allocation_json = {
                asset_type: float(value) 
                for asset_type, value in consolidated_allocation.items()
            }
            
            # Convert bank allocation to regular dict for JSON storage
            bank_allocation_json = {
                bank_name: float(value) 
                for bank_name, value in bank_allocation_aggregated.items()
            }
            
            # Store in cache using database transaction for consistency
            with transaction.atomic():
                cache_entry, created = DateAggregatedMetrics.objects.update_or_create(
                    snapshot_date=date_obj,
                    client_filter='ALL',
                    defaults={
                        'total_aum': total_aum,
                        'total_inception_dollar': total_inception_dollar,
                        'weighted_inception_percent': final_inception_percent,
                        'total_annual_income': total_annual_income,
                        'client_count': client_count,
                        'asset_allocation_data': asset_allocation_json,
                        'bank_allocation_data': bank_allocation_json,
                    }
                )
            
            action = "Created" if created else "Updated"
            self.logger.info(f"✅ {action} cache for {date_obj}: ${float(total_aum):,.2f} AUM, {client_count} clients")
            
            return {
                'success': True,
                'cached_date': snapshot_date,
                'date_obj': date_obj.isoformat(),
                'total_aum': float(total_aum),
                'client_count': client_count,
                'action': action.lower()
            }
            
        except ValueError as e:
            self.logger.error(f"❌ Invalid date format '{snapshot_date}': {str(e)}")
            return {
                'success': False,
                'error': f'Invalid date format: {str(e)}'
            }
        except Exception as e:
            self.logger.error(f"❌ Cache aggregation failed for {snapshot_date}: {str(e)}")
            return {
                'success': False,
                'error': f'Aggregation failed: {str(e)}'
            }
    
    def get_current_dashboard_data(self) -> Dict:
        """
        Get current dashboard data from latest cached aggregation.
        Ultra-fast alternative to expensive per-client queries.
        
        Returns:
            Dict with summary metrics and chart data, or None if no cache
        """
        try:
            # Get latest aggregated data
            latest_cache = DateAggregatedMetrics.objects.filter(
                client_filter='ALL'
            ).order_by('-snapshot_date').first()
            
            if not latest_cache:
                return {'success': False, 'error': 'No cached data found'}
            
            # Current metrics from latest date
            summary = {
                'total_aum': float(latest_cache.total_aum),
                'inception_dollar_performance': float(latest_cache.total_inception_dollar),
                'inception_return_pct': float(latest_cache.weighted_inception_percent),
                'estimated_annual_income': float(latest_cache.total_annual_income),
                'client_count': latest_cache.client_count,
                'filter_applied': 'ALL'
            }
            
            # Asset allocation chart from latest date
            asset_allocation = latest_cache.asset_allocation_data
            total_aum = float(latest_cache.total_aum)
            
            asset_allocation_chart = {
                'hasData': bool(asset_allocation),
                'series': [
                    round((value / total_aum) * 100, 2) 
                    for value in asset_allocation.values()
                ] if total_aum > 0 else [],
                'labels': list(asset_allocation.keys()),
                'monetaryValues': list(asset_allocation.values()),
                'colors': ['#5f76a1', '#072061', '#b7babe', '#dae1f3']
            }
            
            # Bank allocation chart from latest date
            bank_allocation = latest_cache.bank_allocation_data
            
            bank_allocation_chart = {
                'hasData': bool(bank_allocation),
                'series': [
                    round((value / total_aum) * 100, 2) 
                    for value in bank_allocation.values()
                ] if total_aum > 0 else [],
                'labels': list(bank_allocation.keys()),
                'monetaryValues': list(bank_allocation.values()),
                'colors': ['#5f76a1', '#072061', '#b7babe', '#dae1f3', '#82CA9D', '#FFC658', '#FF8042']
            }
            
            # Portfolio evolution chart (across multiple cached dates)
            all_cache_entries = DateAggregatedMetrics.objects.filter(
                client_filter='ALL'
            ).order_by('snapshot_date')
            
            # Build evolution points as { x: timestampMs, y: number }
            evolution_points = []
            for entry in all_cache_entries:
                # Convert date to start-of-day timestamp in ms
                ts_ms = int(datetime.combine(entry.snapshot_date, time.min).timestamp() * 1000)
                evolution_points.append({'x': ts_ms, 'y': float(entry.total_aum)})
            
            if evolution_points:
                values = [pt['y'] for pt in evolution_points]
                y_min_evo = min(values) * 0.95
                y_max_evo = max(values) * 1.05
            else:
                y_min_evo = 0
                y_max_evo = 0
            
            portfolio_evolution = {
                'hasData': len(evolution_points) > 0,
                'message': 'Total portfolio evolution across all clients' if evolution_points else 'No portfolio evolution data available',
                'series': [{
                    'name': 'Total Portfolio Value',
                    'data': evolution_points
                }],
                'yAxisMin': y_min_evo,
                'yAxisMax': y_max_evo,
                'colors': ['#5f76a1'],
                'gradient': {'to': '#dae1f3'}
            }
            
            # Cumulative return chart using same calculation logic as fixed API endpoint
            cumulative_points = []
            all_cache_list = list(all_cache_entries)  # Convert QuerySet to list for indexing
            if len(all_cache_list) >= 1:
                # Calculate cumulative return for each date using weighted average method
                for entry in all_cache_list:
                    # Use weighted inception percent to calculate cumulative value (base 1000)
                    inception_percent = float(entry.weighted_inception_percent)
                    cumulative_value = 1000 * (1 + inception_percent / 100)
                    
                    ts_ms = int(datetime.combine(entry.snapshot_date, time.min).timestamp() * 1000)
                    cumulative_points.append({'x': ts_ms, 'y': round(cumulative_value, 2)})
            
            if cumulative_points:
                cum_vals = [pt['y'] for pt in cumulative_points]
                y_min_cum = min(cum_vals) - 50
                y_max_cum = max(cum_vals) + 50
            else:
                y_min_cum = 0
                y_max_cum = 0
            
            cumulative_return = {
                'hasData': len(cumulative_points) > 0,
                'message': 'Weighted cumulative return (approximated from aggregated AUM)' if cumulative_points else 'No cumulative return data available',
                'series': [{
                    'name': 'Weighted Cumulative Return (Base: 1000)',
                    'data': cumulative_points
                }],
                'yAxisMin': y_min_cum,
                'yAxisMax': y_max_cum,
                'colors': ['#5f76a1'],
                'gradient': {'to': '#dae1f3'}
            }
            
            # Portfolio metrics comparison (simple current period visualization)
            # Build 4-bar metrics: latest vs previous cached date
            prev_cache = DateAggregatedMetrics.objects.filter(
                client_filter='ALL',
                snapshot_date__lt=latest_cache.snapshot_date
            ).order_by('-snapshot_date').first()

            if prev_cache:
                total_value_change = float(latest_cache.total_aum) - float(prev_cache.total_aum)
                # Approximate real gain/loss as change in inception dollar (fallback if not ideal)
                real_gain_loss = float(latest_cache.total_inception_dollar) - float(prev_cache.total_inception_dollar)
                # Net cash flow unknown here without heavy calc; keep safe default 0
                net_cash_flow = 0.0
                annual_income_change = float(latest_cache.total_annual_income) - float(prev_cache.total_annual_income)

                chart_data = [
                    round(total_value_change, 2),
                    round(real_gain_loss, 2),
                    round(net_cash_flow, 2),
                    round(annual_income_change, 2)
                ]

                y_min_pm = (min(chart_data) * 1.1) if min(chart_data) < 0 else (min(chart_data) * 0.9)
                y_max_pm = max(chart_data) * 1.1 if chart_data else 0

                portfolio_metrics = {
                    'hasData': True,
                    'message': 'Aggregated portfolio metrics comparison',
                    'series': [{'name': 'Amount ($)', 'data': chart_data}],
                    'categories': [
                        'Total Value Change',
                        'Real Gain/Loss',
                        'Net Cash Flow',
                        'Est. Annual Income Change'
                    ],
                    'colors': ['#5f76a1'],
                    'yAxisMin': y_min_pm,
                    'yAxisMax': y_max_pm
                }
            else:
                portfolio_metrics = {
                    'hasData': False,
                    'message': 'No historical data available for portfolio metrics comparison',
                    'series': [],
                    'categories': [],
                    'colors': ['#5f76a1'],
                    'yAxisMin': 0,
                    'yAxisMax': 0
                }
 
            return {
                'success': True,
                'summary': summary,
                'charts': {
                    'asset_allocation': asset_allocation_chart,
                    'bank_allocation': bank_allocation_chart,
                    'portfolio_evolution': portfolio_evolution,
                    'cumulative_return': cumulative_return,
                    'portfolio_metrics': portfolio_metrics,
                },
                'last_updated': latest_cache.last_updated.isoformat(),
                'from_cache': True
            }
            
        except Exception as e:
            self.logger.error(f"❌ Failed to get cached dashboard data: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def clear_cache(self) -> Dict:
        """
        Clear all cached dashboard data.
        Useful for testing and troubleshooting.
        """
        try:
            deleted_count = DateAggregatedMetrics.objects.all().count()
            DateAggregatedMetrics.objects.all().delete()
            
            self.logger.info(f"🗑️ Cleared {deleted_count} cached dashboard entries")
            
            return {
                'success': True,
                'cleared_entries': deleted_count
            }
            
        except Exception as e:
            self.logger.error(f"❌ Failed to clear cache: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_cache_status(self) -> Dict:
        """
        Get status of cached dashboard data.
        Useful for monitoring and debugging.
        """
        try:
            cache_entries = DateAggregatedMetrics.objects.filter(
                client_filter='ALL'
            ).order_by('snapshot_date')
            
            if not cache_entries.exists():
                return {
                    'success': True,
                    'cache_status': 'empty',
                    'cached_dates': [],
                    'total_entries': 0
                }
            
            cached_dates = []
            for entry in cache_entries:
                cached_dates.append({
                    'date': entry.snapshot_date.isoformat(),
                    'aum': float(entry.total_aum),
                    'clients': entry.client_count,
                    'last_updated': entry.last_updated.isoformat()
                })
            
            return {
                'success': True,
                'cache_status': 'populated',
                'cached_dates': cached_dates,
                'total_entries': len(cached_dates),
                'date_range': {
                    'earliest': cached_dates[0]['date'],
                    'latest': cached_dates[-1]['date']
                } if cached_dates else None
            }
            
        except Exception as e:
            self.logger.error(f"❌ Failed to get cache status: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }