"""
Django views for AurumFinance portfolio management.
Clean Django-only implementation with no ProjectAurum imports.
"""

from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.contrib.auth.decorators import login_required
from django.core.exceptions import ObjectDoesNotExist
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status

from .models import Client, PortfolioSnapshot, Position, Transaction, Report, ProcessingStatus
from .services.portfolio_population_service import PortfolioPopulationService
from .services.portfolio_calculation_service import PortfolioCalculationService
from .services.enhanced_report_service import EnhancedReportService
from .services.processing_service import ProcessingService
from .services.excel_export_service import ExcelExportService
from .permissions import IsAdminUser, IsClientUser

import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from django.conf import settings
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile

logger = logging.getLogger(__name__)

def generate_all_bond_issuer_weight_reports():
    """Generate Bond Issuer Weight reports for all clients with Fixed Income positions."""
    from .utils.report_utils import save_report_html
    from .services.bond_issuer_report_service import BondIssuerReportService
    from django.db import IntegrityError, transaction
    
    logger.info("Starting bulk Bond Issuer Weight report generation")
    
    try:
        # Delete existing bond issuer weight reports
        Report.objects.filter(report_type='BOND_ISSUER_WEIGHT').delete()
        logger.info("Deleted existing Bond Issuer Weight reports")
        
        # Get all clients with actual bonds (Fixed Income with maturity dates)
        # This excludes clients who only have ETFs and mutual funds
        clients_with_bonds = Client.objects.filter(
            snapshots__positions__asset__asset_type='Fixed Income',
            snapshots__positions__asset__maturity_date__isnull=False
        ).distinct()
        
        if not clients_with_bonds.exists():
            return Response({
                'success': False,
                'error': 'No clients with actual bond positions found'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # Initialize report service
        report_service = BondIssuerReportService()
        
        generated_reports = []
        failed_reports = []
        
        # Generate report for each client
        for client in clients_with_bonds:
            try:
                logger.info(f"Generating Bond Issuer Weight report for {client.code}")
                
                # Generate report HTML
                html_content = report_service.generate_bond_issuer_weight_report(client.code)
                
                # Save report file
                current_date = datetime.now().strftime('%Y-%m-%d')
                file_path, file_size = save_report_html(
                    client.code, 
                    'bond_issuer_weight', 
                    current_date,
                    html_content
                )
                
                # Create database record
                with transaction.atomic():
                    report = Report.objects.create(
                        client=client,
                        report_type='BOND_ISSUER_WEIGHT',
                        report_date=datetime.now().date(),
                        file_path=file_path,
                        file_size=file_size,
                        generation_time=0  # Could add timing if needed
                    )
                
                generated_reports.append({
                    'client_code': client.code,
                    'client_name': client.name,
                    'report_id': report.id,
                    'file_path': file_path
                })
                
                logger.info(f"Successfully generated Bond Issuer Weight report for {client.code}")
                
            except Exception as e:
                logger.error(f"Failed to generate Bond Issuer Weight report for {client.code}: {e}")
                failed_reports.append({
                    'client_code': client.code,
                    'error': str(e)
                })
        
        # Return results
        return Response({
            'success': True,
            'message': f'Generated {len(generated_reports)} Bond Issuer Weight reports',
            'generated_reports': generated_reports,
            'failed_reports': failed_reports,
            'total_generated': len(generated_reports),
            'total_failed': len(failed_reports)
        })
        
    except Exception as e:
        logger.error(f"Error in bulk Bond Issuer Weight report generation: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

def generate_all_cash_position_reports():
    """Generate Cash Position reports for all clients with cash positions."""
    from .utils.report_utils import save_report_html
    from .services.cash_report_service import CashReportService
    from django.db import IntegrityError, transaction
    
    logger.info("Starting bulk Cash Position report generation")
    
    try:
        # Delete existing cash position reports
        Report.objects.filter(report_type='CASH_POSITION').delete()
        logger.info("Deleted existing Cash Position reports")
        
        # Initialize report service
        report_service = CashReportService()
        
        generated_reports = []
        failed_reports = []
        
        # STEP 1: Generate consolidated report for "All Clients"
        try:
            logger.info("Generating consolidated Cash Position report")
            
            html_content = report_service.generate_cash_position_report('ALL', 'consolidated')
            
            # Save consolidated report file
            current_date = datetime.now().strftime('%Y-%m-%d')
            file_path, file_size = save_report_html(
                'ALL', 
                'cash_position_reports', 
                current_date,
                html_content
            )
            
            # Create database record for consolidated report (create ALL client)
            all_client, created = Client.objects.get_or_create(
                code='ALL',
                defaults={'name': 'All Clients'}
            )
            with transaction.atomic():
                report = Report.objects.create(
                    client=all_client,  # Use ALL client for consolidated reports
                    report_type='CASH_POSITION',
                    report_date=datetime.now().date(),
                    file_path=file_path,
                    file_size=file_size,
                    generation_time=0
                )
            
            generated_reports.append({
                'client_code': 'ALL',
                'client_name': 'All Clients',
                'report_id': report.id,
                'file_path': file_path
            })
            
            logger.info("Successfully generated consolidated Cash Position report")
            
        except Exception as e:
            logger.error(f"Failed to generate consolidated Cash Position report: {e}")
            failed_reports.append({
                'client_code': 'ALL',
                'error': str(e)
            })
        
        # STEP 2: Generate individual reports for each client with cash positions
        clients_with_cash = Client.objects.filter(
            snapshots__positions__asset__asset_type__in=['Cash', 'Money Market'],
            snapshots__positions__market_value__gt=0
        ).distinct()
        
        for client in clients_with_cash:
            try:
                logger.info(f"Generating Cash Position report for {client.code}")
                
                # Generate individual report HTML
                html_content = report_service.generate_cash_position_report(client.code, 'individual')
                
                # Save report file
                file_path, file_size = save_report_html(
                    client.code, 
                    'cash_position_reports', 
                    current_date,
                    html_content
                )
                
                # Create database record
                with transaction.atomic():
                    report = Report.objects.create(
                        client=client,
                        report_type='CASH_POSITION',
                        report_date=datetime.now().date(),
                        file_path=file_path,
                        file_size=file_size,
                        generation_time=0
                    )
                
                generated_reports.append({
                    'client_code': client.code,
                    'client_name': client.name,
                    'report_id': report.id,
                    'file_path': file_path
                })
                
                logger.info(f"Successfully generated Cash Position report for {client.code}")
                
            except Exception as e:
                logger.error(f"Failed to generate Cash Position report for {client.code}: {e}")
                failed_reports.append({
                    'client_code': client.code,
                    'error': str(e)
                })
        
        # Return results
        return Response({
            'success': True,
            'message': f'Generated {len(generated_reports)} Cash Position reports',
            'generated_reports': generated_reports,
            'failed_reports': failed_reports,
            'total_generated': len(generated_reports),
            'total_failed': len(failed_reports)
        })
        
    except Exception as e:
        logger.error(f"Error in bulk Cash Position report generation: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

def generate_all_clients_reports(current_date, comparison_date=None):
    """Generate weekly reports for all clients on given date with robust error handling."""
    from .utils.report_utils import save_report_html
    from .services.enhanced_report_service import EnhancedReportService
    from django.db import IntegrityError, transaction
    import time
    
    start_time = time.time()
    clients = Client.objects.all()
    results = []
    
    logger.info(f"Starting bulk generation for {len(clients)} clients on {current_date}")
    
    for client in clients:
        try:
            # Check database for existing report (source of truth)
            existing_report = Report.objects.filter(
                client=client,
                report_type='WEEKLY',
                report_date=current_date
            ).first()
            
            if not existing_report:
                logger.info(f"Generating report for client {client.code}")
                
                # Use transaction for atomicity
                with transaction.atomic():
                    report_service = EnhancedReportService()
                    html_content = report_service.generate_weekly_report(
                        client.code, current_date, comparison_date
                    )
                    
                    # Save report to file system
                    file_path, file_size = save_report_html(client.code, 'weekly', current_date, html_content)
                    
                    # Create Report database record
                    Report.objects.create(
                        client=client,
                        report_type='WEEKLY',
                        report_date=current_date,
                        file_path=file_path,
                        file_size=file_size,
                        generation_time=0
                    )
                
                results.append({'client': client.code, 'status': 'success'})
                logger.info(f"Successfully generated report for client {client.code}")
                
            else:
                results.append({'client': client.code, 'status': 'already_exists'})
                logger.info(f"Report already exists for client {client.code}")
                
        except IntegrityError as e:
            # Specific handling for unique constraint violations
            logger.warning(f"Integrity error for client {client.code}: {str(e)}")
            results.append({'client': client.code, 'status': 'already_exists_db', 'error': 'Database record already exists'})
            
        except Exception as e:
            logger.error(f"Failed to generate report for client {client.code}: {str(e)}")
            results.append({'client': client.code, 'status': 'error', 'error': str(e)})
    
    # Calculate totals
    success_count = len([r for r in results if r['status'] == 'success'])
    existing_count = len([r for r in results if r['status'] in ['already_exists', 'already_exists_db']])
    error_count = len([r for r in results if r['status'] == 'error'])
    total_time = time.time() - start_time
    
    logger.info(f"Bulk generation completed: {success_count} generated, {existing_count} already existed, {error_count} errors in {total_time:.2f}s")
    
    return Response({
        'status': 'success',
        'message': f'Bulk generation completed: {success_count} generated, {existing_count} already existed, {error_count} errors',
        'results': results,
        'summary': {
            'success': success_count,
            'already_exists': existing_count,
            'errors': error_count,
            'total': len(results),
            'generation_time': total_time
        },
        'no_auto_open': True  # Flag to prevent auto-opening
    })

# Portfolio Data API Endpoints

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def portfolio_summary(request, client_code=None):
    """Get portfolio summary for dashboard."""
    try:
        # If no client_code provided, use user's client_code (for client users)
        if not client_code:
            if request.user.role == 'client':
                client_code = request.user.client_code
            else:
                return Response({'error': 'Client code required for admin users'}, 
                              status=status.HTTP_400_BAD_REQUEST)
        
        # Check permissions
        if request.user.role == 'client' and request.user.client_code != client_code:
            return Response({'error': 'Access denied'}, status=status.HTTP_403_FORBIDDEN)
        
        # Get latest snapshot
        client = Client.objects.get(code=client_code)
        latest_snapshot = PortfolioSnapshot.objects.filter(
            client=client
        ).order_by('-snapshot_date').first()
        
        if not latest_snapshot:
            return Response({'error': 'No portfolio data found'}, 
                          status=status.HTTP_404_NOT_FOUND)
        
        calculation_service = PortfolioCalculationService()
        summary = calculation_service.get_portfolio_summary(
            client_code, latest_snapshot.snapshot_date
        )
        
        return Response(summary)
        
    except Client.DoesNotExist:
        return Response({'error': 'Client not found'}, status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        logger.error(f"Error getting portfolio summary: {e}")
        return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def portfolio_metrics(request, client_code, snapshot_date):
    """Get detailed portfolio metrics for a specific date."""
    try:
        # Check permissions
        if request.user.role == 'client' and request.user.client_code != client_code:
            return Response({'error': 'Access denied'}, status=status.HTTP_403_FORBIDDEN)
        
        client = Client.objects.get(code=client_code)
        snapshot = PortfolioSnapshot.objects.get(
            client=client, snapshot_date=snapshot_date
        )
        
        return Response(snapshot.portfolio_metrics)
        
    except (Client.DoesNotExist, PortfolioSnapshot.DoesNotExist):
        return Response({'error': 'Data not found'}, status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        logger.error(f"Error getting portfolio metrics: {e}")
        return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def available_snapshots(request, client_code=None):
    """Get list of available portfolio snapshots."""
    try:
        queryset = PortfolioSnapshot.objects.all()
        
        if client_code:
            # Check permissions
            if request.user.role == 'client' and request.user.client_code != client_code:
                return Response({'error': 'Access denied'}, status=status.HTTP_403_FORBIDDEN)
            
            client = Client.objects.get(code=client_code)
            queryset = queryset.filter(client=client)
        elif request.user.role == 'client':
            # Client users only see their own data
            client = Client.objects.get(code=request.user.client_code)
            queryset = queryset.filter(client=client)
        
        snapshots = queryset.select_related('client').order_by('-snapshot_date')
        
        data = [{
            'client_code': snapshot.client.code,
            'client_name': snapshot.client.name,
            'snapshot_date': snapshot.snapshot_date,
            'total_value': snapshot.portfolio_metrics.get('total_value', 0),
            'has_metrics': bool(snapshot.portfolio_metrics),
            'last_updated': snapshot.updated_at.isoformat()
        } for snapshot in snapshots]
        
        return Response(data)
        
    except Client.DoesNotExist:
        return Response({'error': 'Client not found'}, status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        logger.error(f"Error getting available snapshots: {e}")
        return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Database Update API Endpoints

@api_view(['POST'])
@permission_classes([IsAuthenticated, IsAdminUser])
def update_database(request):
    """Update database with latest Excel files."""
    try:
        data = request.data
        snapshot_date = data.get('snapshot_date', datetime.now().strftime('%Y-%m-%d'))
        
        # Check if processing is already in progress
        processing_status, created = ProcessingStatus.objects.get_or_create(
            process_type='database_update',
            defaults={'status': 'IDLE'}
        )
        
        if processing_status.status == 'PROCESSING':
            return Response({
                'success': False,
                'error': 'Database update is already in progress. Please wait for it to complete.',
                'status': 'PROCESSING',
                'message': processing_status.progress_message or 'Processing...'
            }, status=status.HTTP_409_CONFLICT)
        
        # Set status to processing
        processing_status.status = 'PROCESSING'
        processing_status.started_at = datetime.now()
        processing_status.progress_message = 'Initializing database update...'
        processing_status.error_message = ''
        processing_status.save()
        
        # Get file paths from request or build dynamically
        securities_file = data.get('securities_file')
        transactions_file = data.get('transactions_file')
        
        # If not provided, try to build from snapshot_date
        if not securities_file or not transactions_file:
            # Convert YYYY-MM-DD back to DD_MM_YYYY for file naming
            date_parts = snapshot_date.split('-')
            file_date = f"{date_parts[2]}_{date_parts[1]}_{date_parts[0]}"
            securities_file = f"data/excel/securities_{file_date}.xlsx"
            transactions_file = f"data/excel/transactions_{file_date}.xlsx"
        
        logger.info(f"Starting database update for date: {snapshot_date}")
        logger.info(f"Securities file: {securities_file}")
        logger.info(f"Transactions file: {transactions_file}")
        
        # Verify files exist
        if not Path(securities_file).exists():
            return Response({
                'success': False,
                'error': f'Securities file not found: {securities_file}'
            }, status=400)
            
        if not Path(transactions_file).exists():
            return Response({
                'success': False,
                'error': f'Transactions file not found: {transactions_file}'
            }, status=400)
        
        # Step 1: Populate from Excel
        processing_status.progress_message = 'Populating database from Excel files...'
        processing_status.save()
        
        population_service = PortfolioPopulationService()
        population_results = population_service.populate_from_excel(
            securities_file, transactions_file, snapshot_date
        )
        
        # Step 2: Apply enhanced rollover logic (BEFORE metrics calculation)
        processing_status.progress_message = 'Applying enhanced rollover logic...'
        processing_status.save()
        logger.info(f"🔄 SCENARIO 2: Enhanced rollover BEFORE metrics - Step 2")
        
        from .services.account_rollover_service import AccountRolloverService
        rollover_service = AccountRolloverService()
        
        try:
            missing_accounts = rollover_service.detect_missing_accounts(snapshot_date)
            
            rollover_results = {}
            rollover_summary = {
                'clients_processed': 0,
                'accounts_rolled': 0,
                'positions_copied': 0,
                'failures': 0
            }
            
            if missing_accounts:
                logger.info(f"🔄 Found {len(missing_accounts)} clients with missing accounts")
                
                for client_code, accounts in missing_accounts.items():
                    rollover_results[client_code] = {}
                    rollover_summary['clients_processed'] += 1
                    
                    for account_key, from_date in accounts.items():
                        bank, account = account_key.split('_', 1)
                        
                        try:
                            result = rollover_service.copy_account_positions(
                                client_code, bank, account, from_date, snapshot_date
                            )
                            
                            rollover_results[client_code][account_key] = result
                            
                            if result['success']:
                                rollover_summary['accounts_rolled'] += 1
                                rollover_summary['positions_copied'] += result.get('positions_copied', 0)
                            else:
                                rollover_summary['failures'] += 1
                                
                        except Exception as e:
                            logger.error(f"Rollover failed for {client_code} {bank}_{account}: {e}")
                            rollover_results[client_code][account_key] = {
                                'success': False,
                                'error': str(e),
                                'positions_copied': 0
                            }
                            rollover_summary['failures'] += 1
                
                logger.info(f"✅ Rollover complete: {rollover_summary}")
            else:
                logger.info("✅ No missing accounts detected - enhanced rollover logic working correctly")
                
        except Exception as e:
            logger.error(f"Rollover service error: {e}")
            rollover_results = {'error': str(e)}
        
        # Step 3: Cleanup phantom rollover positions (BEFORE metrics calculation)
        logger.info("🧹 SCENARIO 2: Running phantom rollover cleanup - Step 3")
        processing_status.progress_message = 'Cleaning phantom rollover positions...'
        processing_status.save()
        
        try:
            rollover_service._cleanup_phantom_rollover_positions(snapshot_date, set())
            logger.info("✅ Phantom rollover cleanup completed")
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
        
        # Step 4: Update PortfolioSnapshot rollover tracking for report alerts
        logger.info("📸 Updating snapshot rollover tracking for report alerts")
        
        # Get all clients with rollover positions on this date
        rollover_positions = Position.objects.filter(
            snapshot__snapshot_date=snapshot_date,
            is_rolled_over=True
        ).values('snapshot__client__code', 'bank', 'account').distinct()
        
        clients_with_rollover = set(pos['snapshot__client__code'] for pos in rollover_positions)
        
        for client_code in clients_with_rollover:
            # Build rollover summary for this client
            client_rollover_summary = {}
            for pos in rollover_positions:
                if pos['snapshot__client__code'] == client_code:
                    account_key = f"{pos['bank']}_{pos['account']}"
                    client_rollover_summary[account_key] = "rolled-over"
            
            # Update snapshot metadata
            try:
                snapshot = PortfolioSnapshot.objects.get(
                    client__code=client_code,
                    snapshot_date=snapshot_date
                )
                snapshot.has_rolled_accounts = True
                snapshot.rollover_summary = client_rollover_summary
                snapshot.save()
                
                logger.info(f"📸 Updated {client_code} rollover tracking: {len(client_rollover_summary)} accounts")
            except PortfolioSnapshot.DoesNotExist:
                logger.warning(f"📸 Snapshot not found for {client_code} on {snapshot_date}")
        
        logger.info(f"✅ Rollover tracking updated for {len(clients_with_rollover)} clients")
        
        # Step 5: Calculate metrics for all clients (AFTER rollover and cleanup)
        processing_status.progress_message = 'Calculating portfolio metrics...'
        processing_status.save()
        calculation_service = PortfolioCalculationService()
        calculation_results = {}
        
        for client in Client.objects.all():
            try:
                metrics = calculation_service.calculate_portfolio_metrics(
                    client.code, snapshot_date
                )
                calculation_results[client.code] = {
                    'success': True,
                    'total_value': metrics['total_value'],
                    'position_count': metrics['position_count']
                }
            except Exception as e:
                logger.error(f"Error calculating metrics for {client.code}: {e}")
                calculation_results[client.code] = {
                    'success': False,
                    'error': str(e)
                }
        
        # Mark processing as completed
        processing_status.status = 'IDLE'
        processing_status.completed_at = datetime.now()
        processing_status.progress_message = 'Database update completed successfully'
        processing_status.save()
        
        # Step 5: Update dashboard cache with CORRECT logic
        try:
            from .services.correct_dashboard_cache_service import CorrectDashboardCacheService
            cache_service = CorrectDashboardCacheService()
            
            # Convert date format: 'YYYY-MM-DD' -> 'DD_MM_YYYY' for cache service
            if '-' in snapshot_date:
                date_parts = snapshot_date.split('-')
                cache_date_format = f"{date_parts[2]}_{date_parts[1]}_{date_parts[0]}"
            else:
                cache_date_format = snapshot_date  # Already in correct format
                
            cache_result = cache_service.aggregate_date_data(cache_date_format)
            
            if cache_result['success']:
                logger.info(f"✅ Dashboard cache aggregated correctly for {snapshot_date}")
                logger.info(f"   - AUM: ${cache_result['total_aum']:,.2f}")
                logger.info(f"   - Clients: {cache_result['client_count']}")
            else:
                logger.warning(f"⚠️  Dashboard cache aggregation failed: {cache_result.get('error')}")
                
        except Exception as e:
            logger.error(f"⚠️  Dashboard cache aggregation error: {str(e)}")
        
        return Response({
            'success': True,
            'snapshot_date': snapshot_date,
            'population_results': population_results,
            'calculation_results': calculation_results,
            'rollover': {
                'applied': len(rollover_results) > 0 if 'rollover_results' in locals() else False,
                'summary': rollover_summary,
                'details': rollover_results if 'rollover_results' in locals() else {}
            },
            'message': f'Database updated successfully for {len(calculation_results)} clients'
        })
        
    except Exception as e:
        logger.error(f"Error updating database: {e}")
        
        # Mark processing as failed
        try:
            processing_status.status = 'ERROR'
            processing_status.completed_at = datetime.now()
            processing_status.error_message = str(e)
            processing_status.progress_message = 'Database update failed'
            processing_status.save()
        except:
            pass  # Don't let status update errors mask the original error
        
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@permission_classes([IsAuthenticated, IsAdminUser])
def processing_status(request):
    """Get current processing status for UX updates."""
    try:
        status_obj = ProcessingStatus.objects.get(process_type='database_update')
        return Response({
            'success': True,
            'status': status_obj.status,
            'progress_message': status_obj.progress_message,
            'started_at': status_obj.started_at,
            'completed_at': status_obj.completed_at,
            'error_message': status_obj.error_message
        })
    except ProcessingStatus.DoesNotExist:
        return Response({
            'success': True,
            'status': 'IDLE',
            'progress_message': '',
            'started_at': None,
            'completed_at': None,
            'error_message': ''
        })
    except Exception as e:
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['POST'])
@permission_classes([IsAuthenticated, IsAdminUser])
def recalculate_metrics(request):
    """Recalculate metrics for existing snapshots."""
    try:
        data = request.data
        client_code = data.get('client_code')
        snapshot_date = data.get('snapshot_date')
        
        calculation_service = PortfolioCalculationService()
        
        if client_code and snapshot_date:
            # Recalculate for specific client and date
            metrics = calculation_service.calculate_portfolio_metrics(
                client_code, snapshot_date
            )
            return Response({
                'success': True,
                'client_code': client_code,
                'snapshot_date': snapshot_date,
                'metrics': metrics
            })
        else:
            # Recalculate for all recent snapshots
            results = {}
            snapshots = PortfolioSnapshot.objects.select_related('client').order_by('-snapshot_date')[:10]
            
            for snapshot in snapshots:
                try:
                    metrics = calculation_service.calculate_portfolio_metrics(
                        snapshot.client.code, snapshot.snapshot_date
                    )
                    results[f"{snapshot.client.code}_{snapshot.snapshot_date}"] = {
                        'success': True,
                        'total_value': metrics['total_value']
                    }
                except Exception as e:
                    results[f"{snapshot.client.code}_{snapshot.snapshot_date}"] = {
                        'success': False,
                        'error': str(e)
                    }
            
            return Response({
                'success': True,
                'results': results,
                'message': f'Recalculated metrics for {len(results)} snapshots'
            })
        
    except Exception as e:
        logger.error(f"Error recalculating metrics: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_available_dates_by_type(request, report_type):
    """Get available dates for any report type, filtered by client."""
    client_code = request.GET.get('client_code')
    
    # Map report types to database enum values
    report_type_mapping = {
        'weekly_investment': 'WEEKLY',
        'bond_maturity': 'BOND_MATURITY',
        'bond_issuer_weight': 'BOND_ISSUER_WEIGHT',
        'equity_breakdown': 'EQUITY_BREAKDOWN',
        'cash_position': 'CASH_POSITION'
    }
    
    db_report_type = report_type_mapping.get(report_type)
    if not db_report_type:
        return Response({'error': f'Unknown report type: {report_type}'}, status=400)
    
    # Get all snapshot dates
    snapshot_dates = PortfolioSnapshot.objects.values_list('snapshot_date', flat=True).distinct()
    
    if client_code and client_code != 'ALL':
        # For specific client: exclude only THEIR existing reports
        try:
            client_instance = Client.objects.get(code=client_code)
            existing_report_dates = Report.objects.filter(
                report_type=db_report_type,
                client=client_instance
            ).values_list('report_date', flat=True)
        except Client.DoesNotExist:
            return Response({'error': f'Client {client_code} not found'}, status=404)
    else:
        # For "All Clients": show all snapshot dates
        existing_report_dates = []
    
    available_dates = [date.strftime('%d/%m/%Y') for date in snapshot_dates
                      if date not in existing_report_dates]
    
    return Response({
        'success': True,
        'available_dates': available_dates,
        'client_code': client_code,
        'report_type': report_type
    })

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def list_generated_reports_by_type(request, report_type):
    """List generated reports for any report type."""
    client_code = request.GET.get('client_code')

    # Auto-scope for client users regardless of query to prevent cross-client access
    if hasattr(request.user, 'role') and getattr(request.user, 'role', None) == 'client':
        client_code = request.user.client_code
    
    # Map report types to database enum values
    report_type_mapping = {
        'weekly_investment': 'WEEKLY',
        'bond_maturity': 'BOND_MATURITY',
        'bond_issuer_weight': 'BOND_ISSUER_WEIGHT',
        'equity_breakdown': 'EQUITY_BREAKDOWN',
        'cash_position': 'CASH_POSITION'
    }
    
    db_report_type = report_type_mapping.get(report_type)
    if not db_report_type:
        return Response({'error': f'Unknown report type: {report_type}'}, status=400)
    
    # Build filter
    filters = {'report_type': db_report_type}
    if client_code and client_code != 'all-clients':
        try:
            client_instance = Client.objects.get(code=client_code)
            filters['client'] = client_instance
        except Client.DoesNotExist:
            return Response({'error': f'Client {client_code} not found'}, status=404)
    
    # Get reports
    reports = Report.objects.filter(**filters).select_related('client').order_by('client__code')
    
    reports_data = []
    for report in reports:
        reports_data.append({
            'id': report.id,
            'client_code': report.client.code,
            'client_name': report.client.name,
            'report_date': report.report_date.strftime('%d/%m/%Y'),
            'file_path': report.file_path,
            'file_size': report.file_size,
            'generation_time': report.generation_time,
            'created_at': report.created_at.isoformat()
        })
    
    # Custom sort: ALL first for cash reports, then alphabetical
    if db_report_type == 'CASH_POSITION':
        reports_data.sort(key=lambda r: (r['client_code'] != 'ALL', r['client_code']))
    else:
        reports_data.sort(key=lambda r: r['client_code'])
    
    return Response({
        'success': True,
        'reports': reports_data,
        'count': len(reports_data),
        'report_type': report_type,
        'client_code': client_code
    })

# Report Generation API Endpoints

def parse_flexible_date(date_string):
    """Parse date from multiple formats and return YYYY-MM-DD string for database operations."""
    if not date_string:
        return None
    
    from datetime import datetime
    
    # List of supported date formats
    date_formats = ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%d.%m.%Y']
    
    for fmt in date_formats:
        try:
            parsed_date = datetime.strptime(date_string, fmt)
            return parsed_date.strftime('%Y-%m-%d')  # Always return in database format
        except ValueError:
            continue
    
    # If no format matches, return original (for error handling downstream)
    return date_string

@api_view(['POST'])
@permission_classes([IsAuthenticated])
def generate_report_no_open(request):
    """Generate report without returning HTML content for viewing."""
    import time
    from .utils.report_utils import report_exists, save_report_html
    from .models import Client, Report
    
    start_time = time.time()
    
    try:
        data = request.data
        report_type = data.get('report_type', 'weekly_investment')
        client_code = data.get('client_code')
        current_date = data.get('current_date')
        comparison_date = data.get('comparison_date')
        
        # Parse dates to ensure they're in the correct format for database operations
        if current_date and current_date.strip():
            current_date = parse_flexible_date(current_date)
        else:
            current_date = datetime.now().date()  # Use today's date if not provided
        if comparison_date and comparison_date.strip():
            comparison_date = parse_flexible_date(comparison_date)
        
        # Check permissions
        if hasattr(request.user, 'role') and request.user.role == 'client':
            if not client_code:
                client_code = request.user.client_code
            elif client_code != request.user.client_code:
                return Response({'error': 'Access denied'}, status=status.HTTP_403_FORBIDDEN)
        
        # Map report types to database enum values
        report_type_mapping = {
            'weekly_investment': 'WEEKLY',
            'bond_maturity': 'BOND_MATURITY',
            'bond_issuer_weight': 'BOND_ISSUER_WEIGHT',
            'equity_breakdown': 'EQUITY_BREAKDOWN',
            'cash_position': 'CASH_POSITION'
        }
        
        db_report_type = report_type_mapping.get(report_type)
        if not db_report_type:
            return Response({'error': f'Unknown report type: {report_type}'}, 
                          status=status.HTTP_400_BAD_REQUEST)
        
        # For weekly reports, require current_date
        if report_type == 'weekly_investment' and not current_date:
            return Response({'error': 'current_date is required for weekly investment reports'}, 
                          status=status.HTTP_400_BAD_REQUEST)
        
        # Check if report already exists
        if report_exists(client_code, 'weekly' if report_type == 'weekly_investment' else report_type, current_date):
            return Response({'error': f'Report already exists for {client_code or "ALL_CLIENTS"} on {current_date}'}, 
                          status=status.HTTP_400_BAD_REQUEST)
        
        # Generate report based on type
        html_content = None
        
        if report_type == 'weekly_investment':
            if not client_code or client_code == 'ALL':
                # Generate for all clients
                return generate_all_clients_reports(current_date, comparison_date)
            else:
                # Generate for specific client (existing logic)
                report_service = EnhancedReportService()
                if current_date:
                    html_content = report_service.generate_weekly_report(
                        client_code, current_date, comparison_date
                    )
                else:
                    html_content = report_service.generate_report_for_client(client_code)
                
        elif report_type == 'bond_maturity':
            from .services.report_generation_service import ReportGenerationService
            report_service = ReportGenerationService()
            result = report_service.generate_bond_maturity_report(current_date or '2025-07-24', client_code)
            if result.get('success'):
                html_content = f"<html><body><h1>Bond Maturity Report</h1><p>{result.get('message', 'Report generated successfully')}</p></body></html>"
            else:
                return Response({'success': False, 'error': result.get('error', 'Bond maturity report generation failed')})
                
        elif report_type == 'bond_issuer_weight':
            if not client_code or client_code == 'ALL':
                # Generate for all clients (bulk generation)
                return generate_all_bond_issuer_weight_reports()
            else:
                # Generate for specific client
                from .services.bond_issuer_report_service import BondIssuerReportService
                report_service = BondIssuerReportService()
                html_content = report_service.generate_bond_issuer_weight_report(client_code)
                
        elif report_type == 'cash_position':
            if not client_code or client_code == 'ALL':
                # Generate for all clients (bulk generation)
                return generate_all_cash_position_reports()
            else:
                # Generate for specific client
                from .services.cash_report_service import CashReportService
                report_service = CashReportService()
                html_content = report_service.generate_cash_position_report(client_code, 'individual')
                
        elif report_type == 'equity_breakdown':
            from .services.report_generation_service import ReportGenerationService
            report_service = ReportGenerationService()
            result = report_service.generate_equity_breakdown_report(current_date or '2025-07-24', client_code)
            if result.get('success'):
                html_content = f"<html><body><h1>Equity Breakdown Report</h1><p>{result.get('message', 'Report generated successfully')}</p></body></html>"
            else:
                return Response({'success': False, 'error': result.get('error', 'Equity breakdown report generation failed')})
        
        if not html_content:
            return Response({'error': 'Failed to generate report content'}, 
                          status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        # Save report to organized file structure
        report_type_for_file = 'weekly' if report_type == 'weekly_investment' else report_type
        relative_path, file_size = save_report_html(
            client_code, 
            report_type_for_file, 
            current_date, 
            html_content
        )
        
        # Calculate generation time
        generation_time = time.time() - start_time
        
        # Create database record
        client_instance = None
        if client_code:
            try:
                client_instance = Client.objects.get(code=client_code)
            except Client.DoesNotExist:
                return Response({'error': f'Client not found: {client_code}'}, 
                              status=status.HTTP_404_NOT_FOUND)
        else:
            # For ALL_CLIENTS reports, we'll need to handle this differently
            # For now, let's create a virtual client or use the first client
            client_instance = Client.objects.first()
        
        # Create Report record
        report_record = Report.objects.create(
            client=client_instance,
            report_type=db_report_type,
            report_date=current_date,
            file_path=relative_path,
            file_size=file_size,
            generation_time=generation_time
        )
        
        logger.info(f"Report generated and saved: {relative_path} (ID: {report_record.id})")
        
        return Response({
            'status': 'success',
            'report_id': report_record.id,
            'file_path': relative_path,
            'message': f'Report generated successfully for {client_code or "ALL_CLIENTS"}',
            # NO html_content field = no auto-opening
        })
        
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        return Response({
            'status': 'error',
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['POST'])
@permission_classes([IsAuthenticated])
def generate_report(request):
    """Generate HTML report from Django data and save to organized file structure."""
    import time
    from .utils.report_utils import report_exists, save_report_html
    from .models import Client, Report
    
    start_time = time.time()
    
    try:
        data = request.data
        report_type = data.get('report_type', 'weekly_investment')
        client_code = data.get('client_code')
        current_date = data.get('current_date')
        comparison_date = data.get('comparison_date')
        
        # Check permissions
        if hasattr(request.user, 'role') and request.user.role == 'client':
            if not client_code:
                client_code = request.user.client_code
            elif client_code != request.user.client_code:
                return Response({'error': 'Access denied'}, status=status.HTTP_403_FORBIDDEN)
        
        # Map report types to database enum values
        report_type_mapping = {
            'weekly_investment': 'WEEKLY',
            'bond_maturity': 'BOND_MATURITY',
            'bond_issuer_weight': 'BOND_ISSUER_WEIGHT',
            'equity_breakdown': 'EQUITY_BREAKDOWN',
            'cash_position': 'CASH_POSITION'
        }
        
        db_report_type = report_type_mapping.get(report_type)
        if not db_report_type:
            return Response({'error': f'Unknown report type: {report_type}'}, 
                          status=status.HTTP_400_BAD_REQUEST)
        
        # For weekly reports, require current_date
        if report_type == 'weekly_investment' and not current_date:
            return Response({'error': 'current_date is required for weekly investment reports'}, 
                          status=status.HTTP_400_BAD_REQUEST)
        
        # Check if report already exists
        if report_exists(client_code, 'weekly' if report_type == 'weekly_investment' else report_type, current_date):
            return Response({'error': f'Report already exists for {client_code or "ALL_CLIENTS"} on {current_date}'}, 
                          status=status.HTTP_400_BAD_REQUEST)
        
        # Generate report based on type
        html_content = None
        
        if report_type == 'weekly_investment':
            if not client_code or client_code == 'ALL':
                # Generate for all clients
                return generate_all_clients_reports(current_date, comparison_date)
            else:
                # Generate for specific client (existing logic)
                report_service = EnhancedReportService()
                if current_date:
                    html_content = report_service.generate_weekly_report(
                        client_code, current_date, comparison_date
                    )
                else:
                    html_content = report_service.generate_report_for_client(client_code)
                
        elif report_type == 'bond_maturity':
            from .services.report_generation_service import ReportGenerationService
            report_service = ReportGenerationService()
            result = report_service.generate_bond_maturity_report(current_date or '2025-07-24', client_code)
            if result.get('success'):
                html_content = f"<html><body><h1>Bond Maturity Report</h1><p>{result.get('message', 'Report generated successfully')}</p></body></html>"
            else:
                return Response({'success': False, 'error': result.get('error', 'Bond maturity report generation failed')})
                
        elif report_type == 'bond_issuer_weight':
            from .services.report_generation_service import ReportGenerationService
            report_service = ReportGenerationService()
            result = report_service.generate_bond_issuer_report(current_date or '2025-07-24', client_code)
            if result.get('success'):
                html_content = f"<html><body><h1>Bond Issuer Weight Report</h1><p>{result.get('message', 'Report generated successfully')}</p></body></html>"
            else:
                return Response({'success': False, 'error': result.get('error', 'Bond issuer report generation failed')})
                
        elif report_type == 'cash_position':
            from .services.cash_report_service import CashReportService
            report_service = CashReportService()
            if client_code == 'ALL':
                html_content = report_service.generate_cash_position_report('ALL', 'consolidated')
            else:
                html_content = report_service.generate_cash_position_report(client_code, 'individual')
                
        elif report_type == 'equity_breakdown':
            from .services.report_generation_service import ReportGenerationService
            report_service = ReportGenerationService()
            result = report_service.generate_equity_breakdown_report(current_date or '2025-07-24', client_code)
            if result.get('success'):
                html_content = f"<html><body><h1>Equity Breakdown Report</h1><p>{result.get('message', 'Report generated successfully')}</p></body></html>"
            else:
                return Response({'success': False, 'error': result.get('error', 'Equity breakdown report generation failed')})
                
        elif report_type == 'cash_position':
            from .services.cash_report_service import CashReportService
            report_service = CashReportService()
            
            # Determine if this is a consolidated report (client_code == 'ALL' or None)
            report_subtype = 'consolidated' if (not client_code or client_code == 'ALL') else 'individual'
            
            try:
                if report_subtype == 'consolidated':
                    html_content = report_service.generate_cash_position_report('ALL', 'consolidated')
                else:
                    html_content = report_service.generate_cash_position_report(client_code, 'individual')
            except Exception as e:
                return Response({'success': False, 'error': f'Cash position report generation failed: {str(e)}'})
        
        if not html_content:
            return Response({'error': 'Failed to generate report content'}, 
                          status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        # Save report to organized file structure
        report_type_for_file = 'weekly' if report_type == 'weekly_investment' else report_type
        relative_path, file_size = save_report_html(
            client_code, 
            report_type_for_file, 
            current_date, 
            html_content
        )
        
        # Calculate generation time
        generation_time = time.time() - start_time
        
        # Create database record
        client_instance = None
        if client_code:
            try:
                client_instance = Client.objects.get(code=client_code)
            except Client.DoesNotExist:
                return Response({'error': f'Client not found: {client_code}'}, 
                              status=status.HTTP_404_NOT_FOUND)
        else:
            # For ALL_CLIENTS reports, we'll need to handle this differently
            # For now, let's create a virtual client or use the first client
            client_instance = Client.objects.first()
        
        # Create Report record
        report_record = Report.objects.create(
            client=client_instance,
            report_type=db_report_type,
            report_date=current_date,
            file_path=relative_path,
            file_size=file_size,
            generation_time=generation_time
        )
        
        logger.info(f"Report generated and saved: {relative_path} (ID: {report_record.id})")
        
        return Response({
            'success': True,
            'html_content': html_content,
            'report_type': report_type,
            'client_code': client_code,
            'report_date': current_date,
            'report_id': report_record.id,
            'file_path': relative_path,
            'file_size': file_size,
            'generation_time': generation_time,
            'message': f'Report generated and saved to {relative_path}'
        })
        
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def available_weekly_report_dates(request):
    """Get snapshot dates available for weekly report generation."""
    try:
        client_code = request.GET.get('client_code')
        
        # Check permissions
        if hasattr(request.user, 'role') and request.user.role == 'client':
            client_code = request.user.client_code
        
        # Get all snapshot dates
        from .models import PortfolioSnapshot, Report, Client
        snapshot_dates = PortfolioSnapshot.objects.values_list('snapshot_date', flat=True).distinct().order_by('snapshot_date')
        
        # NEW LOGIC: Handle different client scenarios
        if client_code and client_code != 'ALL':
            # For SPECIFIC client: exclude only THEIR existing reports
            try:
                client_instance = Client.objects.get(code=client_code)
                existing_report_dates = Report.objects.filter(
                    report_type='WEEKLY',
                    client=client_instance  # Only this client's reports
                ).values_list('report_date', flat=True)
            except Client.DoesNotExist:
                return Response({'error': f'Client not found: {client_code}'}, 
                              status=status.HTTP_404_NOT_FOUND)
        else:
            # For "ALL" clients or no client specified: show all snapshot dates
            # They can always generate because it creates individual reports
            existing_report_dates = []
        
        # Return available dates (snapshot dates minus existing report dates)
        available_dates = [
            date.strftime('%Y-%m-%d') for date in snapshot_dates 
            if date not in existing_report_dates
        ]
        
        return Response({
            'success': True,
            'available_dates': available_dates,
            'client_code': client_code,
            'total_snapshots': len(snapshot_dates),
            'total_available': len(available_dates)
        })
        
    except Exception as e:
        logger.error(f"Error getting available dates: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def list_generated_weekly_reports(request):
    """Get list of generated weekly reports for browsing."""
    try:
        client_code = request.GET.get('client_code')
        
        # Check permissions
        if hasattr(request.user, 'role') and request.user.role == 'client':
            client_code = request.user.client_code
        
        from .models import Report, Client
        
        # Build filter for weekly reports
        reports_filter = {'report_type': 'WEEKLY'}
        if client_code:
            try:
                client_instance = Client.objects.get(code=client_code)
                reports_filter['client'] = client_instance
            except Client.DoesNotExist:
                return Response({'error': f'Client not found: {client_code}'}, 
                              status=status.HTTP_404_NOT_FOUND)
        
        # Get reports with client information
        reports = Report.objects.filter(**reports_filter).select_related('client').order_by('-report_date')
        
        report_list = []
        for report in reports:
            report_list.append({
                'id': report.id,
                'client_code': report.client.code,
                'client_name': report.client.name,
                'report_date': report.report_date.strftime('%Y-%m-%d'),
                'file_path': report.file_path,
                'file_size': report.file_size,
                'generation_time': report.generation_time,
                'created_at': report.created_at.isoformat()
            })
        
        return Response({
            'success': True,
            'reports': report_list,
            'total_count': len(report_list),
            'client_code': client_code
        })
        
    except Exception as e:
        logger.error(f"Error listing generated reports: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def serve_report_file(request, report_id):
    """Serve HTML content of a saved report file."""
    try:
        from .models import Report
        from .utils.report_utils import load_report_html
        
        # Get report record
        try:
            report = Report.objects.select_related('client').get(id=report_id)
        except Report.DoesNotExist:
            return Response({'error': 'Report not found'}, 
                          status=status.HTTP_404_NOT_FOUND)
        
        # Check permissions
        if hasattr(request.user, 'role') and request.user.role == 'client':
            if request.user.client_code != report.client.code:
                return Response({'error': 'Access denied'}, 
                              status=status.HTTP_403_FORBIDDEN)
        
        # Load HTML content from file
        try:
            html_content = load_report_html(report.file_path)
        except FileNotFoundError:
            return Response({'error': 'Report file not found on disk'}, 
                          status=status.HTTP_404_NOT_FOUND)
        
        return Response({
            'success': True,
            'html_content': html_content,
            'report_id': report.id,
            'client_code': report.client.code,
            'client_name': report.client.name,
            'report_date': report.report_date.strftime('%Y-%m-%d'),
            'report_type': report.report_type,
            'file_path': report.file_path
        })
        
    except Exception as e:
        logger.error(f"Error serving report file: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def available_reports(request):
    """Get list of available reports."""
    try:
        client_code = request.GET.get('client_code')
        
        # Check permissions
        if request.user.role == 'client':
            client_code = request.user.client_code
        
        report_service = EnhancedReportService()
        reports = report_service.get_available_reports(client_code)
        
        return Response(reports)
        
    except Exception as e:
        logger.error(f"Error getting available reports: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Admin Dashboard API Endpoints

@api_view(['GET'])
@permission_classes([IsAuthenticated, IsAdminUser])
def admin_dashboard_data(request):
    """
    ULTRA-FAST admin dashboard using CORRECT pre-aggregated cache.
    Falls back to real-time calculation if cache fails.
    
    Performance: <500ms (vs 10+ seconds without cache)
    Supports client filtering via ?client_code=ABC or ?client_code=ALL
    """
    try:
        client_filter = request.GET.get('client_code') or request.GET.get('client', 'ALL')
        logger.info(f"Admin dashboard request with filter: {client_filter} (using CORRECT cache)")
        
        if client_filter == 'ALL':
            # Try CORRECT cache first for ultra-fast performance
            from .services.correct_dashboard_cache_service import CorrectDashboardCacheService
            cache_service = CorrectDashboardCacheService()
            cache_result = cache_service.get_current_dashboard_data()
            
            if cache_result.get('success'):
                # annotate filter applied
                if 'summary' in cache_result:
                    cache_result['summary']['filter_applied'] = 'ALL'
                logger.info(f"✅ Dashboard data served from CORRECT cache in <500ms")
                return Response(cache_result)
            else:
                # Fallback to original logic if cache fails
                logger.warning(f"⚠️ Cache failed ({cache_result.get('error')}), falling back to real-time calculation")
                # Convert DRF Request to Django HttpRequest for service compatibility
                django_request = request._request if hasattr(request, '_request') else request
                return admin_dashboard_data_original(django_request, client_filter)
        else:
            # Individual client requests use original logic for now
            logger.info(f"Individual client request ({client_filter}), using real-time calculation")
            # Convert DRF Request to Django HttpRequest for service compatibility
            django_request = request._request if hasattr(request, '_request') else request
            return admin_dashboard_data_original(django_request, client_filter)
            
    except Exception as e:
        logger.error(f"Error getting admin dashboard data: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated, IsAdminUser])
def admin_dashboard_data_original(request, client_filter=None):
    """
    ORIGINAL admin dashboard logic preserved as fallback.
    Only used when cache fails - ensures system always works.
    """
    try:
        # Get client filter if not provided (handle DRF Request properly)
        if client_filter is None:
            client_filter = request.query_params.get('client_code') or request.query_params.get('client', 'ALL')
        
        logger.info(f"Admin dashboard FALLBACK calculation with filter: {client_filter}")
        
        # Filter clients based on request
        if client_filter and client_filter != 'ALL':
            clients = Client.objects.filter(code=client_filter)
        else:
            clients = Client.objects.all()
        
        # Initialize aggregated metrics
        total_aum = 0
        total_inception_dollar = 0
        weighted_inception_percent = 0
        total_annual_income = 0
        clients_data = []
        asset_allocation_aggregated = {}
        
        # Calculate aggregated metrics from each client
        for client in clients:
            latest_snapshot = PortfolioSnapshot.objects.filter(
                client=client
            ).order_by('-snapshot_date').first()
            
            if latest_snapshot:
                metrics = latest_snapshot.portfolio_metrics
                
                # Individual client metrics
                client_total_value = float(metrics.get('total_value', 0))
                client_inception_dollar = float(metrics.get('inception_gain_loss_dollar', 0))
                client_inception_percent = float(metrics.get('inception_gain_loss_percent', 0))
                client_annual_income = float(metrics.get('estimated_annual_income', 0))
                
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
                            asset_allocation_aggregated[asset_type] = 0
                        asset_allocation_aggregated[asset_type] += float(allocation_data['value'])
                
                clients_data.append({
                    'client_code': client.code,
                    'client_name': client.name,
                    'total_value': client_total_value,
                    'inception_dollar': client_inception_dollar,
                    'inception_percent': client_inception_percent,
                    'annual_income': client_annual_income,
                    'asset_allocation': client_asset_allocation,
                    'last_updated': latest_snapshot.updated_at.isoformat(),
                    'snapshot_date': latest_snapshot.snapshot_date.isoformat()
                })
        
        # Calculate weighted average inception percentage
        final_inception_percent = weighted_inception_percent / total_aum if total_aum > 0 else 0
        
        # Generate chart data structures
        chart_data = _generate_admin_chart_data(clients_data, asset_allocation_aggregated, client_filter)
        
        # Prepare summary in the format frontend expects
        summary = {
            'total_aum': total_aum,
            'inception_dollar_performance': total_inception_dollar,
            'inception_return_pct': final_inception_percent,
            'estimated_annual_income': total_annual_income,
            'client_count': len(clients_data),
            'filter_applied': client_filter
        }
        
        response_data = {
            'summary': summary,
            'charts': chart_data,
            'clients_data': clients_data,  # For admin analysis
            'last_updated': datetime.now().isoformat()
        }
        
        logger.info(f"Admin dashboard data generated: {len(clients_data)} clients, ${total_aum:,.2f} AUM")
        return Response(response_data)
        
    except Exception as e:
        logger.error(f"Error getting admin dashboard data: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated, IsClientUser])
def client_dashboard_with_charts(request):
    """
    Client dashboard with full metrics and chart data.
    Returns same structure as admin dashboard but filtered to client's own data only.
    Uses identical calculation logic as admin dashboard for consistency.
    """
    try:
        # Security: Force client_code to authenticated user's client_code
        client_code = request.user.client_code
        logger.info(f"Client dashboard request for client: {client_code}")
        
        # Get the specific client
        try:
            client = Client.objects.get(code=client_code)
        except Client.DoesNotExist:
            return Response({
                'success': False,
                'error': f'Client {client_code} not found'
            }, status=status.HTTP_404_NOT_FOUND)
        
        # Get latest snapshot for this client
        latest_snapshot = PortfolioSnapshot.objects.filter(
            client=client
        ).order_by('-snapshot_date').first()
        
        if not latest_snapshot:
            return Response({
                'success': False,
                'error': f'No portfolio data found for client {client_code}'
            }, status=status.HTTP_404_NOT_FOUND)
        
        # Extract metrics from snapshot (using same logic as admin dashboard)
        metrics = latest_snapshot.portfolio_metrics
        
        # Individual client metrics (same field names as admin dashboard)
        total_aum = float(metrics.get('total_value', 0))
        inception_dollar = float(metrics.get('inception_gain_loss_dollar', 0))
        inception_percent = float(metrics.get('inception_gain_loss_percent', 0))
        annual_income = float(metrics.get('estimated_annual_income', 0))
        
        # Asset allocation for charts
        client_asset_allocation = metrics.get('asset_allocation', {})
        asset_allocation_aggregated = {}
        for asset_type, allocation_data in client_asset_allocation.items():
            if isinstance(allocation_data, dict) and 'value' in allocation_data:
                asset_allocation_aggregated[asset_type] = float(allocation_data['value'])
        
        # Prepare client data structure for chart generation (single client)
        clients_data = [{
            'client_code': client.code,
            'client_name': client.name,
            'total_value': total_aum,
            'inception_dollar': inception_dollar,
            'inception_percent': inception_percent,
            'annual_income': annual_income,
            'asset_allocation': client_asset_allocation
        }]
        
        # Generate chart data using same function as admin dashboard
        chart_data = _generate_admin_chart_data(clients_data, asset_allocation_aggregated, client_code)
        
        # Prepare summary in exact same format as admin dashboard
        summary = {
            'total_aum': total_aum,
            'inception_dollar_performance': inception_dollar,
            'inception_return_pct': inception_percent,
            'estimated_annual_income': annual_income,
            'client_count': 1,  # Always 1 for client dashboard
            'filter_applied': client_code
        }
        
        # Return identical structure as admin dashboard
        response_data = {
            'summary': summary,
            'charts': chart_data,
            'last_updated': datetime.now().isoformat()
        }
        
        logger.info(f"Client dashboard data generated for {client_code}: ${total_aum:,.2f} AUM")
        return Response(response_data)
        
    except Exception as e:
        logger.error(f"Error getting client dashboard data for {request.user.client_code}: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


def _generate_admin_chart_data(clients_data, asset_allocation_aggregated, client_filter):
    """
    Generate chart data structures for admin dashboard.
    REUSES existing ApexCharts configurations from weekly reports.
    """
    try:
        from .services.enhanced_report_service import EnhancedReportService
        report_service = EnhancedReportService()
        
        if client_filter and client_filter != 'ALL':
            # SINGLE CLIENT - Use existing methods directly
            return _generate_single_client_charts(client_filter, report_service)
        else:
            # ALL CLIENTS - Aggregate data with proper weighting
            return _generate_aggregated_charts(clients_data, asset_allocation_aggregated, report_service)
            
    except Exception as e:
        logger.error(f"Error generating admin chart data: {e}")
        return _get_empty_chart_data()


def _generate_single_client_charts(client_code, report_service):
    """Generate charts for single client using existing weekly report methods."""
    try:
        client = Client.objects.get(code=client_code)
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        # Get latest snapshot for positions
        latest_snapshot = PortfolioSnapshot.objects.filter(
            client=client
        ).order_by('-snapshot_date').first()
        
        if not latest_snapshot:
            return _get_empty_chart_data()
        
        positions = latest_snapshot.positions.select_related('asset').all()
        
        # 1. Asset Allocation - REUSE existing method
        asset_allocation_data = report_service._calculate_asset_allocation(positions)
        asset_allocation_chart = {
            'hasData': bool(asset_allocation_data),
            'series': [data['percentage'] for data in asset_allocation_data.values()],
            'labels': list(asset_allocation_data.keys()),
            'monetaryValues': [data['market_value'] for data in asset_allocation_data.values()],
            'colors': ['#5f76a1', '#072061', '#b7babe', '#dae1f3']
        }
        
        # 2. Portfolio Evolution - REUSE existing method
        portfolio_history = report_service._generate_portfolio_history_chart(client, current_date)
        
        # 3. Cumulative Return - REUSE existing method  
        cumulative_return = report_service._generate_cumulative_return_chart(client, current_date)
        
        # 4. Portfolio Metrics - Generate comparison with previous period using enhanced function
        portfolio_metrics = _generate_single_client_metrics_comparison_enhanced(client, report_service)
        
        return {
            'asset_allocation': asset_allocation_chart,
            'portfolio_evolution': portfolio_history,
            'cumulative_return': cumulative_return,
            'portfolio_metrics': portfolio_metrics
        }
        
    except Exception as e:
        logger.error(f"Error generating single client charts: {e}")
        return _get_empty_chart_data()


def _generate_aggregated_charts(clients_data, asset_allocation_aggregated, report_service):
    """Generate aggregated charts for all clients with proper weighting."""
    try:
        # 1. AGGREGATED ASSET ALLOCATION - Consolidate Cash + Money Market
        consolidated_allocation = {}
        for asset_type, value in asset_allocation_aggregated.items():
            if asset_type in ['Cash', 'Money Market']:
                if 'Cash/Money Market' not in consolidated_allocation:
                    consolidated_allocation['Cash/Money Market'] = 0
                consolidated_allocation['Cash/Money Market'] += value
            else:
                consolidated_allocation[asset_type] = value
        
        total_value = sum(consolidated_allocation.values())
        asset_allocation_chart = {
            'hasData': bool(consolidated_allocation),
            'series': [round((value / total_value) * 100, 2) for value in consolidated_allocation.values()],
            'labels': list(consolidated_allocation.keys()),
            'monetaryValues': list(consolidated_allocation.values()),
            'colors': ['#5f76a1', '#072061', '#b7babe', '#dae1f3']
        }
        
        # 2. AGGREGATED PORTFOLIO EVOLUTION
        portfolio_evolution = _generate_aggregated_portfolio_evolution(clients_data, report_service)
        
        # 3. AGGREGATED CUMULATIVE RETURN (weighted by AUM)
        cumulative_return = _generate_weighted_cumulative_return(clients_data, report_service)
        
        # 4. PORTFOLIO METRICS COMPARISON (all clients) - Use aggregated function
        portfolio_metrics = _generate_multi_client_metrics_comparison_aggregated(clients_data, report_service)
        
        return {
            'asset_allocation': asset_allocation_chart,
            'portfolio_evolution': portfolio_evolution,
            'cumulative_return': cumulative_return,
            'portfolio_metrics': portfolio_metrics
        }
        
    except Exception as e:
        logger.error(f"Error generating aggregated charts: {e}")
        return _get_empty_chart_data()


def _generate_aggregated_portfolio_evolution(clients_data, report_service):
    """
    Generate aggregated portfolio evolution across all clients.
    Combines individual client portfolio histories by summing total values for each date.
    """
    try:
        from collections import defaultdict
        all_client_histories = {}
        
        # Get portfolio history for each client
        for client_data in clients_data:
            client_code = client_data['client_code']
            
            try:
                client = Client.objects.get(code=client_code)
                history = report_service._generate_portfolio_history_chart(client)
                
                if history['hasData']:
                    # Store client history data (no weighting needed)
                    all_client_histories[client_code] = {
                        'data': history['series'][0]['data']
                    }
            except Exception as e:
                logger.warning(f"Could not get history for {client_code}: {e}")
                continue
        
        if not all_client_histories:
            return _get_empty_portfolio_evolution()
        
        # Aggregate by date - sum all client portfolio values for each date
        date_aggregates = defaultdict(float)
        
        for client_code, client_data in all_client_histories.items():
            for point in client_data['data']:
                date_key = point['x']  # timestamp
                date_aggregates[date_key] += point['y']  # Sum portfolio values (no weighting)
        
        # Format aggregated data for ApexCharts
        aggregated_data = []
        for timestamp in sorted(date_aggregates.keys()):
            total_value = date_aggregates[timestamp]
            aggregated_data.append({'x': timestamp, 'y': round(total_value, 2)})
        
        if not aggregated_data:
            return _get_empty_portfolio_evolution()
        
        # Calculate y-axis bounds
        values = [point['y'] for point in aggregated_data]
        y_min = min(values) * 0.95
        y_max = max(values) * 1.05
        
        return {
            'hasData': True,
            'message': 'Total portfolio evolution across all clients',
            'series': [{'name': 'Total Portfolio Value', 'data': aggregated_data}],
            'currentValue': f"${aggregated_data[-1]['y']:,.2f}",
            'currentDate': datetime.fromtimestamp(aggregated_data[-1]['x'] / 1000).strftime('%Y-%m-%d'),
            'yAxisMin': y_min,
            'yAxisMax': y_max,
            'colors': ['#5f76a1'],
            'gradient': {'to': '#dae1f3'}
        }
        
    except Exception as e:
        logger.error(f"Error generating aggregated portfolio evolution: {e}")
        return _get_empty_portfolio_evolution()


def _generate_weighted_cumulative_return(clients_data, report_service):
    """
    Generate weighted cumulative return across all clients.
    Uses the same calculation method as dashboard 'Since Inception %' for consistency.
    """
    try:
        # Calculate weighted average inception percentage (same as dashboard)
        total_weighted_return = 0
        total_aum = 0
        
        for client_data in clients_data:
            aum = float(client_data.get('total_value', 0))
            inception_pct = float(client_data.get('inception_percent', 0))
            
            if aum > 0:
                total_weighted_return += inception_pct * aum
                total_aum += aum
        
        if total_aum == 0:
            return _get_empty_cumulative_return()
        
        # Calculate final weighted percentage (identical to dashboard calculation)
        final_inception_percent = total_weighted_return / total_aum
        
        # Convert to base 1000 for chart display
        final_cumulative_value = 1000 * (1 + final_inception_percent / 100)
        
        # Create simple 2-point chart (inception to current)
        # Use timestamps that represent the inception period
        from datetime import datetime, timedelta
        current_time = datetime.now()
        inception_time = current_time - timedelta(days=70)  # Approximate inception period
        
        inception_timestamp = int(inception_time.timestamp() * 1000)
        current_timestamp = int(current_time.timestamp() * 1000)
        
        chart_data = [
            {'x': inception_timestamp, 'y': 1000},  # Starting base value
            {'x': current_timestamp, 'y': round(final_cumulative_value, 2)}  # Final value
        ]
        
        # Calculate y-axis bounds
        y_min = min(1000, final_cumulative_value) - 50
        y_max = max(1000, final_cumulative_value) + 50
        
        return {
            'hasData': True,
            'message': 'Weighted cumulative return (consistent with dashboard)',
            'series': [{'name': 'Weighted Cumulative Return (Base: 1000)', 'data': chart_data}],
            'currentValue': f"{final_cumulative_value:.2f}",
            'currentDate': current_time.strftime('%Y-%m-%d'),
            'yAxisMin': y_min,
            'yAxisMax': y_max,
            'colors': ['#5f76a1'],
            'gradient': {'to': '#dae1f3'}
        }
        
    except Exception as e:
        logger.error(f"Error generating weighted cumulative return: {e}")
        return _get_empty_cumulative_return()


def _generate_single_client_metrics_comparison_enhanced(client, report_service):
    """Generate portfolio metrics comparison for single client using same 4-bar structure."""
    try:
        from datetime import datetime
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        # Get current and previous snapshots (like reports do)
        current_snapshot = PortfolioSnapshot.objects.filter(
            client=client
        ).order_by('-snapshot_date').first()
        
        if not current_snapshot:
            return _get_empty_portfolio_metrics()
        
        previous_snapshot = PortfolioSnapshot.objects.filter(
            client=client,
            snapshot_date__lt=current_snapshot.snapshot_date
        ).order_by('-snapshot_date').first()
        
        if not previous_snapshot:
            return {
                'hasData': False,
                'message': f'No historical data available for {client.code}',
                'series': [],
                'categories': []
            }
        
        # Calculate metrics using report service logic
        current_metrics = report_service._calculate_enhanced_metrics(current_snapshot)
        previous_metrics = report_service._calculate_enhanced_metrics(previous_snapshot)
        
        # 1. Total Value Change
        total_value_change = (current_metrics.get('total_value', 0) - 
                             previous_metrics.get('total_value', 0))
        
        # 2. Real Gain/Loss (Modified Dietz)
        real_gain_loss = current_metrics.get('real_gain_loss_dollar', 0)
        
        # 3. Net Cash Flow
        try:
            previous_date = str(previous_snapshot.snapshot_date)
            net_cash_flow = report_service._calculate_period_investment_cash_flow(
                client, previous_date, current_date
            )
        except:
            net_cash_flow = 0
        
        # 4. Annual Income Change
        annual_income_change = (current_metrics.get('estimated_annual_income', 0) - 
                               previous_metrics.get('estimated_annual_income', 0))
        
        # Same 4-bar structure as aggregated version
        chart_data = [total_value_change, real_gain_loss, net_cash_flow, annual_income_change]
        categories = [
            'Total Value Change',
            'Real Gain/Loss', 
            'Net Cash Flow',
            'Est. Annual Income Change'
        ]
        
        return {
            'hasData': True,
            'message': f'Portfolio metrics for {client.code}',
            'series': [{'name': 'Amount ($)', 'data': chart_data}],
            'categories': categories,
            'colors': ['#5f76a1'],
            'yAxisMin': min(chart_data) * 1.1 if min(chart_data) < 0 else min(chart_data) * 0.9,
            'yAxisMax': max(chart_data) * 1.1
        }
        
    except Exception as e:
        logger.error(f"Error generating single client metrics for {client.code}: {e}")
        return _get_empty_portfolio_metrics()


def _generate_single_client_metrics_comparison(latest_snapshot):
    """Generate portfolio metrics comparison for single client."""
    try:
        metrics = latest_snapshot.portfolio_metrics
        client_code = latest_snapshot.client.code
        
        return {
            'hasData': True,
            'message': f'Portfolio metrics for {client_code}',
            'series': [
                {
                    'name': 'Current Value ($)', 
                    'data': [float(metrics.get('total_value', 0))]
                },
                {
                    'name': 'Inception Performance ($)', 
                    'data': [float(metrics.get('inception_gain_loss_dollar', 0))]
                },
                {
                    'name': 'Annual Income ($)', 
                    'data': [float(metrics.get('estimated_annual_income', 0))]
                }
            ],
            'categories': [client_code],
            'colors': ['#5f76a1', '#072061', '#b7babe'],
            'yAxisMin': 0,
            'yAxisMax': float(metrics.get('total_value', 100000)) * 1.1
        }
        
    except Exception as e:
        logger.error(f"Error generating single client metrics: {e}")
        return _get_empty_portfolio_metrics()


def _generate_multi_client_metrics_comparison_aggregated(clients_data, report_service):
    """Generate aggregated 4-bar portfolio metrics comparison using historical data."""
    try:
        from datetime import datetime
        
        # Initialize aggregated totals
        total_value_change = 0
        total_real_gain_loss = 0
        total_net_cash_flow = 0
        total_annual_income_change = 0
        processed_clients = 0
        
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        # Calculate aggregated metrics across all clients
        for client_data in clients_data:
            client_code = client_data['client_code']
            
            try:
                client = Client.objects.get(code=client_code)
                
                # Get current and previous snapshots (like reports do)
                current_snapshot = PortfolioSnapshot.objects.filter(
                    client=client
                ).order_by('-snapshot_date').first()
                
                if not current_snapshot:
                    continue
                
                previous_snapshot = PortfolioSnapshot.objects.filter(
                    client=client,
                    snapshot_date__lt=current_snapshot.snapshot_date
                ).order_by('-snapshot_date').first()
                
                if not previous_snapshot:
                    continue  # Skip clients without historical data
                
                # Calculate metrics for this client (using report service logic)
                current_metrics = report_service._calculate_enhanced_metrics(current_snapshot)
                previous_metrics = report_service._calculate_enhanced_metrics(previous_snapshot)
                
                # 1. Total Value Change
                client_value_change = (current_metrics.get('total_value', 0) - 
                                     previous_metrics.get('total_value', 0))
                total_value_change += client_value_change
                
                # 2. Real Gain/Loss (Modified Dietz) - aggregate period performance
                client_real_gain_loss = current_metrics.get('real_gain_loss_dollar', 0)
                total_real_gain_loss += client_real_gain_loss
                
                # 3. Net Cash Flow - use investment cash flow service
                try:
                    previous_date = str(previous_snapshot.snapshot_date)
                    client_net_cash_flow = report_service._calculate_period_investment_cash_flow(
                        client, previous_date, current_date
                    )
                    total_net_cash_flow += client_net_cash_flow
                except:
                    pass  # Skip if cash flow calculation fails
                
                # 4. Annual Income Change
                client_income_change = (current_metrics.get('estimated_annual_income', 0) - 
                                      previous_metrics.get('estimated_annual_income', 0))
                total_annual_income_change += client_income_change
                
                processed_clients += 1
                
            except Exception as e:
                logger.warning(f"Could not calculate metrics for {client_code}: {e}")
                continue
        
        if processed_clients == 0:
            return {
                'hasData': False,
                'message': 'No historical data available for portfolio metrics comparison',
                'series': [],
                'categories': []
            }
        
        # Prepare 4-bar aggregated chart data
        chart_data = [total_value_change, total_real_gain_loss, total_net_cash_flow, total_annual_income_change]
        categories = [
            'Total Value Change',
            'Real Gain/Loss', 
            'Net Cash Flow',
            'Est. Annual Income Change'
        ]
        
        return {
            'hasData': True,
            'message': f'Aggregated portfolio metrics comparison ({processed_clients} clients)',
            'series': [{'name': 'Amount ($)', 'data': chart_data}],
            'categories': categories,
            'colors': ['#5f76a1'],
            'yAxisMin': min(chart_data) * 1.1 if min(chart_data) < 0 else min(chart_data) * 0.9,
            'yAxisMax': max(chart_data) * 1.1
        }
        
    except Exception as e:
        logger.error(f"Error generating aggregated metrics comparison: {e}")
        return {
            'hasData': False,
            'message': 'No aggregated metrics data available',
            'series': [],
            'categories': []
        }


def _generate_multi_client_metrics_comparison(clients_data):
    """Generate portfolio metrics comparison across all clients."""
    try:
        # Prepare data for column chart showing key metrics per client
        categories = []  # Client codes
        total_values = []
        inception_dollars = []
        annual_incomes = []
        
        # Sort clients by total value descending
        sorted_clients = sorted(clients_data, key=lambda x: x['total_value'], reverse=True)
        
        for client_data in sorted_clients[:10]:  # Top 10 clients
            categories.append(client_data['client_code'])
            total_values.append(client_data['total_value'])
            inception_dollars.append(client_data['inception_dollar'])
            annual_incomes.append(client_data['annual_income'])
        
        return {
            'hasData': bool(categories),
            'message': 'Portfolio metrics comparison',
            'series': [
                {'name': 'Total Value ($)', 'data': total_values},
                {'name': 'Inception $ Performance', 'data': inception_dollars},
                {'name': 'Annual Income ($)', 'data': annual_incomes}
            ],
            'categories': categories,
            'colors': ['#5f76a1', '#072061', '#b7babe'],
            'yAxisMin': min(min(total_values, default=0), min(inception_dollars, default=0), min(annual_incomes, default=0)) * 0.9,
            'yAxisMax': max(max(total_values, default=0), max(inception_dollars, default=0), max(annual_incomes, default=0)) * 1.1
        }
        
    except Exception as e:
        logger.error(f"Error generating multi-client metrics comparison: {e}")
        return {
            'hasData': False,
            'message': 'No metrics comparison data available',
            'series': [],
            'categories': []
        }


# Helper functions for empty chart data
def _get_empty_chart_data():
    return {
        'asset_allocation': _get_empty_asset_allocation(),
        'portfolio_evolution': _get_empty_portfolio_evolution(),
        'cumulative_return': _get_empty_cumulative_return(),
        'portfolio_metrics': _get_empty_portfolio_metrics()
    }


def _get_empty_asset_allocation():
    return {
        'hasData': False,
        'series': [],
        'labels': [],
        'monetaryValues': [],
        'colors': ['#5f76a1', '#072061', '#b7babe', '#dae1f3']
    }


def _get_empty_portfolio_evolution():
    return {
        'hasData': False,
        'message': 'No portfolio evolution data available',
        'series': [],
        'yAxisMin': 0,
        'yAxisMax': 100,
        'colors': ['#5f76a1'],
        'gradient': {'to': '#dae1f3'}
    }


def _get_empty_cumulative_return():
    return {
        'hasData': False,
        'message': 'No cumulative return data available',
        'series': [],
        'yAxisMin': 950,
        'yAxisMax': 1050,
        'colors': ['#5f76a1'],
        'gradient': {'to': '#dae1f3'}
    }


def _get_empty_portfolio_metrics():
    return {
        'hasData': False,
        'message': 'No portfolio metrics data available',
        'series': [],
        'categories': []
    }


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def client_dashboard_data(request, client_code=None):
    """Get data for client dashboard."""
    try:
        # Determine client code
        if request.user.role == 'client':
            client_code = request.user.client_code
        elif not client_code:
            return Response({'error': 'Client code required for admin users'}, 
                          status=status.HTTP_400_BAD_REQUEST)
        
        # Check permissions
        if request.user.role == 'client' and request.user.client_code != client_code:
            return Response({'error': 'Access denied'}, status=status.HTTP_403_FORBIDDEN)
        
        # Get client data
        client = Client.objects.get(code=client_code)
        latest_snapshot = PortfolioSnapshot.objects.filter(
            client=client
        ).order_by('-snapshot_date').first()
        
        if not latest_snapshot:
            return Response({
                'client_code': client_code,
                'client_name': client.name,
                'has_data': False,
                'message': 'No portfolio data available'
            })
        
        metrics = latest_snapshot.portfolio_metrics
        
        return Response({
            'client_code': client_code,
            'client_name': client.name,
            'has_data': True,
            'snapshot_date': latest_snapshot.snapshot_date,
            'total_value': metrics.get('total_value', 0),
            'unrealized_gain_loss': metrics.get('unrealized_gain_loss', 0),
            'unrealized_gain_loss_pct': metrics.get('unrealized_gain_loss_pct', 0),
            'estimated_annual_income': metrics.get('estimated_annual_income', 0),
            'position_count': metrics.get('position_count', 0),
            'asset_allocation': metrics.get('asset_allocation', {}),
            'custody_allocation': metrics.get('custody_allocation', {}),
            'chart_data': metrics.get('chart_data', {}),
            'last_updated': latest_snapshot.updated_at.isoformat()
        })
        
    except Client.DoesNotExist:
        return Response({'error': 'Client not found'}, status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        logger.error(f"Error getting client dashboard data: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# File Management API Endpoints

def organize_uploaded_file(uploaded_file, bank_code, extracted_date):
    """Organize uploaded file directly to correct bank processing location."""
    try:
        # Base directory for all processing files
        base_processing_dir = Path('data/excel/input_files')
        base_processing_dir.mkdir(parents=True, exist_ok=True)
        
        if not bank_code:
            # Save unidentified files to uploads fallback
            fallback_dir = Path(settings.BASE_DIR) / 'aurum_backend' / 'uploads' / 'unidentified'
            fallback_dir.mkdir(parents=True, exist_ok=True)
            final_file_path = fallback_dir / uploaded_file.name
            
            with open(final_file_path, 'wb+') as destination:
                for chunk in uploaded_file.chunks():
                    destination.write(chunk)
            
            return {
                'status': 'warning',
                'final_location': str(final_file_path),
                'organized_path': 'unidentified',
                'message': f'Bank not detected - saved to unidentified folder'
            }
        
        # Determine correct destination based on bank processing requirements
        bank_destinations = {
            # Special processing banks (enrichment + combination)
            'Pershing': base_processing_dir / 'pershing' / 'nonenriched_pershing',
            'LO': base_processing_dir / 'lombard' / 'nonenriched_lombard',
            
            # Combination processing banks
            'Banchile': base_processing_dir / 'banchile',
            'CS': base_processing_dir / 'cs',
            'CSC': base_processing_dir / 'csc',
            'JB': base_processing_dir / 'jb',
            'Valley': base_processing_dir / 'valley',
            'IDB': base_processing_dir / 'idb',  # Fixed: IDB needs combination processing
            
            # Enrichment processing banks
            'HSBC': base_processing_dir / 'hsbc',
            
            # Alternative assets - ready for combination
            'ALT': base_processing_dir / 'alternatives',
            
            # Simple processing banks (ready for transform)
            'JPM': base_processing_dir,  # Root directory
            'MS': base_processing_dir,   # Root directory
            'Safra': base_processing_dir, # Root directory
            'Citi': base_processing_dir  # Root directory
        }
        
        # Get destination directory for this bank
        destination_dir = bank_destinations.get(bank_code)
        if not destination_dir:
            # Unknown bank - save to uploads fallback
            fallback_dir = Path(settings.BASE_DIR) / 'aurum_backend' / 'uploads' / 'unknown_banks'
            fallback_dir.mkdir(parents=True, exist_ok=True)
            final_file_path = fallback_dir / uploaded_file.name
            
            with open(final_file_path, 'wb+') as destination:
                for chunk in uploaded_file.chunks():
                    destination.write(chunk)
            
            return {
                'status': 'warning',
                'final_location': str(final_file_path),
                'organized_path': 'unknown_banks',
                'message': f'Unknown bank {bank_code} - saved to unknown_banks folder'
            }
        
        # Create destination directory
        destination_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"📁 Created/verified directory: {destination_dir}")
        
        # Save file to correct processing location
        final_file_path = destination_dir / uploaded_file.name
        logger.info(f"💾 Saving {bank_code} file to: {final_file_path}")
        
        with open(final_file_path, 'wb+') as destination:
            for chunk in uploaded_file.chunks():
                destination.write(chunk)
        
        # Verify file was saved
        if final_file_path.exists():
            logger.info(f"✅ File successfully saved: {final_file_path}")
        else:
            logger.error(f"❌ File save failed: {final_file_path}")
        
        # Determine processing readiness
        processing_status = {
            'JPM': 'Ready for immediate transformation',
            'MS': 'Ready for immediate transformation', 
            'IDB': 'Ready for immediate transformation',
            'Safra': 'Ready for immediate transformation',
            'Citi': 'Ready for immediate transformation',
            'HSBC': 'Ready for enrichment processing',
            'Pershing': 'Ready for enrichment + combination processing',
            'LO': 'Ready for enrichment + combination processing'
        }.get(bank_code, 'Ready for combination processing')
        
        return {
            'status': 'success',
            'final_location': str(final_file_path),
            'organized_path': str(destination_dir.relative_to(Path('data/excel/input_files'))),
            'processing_readiness': processing_status,
            'message': f'Successfully uploaded {bank_code} file to processing location - {processing_status}'
        }
        
    except Exception as e:
        logger.error(f"Error organizing file {uploaded_file.name}: {e}")
        # Fallback to uploads directory
        fallback_dir = Path(settings.BASE_DIR) / 'aurum_backend' / 'uploads' / 'errors'
        fallback_dir.mkdir(parents=True, exist_ok=True)
        fallback_path = fallback_dir / uploaded_file.name
        
        with open(fallback_path, 'wb+') as destination:
            for chunk in uploaded_file.chunks():
                destination.write(chunk)
        
        return {
            'status': 'error',
            'final_location': str(fallback_path),
            'organized_path': 'errors',
            'message': f'Error organizing file: {str(e)}'
        }

@api_view(['POST'])
@permission_classes([IsAuthenticated, IsAdminUser])
def upload_files(request):
    """Enhanced upload with smart organization."""
    try:
        uploaded_files = request.FILES.getlist('files')
        if not uploaded_files:
            return Response({
                'success': False,
                'error': 'No files provided'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        processing_service = ProcessingService()
        results = []
        total_size = 0
        start_time = datetime.now()
        
        for uploaded_file in uploaded_files:
            try:
                # Validate file type
                if not uploaded_file.name.lower().endswith(('.xlsx', '.xls')):
                    results.append({
                        'filename': uploaded_file.name,
                        'success': False,
                        'error': 'Invalid file type. Only .xlsx and .xls files are allowed.',
                        'bank_detected': None,
                        'date_detected': None,
                        'size': uploaded_file.size,
                        'organized_path': None
                    })
                    continue
                
                # Detect bank and date
                bank_code = processing_service.detect_bank(uploaded_file.name)
                extracted_date = processing_service.extract_date_from_filename(uploaded_file.name)
                
                # Organize file
                organization_result = organize_uploaded_file(uploaded_file, bank_code, extracted_date)
                
                total_size += uploaded_file.size
                
                results.append({
                    'filename': uploaded_file.name,
                    'success': organization_result['status'] == 'success',
                    'status': organization_result['status'],
                    'bank_detected': bank_code,
                    'date_detected': extracted_date,
                    'size': uploaded_file.size,
                    'final_location': organization_result['final_location'],
                    'organized_path': organization_result['organized_path'],
                    'message': organization_result['message']
                })
                
                logger.info(f"Successfully uploaded {uploaded_file.name} (Bank: {bank_code}, Date: {extracted_date})")
                
            except Exception as e:
                logger.error(f"Error processing file {uploaded_file.name}: {e}")
                results.append({
                    'filename': uploaded_file.name,
                    'success': False,
                    'status': 'error',
                    'error': str(e),
                    'bank_detected': None,
                    'date_detected': None,
                    'size': uploaded_file.size if hasattr(uploaded_file, 'size') else 0,
                    'organized_path': None
                })
        
        processing_time = (datetime.now() - start_time).total_seconds()
        successful_uploads = len([r for r in results if r['success']])
        
        return Response({
            'success': True,
            'files_processed': len(uploaded_files),
            'successful_uploads': successful_uploads,
            'failed_uploads': len(uploaded_files) - successful_uploads,
            'results': results,
            'total_size': total_size,
            'processing_time': processing_time,
            'message': f'Processed {len(uploaded_files)} files, {successful_uploads} successful'
        })
        
    except Exception as e:
        logger.error(f"Error in file upload: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@permission_classes([IsAuthenticated, IsAdminUser])
def list_files(request):
    """List available Excel files in upload directory."""
    try:
        upload_dir = Path(settings.BASE_DIR) / 'aurum_backend' / 'uploads'
        
        if not upload_dir.exists():
            return Response({
                'success': True,
                'files': [],
                'total_files': 0,
                'total_size': 0,
                'message': 'Upload directory does not exist'
            })
        
        processing_service = ProcessingService()
        files_data = []
        total_size = 0
        
        # Group files by bank and date
        banks_data = {}
        
        for file_path in upload_dir.glob('*.xlsx'):
            try:
                file_stat = file_path.stat()
                bank_code = processing_service.detect_bank(file_path.name)
                date_str = processing_service.extract_date_from_filename(file_path.name)
                
                file_info = {
                    'filename': file_path.name,
                    'size': file_stat.st_size,
                    'modified': datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
                    'bank_detected': bank_code,
                    'date_detected': date_str,
                    'path': str(file_path)
                }
                
                files_data.append(file_info)
                total_size += file_stat.st_size
                
                # Group by bank
                if bank_code:
                    if bank_code not in banks_data:
                        banks_data[bank_code] = []
                    banks_data[bank_code].append(file_info)
                
            except Exception as e:
                logger.error(f"Error processing file {file_path.name}: {e}")
                continue
        
        # Also check for .xls files
        for file_path in upload_dir.glob('*.xls'):
            try:
                file_stat = file_path.stat()
                bank_code = processing_service.detect_bank(file_path.name)
                date_str = processing_service.extract_date_from_filename(file_path.name)
                
                file_info = {
                    'filename': file_path.name,
                    'size': file_stat.st_size,
                    'modified': datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
                    'bank_detected': bank_code,
                    'date_detected': date_str,
                    'path': str(file_path)
                }
                
                files_data.append(file_info)
                total_size += file_stat.st_size
                
                # Group by bank
                if bank_code:
                    if bank_code not in banks_data:
                        banks_data[bank_code] = []
                    banks_data[bank_code].append(file_info)
                
            except Exception as e:
                logger.error(f"Error processing file {file_path.name}: {e}")
                continue
        
        return Response({
            'success': True,
            'files': files_data,
            'banks': banks_data,
            'total_files': len(files_data),
            'total_size': total_size,
            'upload_directory': str(upload_dir)
        })
        
    except Exception as e:
        logger.error(f"Error listing files: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['DELETE'])
@permission_classes([IsAuthenticated, IsAdminUser])
def delete_file(request, filename):
    """Delete uploaded file."""
    try:
        upload_dir = Path(settings.BASE_DIR) / 'aurum_backend' / 'uploads'
        file_path = upload_dir / filename
        
        # Validate filename (security check)
        if not filename or '..' in filename or '/' in filename or '\\' in filename:
            return Response({
                'success': False,
                'error': 'Invalid filename'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # Check if file exists
        if not file_path.exists():
            return Response({
                'success': False,
                'error': 'File not found'
            }, status=status.HTTP_404_NOT_FOUND)
        
        # Check if it's actually a file (not a directory)
        if not file_path.is_file():
            return Response({
                'success': False,
                'error': 'Not a file'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # Get file info before deletion
        file_size = file_path.stat().st_size
        
        # Delete the file
        file_path.unlink()
        
        logger.info(f"Successfully deleted file: {filename}")
        
        return Response({
            'success': True,
            'filename': filename,
            'size': file_size,
            'message': f'File {filename} deleted successfully'
        })
        
    except Exception as e:
        logger.error(f"Error deleting file {filename}: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Preprocessing API Endpoints

@api_view(['POST'])
@permission_classes([IsAuthenticated, IsAdminUser])
def start_preprocessing(request):
    """Start preprocessing for specific date."""
    try:
        data = request.data
        date = data.get('date')
        banks_filter = data.get('banks', None)  # Optional list of specific banks
        
        if not date:
            return Response({
                'success': False,
                'error': 'Date is required (format: DD_MM_YYYY)'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        processing_service = ProcessingService()
        
        if banks_filter:
            # Process specific banks
            results = {}
            for bank_code in banks_filter:
                if processing_service.is_bank_supported(bank_code):
                    result = processing_service.process_bank_files(bank_code, date)
                    results[bank_code] = result
                else:
                    results[bank_code] = {
                        'success': False,
                        'error': f'Bank {bank_code} is not supported'
                    }
            
            return Response({
                'success': True,
                'date': date,
                'banks_processed': banks_filter,
                'results': results,
                'message': f'Preprocessing completed for {len(banks_filter)} banks'
            })
        else:
            # Process all banks
            result = processing_service.run_complete_pipeline(date)
            return Response(result)
        
    except Exception as e:
        logger.error(f"Error starting preprocessing: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@permission_classes([IsAuthenticated, IsAdminUser])
def preprocessing_status(request):
    """Get preprocessing status for all banks."""
    try:
        date = request.GET.get('date')
        processing_service = ProcessingService()
        
        status_result = processing_service.get_bank_status(date)
        
        return Response(status_result)
        
    except Exception as e:
        logger.error(f"Error getting preprocessing status: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Client Management API Endpoints

@api_view(['GET'])
@permission_classes([IsAuthenticated, IsAdminUser])
def get_clients(request):
    """Get list of all clients for admin dashboard."""
    try:
        clients = Client.objects.all().order_by('code')
        
        data = [{
            'id': client.id,
            'name': client.name,
            'client_code': client.code
        } for client in clients]
        
        return Response(data)
        
    except Exception as e:
        logger.error(f"Error getting clients: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

def scan_bank_files(bank_code, base_processing_dir, target_date=None):
    """Scan bank processing directories for files and return comprehensive status.
    
    Args:
        bank_code: Bank code to scan for
        base_processing_dir: Base directory to scan
        target_date: Optional specific date to filter files by (DD_MM_YYYY format)
    """
    from collections import defaultdict
    
    file_info = {
        'total_files': 0,
        'dates_available': set(),
        'last_upload_date': None,  # Use extracted date, not file timestamp
        'file_types': defaultdict(int)  # securities, transactions, unitcost, cashmovements
    }
    
    # Define scanning locations for each bank
    scan_locations = {
        # Special processing banks (enrichment + combination)
        'Pershing': [
            base_processing_dir / 'pershing' / 'nonenriched_pershing',  # Upload location
            base_processing_dir / 'pershing',  # Post-processing location
            base_processing_dir  # Final processing location
        ],
        'LO': [
            base_processing_dir / 'lombard' / 'nonenriched_lombard',  # Upload location
            base_processing_dir / 'lombard',  # Post-processing location 
            base_processing_dir  # Final processing location
        ],
        
        # Combination processing banks
        'Banchile': [
            base_processing_dir / 'banchile',  # Upload location
            base_processing_dir  # Final processing location
        ],
        'CS': [
            base_processing_dir / 'cs',  # Upload location
            base_processing_dir  # Final processing location
        ],
        'CSC': [
            base_processing_dir / 'csc',  # Upload location
            base_processing_dir  # Final processing location
        ],
        'JB': [
            base_processing_dir / 'jb',  # Upload location
            base_processing_dir  # Final processing location
        ],
        'Valley': [
            base_processing_dir / 'valley',  # Upload location
            base_processing_dir  # Final processing location
        ],
        'IDB': [
            base_processing_dir / 'idb',  # Upload location
            base_processing_dir  # Final processing location
        ],
        
        # Enrichment processing banks
        'HSBC': [
            base_processing_dir / 'hsbc',  # Upload location
            base_processing_dir  # Final processing location
        ],
        
        # Alternative assets - combination processing
        'ALT': [
            base_processing_dir / 'alternatives',  # Upload location  
            base_processing_dir  # Final processing location (post-combination)
        ],
        
        # Simple processing banks (already in final location)
        'JPM': [base_processing_dir],  # Root directory only
        'MS': [base_processing_dir],   # Root directory only
        'Safra': [base_processing_dir], # Root directory only
        'Citi': [base_processing_dir]  # Root directory only
    }
    
    # Get scan locations for this bank
    locations = scan_locations.get(bank_code, [])
    
    for location in locations:
        if not location.exists():
            continue
            
        # Find all files for this bank in this location
        if bank_code == 'ALT':
            # ALT files use lowercase prefix, scan case-insensitively
            bank_files = list(location.glob(f"*alt*.xlsx")) + list(location.glob(f"*ALT*.xlsx"))
        else:
            bank_files = list(location.glob(f"*{bank_code}*.xlsx"))
        
        # Process found files
        for file_path in bank_files:
            # Extract date using filename (never file modification time)
            processing_service = ProcessingService()
            extracted_date = processing_service.extract_date_from_filename(file_path.name)
            if extracted_date:
                file_info['dates_available'].add(extracted_date)
                
                # Track latest date (not file timestamp)
                if not file_info['last_upload_date'] or extracted_date > file_info['last_upload_date']:
                    file_info['last_upload_date'] = extracted_date
            
            # If target_date is specified, only count files for that date
            if target_date and extracted_date != target_date:
                continue  # Skip files not matching target date
            
            # Determine file type with enhanced detection
            filename_lower = file_path.name.lower()
            
            # Special handling for Banchile - client files contain both securities and transactions in sheets
            if bank_code == 'Banchile' and not any(keyword in filename_lower for keyword in ['securities', 'transactions', 'unitcost', 'cashmovements']):
                # Banchile client files (e.g., Banchile_CI_CI_31_07_2025.xlsx) contain both data types
                file_info['file_types']['securities'] += 1
                file_info['file_types']['transactions'] += 1
            elif 'securities' in filename_lower:
                file_info['file_types']['securities'] += 1
            elif 'transactions' in filename_lower:
                file_info['file_types']['transactions'] += 1
            elif 'unitcost' in filename_lower:
                file_info['file_types']['unitcost'] += 1
            elif 'cashmovements' in filename_lower:
                file_info['file_types']['cashmovements'] += 1
            
            file_info['total_files'] += 1
    
    return file_info

def determine_bank_status(bank_info, bank_code, processing_type):
    """Determine bank status based on files and correct processing requirements."""
    total_files = bank_info['total_files']
    file_types = bank_info['file_types']
    
    if total_files == 0:
        return {
            'status': 'EMPTY',
            'percentage': 0,
            'completion_reason': 'No files uploaded'
        }
    
    # Correct requirements based on actual bank needs
    bank_requirements = {
        # Simple processing banks (have securities + transactions)
        'JPM': ['securities', 'transactions'],
        'MS': ['securities', 'transactions'],
        'IDB': ['securities', 'transactions'],
        'Safra': ['securities', 'transactions'],
        'Citi': ['securities', 'transactions'],
        
        # Enrichment processing banks
        'HSBC': ['securities', 'transactions', 'unitcost'],
        
        # Combination processing banks
        'CS': ['securities', 'transactions'],
        'Valley': ['securities', 'transactions'],
        'JB': ['securities', 'transactions'],
        'CSC': ['securities', 'transactions'],
        'Banchile': ['securities', 'transactions'],
        
        # Enrichment + combination processing banks
        'Pershing': ['securities', 'transactions', 'unitcost'],
        'LO': ['securities', 'transactions', 'cashmovements']  # Lombard needs cashmovements, not unitcost
    }
    
    required_types = bank_requirements.get(bank_code, ['securities', 'transactions'])
    found_types = [ftype for ftype in required_types if file_types[ftype] > 0]
    
    completion_percentage = (len(found_types) / len(required_types)) * 100
    
    if completion_percentage == 100:
        status = 'COMPLETE'
        reason = f'All required files present: {", ".join(found_types)}'
    elif completion_percentage > 0:
        status = 'PARTIAL'
        missing = set(required_types) - set(found_types)
        reason = f'Missing: {", ".join(missing)}. Have: {", ".join(found_types)}'
    else:
        status = 'EMPTY'
        reason = 'No required files found'
    
    return {
        'status': status,
        'percentage': int(completion_percentage),
        'completion_reason': reason
    }

@api_view(['GET'])
@permission_classes([IsAuthenticated, IsAdminUser])
def get_bank_status(request):
    """Enhanced bank status with real file scanning."""
    try:
        # The 14 ProjectAurum Banks with their processing types
        PROJECTAURUM_BANKS = [
            {'code': 'JPM', 'name': 'JPMorgan', 'type': 'simple'},
            {'code': 'MS', 'name': 'Morgan Stanley', 'type': 'simple'},
            {'code': 'IDB', 'name': 'Inter-American Development Bank', 'type': 'combination'},
            {'code': 'Safra', 'name': 'Safra', 'type': 'simple'},
            {'code': 'HSBC', 'name': 'HSBC', 'type': 'enrichment'},
            {'code': 'CS', 'name': 'Credit Suisse', 'type': 'combination'},
            {'code': 'Valley', 'name': 'Valley Bank', 'type': 'combination'},
            {'code': 'JB', 'name': 'JB Private Bank', 'type': 'combination'},
            {'code': 'CSC', 'name': 'Charles Schwab', 'type': 'combination'},
            {'code': 'Banchile', 'name': 'Banchile', 'type': 'combination'},
            {'code': 'Pershing', 'name': 'Pershing', 'type': 'enrichment_combination'},
            {'code': 'LO', 'name': 'Lombard', 'type': 'enrichment_combination'},
            {'code': 'ALT', 'name': 'Alternative Assets', 'type': 'combination'},
            {'code': 'Citi', 'name': 'Citi Bank', 'type': 'simple'}
        ]
        
        def get_next_steps(processing_type):
            steps = {
                'simple': 'Ready for processing',
                'enrichment': 'Requires enrichment step',
                'combination': 'Requires combination step',
                'enrichment_combination': 'Requires enrichment + combination'
            }
            return steps.get(processing_type, 'Unknown processing type')
        
        # Base processing directory for scanning
        base_processing_dir = Path('data/excel/input_files')
        
        # Get the latest available date to show status for
        # First scan all files to find the latest date with uploads
        latest_date = None
        all_dates = set()
        
        # Quick scan to find all available dates
        for bank in PROJECTAURUM_BANKS:
            temp_info = scan_bank_files(bank['code'], base_processing_dir)  # No target_date = get all
            all_dates.update(temp_info['dates_available'])
        
        # Get latest date that has uploaded files
        if all_dates:
            latest_date = max(all_dates)
        
        bank_statuses = []
        
        for bank in PROJECTAURUM_BANKS:
            bank_code = bank['code']
            processing_type = bank['type']
            
            # Scan for files using date-specific logic
            bank_info = scan_bank_files(bank_code, base_processing_dir, latest_date)
            
            # Determine status using corrected requirements
            status_info = determine_bank_status(bank_info, bank_code, processing_type)
            
            # Format last upload date (from filename, not file timestamp)
            last_upload_date = bank_info['last_upload_date']
            
            bank_statuses.append({
                'bank_code': bank_code,
                'bank_name': bank['name'],
                'status': status_info['status'],
                'percentage': status_info['percentage'],
                'completion_reason': status_info['completion_reason'],
                'file_count': bank_info['total_files'],
                'processing_type': processing_type,
                'last_upload_date': last_upload_date,  # Date from filename, not timestamp
                'available_dates': sorted(list(bank_info['dates_available'])),
                'file_types': dict(bank_info['file_types']),
                'next_steps': get_next_steps(processing_type)
            })
        
        return Response({
            'success': True,
            'banks': bank_statuses,
            'current_date': latest_date,  # Date that status is showing for
            'summary': {
                'total_banks': len(PROJECTAURUM_BANKS),
                'complete_banks': len([b for b in bank_statuses if b['status'] == 'COMPLETE']),
                'partial_banks': len([b for b in bank_statuses if b['status'] == 'PARTIAL']),
                'empty_banks': len([b for b in bank_statuses if b['status'] == 'EMPTY']),
                'status_date': latest_date  # Date context for the status
            }
        })
        
    except Exception as e:
        logger.error(f"Error getting bank status: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_available_dates(request):
    """Get available dates from both uploads and processed files."""
    try:
        from collections import defaultdict
        
        date_info = defaultdict(lambda: {
            'upload_files': 0,
            'processed_files': 0,
            'banks_with_uploads': set(),
            'banks_processed': set(),
            'status': 'unknown'
        })
        
        # Scan input_files directories (where files actually get organized)
        input_files_dir = Path('data/excel/input_files')
        if input_files_dir.exists():
            processing_service = ProcessingService()
            
            # Scan all files in input_files directory and subdirectories
            all_files = []
            
            # Get files from root directory (JPM, MS, IDB, Safra)
            all_files.extend(input_files_dir.glob("*.xlsx"))
            
            # Get files from bank subdirectories
            for bank_dir in input_files_dir.glob("*/"):
                if bank_dir.is_dir():
                    # Regular bank directories
                    all_files.extend(bank_dir.glob("*.xlsx"))
                    
                    # Special case: nonenriched subdirectories (Lombard, Pershing)
                    for subdir in bank_dir.glob("nonenriched_*/"):
                        if subdir.is_dir():
                            all_files.extend(subdir.glob("*.xlsx"))
            
            # Extract dates from all found files
            for file_path in all_files:
                # Skip the Mappings.xlsx file
                if file_path.name == 'Mappings.xlsx':
                    continue
                    
                extracted_date = processing_service.extract_date_from_filename(file_path.name)
                bank_code = processing_service.detect_bank(file_path.name)
                
                if extracted_date and bank_code:
                    date_info[extracted_date]['upload_files'] += 1
                    date_info[extracted_date]['banks_with_uploads'].add(bank_code)
        
        # Scan for final processed files to determine which dates are already processed
        excel_dir = Path('data/excel')
        processed_dates = set()
        
        if excel_dir.exists():
            # Check for both securities and transactions files (both must exist for date to be processed)
            securities_files = list(excel_dir.glob("securities_*.xlsx"))
            transactions_files = list(excel_dir.glob("transactions_*.xlsx"))
            
            # Extract dates from securities files
            securities_dates = set()
            for file_path in securities_files:
                processing_service = ProcessingService()
                extracted_date = processing_service.extract_date_from_filename(file_path.name)
                if extracted_date:
                    securities_dates.add(extracted_date)
            
            # Extract dates from transactions files  
            transactions_dates = set()
            for file_path in transactions_files:
                processing_service = ProcessingService()
                extracted_date = processing_service.extract_date_from_filename(file_path.name)
                if extracted_date:
                    transactions_dates.add(extracted_date)
            
            # Only dates that have BOTH securities AND transactions are fully processed
            processed_dates = securities_dates.intersection(transactions_dates)
        
        # Build available dates list - only include dates that are NOT fully processed
        formatted_dates = []
        for date_key, info in date_info.items():
            # Skip dates that are already fully processed
            if date_key in processed_dates:
                continue  # Don't show processed dates as available
                
            # Only show dates that have uploaded files and are NOT processed
            if info['upload_files'] > 0:
                formatted_dates.append({
                    'date': date_key,
                    'status': 'uploaded',
                    'upload_files': info['upload_files'],
                    'processed_files': 0,  # Not processed (or we wouldn't show it)
                    'banks_with_uploads': sorted(list(info['banks_with_uploads'])),
                    'can_process': True  # Available for processing since not processed yet
                })
        
        # Sort by date (newest first)
        formatted_dates.sort(key=lambda x: x['date'], reverse=True)
        
        # Frontend expects simple array of date strings
        simple_dates = [item['date'] for item in formatted_dates]
        
        return Response({
            'success': True,
            'dates': simple_dates,  # Simple array: ["24_07_2025", "23_07_2025"]
            'summary': {
                'total_dates_available': len(simple_dates),  # Only unprocessed dates
                'total_processed_dates': len(processed_dates),  # Dates already processed
                'latest_available': simple_dates[0] if simple_dates else None
            }
        })
        
    except Exception as e:
        logger.error(f"Error getting available dates: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['POST'])
@permission_classes([IsAuthenticated])
def promote_files_for_processing(request):
    """Move uploaded files to processing-ready location."""
    try:
        selected_date = request.data.get('date')
        if not selected_date:
            return Response({
                'success': False,
                'error': 'Date parameter required'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # Source: uploads/DD_MM_YYYY/
        source_dir = Path(settings.BASE_DIR) / 'aurum_backend' / 'uploads' / selected_date
        
        # Destination: data/excel/input_files/
        dest_dir = Path('data/excel/input_files')
        dest_dir.mkdir(parents=True, exist_ok=True)
        
        if not source_dir.exists():
            return Response({
                'success': False,
                'error': f'No uploaded files found for date {selected_date}'
            }, status=status.HTTP_404_NOT_FOUND)
        
        promotion_results = []
        
        # Process each bank directory
        for bank_dir in source_dir.glob("*/"):
            if not bank_dir.is_dir():
                continue
                
            bank_code = bank_dir.name.upper()
            bank_dest_dir = dest_dir / bank_dir.name.lower()
            bank_dest_dir.mkdir(exist_ok=True)
            
            files_moved = 0
            files_failed = 0
            
            # Move each file
            for file_path in bank_dir.glob("*.xlsx"):
                try:
                    dest_file_path = bank_dest_dir / file_path.name
                    
                    # Copy file (don't move in case of issues)
                    import shutil
                    shutil.copy2(str(file_path), str(dest_file_path))
                    files_moved += 1
                    
                except Exception as e:
                    logger.error(f"Failed to promote {file_path}: {e}")
                    files_failed += 1
            
            if files_moved > 0:
                promotion_results.append({
                    'bank_code': bank_code,
                    'files_moved': files_moved,
                    'files_failed': files_failed,
                    'destination': str(bank_dest_dir)
                })
        
        return Response({
            'success': True,
            'date': selected_date,
            'promotion_results': promotion_results,
            'message': f'Promoted files for {len(promotion_results)} banks to processing location'
        })
        
    except Exception as e:
        logger.error(f"Error promoting files: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@permission_classes([IsAuthenticated, IsAdminUser])
def get_population_ready_dates(request):
    """Get dates with processed files but no database snapshots."""
    try:
        # 1. Scan for processed Excel files
        excel_dir = Path("data/excel")
        securities_files = list(excel_dir.glob("securities_*.xlsx"))
        transactions_files = list(excel_dir.glob("transactions_*.xlsx"))
        
        # 2. Extract dates from filenames (DD_MM_YYYY format)
        securities_dates = set()
        for file_path in securities_files:
            # Extract date from "securities_24_07_2025.xlsx"
            date_match = re.search(r'securities_(\d{2}_\d{2}_\d{4})\.xlsx', file_path.name)
            if date_match:
                securities_dates.add(date_match.group(1))
        
        transactions_dates = set()
        for file_path in transactions_files:
            date_match = re.search(r'transactions_(\d{2}_\d{2}_\d{4})\.xlsx', file_path.name)
            if date_match:
                transactions_dates.add(date_match.group(1))
        
        # 3. Find dates with BOTH securities and transactions files
        complete_file_dates = securities_dates.intersection(transactions_dates)
        
        # 4. Convert to YYYY-MM-DD format for snapshot comparison
        def file_to_snapshot_format(file_date):
            day, month, year = file_date.split('_')
            return f"{year}-{month}-{day}"
        
        # 5. Get existing snapshot dates
        existing_snapshots = set(
            str(snapshot_date) for snapshot_date in 
            PortfolioSnapshot.objects.values_list('snapshot_date', flat=True).distinct()
        )
        
        # 6. Find dates ready for population (have files but no snapshots)
        ready_dates = []
        for file_date in complete_file_dates:
            snapshot_format = file_to_snapshot_format(file_date)
            if snapshot_format not in existing_snapshots:
                ready_dates.append(file_date)
        
        return Response({
            'success': True,
            'ready_dates': sorted(ready_dates, reverse=True),  # Latest first
            'message': f'Found {len(ready_dates)} dates ready for population'
        })
        
    except Exception as e:
        logger.error(f"Error getting population ready dates: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=500)

# Health Check Endpoint

@api_view(['GET'])
def health_check(request):
    """Health check endpoint."""
    try:
        # Check database connectivity
        client_count = Client.objects.count()
        snapshot_count = PortfolioSnapshot.objects.count()
        
        return Response({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'database': {
                'clients': client_count,
                'snapshots': snapshot_count
            }
        })
    except Exception as e:
        return Response({
            'status': 'unhealthy',
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# Excel Export Endpoints (Admin Only)

@api_view(['POST'])
@permission_classes([IsAdminUser])
def export_positions_excel(request):
    """
    Export positions data to Excel format.
    
    POST body: {
        "client_code": "ALL" or specific client code like "BK",
        "snapshot_date": "2025-07-31"
    }
    
    Returns: Excel file download
    """
    try:
        data = json.loads(request.body)
        client_code = data.get('client_code', 'ALL')
        snapshot_date = data.get('snapshot_date')
        
        if not snapshot_date:
            return Response({
                'success': False,
                'error': 'snapshot_date is required'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # Validate date format
        try:
            datetime.strptime(snapshot_date, '%Y-%m-%d')
        except ValueError:
            return Response({
                'success': False,
                'error': 'snapshot_date must be in YYYY-MM-DD format'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # Initialize export service and generate Excel
        export_service = ExcelExportService()
        excel_bytes, filename = export_service.export_positions_excel(client_code, snapshot_date)
        
        # Return file as download
        response = HttpResponse(
            excel_bytes,
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        
        logger.info(f"Positions Excel export successful: {filename} for client={client_code}")
        
        return response
        
    except ValueError as e:
        logger.error(f"Error exporting positions: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_400_BAD_REQUEST)
    except Exception as e:
        logger.error(f"Error exporting positions: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST'])
@permission_classes([IsAdminUser])
def export_transactions_excel(request):
    """
    Export transactions data to Excel format.
    
    POST body: {
        "client_code": "ALL" or specific client code like "BK",
        "start_date": "2025-07-11", 
        "end_date": "2025-07-27"
    }
    
    Returns: Excel file download
    """
    try:
        data = json.loads(request.body)
        client_code = data.get('client_code', 'ALL')
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        if not start_date or not end_date:
            return Response({
                'success': False,
                'error': 'start_date and end_date are required'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # Validate date formats
        try:
            datetime.strptime(start_date, '%Y-%m-%d')
            datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            return Response({
                'success': False,
                'error': 'Dates must be in YYYY-MM-DD format'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # Initialize export service and generate Excel
        export_service = ExcelExportService()
        excel_bytes, filename = export_service.export_transactions_excel(client_code, start_date, end_date)
        
        # Return file as download
        response = HttpResponse(
            excel_bytes,
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        
        logger.info(f"Transactions Excel export successful: {filename} for client={client_code}")
        
        return response
        
    except ValueError as e:
        logger.error(f"Error exporting transactions: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_400_BAD_REQUEST)
    except Exception as e:
        logger.error(f"Error exporting transactions: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAdminUser])
def get_export_available_dates(request):
    """
    Get available dates for export functionality.
    
    Returns: {
        "snapshot_dates": ["2025-07-31", "2025-07-24", ...],
        "transaction_date_range": {
            "min_date": "2025-01-01",
            "max_date": "2025-07-31"
        },
        "clients": [{"code": "BK", "name": "..."}, ...]
    }
    """
    try:
        export_service = ExcelExportService()
        available_data = export_service.get_available_export_dates()
        
        return Response(available_data, status=status.HTTP_200_OK)
        
    except Exception as e:
        logger.error(f"Error getting export available dates: {e}")
        return Response({
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# Database Backup and Restore Endpoints

@api_view(['POST'])
@permission_classes([IsAuthenticated, IsAdminUser])
def create_database_backup(request):
    """
    Create database backup on demand.
    Expected Response Time: 1-3 seconds depending on database size
    """
    try:
        from .services.database_backup_service import DatabaseBackupService
        
        backup_service = DatabaseBackupService()
        result = backup_service.create_backup()
        
        if result['success']:
            logger.info(f"Database backup created by {request.user.username}: {result['filename']}")
            return Response({
                'status': 'success',
                'message': f"Backup created successfully: {result['display_name']}",
                'backup_info': result
            })
        else:
            return Response({
                'status': 'error',
                'message': result['error']
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
    except Exception as e:
        logger.error(f"Backup creation failed: {str(e)}")
        return Response({
            'status': 'error',
            'message': f'Backup failed: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated, IsAdminUser])
def list_database_backups(request):
    """
    List all available backups for restore dropdown.
    """
    try:
        from .services.database_backup_service import DatabaseBackupService
        
        backup_service = DatabaseBackupService()
        backups = backup_service.list_backups()
        
        return Response({
            'status': 'success',
            'backups': backups,
            'count': len(backups)
        })
        
    except Exception as e:
        logger.error(f"Failed to list backups: {str(e)}")
        return Response({
            'status': 'error',
            'message': f'Failed to list backups: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST'])
@permission_classes([IsAuthenticated, IsAdminUser])
def restore_database_backup(request):
    """
    Restore database from selected backup.
    
    Expected payload:
    {
        "backup_filename": "db_backup_20250118_143022.sqlite3",
        "create_pre_restore_backup": true
    }
    """
    try:
        from .services.database_backup_service import DatabaseBackupService
        
        backup_filename = request.data.get('backup_filename')
        create_pre_restore_backup = request.data.get('create_pre_restore_backup', True)
        
        if not backup_filename:
            return Response({
                'status': 'error',
                'message': 'backup_filename is required'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        backup_service = DatabaseBackupService()
        result = backup_service.restore_backup(backup_filename, create_pre_restore_backup)
        
        if result['success']:
            logger.warning(f"Database restored by {request.user.username} from {backup_filename}")
            return Response({
                'status': 'success',
                'message': f"Database restored successfully from {result['restored_from']}",
                'restore_info': result
            })
        else:
            return Response({
                'status': 'error',
                'message': result['error']
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
    except Exception as e:
        logger.error(f"Database restore failed: {str(e)}")
        return Response({
            'status': 'error',
            'message': f'Restore failed: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['DELETE'])
@permission_classes([IsAuthenticated, IsAdminUser])
def delete_database_backup(request, backup_filename):
    """
    Delete specific backup file.
    URL: DELETE /api/admin/delete-database-backup/<backup_filename>/
    """
    try:
        from .services.database_backup_service import DatabaseBackupService
        
        backup_service = DatabaseBackupService()
        result = backup_service.delete_backup(backup_filename)
        
        if result['success']:
            logger.info(f"Backup deleted by {request.user.username}: {backup_filename}")
            return Response({
                'status': 'success',
                'message': f"Backup deleted successfully",
                'deletion_info': result
            })
        else:
            return Response({
                'status': 'error',
                'message': result['error']
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
    except Exception as e:
        logger.error(f"Backup deletion failed: {str(e)}")
        return Response({
            'status': 'error',
            'message': f'Deletion failed: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)