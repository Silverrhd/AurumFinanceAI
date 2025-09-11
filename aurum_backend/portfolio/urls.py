"""
URL configuration for AurumFinance portfolio app.
Clean Django-only URL patterns.
"""

from django.urls import path
from . import views

app_name = 'portfolio'

urlpatterns = [
    # Health check
    path('health/', views.health_check, name='health_check'),
    
    # Portfolio data endpoints
    path('summary/', views.portfolio_summary, name='portfolio_summary'),
    path('summary/<str:client_code>/', views.portfolio_summary, name='portfolio_summary_client'),
    path('metrics/<str:client_code>/<str:snapshot_date>/', views.portfolio_metrics, name='portfolio_metrics'),
    path('snapshots/', views.available_snapshots, name='available_snapshots'),
    path('snapshots/<str:client_code>/', views.available_snapshots, name='available_snapshots_client'),
    
    # Database update endpoints (admin only)
    path('update-database/', views.update_database, name='update_database'),
    path('processing-status/', views.processing_status, name='processing_status'),
    path('recalculate-metrics/', views.recalculate_metrics, name='recalculate_metrics'),
    
    # Report generation endpoints
    path('generate-report/', views.generate_report, name='generate_report'),
    path('generate-report-no-open/', views.generate_report_no_open, name='generate_report_no_open'),
    path('available-reports/', views.available_reports, name='available_reports'),
    path('available-dates/<str:report_type>/', views.get_available_dates_by_type, name='available_dates_by_type'),
    path('reports/<str:report_type>/generated/', views.list_generated_reports_by_type, name='list_reports_by_type'),
    
    # Weekly report specific endpoints
    path('weekly-reports/available-dates/', views.available_weekly_report_dates, name='available_weekly_report_dates'),
    path('weekly-reports/generated/', views.list_generated_weekly_reports, name='list_generated_weekly_reports'),
    path('reports/<int:report_id>/view/', views.serve_report_file, name='serve_report_file'),
    path('reports/<int:report_id>/html/', views.serve_report_html_direct, name='serve_report_html_direct'),
    
    # Dashboard data endpoints
    path('admin/dashboard/', views.admin_dashboard_data, name='admin_dashboard_data'),
    path('client/dashboard/', views.client_dashboard_data, name='client_dashboard_data'),
    path('client/dashboard/<str:client_code>/', views.client_dashboard_data, name='client_dashboard_data_specific'),
    path('client/dashboard-with-charts/', views.client_dashboard_with_charts, name='client_dashboard_with_charts'),
    
    # File management endpoints (admin only)
    path('files/upload/', views.upload_files, name='upload_files'),
    path('files/list/', views.list_files, name='list_files'),
    path('files/<str:filename>/', views.delete_file, name='delete_file'),
    
    # Preprocessing endpoints (admin only)
    path('preprocess/start/', views.start_preprocessing, name='start_preprocessing'),
    path('preprocess/status/', views.preprocessing_status, name='preprocessing_status'),
    
    # Client management endpoints (admin only)
    path('clients/', views.get_clients, name='get_clients'),
    path('bank-status/', views.get_bank_status, name='get_bank_status'),
    
    # Enhanced file management endpoints
    path('available-dates/', views.get_available_dates, name='get_available_dates'),
    path('promote-files/', views.promote_files_for_processing, name='promote_files'),
    
    # Population management endpoints
    path('population-ready-dates/', views.get_population_ready_dates, name='population_ready_dates'),
    
    # Excel Export endpoints (admin only)
    path('export/positions/', views.export_positions_excel, name='export_positions'),
    path('export/transactions/', views.export_transactions_excel, name='export_transactions'),
    path('export/monthly-returns/', views.export_monthly_returns_excel, name='export_monthly_returns'),
    path('export/available-dates/', views.get_export_available_dates, name='export_dates'),
    
    # Database Backup and Restore endpoints (admin only)
    path('admin/create-database-backup/', views.create_database_backup, name='create_database_backup'),
    path('admin/list-database-backups/', views.list_database_backups, name='list_database_backups'),
    path('admin/restore-database-backup/', views.restore_database_backup, name='restore_database_backup'),
    path('admin/delete-database-backup/<str:backup_filename>/', views.delete_database_backup, name='delete_database_backup'),

]