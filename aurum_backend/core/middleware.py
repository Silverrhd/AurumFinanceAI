"""
Maintenance Mode Middleware for AurumFinance
Handles requests during maintenance periods with enterprise-grade features.
"""

import logging
from django.http import JsonResponse, HttpResponse
from django.template import Template, Context
from django.utils.deprecation import MiddlewareMixin
from .maintenance import maintenance_mode

logger = logging.getLogger(__name__)

class MaintenanceModeMiddleware(MiddlewareMixin):
    """
    Enterprise middleware for handling maintenance mode.
    
    Features:
    - Graceful 503 responses during maintenance
    - Admin bypass capability  
    - JSON responses for API requests
    - HTML responses for browser requests
    - Detailed logging
    - Customizable maintenance messages
    """
    
    def __init__(self, get_response=None):
        super().__init__(get_response)
        self.get_response = get_response
        self.maintenance = maintenance_mode
        
        # HTML template for maintenance page
        self.maintenance_html_template = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>System Maintenance - AurumFinance</title>
            <style>
                body {
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    margin: 0;
                    padding: 0;
                    min-height: 100vh;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                }
                .maintenance-container {
                    background: white;
                    border-radius: 12px;
                    padding: 40px;
                    text-align: center;
                    box-shadow: 0 20px 40px rgba(0,0,0,0.1);
                    max-width: 500px;
                    margin: 20px;
                }
                .maintenance-icon {
                    font-size: 48px;
                    color: #667eea;
                    margin-bottom: 20px;
                }
                h1 {
                    color: #333;
                    margin-bottom: 20px;
                    font-size: 28px;
                }
                .message {
                    color: #666;
                    font-size: 16px;
                    line-height: 1.6;
                    margin-bottom: 30px;
                }
                .details {
                    background: #f8f9fa;
                    border-radius: 8px;
                    padding: 20px;
                    margin-top: 20px;
                }
                .details h3 {
                    color: #333;
                    margin-top: 0;
                    font-size: 16px;
                }
                .details p {
                    color: #666;
                    margin: 5px 0;
                    font-size: 14px;
                }
                .retry-info {
                    color: #888;
                    font-size: 14px;
                    margin-top: 20px;
                }
            </style>
            <script>
                // Auto-refresh page every 30 seconds
                setTimeout(function() {
                    window.location.reload();
                }, 30000);
            </script>
        </head>
        <body>
            <div class="maintenance-container">
                <div class="maintenance-icon">🔧</div>
                <h1>System Maintenance</h1>
                <div class="message">
                    {{ message }}
                </div>
                {% if show_details %}
                <div class="details">
                    <h3>Maintenance Details</h3>
                    <p><strong>Started:</strong> {{ start_time }}</p>
                    {% if duration %}
                    <p><strong>Duration:</strong> {{ duration }} minutes</p>
                    {% endif %}
                    <p><strong>Status:</strong> Database restore in progress</p>
                </div>
                {% endif %}
                <div class="retry-info">
                    This page will automatically refresh every 30 seconds.<br>
                    Thank you for your patience.
                </div>
            </div>
        </body>
        </html>
        """
    
    def process_request(self, request):
        """
        Process incoming request and handle maintenance mode.
        
        Args:
            request: Django request object
            
        Returns:
            HttpResponse if in maintenance mode, None to continue normally
        """
        try:
            # Check if maintenance mode is active
            if not self.maintenance.is_active():
                return None
            
            # Check for admin bypass
            if self.maintenance.should_bypass_for_admin(request):
                logger.debug(f"🔐 Admin bypass for {request.path} - {request.user}")
                return None
            
            # Get maintenance information
            maintenance_info = self.maintenance.get_info()
            if not maintenance_info:
                # If we can't get info but maintenance is active, use defaults
                maintenance_info = {
                    'message': 'System maintenance in progress. Please try again in a few minutes.',
                    'start_time': 'Unknown'
                }
            
            # Log maintenance request (throttled to avoid spam)
            if not hasattr(self, '_last_log_time') or \
               hasattr(self, '_last_log_time') and (
                   __import__('time').time() - getattr(self, '_last_log_time', 0) > 60
               ):
                logger.info(f"⚠️ Maintenance mode active - blocking request to {request.path}")
                self._last_log_time = __import__('time').time()
            
            # Return appropriate response based on request type
            if self._is_api_request(request):
                return self._create_api_maintenance_response(maintenance_info)
            else:
                return self._create_html_maintenance_response(maintenance_info)
                
        except Exception as e:
            logger.error(f"❌ Error in maintenance middleware: {str(e)}")
            # Fail safe - allow request to continue if middleware fails
            return None
    
    def _is_api_request(self, request):
        """
        Determine if request is an API request that should get JSON response.
        
        Args:
            request: Django request object
            
        Returns:
            bool: True if API request, False if browser request
        """
        # Check if request path indicates API
        if request.path.startswith('/api/'):
            return True
        
        # Check Accept header
        accept_header = request.META.get('HTTP_ACCEPT', '')
        if 'application/json' in accept_header:
            return True
        
        # Check Content-Type for POST/PUT requests
        content_type = request.META.get('CONTENT_TYPE', '')
        if 'application/json' in content_type:
            return True
        
        # Check for AJAX requests
        if request.META.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest':
            return True
        
        return False
    
    def _create_api_maintenance_response(self, maintenance_info):
        """
        Create JSON maintenance response for API requests.
        
        Args:
            maintenance_info: Dict with maintenance information
            
        Returns:
            JsonResponse with maintenance details
        """
        response_data = {
            'error': 'maintenance_mode',
            'message': maintenance_info.get('message', 'System maintenance in progress'),
            'maintenance': True,
            'retry_after': 30,  # Suggest retry after 30 seconds
            'details': {
                'start_time': maintenance_info.get('start_time'),
                'operation': 'database_restore'
            }
        }
        
        response = JsonResponse(response_data, status=503)
        response['Retry-After'] = '30'
        response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        return response
    
    def _create_html_maintenance_response(self, maintenance_info):
        """
        Create HTML maintenance page for browser requests.
        
        Args:
            maintenance_info: Dict with maintenance information
            
        Returns:
            HttpResponse with maintenance page HTML
        """
        try:
            template = Template(self.maintenance_html_template)
            
            # Prepare context data
            context_data = {
                'message': maintenance_info.get('message', 'System maintenance in progress'),
                'show_details': True,
                'start_time': self._format_start_time(maintenance_info.get('start_time')),
                'duration': maintenance_info.get('timeout_minutes', 'Unknown')
            }
            
            context = Context(context_data)
            html_content = template.render(context)
            
            response = HttpResponse(html_content, status=503)
            response['Retry-After'] = '30'
            response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
            return response
            
        except Exception as e:
            logger.error(f"❌ Error creating HTML maintenance response: {str(e)}")
            
            # Fallback to simple HTML response
            simple_html = f"""
            <html>
            <head><title>System Maintenance</title></head>
            <body>
                <h1>System Maintenance</h1>
                <p>{maintenance_info.get('message', 'System maintenance in progress')}</p>
                <p>Please try again in a few minutes.</p>
                <script>setTimeout(function(){{ window.location.reload(); }}, 30000);</script>
            </body>
            </html>
            """
            
            response = HttpResponse(simple_html, status=503)
            response['Retry-After'] = '30'
            return response
    
    def _format_start_time(self, start_time_iso):
        """
        Format ISO timestamp for display.
        
        Args:
            start_time_iso: ISO format timestamp string
            
        Returns:
            Formatted time string
        """
        try:
            if not start_time_iso:
                return 'Unknown'
            
            from datetime import datetime
            dt = datetime.fromisoformat(start_time_iso)
            return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
            
        except Exception:
            return start_time_iso or 'Unknown'