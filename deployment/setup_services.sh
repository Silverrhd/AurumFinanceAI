#!/bin/bash

# Setup Services Script
# Usage: ./setup_services.sh

set -e

echo "🚀 Setting up services..."

# Copy Supervisor configuration files
echo "📋 Setting up Supervisor configurations..."
sudo cp /tmp/AurumFinanceAI/deployment/aurum-backend.conf /etc/supervisor/conf.d/
sudo cp /tmp/AurumFinanceAI/deployment/aurum-frontend.conf /etc/supervisor/conf.d/

# Copy Nginx configuration
echo "🌐 Setting up Nginx configuration..."
sudo cp /tmp/AurumFinanceAI/deployment/aurumfinance.nginx /etc/nginx/sites-available/aurumfinance
sudo ln -sf /etc/nginx/sites-available/aurumfinance /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default

# Restart services
echo "🔄 Restarting services..."
sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl restart aurum-backend aurum-frontend
sudo systemctl restart nginx

echo "✅ Services setup completed!"
echo ""
echo "🎉 AurumFinanceAI should now be accessible at:"
echo "   Frontend: http://18.231.106.188"
echo "   Admin:    http://18.231.106.188/admin (admin/ARDNd1163?)"
echo "   API:      http://18.231.106.188/api"
echo ""
echo "Run the following to verify deployment:"
echo "curl -I http://18.231.106.188"