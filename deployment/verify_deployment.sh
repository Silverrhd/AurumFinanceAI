#!/bin/bash

# Verify AurumFinance Deployment
# Usage: ./verify_deployment.sh YOUR_AWS_IP

set -e

if [ -z "$1" ]; then
    echo "❌ Error: Please provide your AWS public IP"
    echo "Usage: ./verify_deployment.sh YOUR_AWS_IP"
    exit 1
fi

AWS_IP=$1
echo "🔍 Verifying AurumFinance deployment at IP: $AWS_IP"

# Check service statuses
echo "📊 Checking service statuses..."
echo "Supervisor status:"
supervisorctl status

echo ""
echo "Nginx status:"
systemctl status nginx --no-pager -l

echo ""
echo "PostgreSQL status:"
systemctl status postgresql --no-pager -l

# Check if services are responding
echo ""
echo "🌐 Testing HTTP endpoints..."

# Test frontend
echo "Testing frontend..."
if curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:3000 | grep -q "200\|301\|302"; then
    echo "✅ Frontend (Next.js) is responding"
else
    echo "❌ Frontend (Next.js) is not responding"
fi

# Test backend via socket
echo "Testing backend..."
if [ -S /opt/aurumfinance/aurum_backend.sock ]; then
    echo "✅ Backend socket exists"
else
    echo "❌ Backend socket not found"
fi

# Test via Nginx
echo "Testing full stack via Nginx..."
if curl -s -o /dev/null -w "%{http_code}" http://$AWS_IP | grep -q "200\|301\|302"; then
    echo "✅ Full stack is accessible via IP"
else
    echo "❌ Full stack not accessible"
fi

# Check logs for errors
echo ""
echo "📋 Recent logs..."
echo "Backend logs:"
tail -n 5 /var/log/supervisor/aurum-backend.log 2>/dev/null || echo "No backend logs yet"

echo ""
echo "Frontend logs:"
tail -n 5 /var/log/supervisor/aurum-frontend.log 2>/dev/null || echo "No frontend logs yet"

echo ""
echo "Nginx error logs:"
tail -n 5 /var/log/nginx/error.log 2>/dev/null || echo "No nginx errors"

echo ""
echo "🎯 Deployment verification completed!"
echo ""
echo "If all checks passed, your AurumFinance is ready at:"
echo "🌐 http://$AWS_IP"
echo "👤 http://$AWS_IP/admin (login with the superuser you created)"