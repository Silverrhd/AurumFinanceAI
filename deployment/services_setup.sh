#!/bin/bash

# Setup Services (Supervisor + Nginx) for AurumFinance
# Usage: ./services_setup.sh YOUR_AWS_IP

set -e

if [ -z "$1" ]; then
    echo "❌ Error: Please provide your AWS public IP"
    echo "Usage: ./services_setup.sh YOUR_AWS_IP"
    exit 1
fi

AWS_IP=$1
echo "🔧 Setting up services for IP: $AWS_IP"

# Create Supervisor configuration for Django
echo "📋 Creating Supervisor configuration for Django..."
cat > /etc/supervisor/conf.d/aurum-backend.conf << EOF
[program:aurum-backend]
command=/opt/aurumfinance/venv/bin/gunicorn --workers 3 --bind unix:/opt/aurumfinance/aurum_backend.sock aurum_backend.wsgi:application
directory=/opt/aurumfinance/aurum_backend
user=aurumapp
autostart=true
autorestart=true
redirect_stderr=true
stdout_logfile=/var/log/supervisor/aurum-backend.log
environment=PATH="/opt/aurumfinance/venv/bin"
EOF

# Create Supervisor configuration for Next.js
echo "📋 Creating Supervisor configuration for Next.js..."
cat > /etc/supervisor/conf.d/aurum-frontend.conf << EOF
[program:aurum-frontend]
command=/usr/bin/npm start
directory=/opt/aurumfinance/aurum_frontend
user=aurumapp
autostart=true
autorestart=true
redirect_stderr=true
stdout_logfile=/var/log/supervisor/aurum-frontend.log
environment=NODE_ENV=production,PORT=3000
EOF

# Create Nginx configuration
echo "🌐 Creating Nginx configuration..."
cat > /etc/nginx/sites-available/aurumfinance << EOF
# Redirect HTTP to HTTPS is not needed for IP-based setup
server {
    listen 80;
    server_name $AWS_IP;

    # Frontend (Next.js)
    location / {
        proxy_pass http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_cache_bypass \$http_upgrade;
    }

    # Backend API (Django)
    location /api/ {
        proxy_pass http://unix:/opt/aurumfinance/aurum_backend.sock;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }

    # Django admin
    location /admin/ {
        proxy_pass http://unix:/opt/aurumfinance/aurum_backend.sock;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }

    # Static files
    location /static/ {
        alias /opt/aurumfinance/aurum_backend/staticfiles/;
        expires 30d;
        add_header Cache-Control "public, immutable";
    }

    # Media files
    location /media/ {
        alias /opt/aurumfinance/aurum_backend/media/;
        expires 30d;
        add_header Cache-Control "public";
    }
}
EOF

# Enable Nginx site
ln -sf /etc/nginx/sites-available/aurumfinance /etc/nginx/sites-enabled/
rm -f /etc/nginx/sites-enabled/default

# Test Nginx configuration
nginx -t

# Start and enable services
echo "🚀 Starting services..."
supervisorctl reread
supervisorctl update
supervisorctl start aurum-backend
supervisorctl start aurum-frontend

systemctl restart nginx
systemctl enable supervisor
systemctl enable nginx

# Setup firewall
echo "🔥 Configuring firewall..."
ufw --force enable
ufw allow 'Nginx Full'
ufw allow OpenSSH
ufw allow from 127.0.0.1 to any port 3000
ufw allow from 127.0.0.1 to any port 8000

echo "✅ Services setup completed!"
echo ""
echo "🎉 AurumFinance should now be accessible at:"
echo "   Frontend: http://$AWS_IP"
echo "   Admin:    http://$AWS_IP/admin"
echo "   API:      http://$AWS_IP/api"
echo ""
echo "Run: ./verify_deployment.sh $AWS_IP to test everything"