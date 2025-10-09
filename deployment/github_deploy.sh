#!/bin/bash

# GitHub-based AWS EC2 Deployment Script for AurumFinanceAI
# This script deploys from GitHub instead of manual file uploads

set -e

if [ -z "$1" ]; then
    echo "❌ Error: Please provide your AWS public IP"
    echo "Usage: ./github_deploy.sh YOUR_AWS_IP"
    exit 1
fi

AWS_IP=$1
GITHUB_REPO="https://github.com/Silverrhd/AurumFinanceAI.git"
APP_DIR="/opt/aurumfinance"

echo "🚀 Starting GitHub-based deployment for IP: $AWS_IP"

# Step 1: Clean up existing deployment
echo "🧹 Cleaning up existing deployment..."
sudo rm -rf $APP_DIR
sudo mkdir -p $APP_DIR
sudo chown aurumapp:aurumapp $APP_DIR

# Create persistent data directory (survives deployments)
echo "📁 Setting up persistent data directory..."
sudo mkdir -p /var/lib/aurumfinance/backups
sudo chown -R aurumapp:aurumapp /var/lib/aurumfinance
sudo chmod 755 /var/lib/aurumfinance
sudo chmod 755 /var/lib/aurumfinance/backups

# Step 2: Clone repository
echo "📥 Cloning AurumFinanceAI from GitHub..."
cd /tmp
sudo -u aurumapp git clone -b deployment $GITHUB_REPO aurumfinance
sudo mv aurumfinance $APP_DIR/source
sudo chown -R aurumapp:aurumapp $APP_DIR

# Step 3: Setup Python environment
echo "🐍 Setting up Python environment..."
cd $APP_DIR
sudo -u aurumapp python3 -m venv venv
sudo -u aurumapp $APP_DIR/venv/bin/pip install --upgrade pip
sudo -u aurumapp $APP_DIR/venv/bin/pip install -r source/aurum_backend/requirements.txt
sudo -u aurumapp $APP_DIR/venv/bin/pip install gunicorn

# Step 4: Create production environment file
echo "📝 Creating production environment file..."
sudo -u aurumapp cat > $APP_DIR/source/aurum_backend/.env << EOF
DJANGO_ENVIRONMENT=production
DEBUG=False
SECRET_KEY=$($APP_DIR/venv/bin/python -c 'from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())')
ALLOWED_HOSTS=$AWS_IP,127.0.0.1,localhost,aurum.dndpi.cl

# Database
USE_POSTGRESQL=True
DB_NAME=aurum_finance_prod
DB_USER=aurumuser
DB_PASSWORD=AurumSecure2025!
DB_HOST=localhost
DB_PORT=5432

# CORS for frontend
CORS_ALLOWED_ORIGINS=http://$AWS_IP,http://127.0.0.1:3000,http://localhost:3000,https://aurum.dndpi.cl

# Security (disable HTTPS redirect for HTTP deployment)
SECURE_SSL_REDIRECT=False

# Email (optional)
EMAIL_HOST=smtp.gmail.com
EMAIL_HOST_USER=
EMAIL_HOST_PASSWORD=
DEFAULT_FROM_EMAIL=noreply@aurumfinance.com

# API Keys
OPENFIGI_API_KEY=your_openfigi_key_here
EOF

# Step 5: Setup Django
echo "🔧 Setting up Django..."
cd $APP_DIR/source/aurum_backend
sudo -u aurumapp $APP_DIR/venv/bin/python manage.py migrate
sudo -u aurumapp $APP_DIR/venv/bin/python manage.py collectstatic --noinput --clear

# Step 6: Create admin user
echo "👤 Creating admin superuser..."
sudo -u aurumapp $APP_DIR/venv/bin/python manage.py shell << EOF
from django.contrib.auth import get_user_model
User = get_user_model()
if not User.objects.filter(username='admin').exists():
    User.objects.create_superuser(username='admin', email='admin@aurumfinance.com', password='ARDNd1163?', client_code='ADMIN')
    print("Admin user created: username=admin, password=ARDNd1163?")
else:
    print("Admin user already exists")
EOF

# Step 7: Setup Next.js frontend
echo "🎨 Setting up Next.js frontend..."
cd $APP_DIR/source/aurum_frontend

sudo -u aurumapp cat > .env.local << EOF
NEXT_PUBLIC_API_URL=https://aurum.dndpi.cl
NODE_ENV=production
EOF

sudo -u aurumapp npm install
sudo -u aurumapp npm run build

# Step 8: Update Supervisor configurations
echo "📋 Updating Supervisor configurations..."
sudo cat > /etc/supervisor/conf.d/aurum-backend.conf << EOF
[program:aurum-backend]
command=$APP_DIR/venv/bin/gunicorn --workers 3 --bind unix:$APP_DIR/aurum_backend.sock aurum_backend.wsgi:application
directory=$APP_DIR/source/aurum_backend
user=aurumapp
autostart=true
autorestart=true
redirect_stderr=true
stdout_logfile=/var/log/supervisor/aurum-backend.log
environment=PATH="$APP_DIR/venv/bin:/usr/local/bin:/usr/bin:/bin"
EOF

sudo cat > /etc/supervisor/conf.d/aurum-frontend.conf << EOF
[program:aurum-frontend]
command=/usr/bin/npm start
directory=$APP_DIR/source/aurum_frontend
user=aurumapp
autostart=true
autorestart=true
redirect_stderr=true
stdout_logfile=/var/log/supervisor/aurum-frontend.log
environment=NODE_ENV=production,PORT=3000
EOF

# Step 9: Update Nginx configuration
echo "🌐 Updating Nginx configuration..."
sudo cat > /etc/nginx/sites-available/aurumfinance << EOF
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
        proxy_pass http://unix:$APP_DIR/aurum_backend.sock;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }

    # Django admin
    location /admin/ {
        proxy_pass http://unix:$APP_DIR/aurum_backend.sock;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }

    # Static files
    location /static/ {
        alias $APP_DIR/source/aurum_backend/staticfiles/;
        expires 30d;
        add_header Cache-Control "public, immutable";
    }

    # Media files
    location /media/ {
        alias $APP_DIR/source/aurum_backend/media/;
        expires 30d;
        add_header Cache-Control "public";
    }
}
EOF

# Step 10: Restart services
echo "🚀 Restarting services..."
sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl restart aurum-backend aurum-frontend
sudo systemctl restart nginx

echo "✅ GitHub deployment completed!"
echo ""
echo "🎉 AurumFinanceAI should now be accessible at:"
echo "   Frontend: http://$AWS_IP"
echo "   Admin:    http://$AWS_IP/admin (admin/ARDNd1163?)"
echo "   API:      http://$AWS_IP/api"
echo ""
echo "Run the following to verify deployment:"
echo "curl -I http://$AWS_IP"