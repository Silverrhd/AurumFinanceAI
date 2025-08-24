#!/bin/bash

# Configure AurumFinance Application
# Usage: ./configure_app.sh YOUR_AWS_IP

set -e

if [ -z "$1" ]; then
    echo "❌ Error: Please provide your AWS public IP"
    echo "Usage: ./configure_app.sh YOUR_AWS_IP"
    exit 1
fi

AWS_IP=$1
echo "⚙️ Configuring AurumFinance for IP: $AWS_IP"

cd /opt/aurumfinance

# Create Python virtual environment
echo "🐍 Setting up Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies
echo "📦 Installing Python dependencies..."
pip install --upgrade pip
pip install -r aurum_backend/requirements.txt
pip install gunicorn

# Create production environment file for Django
echo "📝 Creating Django production environment..."
cat > aurum_backend/.env << EOF
DJANGO_ENVIRONMENT=production
DEBUG=False
SECRET_KEY=$(python3 -c 'from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())')
ALLOWED_HOSTS=$AWS_IP,127.0.0.1,localhost

# Database
USE_POSTGRESQL=True
DB_NAME=aurum_finance_prod
DB_USER=aurumuser
DB_PASSWORD=AurumSecure2025!
DB_HOST=localhost
DB_PORT=5432

# CORS for frontend
CORS_ALLOWED_ORIGINS=http://$AWS_IP,http://127.0.0.1:3000,http://localhost:3000

# Email (optional - can be configured later)
EMAIL_HOST=smtp.gmail.com
EMAIL_HOST_USER=
EMAIL_HOST_PASSWORD=
DEFAULT_FROM_EMAIL=noreply@aurumfinance.com

# API Keys (add your keys here)
OPENFIGI_API_KEY=bf21060a-0568-489e-8622-efcaf02e52cf
EOF

# Run Django setup
echo "🔧 Setting up Django..."
cd aurum_backend
source ../venv/bin/activate
python manage.py migrate
python manage.py collectstatic --noinput --clear

# Create superuser (non-interactive)
echo "👤 Creating Django superuser..."
python manage.py shell << EOF
from django.contrib.auth import get_user_model
User = get_user_model()
if not User.objects.filter(username='Admin').exists():
    User.objects.create_superuser(username='Admin', email='', password='ARDNd1163?', client_code='ADMIN')
    print("Admin user created: username=Admin, password=ARDNd1163?")
else:
    print("Admin user already exists")
EOF

# Setup Next.js frontend
echo "🎨 Setting up Next.js frontend..."
cd ../aurum_frontend

# Create frontend environment
cat > .env.local << EOF
NEXT_PUBLIC_API_URL=http://$AWS_IP/api
NODE_ENV=production
EOF

# Install dependencies and build
npm install
npm run build

# Set permissions
echo "🔐 Setting file permissions..."
cd /opt/aurumfinance
chown -R aurumapp:aurumapp .
chmod -R 755 .

echo "✅ Application configuration completed!"
echo "Next: Run services_setup.sh $AWS_IP"