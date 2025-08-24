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

# Create users (admin + all clients)
echo "👤 Creating Django users (admin + all clients)..."
python manage.py shell << EOF
from django.contrib.auth import get_user_model
User = get_user_model()

# Create admin user (fixed role and client_code)
if not User.objects.filter(username='Admin').exists():
    admin_user = User.objects.create_superuser(username='Admin', email='admin@aurumfinance.com', password='ARDNd1163?')
    admin_user.role = 'admin'
    admin_user.client_code = None
    admin_user.save()
    print("✅ Admin user created: username=Admin, password=ARDNd1163?")
else:
    print("✅ Admin user already exists")

# Create client users from temp_credentials.txt (all 42 clients)
client_credentials = [
    ('AA', '@1tQw4Z3'), ('AC', 'w$YB0f^F'), ('AG', 'm6vC$atg'), ('AU', 'i9wb*G4Y'), ('BK', 'z*wpH7f6'),
    ('CH', 'dUoo!0Fe'), ('CI', '^dMLVw9&'), ('CR', 'Tx*g7$1j'), ('DA', '2!Qws7I9'), ('DG', '%nyUt$m8'),
    ('EG', 'z7#FY*AS'), ('ELP', '%B5U7qI#'), ('EV', 'IJo0x*lW'), ('FG', 'Qr&tTF6F'), ('FU', 'x^JMtb3q'),
    ('GG', '0^0QlBgL'), ('GW', 'vFSYa8%0'), ('GZ', 'NFQi*J12'), ('HH', 'T0^kH*iq'), ('HS', 'E^q7YHwj'),
    ('HZ', 'HPR&ec7^'), ('IA', 'RkS!fm7C'), ('ID', 'w6Kw#rTc'), ('IZ', 'zwy!0N!K'), ('JAI', 'FCsOB3!d'),
    ('JAV', 'yNHJ5t$1'), ('JC', '%%oO4Gz8'), ('JG', 'P#eKI$Z5'), ('JN', 'nt&7I&f*'), ('JPU', '@#OZU9o5'),
    ('KP', 'YrGd5!H!'), ('LP', 'lEcH57F^'), ('LV', '%0cT8wNw'), ('MAC', '6etU3%S@'), ('MB', 'D%Mo99Fn'),
    ('MZ', '1u%&^*Ir'), ('NAC', '&8#ZJM3r'), ('PS', '&!Cf8*rz'), ('RB', 'oUR#l9Te'), ('RM', 'vlXl8@4W'),
    ('RP', 'Ya#X5*fS'), ('RS', '8Nj9$Kr7'), ('VH', 'QXg&L5#Z'), ('VLP', '7oBY4!za'), ('VP', '*2uN@4zZ')
]

created_count = 0
for client_code, password in client_credentials:
    username = f"{client_code}_user"
    if not User.objects.filter(username=username).exists():
        client_user = User.objects.create_user(
            username=username, 
            email=f"{client_code.lower()}@aurumfinance.com",
            password=password
        )
        client_user.role = 'client'
        client_user.client_code = client_code
        client_user.save()
        created_count += 1

print(f"✅ Created {created_count} new client users (42 total)")
print("🔐 All client users use same passwords as temp_credentials.txt")
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