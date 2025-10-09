#!/bin/bash

# PostgreSQL Database Setup for AurumFinance
# Run this script after server_setup.sh

set -e

echo "❌ Database already exists. This script is not needed"
exit 1

echo "🐘 Setting up PostgreSQL database..."

# Start PostgreSQL
systemctl start postgresql
systemctl enable postgresql

# Create database and user
echo "Creating database and user..."
sudo -u postgres psql << EOF
CREATE DATABASE aurum_finance_prod;
CREATE USER aurumuser WITH ENCRYPTED PASSWORD 'AurumSecure2025!';
GRANT ALL PRIVILEGES ON DATABASE aurum_finance_prod TO aurumuser;
ALTER USER aurumuser CREATEDB;
\q
EOF

# Configure PostgreSQL for local connections
echo "📝 Configuring PostgreSQL..."
# Backup original files
cp /etc/postgresql/*/main/postgresql.conf /etc/postgresql/*/main/postgresql.conf.backup
cp /etc/postgresql/*/main/pg_hba.conf /etc/postgresql/*/main/pg_hba.conf.backup

# Allow local connections
sed -i "s/#listen_addresses = 'localhost'/listen_addresses = 'localhost'/" /etc/postgresql/*/main/postgresql.conf

# Add authentication for aurumuser
echo "local   aurum_finance_prod    aurumuser                               md5" >> /etc/postgresql/*/main/pg_hba.conf

# Restart PostgreSQL
systemctl restart postgresql

# Test connection
echo "🔍 Testing database connection..."
sudo -u postgres psql -c "SELECT version();"

echo "✅ Database setup completed successfully!"
echo "Database: aurum_finance_prod"
echo "User: aurumuser"
echo "Password: AurumSecure2025!"
echo "Next: Run application_deploy.sh with your AWS IP"