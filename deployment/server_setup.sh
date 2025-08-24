#!/bin/bash

# AurumFinance AWS EC2 Server Setup Script
# Run this script on your AWS EC2 instance as root or with sudo

set -e  # Exit on any error

echo "🚀 Starting AurumFinance AWS EC2 Setup..."

# Update system
echo "📦 Updating system packages..."
apt update && apt upgrade -y

# Install core dependencies
echo "⚡ Installing core dependencies..."
apt install -y \
    python3 \
    python3-pip \
    python3-venv \
    nodejs \
    npm \
    nginx \
    postgresql \
    postgresql-contrib \
    redis-server \
    supervisor \
    git \
    curl \
    unzip

# Install Node.js 18+ (for Next.js)
echo "🟢 Installing Node.js 18..."
curl -fsSL https://deb.nodesource.com/setup_18.x | bash -
apt-get install -y nodejs

# Create 1GB swap file for frontend builds
echo "💾 Setting up swap file for frontend builds..."
fallocate -l 1G /swapfile
chmod 600 /swapfile
mkswap /swapfile
swapon /swapfile
echo '/swapfile none swap sw 0 0' >> /etc/fstab
echo "✅ Swap file created and configured"

# Verify installations
echo "✅ Verifying installations..."
python3 --version
node --version
npm --version
psql --version

echo "🎉 Server setup completed successfully!"
echo "Next: Run database_setup.sh"