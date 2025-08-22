#!/bin/bash

# AurumFinance Application Deployment Script
# Usage: ./application_deploy.sh YOUR_AWS_IP

set -e

if [ -z "$1" ]; then
    echo "❌ Error: Please provide your AWS public IP"
    echo "Usage: ./application_deploy.sh YOUR_AWS_IP"
    echo "Example: ./application_deploy.sh 54.123.45.67"
    exit 1
fi

AWS_IP=$1
echo "🚀 Deploying AurumFinance to IP: $AWS_IP"

# Create application directory
echo "📁 Creating application directory..."
mkdir -p /opt/aurumfinance
cd /opt/aurumfinance

# Create application user
useradd --system --shell /bin/bash --home /opt/aurumfinance --create-home aurumapp || true
chown aurumapp:aurumapp /opt/aurumfinance

echo "📥 Please upload your AurumFinance code to /opt/aurumfinance"
echo "You can use: scp -r /path/to/AurumFinance/* ubuntu@$AWS_IP:/opt/aurumfinance/"
echo ""
echo "After uploading, run: ./configure_app.sh $AWS_IP"