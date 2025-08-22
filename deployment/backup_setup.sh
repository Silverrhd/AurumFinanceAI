#!/bin/bash

# Setup automated database backups for AurumFinance

set -e

echo "💾 Setting up automated database backups..."

# Create backup directory
mkdir -p /opt/aurumfinance/backups
chown aurumapp:aurumapp /opt/aurumfinance/backups

# Create backup script
cat > /opt/aurumfinance/backup_db.sh << 'EOF'
#!/bin/bash
BACKUP_DIR="/opt/aurumfinance/backups"
DATE=$(date +%Y%m%d_%H%M%S)
mkdir -p $BACKUP_DIR

# Export database
PGPASSWORD=AurumSecure2025! pg_dump -h localhost -U aurumuser aurum_finance_prod > $BACKUP_DIR/aurum_backup_$DATE.sql

# Compress backup
gzip $BACKUP_DIR/aurum_backup_$DATE.sql

# Keep only last 7 days of backups
find $BACKUP_DIR -name "aurum_backup_*.sql.gz" -mtime +7 -delete

echo "$(date): Database backup completed: aurum_backup_$DATE.sql.gz"
EOF

# Make backup script executable
chmod +x /opt/aurumfinance/backup_db.sh
chown aurumapp:aurumapp /opt/aurumfinance/backup_db.sh

# Add to crontab for daily backups at 2 AM
(crontab -l 2>/dev/null; echo "0 2 * * * /opt/aurumfinance/backup_db.sh >> /var/log/aurum-backup.log 2>&1") | crontab -

# Create restore script
cat > /opt/aurumfinance/restore_db.sh << 'EOF'
#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: ./restore_db.sh backup_file.sql.gz"
    echo "Available backups:"
    ls -la /opt/aurumfinance/backups/
    exit 1
fi

BACKUP_FILE=$1

echo "⚠️  WARNING: This will overwrite the current database!"
read -p "Are you sure? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    echo "Restore cancelled"
    exit 1
fi

echo "Restoring database from $BACKUP_FILE..."

# Drop existing database and recreate
sudo -u postgres psql << PSQL
DROP DATABASE IF EXISTS aurum_finance_prod;
CREATE DATABASE aurum_finance_prod;
GRANT ALL PRIVILEGES ON DATABASE aurum_finance_prod TO aurumuser;
PSQL

# Restore from backup
if [[ $BACKUP_FILE == *.gz ]]; then
    gunzip -c $BACKUP_FILE | PGPASSWORD=AurumSecure2025! psql -h localhost -U aurumuser aurum_finance_prod
else
    PGPASSWORD=AurumSecure2025! psql -h localhost -U aurumuser aurum_finance_prod < $BACKUP_FILE
fi

echo "Database restore completed!"
EOF

chmod +x /opt/aurumfinance/restore_db.sh
chown aurumapp:aurumapp /opt/aurumfinance/restore_db.sh

# Test backup
echo "🧪 Running test backup..."
/opt/aurumfinance/backup_db.sh

echo "✅ Backup system setup completed!"
echo "📅 Daily backups scheduled for 2:00 AM"
echo "📁 Backup location: /opt/aurumfinance/backups/"
echo "🔧 Manual backup: /opt/aurumfinance/backup_db.sh"
echo "🔄 Restore: /opt/aurumfinance/restore_db.sh backup_file.sql.gz"