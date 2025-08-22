# AurumFinance AWS EC2 Deployment Guide

This directory contains all the scripts needed to deploy AurumFinance to AWS EC2 using your public IP address.

## 🚀 Quick Start

1. **Launch AWS EC2 Instance**
   - Instance Type: t3.medium (minimum)
   - OS: Ubuntu 24.04 LTS
   - Storage: 20GB SSD
   - Security Group: Allow HTTP (80), HTTPS (443), SSH (22)

2. **Upload deployment scripts to your EC2 instance**
   ```bash
   scp -r deployment/ ubuntu@YOUR_AWS_IP:/home/ubuntu/
   ```

3. **Connect to your EC2 instance**
   ```bash
   ssh -i your-key.pem ubuntu@YOUR_AWS_IP
   cd deployment
   chmod +x *.sh
   ```

4. **Run deployment scripts in order**
   ```bash
   sudo ./server_setup.sh
   sudo ./database_setup.sh
   sudo ./application_deploy.sh YOUR_AWS_IP
   ```

5. **Upload your AurumFinance code**
   ```bash
   # On your local machine:
   scp -r aurum_backend/ aurum_frontend/ ubuntu@YOUR_AWS_IP:/opt/aurumfinance/
   ```

6. **Complete the deployment**
   ```bash
   # Back on EC2:
   sudo ./configure_app.sh YOUR_AWS_IP
   sudo ./services_setup.sh YOUR_AWS_IP
   ./verify_deployment.sh YOUR_AWS_IP
   ```

7. **Setup backups (optional)**
   ```bash
   sudo ./backup_setup.sh
   ```

## 📋 Script Details

### 1. `server_setup.sh`
- Updates system packages
- Installs Python, Node.js, PostgreSQL, Nginx, Supervisor
- Prepares the server environment

### 2. `database_setup.sh`
- Creates PostgreSQL database and user
- Configures database authentication
- Tests database connection

### 3. `application_deploy.sh YOUR_AWS_IP`
- Creates application directory structure
- Sets up application user
- Prepares for code upload

### 4. `configure_app.sh YOUR_AWS_IP`
- Creates production environment files
- Sets up Python virtual environment
- Installs dependencies
- Runs Django migrations
- Builds Next.js production bundle
- Creates superuser account

### 5. `services_setup.sh YOUR_AWS_IP`
- Configures Supervisor for process management
- Sets up Nginx reverse proxy
- Configures firewall
- Starts all services

### 6. `verify_deployment.sh YOUR_AWS_IP`
- Tests all services are running
- Verifies HTTP endpoints
- Shows service logs
- Confirms deployment success

### 7. `backup_setup.sh`
- Creates automated daily database backups
- Sets up backup and restore scripts
- Schedules backups via cron

## 🔧 Configuration

### Default Credentials
- **Database**: `aurum_finance_prod`
- **DB User**: `aurumuser`
- **DB Password**: `AurumSecure2025!`
- **App User**: `aurumapp`

### Important Files Created
- Django settings: `/opt/aurumfinance/aurum_backend/.env`
- Frontend config: `/opt/aurumfinance/aurum_frontend/.env.local`
- Nginx config: `/etc/nginx/sites-available/aurumfinance`
- Supervisor configs: `/etc/supervisor/conf.d/aurum-*.conf`

### URLs After Deployment
- **Frontend**: `http://YOUR_AWS_IP`
- **Admin Panel**: `http://YOUR_AWS_IP/admin`
- **API**: `http://YOUR_AWS_IP/api`

## 🛠️ Troubleshooting

### Check Service Status
```bash
sudo supervisorctl status
sudo systemctl status nginx
sudo systemctl status postgresql
```

### View Logs
```bash
sudo tail -f /var/log/supervisor/aurum-backend.log
sudo tail -f /var/log/supervisor/aurum-frontend.log
sudo tail -f /var/log/nginx/error.log
```

### Restart Services
```bash
sudo supervisorctl restart aurum-backend
sudo supervisorctl restart aurum-frontend
sudo systemctl restart nginx
```

### Common Issues
1. **Port already in use**: Check if services are already running
2. **Permission denied**: Ensure correct file ownership
3. **Database connection**: Verify PostgreSQL is running and credentials are correct
4. **Nginx 502**: Check that backend services are running

## 🔐 Security Notes

- Change default database password in production
- Consider setting up SSL/TLS with Let's Encrypt for domains
- Regular security updates: `sudo apt update && sudo apt upgrade`
- Monitor logs for suspicious activity

## 💾 Backup & Recovery

- **Manual Backup**: `/opt/aurumfinance/backup_db.sh`
- **Restore**: `/opt/aurumfinance/restore_db.sh backup_file.sql.gz`
- **Daily Backups**: Automated at 2:00 AM
- **Backup Location**: `/opt/aurumfinance/backups/`

## 📞 Support

If you encounter issues:
1. Check the troubleshooting section above
2. Review service logs
3. Verify all scripts completed successfully
4. Ensure AWS security groups allow HTTP traffic

---

**Total Deployment Time**: ~30-45 minutes
**AWS Cost**: ~$25-40/month (t3.medium)