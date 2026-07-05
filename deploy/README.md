# OutFlo — EC2 Deployment Guide

Single EC2 instance running all three processes behind an Nginx reverse proxy. MongoDB stays on Atlas.

```
Internet → EC2 Public IP :80
              │
           Nginx
              ├─ /api/*  → FastAPI (localhost:8008)   [outflo-backend]
              ├─ /health → FastAPI (localhost:8008)
              └─ /*      → Next.js (localhost:3000)   [outflo-frontend]

           Scheduler worker (no port)                 [outflo-scheduler]
           MongoDB Atlas (managed, off-instance)
```

---

## 1. Launch EC2 Instance

1. Go to **AWS Console → EC2 → Launch Instance**
2. Configure:
   - **Name**: `outflo`
   - **AMI**: Ubuntu Server 24.04 LTS
   - **Instance type**: `t3.small` (2 vCPU, 2GB RAM)
   - **Key pair**: Create or select one — download the `.pem` file
   - **Security group**:

| Type       | Port | Source    | Purpose    |
|------------|------|-----------|------------|
| SSH        | 22   | My IP     | SSH access |
| HTTP       | 80   | 0.0.0.0/0 | Nginx      |

3. Launch and note the **Public IP**

---

## 2. First-Time Server Setup

```bash
# Copy setup script to the server
scp -i outflo.pem backend/deploy/setup.sh ubuntu@13.233.13.69:/tmp/

# SSH in and run it
ssh -i outflo.pem ubuntu@13.233.13.69
sudo bash /tmp/setup.sh
```

This installs Python 3, Node.js 22, Nginx, creates the `outflo` system user, and sets up `/opt/outflo/`.

---

## 3. Upload .env File

The `.env` file contains secrets and is **never synced** by `deploy.sh`. Upload it once manually:

```bash
# From your local machine (project root)
scp -i outflo.pem backend/.env ubuntu@13.233.13.69:/tmp/.env

# SSH in and move it into place
ssh -i outflo.pem ubuntu@13.233.13.69
sudo mv /tmp/.env /opt/outflo/backend/.env
sudo chown outflo:outflo /opt/outflo/backend/.env
sudo chmod 600 /opt/outflo/backend/.env
```

**Update these values in the .env to match your EC2 IP:**

```
FRONTEND_URL=http://13.233.13.69
CORS_ORIGINS=http://13.233.13.69
BACKEND_BASE_URL=http://13.233.13.69
```

Also add the EC2 public IP to your **MongoDB Atlas → Network Access** allowlist.

---

## 4. Deploy (First Time and Every Update)

Run from the **project root** on your local machine:

```bash
bash backend/deploy/deploy.sh 13.233.13.69 outflo.pem
```

This script:
1. Rsyncs backend code to `/opt/outflo/backend/`
2. Rsyncs frontend source to `/opt/outflo/frontend-src/`
3. Installs Python dependencies
4. Builds the Next.js app on the server (`npm ci && next build`)
5. Copies the standalone output to `/opt/outflo/frontend/`
6. Installs/updates all systemd services and the Nginx config
7. Restarts all services
8. Runs a health check

**Build note:** The frontend is built on the server with your EC2 IP baked in as `NEXT_PUBLIC_API_URL`. If your IP ever changes, redeploy.

---

## 5. Verify

```bash
# Health check
curl http://13.233.13.69/health
# Expected: {"status":"ok","database":"connected"}

# Service status
ssh -i outflo.pem ubuntu@13.233.13.69 \
  'sudo systemctl status outflo-backend outflo-scheduler outflo-frontend nginx'

# Open in browser
open http://13.233.13.69
```

---

## Useful Commands

```bash
SSH_KEY=outflo.pem
EC2=ubuntu@13.233.13.69

# Live logs
ssh -i $SSH_KEY $EC2 'sudo journalctl -u outflo-backend   -f'
ssh -i $SSH_KEY $EC2 'sudo journalctl -u outflo-scheduler -f'
ssh -i $SSH_KEY $EC2 'sudo journalctl -u outflo-frontend  -f'
ssh -i $SSH_KEY $EC2 'sudo journalctl -u nginx            -f'

# Last 50 lines of logs
ssh -i $SSH_KEY $EC2 'sudo journalctl -u outflo-backend -n 50'

# Restart a service
ssh -i $SSH_KEY $EC2 'sudo systemctl restart outflo-backend'
ssh -i $SSH_KEY $EC2 'sudo systemctl restart outflo-frontend'
ssh -i $SSH_KEY $EC2 'sudo systemctl restart nginx'

# Check all services at once
ssh -i $SSH_KEY $EC2 'sudo systemctl status outflo-backend outflo-scheduler outflo-frontend nginx --no-pager'

# Disk usage
ssh -i $SSH_KEY $EC2 'df -h'

# Memory usage
ssh -i $SSH_KEY $EC2 'free -h'
```

---

## Troubleshooting

**Backend won't start**
```bash
sudo journalctl -u outflo-backend -n 50
# Common cause: missing .env or wrong MongoDB URL
```

**Frontend shows blank page / 502**
```bash
sudo journalctl -u outflo-frontend -n 50
sudo systemctl status outflo-frontend
# Common cause: build failed or wrong NEXT_PUBLIC_API_URL
```

**MongoDB connection error**
- Check Atlas → Network Access → Add EC2 public IP
- Verify `MONGODB_URL` in `/opt/outflo/backend/.env`

**502 Bad Gateway from Nginx**
```bash
sudo systemctl status outflo-backend outflo-frontend
sudo nginx -t
sudo journalctl -u nginx -n 20
```

**Permission errors**
```bash
sudo chown -R outflo:outflo /opt/outflo
```

**Out of memory (t3.small)**
```bash
free -h
# Add swap if needed:
sudo fallocate -l 1G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

---

## Adding a Domain + SSL (Future)

When you have a domain pointed at this EC2 IP:

```bash
# Install certbot
sudo apt install certbot python3-certbot-nginx

# Update nginx config: replace `server_name _;` with `server_name your-domain.com;`
sudo nano /etc/nginx/sites-enabled/outflo

# Get SSL certificate (auto-configures Nginx for HTTPS)
sudo certbot --nginx -d your-domain.com

# Update .env on the server
sudo nano /opt/outflo/backend/.env
# Set: FRONTEND_URL=https://your-domain.com
#      CORS_ORIGINS=https://your-domain.com
#      BACKEND_BASE_URL=https://your-domain.com
sudo systemctl restart outflo-backend
```
