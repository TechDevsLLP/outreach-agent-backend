#!/bin/bash
# OutFlo EC2 Ubuntu 24.04 First-Time Setup
# Run once on a fresh instance as root.
# Usage: sudo bash setup.sh

set -euo pipefail

APP_DIR="/opt/outflo"
APP_USER="outflo"

echo "=== OutFlo — EC2 First-Time Setup ==="

if [ "$EUID" -ne 0 ]; then
    echo "Error: run this script with sudo"
    exit 1
fi

# 1. System packages
echo "[1/6] Installing system packages..."
apt-get update -q
apt-get install -y python3 python3-pip python3-venv rsync nginx curl

PYTHON_VERSION=$(python3 --version)
echo "  Python: $PYTHON_VERSION"

# 2. Node.js 22 LTS
echo "[2/6] Installing Node.js 22 LTS..."
curl -fsSL https://deb.nodesource.com/setup_22.x | bash -
apt-get install -y nodejs
NODE_VERSION=$(node --version)
echo "  Node.js: $NODE_VERSION"

# 3. System user
echo "[3/6] Creating system user '$APP_USER'..."
if id "$APP_USER" &>/dev/null; then
    echo "  User '$APP_USER' already exists, skipping"
else
    useradd --system --no-create-home --shell /usr/sbin/nologin "$APP_USER"
    echo "  User '$APP_USER' created"
fi

# 4. Directory structure
echo "[4/6] Creating directory structure at $APP_DIR..."
mkdir -p "$APP_DIR/backend"
mkdir -p "$APP_DIR/frontend"
mkdir -p "$APP_DIR/frontend-src"
chown -R "$APP_USER:$APP_USER" "$APP_DIR"
echo "  Directories: backend/, frontend/, frontend-src/"

# 5. Python venv
echo "[5/6] Creating Python virtual environment at $APP_DIR/venv..."
python3 -m venv "$APP_DIR/venv"
"$APP_DIR/venv/bin/pip" install --upgrade pip --quiet
echo "  Venv created"

# 6. Nginx setup
echo "[6/6] Configuring Nginx..."
# Remove default site
rm -f /etc/nginx/sites-enabled/default

# Copy outflo nginx config (will be placed by deploy.sh on first deploy;
# create a placeholder if not yet deployed)
if [ -f "$APP_DIR/backend/deploy/nginx-outflo.conf" ]; then
    cp "$APP_DIR/backend/deploy/nginx-outflo.conf" /etc/nginx/sites-enabled/outflo
    nginx -t && systemctl reload nginx
    echo "  Nginx config installed"
else
    echo "  Nginx config not found yet — deploy the code first, then run:"
    echo "  sudo cp $APP_DIR/backend/deploy/nginx-outflo.conf /etc/nginx/sites-enabled/outflo && sudo nginx -t && sudo systemctl reload nginx"
fi

systemctl enable nginx
systemctl start nginx
echo "  Nginx enabled"

echo ""
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "  1. From your local machine, run:  bash backend/deploy/deploy.sh <EC2_IP> ~/.ssh/outflo-ec2.pem"
echo "  2. Upload .env:  scp -i ~/.ssh/outflo-ec2.pem backend/.env ubuntu@<EC2_IP>:/tmp/.env"
echo "     Then on the server:  sudo mv /tmp/.env $APP_DIR/backend/.env && sudo chown $APP_USER:$APP_USER $APP_DIR/backend/.env && sudo chmod 600 $APP_DIR/backend/.env"
echo "  3. Verify:  curl http://localhost:8008/health"
