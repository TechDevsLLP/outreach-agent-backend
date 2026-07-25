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
apt-get install -y python3 python3-pip python3-venv rsync nginx curl certbot python3-certbot-nginx

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
mkdir -p "$APP_DIR/releases"
mkdir -p "$APP_DIR/shared/venv"
mkdir -p "$APP_DIR/shared/logs"
mkdir -p /etc/outflo
mkdir -p /var/www/certbot
chown -R "$APP_USER:$APP_USER" "$APP_DIR"
chown root:"$APP_USER" /etc/outflo
chmod 750 /etc/outflo
echo "  Directories: releases/, shared/, /etc/outflo"

# 5. Python venv
echo "[5/6] Creating Python virtual environment at $APP_DIR/shared/venv..."
python3 -m venv "$APP_DIR/shared/venv"
"$APP_DIR/shared/venv/bin/pip" install --upgrade pip --quiet
echo "  Venv created"

# 6. Nginx setup
echo "[6/6] Configuring Nginx..."
# Remove default site
rm -f /etc/nginx/sites-enabled/default

systemctl enable nginx
systemctl start nginx
echo "  Nginx enabled; TLS site is installed by deploy.sh after a certificate exists"

echo ""
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "  1. Store production secrets in /etc/outflo/outflo.env (root:outflo, mode 640)."
echo "  2. Point the domain at the instance Elastic IP and issue a Let's Encrypt certificate."
echo "  3. Run: bash backend/deploy/deploy.sh <HOST> <DOMAIN> <SSH_KEY_PATH>"
