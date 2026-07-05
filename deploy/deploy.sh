#!/bin/bash
# Deploy OutFlo to EC2 (backend + frontend)
# Run from the project root directory.
# Usage: bash backend/deploy/deploy.sh <EC2_IP> [SSH_KEY_PATH]
# Example: bash backend/deploy/deploy.sh 54.123.45.67 ~/.ssh/outflo-ec2.pem

set -euo pipefail

EC2_IP="${1:-}"
SSH_KEY="${2:-~/.ssh/outflo-ec2.pem}"
EC2_USER="ubuntu"
REMOTE_DIR="/opt/outflo"

# Resolve absolute path to project root (one level up from this script)
PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BACKEND_DIR="$PROJECT_ROOT/backend"
FRONTEND_DIR="$PROJECT_ROOT/frontend"

if [ -z "$EC2_IP" ]; then
    echo "Usage: bash backend/deploy/deploy.sh <EC2_IP> [SSH_KEY_PATH]"
    echo "Example: bash backend/deploy/deploy.sh 54.123.45.67 ~/.ssh/outflo-ec2.pem"
    exit 1
fi

SSH_CMD="ssh -i $SSH_KEY -o StrictHostKeyChecking=no $EC2_USER@$EC2_IP"
RSYNC_SSH="ssh -i $SSH_KEY -o StrictHostKeyChecking=no"

echo "=== Deploying OutFlo to $EC2_IP ==="
echo ""

# ── Step 1: Sync backend ────────────────────────────────────────────────────
echo "[1/6] Syncing backend code..."
rsync -az --delete \
    --exclude 'venv/' \
    --exclude '__pycache__/' \
    --exclude '.git/' \
    --exclude '.env' \
    --exclude '*.pyc' \
    --exclude '.DS_Store' \
    --exclude '.claude/' \
    -e "$RSYNC_SSH" \
    "$BACKEND_DIR/" \
    "$EC2_USER@$EC2_IP:/tmp/outflo-backend/"

$SSH_CMD "sudo rsync -a --delete --exclude '.env' /tmp/outflo-backend/ $REMOTE_DIR/backend/ && \
          sudo chown -R outflo:outflo $REMOTE_DIR/backend && \
          rm -rf /tmp/outflo-backend"
echo "  Backend synced to $REMOTE_DIR/backend/"

# ── Step 2: Sync frontend source ────────────────────────────────────────────
echo "[2/6] Syncing frontend source..."
rsync -az --delete \
    --exclude 'node_modules/' \
    --exclude '.next/' \
    --exclude '.git/' \
    --exclude '.DS_Store' \
    -e "$RSYNC_SSH" \
    "$FRONTEND_DIR/" \
    "$EC2_USER@$EC2_IP:/tmp/outflo-frontend-src/"

$SSH_CMD "sudo rsync -a --delete /tmp/outflo-frontend-src/ $REMOTE_DIR/frontend-src/ && \
          sudo chown -R outflo:outflo $REMOTE_DIR/frontend-src && \
          rm -rf /tmp/outflo-frontend-src"
echo "  Frontend source synced to $REMOTE_DIR/frontend-src/"

# ── Step 3: Install Python dependencies ─────────────────────────────────────
echo "[3/6] Installing Python dependencies..."
$SSH_CMD "sudo $REMOTE_DIR/venv/bin/pip install -r $REMOTE_DIR/backend/requirements.txt --quiet"
echo "  Python deps installed"

# ── Step 4: Build frontend ───────────────────────────────────────────────────
echo "[4/6] Building frontend (this may take a minute)..."
$SSH_CMD "cd $REMOTE_DIR/frontend-src && \
          sudo npm ci --quiet && \
          sudo NEXT_PUBLIC_API_URL=http://$EC2_IP npx next build"
echo "  Frontend built"

# ── Step 5: Deploy frontend standalone output ────────────────────────────────
echo "[5/6] Deploying frontend standalone output..."
$SSH_CMD "sudo rm -rf $REMOTE_DIR/frontend && \
          sudo mkdir -p $REMOTE_DIR/frontend && \
          sudo cp -r $REMOTE_DIR/frontend-src/.next/standalone/. $REMOTE_DIR/frontend/ && \
          sudo mkdir -p $REMOTE_DIR/frontend/.next && \
          sudo cp -r $REMOTE_DIR/frontend-src/.next/static $REMOTE_DIR/frontend/.next/static && \
          sudo cp -r $REMOTE_DIR/frontend-src/public $REMOTE_DIR/frontend/public && \
          sudo chown -R outflo:outflo $REMOTE_DIR/frontend"
echo "  Frontend deployed to $REMOTE_DIR/frontend/"

# ── Step 6: Install services and restart ─────────────────────────────────────
echo "[6/6] Installing systemd services and restarting..."
$SSH_CMD "
    # Install nginx config
    sudo cp $REMOTE_DIR/backend/deploy/nginx-outflo.conf /etc/nginx/sites-enabled/outflo
    sudo rm -f /etc/nginx/sites-enabled/default
    sudo nginx -t

    # Install systemd services
    sudo cp $REMOTE_DIR/backend/deploy/outflo-backend.service   /etc/systemd/system/outflo-backend.service
    sudo cp $REMOTE_DIR/backend/deploy/outflo-scheduler.service /etc/systemd/system/outflo-scheduler.service
    sudo cp $REMOTE_DIR/backend/deploy/outflo-frontend.service  /etc/systemd/system/outflo-frontend.service
    sudo systemctl daemon-reload

    # Enable + restart all services
    sudo systemctl enable outflo-backend outflo-scheduler outflo-frontend
    sudo systemctl restart outflo-backend outflo-scheduler outflo-frontend
    sudo systemctl reload nginx
"
echo "  Services restarted"

# ── Health check ─────────────────────────────────────────────────────────────
echo ""
echo "Waiting for backend to start..."
for i in $(seq 1 12); do
    sleep 5
    if curl -sf "http://$EC2_IP/health" > /dev/null 2>&1; then
        echo "Health check passed (${i}x5s):"
        curl -s "http://$EC2_IP/health"
        echo ""
        break
    fi
    echo "  Still starting... ($i/12)"
    if [ "$i" -eq 12 ]; then
        echo "  ERROR: App did not respond after 60s."
        echo "  Check logs: ssh -i $SSH_KEY $EC2_USER@$EC2_IP 'sudo journalctl -u outflo-backend -n 50'"
        exit 1
    fi
done

echo ""
echo "=== Deploy complete ==="
echo ""
echo "  App:    http://$EC2_IP"
echo "  Health: http://$EC2_IP/health"
echo ""
echo "Logs:"
echo "  ssh -i $SSH_KEY $EC2_USER@$EC2_IP 'sudo journalctl -u outflo-backend -f'"
echo "  ssh -i $SSH_KEY $EC2_USER@$EC2_IP 'sudo journalctl -u outflo-scheduler -f'"
echo "  ssh -i $SSH_KEY $EC2_USER@$EC2_IP 'sudo journalctl -u outflo-frontend -f'"
