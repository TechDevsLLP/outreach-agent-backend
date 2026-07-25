#!/usr/bin/env bash
# Atomic EC2 release deployment. Secrets remain in /etc/outflo/outflo.env.
# Usage: bash backend/deploy/deploy.sh <HOST_OR_IP> <DOMAIN> <SSH_KEY_PATH>

set -euo pipefail

HOST="${1:-}"
DOMAIN="${2:-}"
SSH_KEY="${3:-}"
EC2_USER="${EC2_USER:-ubuntu}"
REMOTE_ROOT="/opt/outflo"
RELEASE_ID="$(date -u +%Y%m%dT%H%M%SZ)"
REMOTE_RELEASE="$REMOTE_ROOT/releases/$RELEASE_ID"

if [[ -z "$HOST" || -z "$DOMAIN" || -z "$SSH_KEY" ]]; then
    echo "Usage: bash backend/deploy/deploy.sh <HOST_OR_IP> <DOMAIN> <SSH_KEY_PATH>"
    exit 1
fi

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BACKEND_DIR="$PROJECT_ROOT/backend"
FRONTEND_DIR="$PROJECT_ROOT/frontend"
SSH=(ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new "$EC2_USER@$HOST")
RSYNC_SSH="ssh -i $SSH_KEY -o StrictHostKeyChecking=accept-new"

echo "[1/8] Running offline release gates"
(
    cd "$BACKEND_DIR"
    python3 -m pytest -q \
        tests/unit/test_production_config.py \
        tests/unit/test_wave_a_security_containment.py \
        tests/unit/test_job_queue_service.py \
        tests/unit/test_daily_cap_service.py \
        tests/unit/test_campaign_engine_sequence.py \
        tests/unit/test_campaign_prospect_state.py \
        tests/unit/test_conversation_isolation.py \
        tests/unit/test_connected_system_isolation.py \
        tests/unit/test_notification_isolation.py \
        tests/unit/test_campaign_sequence_launch_guard.py
)
(
    cd "$FRONTEND_DIR"
    npx tsc --noEmit
)

echo "[2/8] Verifying remote prerequisites"
"${SSH[@]}" "test -r /etc/outflo/outflo.env && test -r /etc/letsencrypt/live/$DOMAIN/fullchain.pem && test -x $REMOTE_ROOT/shared/venv/bin/python"

echo "[3/8] Uploading immutable release $RELEASE_ID"
"${SSH[@]}" "mkdir -p /tmp/outflo-$RELEASE_ID/backend /tmp/outflo-$RELEASE_ID/frontend-src"
rsync -az \
    --exclude '.git/' --exclude '.env' --exclude '__pycache__/' \
    --exclude '*.pyc' --exclude '.pytest_cache/' --exclude 'logs/' \
    -e "$RSYNC_SSH" "$BACKEND_DIR/" \
    "$EC2_USER@$HOST:/tmp/outflo-$RELEASE_ID/backend/"
rsync -az \
    --exclude '.git/' --exclude '.env*' --exclude 'node_modules/' \
    --exclude '.next/' --exclude '.eslintcache' \
    -e "$RSYNC_SSH" "$FRONTEND_DIR/" \
    "$EC2_USER@$HOST:/tmp/outflo-$RELEASE_ID/frontend-src/"
"${SSH[@]}" "sudo mkdir -p $REMOTE_RELEASE && sudo rsync -a /tmp/outflo-$RELEASE_ID/ $REMOTE_RELEASE/ && sudo chown -R outflo:outflo $REMOTE_RELEASE && rm -rf /tmp/outflo-$RELEASE_ID"

echo "[4/8] Installing application dependencies"
"${SSH[@]}" "sudo -u outflo $REMOTE_ROOT/shared/venv/bin/pip install --requirement $REMOTE_RELEASE/backend/requirements.txt --quiet && sudo -u outflo $REMOTE_ROOT/shared/venv/bin/python -m compileall -q $REMOTE_RELEASE/backend"

echo "[5/8] Building the production frontend"
"${SSH[@]}" "cd $REMOTE_RELEASE/frontend-src && sudo -u outflo npm ci --ignore-scripts --quiet && sudo -u outflo env NEXT_PUBLIC_API_URL=https://$DOMAIN npm run build"
"${SSH[@]}" "sudo -u outflo mkdir -p $REMOTE_RELEASE/frontend/.next && sudo -u outflo cp -R $REMOTE_RELEASE/frontend-src/.next/standalone/. $REMOTE_RELEASE/frontend/ && sudo -u outflo cp -R $REMOTE_RELEASE/frontend-src/.next/static $REMOTE_RELEASE/frontend/.next/static && sudo -u outflo cp -R $REMOTE_RELEASE/frontend-src/public $REMOTE_RELEASE/frontend/public && sudo -u outflo mkdir -p $REMOTE_RELEASE/backend/logs"

echo "[6/8] Installing verified service and TLS proxy configuration"
"${SSH[@]}" "sudo cp $REMOTE_RELEASE/backend/deploy/outflo-backend.service /etc/systemd/system/outflo-backend.service && sudo cp $REMOTE_RELEASE/backend/deploy/outflo-scheduler.service /etc/systemd/system/outflo-scheduler.service && sudo cp $REMOTE_RELEASE/backend/deploy/outflo-frontend.service /etc/systemd/system/outflo-frontend.service && sed 's/__DOMAIN__/$DOMAIN/g' $REMOTE_RELEASE/backend/deploy/nginx-outflo.conf | sudo tee /etc/nginx/sites-enabled/outflo >/dev/null && sudo rm -f /etc/nginx/sites-enabled/default && sudo nginx -t && sudo systemctl daemon-reload"

echo "[7/8] Atomically activating release"
PREVIOUS_RELEASE="$("${SSH[@]}" "readlink -f $REMOTE_ROOT/current || true")"
"${SSH[@]}" "sudo ln -sfn $REMOTE_RELEASE $REMOTE_ROOT/current && sudo systemctl enable outflo-backend outflo-scheduler outflo-frontend nginx >/dev/null && sudo systemctl restart outflo-backend outflo-scheduler outflo-frontend && sudo systemctl reload nginx"

echo "[8/8] Verifying health and rolling back on failure"
healthy=false
for attempt in $(seq 1 12); do
    if curl --fail --silent --show-error "https://$DOMAIN/health" >/dev/null; then
        healthy=true
        break
    fi
    sleep 5
done

if [[ "$healthy" != true ]]; then
    echo "Health verification failed. Rolling back to: $PREVIOUS_RELEASE"
    if [[ -n "$PREVIOUS_RELEASE" ]]; then
        "${SSH[@]}" "sudo ln -sfn $PREVIOUS_RELEASE $REMOTE_ROOT/current && sudo systemctl restart outflo-backend outflo-scheduler outflo-frontend"
    fi
    exit 1
fi

echo "Release $RELEASE_ID is healthy at https://$DOMAIN"
echo "Rollback target retained at: ${PREVIOUS_RELEASE:-none}"
