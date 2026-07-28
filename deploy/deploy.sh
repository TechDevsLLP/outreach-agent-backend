#!/usr/bin/env bash
# OutFlo backend deploy: pull, install deps, restart services.
#
# This is a copy of the script that lives on the server at
# /opt/outflo/deploy.sh — it is kept here so the deploy is reviewable in git.
# Run it ON the server, not from a workstation:
#
#   ssh -i ~/.ssh/outflo-prod.pem ubuntu@13.232.17.194 "bash /opt/outflo/deploy.sh"
#
# It deploys whatever is on origin/main, so push first. The frontend is not
# involved — AWS Amplify builds it on push to main.
set -euo pipefail
APP_DIR=/opt/outflo/backend
cd "$APP_DIR"
echo "==> git pull"
git pull --ff-only
echo "==> pip install -r requirements.txt"
"$APP_DIR/.venv/bin/pip" install -r requirements.txt
echo "==> restarting services"
sudo systemctl restart outflo-backend.service
sudo systemctl restart outflo-scheduler.service
sleep 5
systemctl --no-pager --lines=0 status outflo-backend.service || true
systemctl --no-pager --lines=0 status outflo-scheduler.service || true
echo "==> done"
