# OutFlo deployment

## What actually runs where

| Piece | Where | How it deploys |
|---|---|---|
| FastAPI API + scheduler | EC2 `outflo-backend` (`i-00d67e6865e9863c9`), ap-south-1 | `bash /opt/outflo/deploy.sh` on the server (git pull + restart) |
| Next.js frontend | AWS Amplify | Automatic on push to `main` — nothing to run |
| MongoDB | Atlas (`outflo_v4`) | n/a |

The API is reached over HTTPS at `https://13-232-17-194.sslip.io` — there is no
custom domain. The certificate is a Let's Encrypt cert for the sslip.io name of
the elastic IP `13.232.17.194`, terminated by nginx, which proxies to uvicorn on
`127.0.0.1:8000`.

## Server layout

```text
/opt/outflo/deploy.sh          the deploy script (mirrored in this directory)
/opt/outflo/backend/           git checkout, tracks origin/main
/opt/outflo/backend/.venv/     python env
/opt/outflo/backend/.env       production secrets — never in git, never rsynced
```

Both services run as `ubuntu` from `/opt/outflo/backend`, with unit files at
`/etc/systemd/system/outflo-{backend,scheduler}.service`. `outflo-frontend.service`
exists on the box but is inactive and unused — the frontend is on Amplify.

## Deploying the backend

```bash
git push origin main
ssh -i ~/.ssh/outflo-prod.pem ubuntu@13.232.17.194 "bash /opt/outflo/deploy.sh"
curl -s https://13-232-17-194.sslip.io/health
```

`/health` returning `{"status":"ok","database":"connected"}` is the check. There
is no automatic rollback: to revert, `git checkout <sha>` in `/opt/outflo/backend`
and restart both services.

SSH is restricted by security group `sg-04a06199c2b95a9fb` to specific source
addresses. A new workstation needs its IP authorized on port 22 before it can
deploy.

## Capacity note

The instance is a `t4g.small`: 2 vCPU, ~1.8 GB RAM, no swap. That is adequate for
the API and scheduler but leaves little headroom during large discovery runs.
Watch memory before raising concurrency knobs.

## History

This directory previously described an EC2 release topology (atomic `releases/`
+ `current` symlink, `/etc/outflo/outflo.env`, a dedicated `outflo` user, port
8008, nginx and systemd units templated from the repo, frontend built on the
box). None of it was ever adopted — the server was set up by hand in the simpler
shape above. Those files were removed in July 2026 because running them against
the live box would have rewritten its nginx and systemd configuration to a
layout that does not exist there.
