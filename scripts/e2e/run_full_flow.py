#!/usr/bin/env python3
"""
QA Flow Harness: Onboarding → Smart Campaign (prithvi@techdevs.in)

Smoke:    10 companies / 20 prospects  (real — shake out bugs cheaply)
Delivery: 200 companies / 400 prospects

Usage:
    cd /Users/prasad/Documents/Projects/outflo/backend
    python3 scripts/e2e/run_full_flow.py --profile smoke
    python3 scripts/e2e/run_full_flow.py --profile delivery
    python3 scripts/e2e/run_full_flow.py --profile smoke --resume
    python3 scripts/e2e/run_full_flow.py --profile smoke --run-dir scripts/e2e/runs/20260703-120000

Prerequisites:
    uvicorn must be running on port 8008.
    Real Gemini + Apify + OpenRouter credits must be available.
"""

import argparse
import asyncio
import json as _json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import httpx

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = os.environ.get("OUTFLO_E2E_BASE_URL", "http://localhost:8008")

QA_EMAIL    = "prithvi@techdevs.in"
QA_PASSWORD = "TechDevs2025!"
QA_NAME     = "Prithvi Jadwani"
QA_COMPANY  = "TechDevs"

SCRIPT_DIR = Path(__file__).parent
RUNS_DIR   = SCRIPT_DIR / "runs"

ICP_INDUSTRIES = [
    "Vape / smoke / tobacco retail",
    "Cannabis dispensaries",
    "CBD / hemp / Delta-8 retail",
    "Independent and regional pharmacy chains",
    "Firearms / gun stores and shooting ranges",
    "Liquor / package stores",
    "Retail",
    "Consumer Goods",
]
ICP_JOB_TITLES = [
    "Owner", "Founder", "Co-founder", "CEO", "President",
    "Chief Executive Officer", "VP Operations", "Vice President Operations",
    "Director of Operations", "Multi-unit franchisee", "Franchise owner",
    "Regional Director", "Managing Partner",
]
ICP_SENIORITY_LEVELS = ["Founder", "C-Level", "VP", "Director", "Owner"]
ICP_COUNTRIES        = ["United States"]

VALUE_PROPOSITION = (
    "We make multi-outlet brands the #1 result on Google Maps and Apple Maps "
    "in every neighbourhood they operate — driving footfall, calls, and bookings "
    "to physical stores."
)
PAIN_POINT = (
    "Cannot run Google Ads due to platform restrictions — "
    "Google Maps IS the primary discovery channel"
)

STEP_ORDER = [
    "register", "login", "onboarding_start",
    "stage1_scrape", "stage1_poll", "stage1_save",
    "stage2_voice", "stage3_icp", "stage4_offer",
    "stage6_chat", "wizard_complete",
    "connect_sender", "create_campaign",
    "discovery_poll", "approve_day1", "approve_day2",
]

PROFILE_CONFIGS = {
    "smoke": {
        "company_count":   10,
        "prospect_count":  25,     # ge=25 model constraint
        "poll_timeout":    900,    # 15 min
        "campaign_name":   f"QA Smoke — {datetime.utcnow().strftime('%Y%m%d-%H%M')}",
    },
    "delivery": {
        "company_count":   200,
        "prospect_count":  400,
        "poll_timeout":    7200,   # 2 hours
        "campaign_name":   f"QA Delivery — {datetime.utcnow().strftime('%Y%m%d-%H%M')}",
    },
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("qa_harness")


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class StepFailed(Exception):
    def __init__(self, step: str, status: int | None, body):
        self.step   = step
        self.status = status
        self.body   = body
        super().__init__(f"[{step}] HTTP {status}: {body}")


# ---------------------------------------------------------------------------
# Logging HTTP client
# ---------------------------------------------------------------------------

_REDACT_KEYS = {"password", "smtp_password"}


class LoggingClient:
    """httpx wrapper that logs every call to api_log.jsonl BEFORE any raise."""

    def __init__(self, run_dir: Path, base_url: str = BASE_URL, timeout: float = 120.0):
        self._client   = httpx.AsyncClient(base_url=base_url, timeout=timeout)
        self._log_path = run_dir / "api_log.jsonl"
        self._seq      = 0
        self._token: str | None = None

    def set_token(self, token: str) -> None:
        self._token = token

    @property
    def _auth_headers(self) -> dict:
        return {"Authorization": f"Bearer {self._token}"} if self._token else {}

    def _redact(self, body: dict | None) -> dict | None:
        if not isinstance(body, dict):
            return body
        return {k: ("***" if k in _REDACT_KEYS else v) for k, v in body.items()}

    def _append_log(self, entry: dict) -> None:
        with open(self._log_path, "a") as f:
            f.write(_json.dumps(entry, default=str) + "\n")

    async def call(
        self,
        step: str,
        method: str,
        path: str,
        json: dict | None = None,
        params: dict | None = None,
        ok_statuses: set[int] | None = None,
        fatal: bool = True,
    ) -> tuple[int, dict | str | list]:
        """Make a request, log it, return (status_code, body). Raises StepFailed if fatal & non-ok."""
        self._seq += 1
        ok_statuses = ok_statuses or set(range(200, 300))
        t0 = time.monotonic()

        entry: dict = {
            "seq":     self._seq,
            "ts":      datetime.utcnow().isoformat(),
            "step":    step,
            "method":  method.upper(),
            "path":    path,
            "request": self._redact(json),
            "params":  params,
        }

        try:
            resp = await self._client.request(
                method, path,
                headers=self._auth_headers,
                json=json,
                params=params,
            )
            elapsed_ms = (time.monotonic() - t0) * 1000
            try:
                body = resp.json()
            except Exception:
                body = resp.text

            entry.update({
                "status":     resp.status_code,
                "response":   body,
                "elapsed_ms": round(elapsed_ms, 1),
                "ok":         resp.status_code in ok_statuses,
            })
            self._append_log(entry)

            if fatal and resp.status_code not in ok_statuses:
                raise StepFailed(step, resp.status_code, body)

            return resp.status_code, body

        except httpx.RequestError as exc:
            elapsed_ms = (time.monotonic() - t0) * 1000
            entry.update({
                "status":        None,
                "response":      str(exc),
                "elapsed_ms":    round(elapsed_ms, 1),
                "ok":            False,
                "network_error": True,
            })
            self._append_log(entry)
            raise StepFailed(step, None, str(exc)) from exc

    async def aclose(self) -> None:
        await self._client.aclose()


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------

class Checkpoint:
    """Persist step statuses and shared state across runs."""

    def __init__(self, run_dir: Path, run_id: str, profile: str):
        self._path = run_dir / "checkpoint.json"
        _default = {
            "run_id":              run_id,
            "profile":             profile,
            "created_at":          datetime.utcnow().isoformat(),
            "last_completed_step": None,
            "steps":               {},
            "state": {
                "token":            None,
                "session_id":       None,
                "job_id":           None,
                "campaign_id":      None,
                "email_account_id": None,
                "stage1_analysis":  None,
                "day1_date":        None,
                "discovery_result": None,
            },
        }
        if self._path.exists():
            self._data = _json.loads(self._path.read_text())
        else:
            self._data = _default

    @property
    def state(self) -> dict:
        return self._data["state"]

    def done(self, step: str) -> bool:
        return self._data["steps"].get(step, {}).get("status") == "done"

    def mark_done(self, step: str, **extra) -> None:
        self._data["steps"][step] = {
            "status": "done",
            "at":     datetime.utcnow().isoformat(),
            **extra,
        }
        self._data["last_completed_step"] = step
        self._save()

    def mark_failed(self, step: str, error: str) -> None:
        self._data["steps"][step] = {
            "status": "failed",
            "at":     datetime.utcnow().isoformat(),
            "error":  error,
        }
        self._save()

    def update_state(self, **kwargs) -> None:
        self._data["state"].update(kwargs)
        self._save()

    def _save(self) -> None:
        with open(self._path, "w") as f:
            _json.dump(self._data, f, indent=2, default=str)

    def summary(self) -> dict:
        return {k: self._data[k] for k in ("run_id", "profile", "steps", "state")}


# ---------------------------------------------------------------------------
# Observations log
# ---------------------------------------------------------------------------

class ObsLog:
    SEVERITIES = ("blocker", "major", "minor", "polish")

    def __init__(self, run_dir: Path):
        self._path = run_dir / "observations.md"
        if not self._path.exists():
            self._path.write_text("# QA Observations\n\nSeverities: blocker | major | minor | polish\n\n")

    def log(self, severity: str, title: str, detail: str) -> None:
        ts    = datetime.utcnow().strftime("%H:%M:%S")
        entry = f"\n---\n## [{severity.upper()}] {title}  `{ts}`\n\n{detail}\n"
        with open(self._path, "a") as f:
            f.write(entry)
        logger.warning("[OBS:%s] %s — %s", severity.upper(), title, detail[:300])


# ---------------------------------------------------------------------------
# Poll helper
# ---------------------------------------------------------------------------

async def poll(
    client:   LoggingClient,
    step:     str,
    path:     str,
    done_fn,
    interval: int = 15,
    timeout:  int = 900,
    params:   dict | None = None,
) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        _, body = await client.call(step, "GET", path, params=params, fatal=False)
        if isinstance(body, dict):
            _log_fields = {
                k: body[k]
                for k in ("discovery_status", "message_gen_status", "status", "progress", "error")
                if k in body
            }
        else:
            _log_fields = body
        logger.info("[poll:%s] %s", step, _log_fields)
        if done_fn(body):
            return body
        await asyncio.sleep(interval)
    raise TimeoutError(f"poll:{step} did not complete within {timeout}s")


# ---------------------------------------------------------------------------
# Step functions
# ---------------------------------------------------------------------------

async def step_register(client: LoggingClient, chk: Checkpoint, obs: ObsLog) -> None:
    if chk.done("register"):
        logger.info("[skip] register")
        return
    status, body = await client.call(
        "register", "POST", "/api/auth/register",
        json={"email": QA_EMAIL, "password": QA_PASSWORD, "name": QA_NAME, "company_name": QA_COMPANY},
        ok_statuses={200, 201, 400, 409, 422},
        fatal=False,
    )
    if status not in (200, 201, 400, 409, 422):
        obs.log("blocker", "register unexpected status", f"HTTP {status}: {body}")
        raise StepFailed("register", status, body)
    chk.mark_done("register")


async def step_login(client: LoggingClient, chk: Checkpoint, obs: ObsLog) -> None:
    if chk.done("login"):
        logger.info("[skip] login — rehydrating token")
        client.set_token(chk.state["token"])
        return
    _, body = await client.call(
        "login", "POST", "/api/auth/login",
        json={"email": QA_EMAIL, "password": QA_PASSWORD},
    )
    token = body.get("access_token") or body.get("token")
    if not token:
        raise StepFailed("login", 200, f"No token in response: {body}")
    client.set_token(token)
    chk.update_state(token=token)
    chk.mark_done("login")


async def step_onboarding_start(client: LoggingClient, chk: Checkpoint, obs: ObsLog) -> None:
    if chk.done("onboarding_start"):
        logger.info("[skip] onboarding_start — session_id=%s", chk.state["session_id"])
        return
    _, body = await client.call(
        "onboarding_start", "POST", "/api/onboarding/start",
        json={"company_url": "https://techdevs.in"},
    )
    session_id = body.get("session_id")
    if not session_id:
        raise StepFailed("onboarding_start", 200, f"No session_id: {body}")
    chk.update_state(session_id=session_id)
    chk.mark_done("onboarding_start", session_id=session_id)


async def step_stage1_scrape(client: LoggingClient, chk: Checkpoint, obs: ObsLog) -> None:
    if chk.done("stage1_scrape"):
        logger.info("[skip] stage1_scrape — job_id=%s", chk.state["job_id"])
        return
    _, body = await client.call(
        "stage1_scrape", "POST", "/api/onboarding/stage-1/scrape-company",
        json={
            "session_id":   chk.state["session_id"],
            "url":          "https://techdevs.in",
            "company_name": "TechDevs",
            "description":  "Local SEO and Google Maps optimization for multi-outlet brands",
        },
    )
    job_id = body.get("job_id")
    if not job_id:
        raise StepFailed("stage1_scrape", 200, f"No job_id: {body}")
    chk.update_state(job_id=job_id)
    chk.mark_done("stage1_scrape", job_id=job_id)


async def step_stage1_poll(client: LoggingClient, chk: Checkpoint, obs: ObsLog) -> None:
    if chk.done("stage1_poll"):
        logger.info("[skip] stage1_poll")
        return
    job_id = chk.state["job_id"]

    def _done(body):
        return isinstance(body, dict) and body.get("status") in ("completed", "done", "failed", "error")

    result = await poll(
        client, "stage1_poll", f"/api/onboarding/stage-1/result/{job_id}",
        done_fn=_done, interval=5, timeout=300,
    )
    if result.get("status") in ("failed", "error"):
        obs.log("major", "Stage-1 company scrape failed — using fallback analysis", str(result.get("error")))
        analysis = {
            "services":        ["Google Maps optimization", "Local SEO"],
            "industries":      ICP_INDUSTRIES[:4],
            "pain_points":     ["Cannot run Google Ads — Maps is primary discovery"],
            "icp_description": "Multi-outlet US brands in ad-restricted verticals",
        }
    else:
        analysis = result.get("result") or {}
    chk.update_state(stage1_analysis=analysis)
    chk.mark_done("stage1_poll")


async def step_stage1_save(client: LoggingClient, chk: Checkpoint, obs: ObsLog) -> None:
    if chk.done("stage1_save"):
        logger.info("[skip] stage1_save")
        return
    _, body = await client.call(
        "stage1_save", "POST", "/api/onboarding/stage-1/save",
        json={"session_id": chk.state["session_id"], "analysis": chk.state["stage1_analysis"] or {}},
    )
    chk.mark_done("stage1_save")


async def step_stage2_voice(client: LoggingClient, chk: Checkpoint, obs: ObsLog) -> None:
    if chk.done("stage2_voice"):
        logger.info("[skip] stage2_voice")
        return
    status, body = await client.call(
        "stage2_voice", "POST", "/api/onboarding/stage-2/linkedin-voice",
        json={"session_id": chk.state["session_id"]},
        fatal=False,
    )
    if status >= 400:
        obs.log("minor", "Stage-2 LinkedIn voice failed (non-fatal)", f"HTTP {status}: {body}")
    chk.mark_done("stage2_voice")


async def step_stage3_icp(client: LoggingClient, chk: Checkpoint, obs: ObsLog) -> None:
    if chk.done("stage3_icp"):
        logger.info("[skip] stage3_icp")
        return
    _, body = await client.call(
        "stage3_icp", "POST", "/api/onboarding/stage-3/icp",
        json={
            "session_id":        chk.state["session_id"],
            "industries":        ICP_INDUSTRIES,
            "job_titles":        ICP_JOB_TITLES,
            "seniority_levels":  ICP_SENIORITY_LEVELS,
            "geographies":       ICP_COUNTRIES,
            "company_size_ranges": ["51-200", "201-1000", "1000+"],
            # locked_industry deliberately OMITTED to prevent auto-campaign creation
        },
    )
    chk.mark_done("stage3_icp")


async def step_stage4_offer(client: LoggingClient, chk: Checkpoint, obs: ObsLog) -> None:
    if chk.done("stage4_offer"):
        logger.info("[skip] stage4_offer")
        return
    _, body = await client.call(
        "stage4_offer", "POST", "/api/onboarding/stage-4/offer",
        json={
            "session_id":            chk.state["session_id"],
            "primary_cta":           "Book a free 20-min Maps audit",
            "discovery_call_agenda": "Show exactly where you're losing footfall to competitors who outrank you on Google Maps",
            "qualifier_questions":   [
                "How many locations do you operate?",
                "Are you running Google Ads for your locations?",
            ],
        },
    )
    chk.mark_done("stage4_offer")


async def step_stage6_chat(client: LoggingClient, chk: Checkpoint, obs: ObsLog) -> None:
    if chk.done("stage6_chat"):
        logger.info("[skip] stage6_chat")
        return
    status, body = await client.call(
        "stage6_chat", "POST", "/api/onboarding/stage-6/chat",
        json={
            "session_id":   chk.state["session_id"],
            "user_message": (
                "Our main competitors are SOCi and Yext. We beat them on per-outlet pricing "
                "and Maps-specific expertise for ad-restricted verticals like vape and cannabis."
            ),
        },
        fatal=False,
    )
    if status >= 400:
        obs.log("minor", "Stage-6 chat failed (non-fatal)", f"HTTP {status}: {body}")
    chk.mark_done("stage6_chat")


async def step_wizard_complete(client: LoggingClient, chk: Checkpoint, obs: ObsLog) -> None:
    if chk.done("wizard_complete"):
        logger.info("[skip] wizard_complete")
        return
    _, body = await client.call(
        "wizard_complete", "POST", "/api/onboarding/wizard/complete",
        json={"session_id": chk.state["session_id"]},
    )
    chk.mark_done("wizard_complete")


async def step_connect_sender(client: LoggingClient, chk: Checkpoint, obs: ObsLog) -> None:
    if chk.done("connect_sender"):
        logger.info("[skip] connect_sender — email_account_id=%s", chk.state["email_account_id"])
        return

    # Check for an existing connected email account first (resume-safe)
    status, body = await client.call("list_email_accounts", "GET", "/api/email-accounts", fatal=False)
    if status == 200 and isinstance(body, list) and body:
        existing = next(
            (a for a in body if a.get("status") in ("connected", "active")),
            None,
        )
        if existing:
            ea_id = existing.get("_id") or existing.get("id") or ""
            if ea_id:
                chk.update_state(email_account_id=str(ea_id))
                chk.mark_done("connect_sender", reused=True, email_account_id=str(ea_id))
                logger.info("[connect_sender] reused existing account %s", ea_id)
                return

    # Connect a dummy SMTP sender — server inserts status=connected without connection validation
    _, body = await client.call(
        "connect_sender", "POST", "/api/email-accounts/smtp",
        json={
            "email":           "qa-sender@techdevs.in",
            "display_name":    "TechDevs QA Sender",
            "smtp_host":       "smtp.techdevs.in",
            "smtp_port":       587,
            "smtp_encryption": "tls",
            "smtp_username":   "qa-sender@techdevs.in",
            "smtp_password":   "QA_SMTP_2026!",
        },
        ok_statuses={200, 201},
    )
    ea_id = body.get("_id") or body.get("id") or ""
    if not ea_id:
        raise StepFailed("connect_sender", 201, f"No _id in response: {body}")
    chk.update_state(email_account_id=str(ea_id))
    chk.mark_done("connect_sender", email_account_id=str(ea_id))


async def step_create_campaign(
    client: LoggingClient, chk: Checkpoint, obs: ObsLog, cfg: dict
) -> None:
    if chk.done("create_campaign"):
        logger.info("[skip] create_campaign — campaign_id=%s", chk.state["campaign_id"])
        return
    _, body = await client.call(
        "create_campaign", "POST", "/api/campaigns/smart",
        json={
            "name":                          cfg["campaign_name"],
            "type":                          "custom",
            "discovery_mode":                "curated",
            "curated_company_count_target":  cfg["company_count"],
            "prospect_count_target":         cfg["prospect_count"],
            "discovery_enrollment_cap":      2,
            "max_prospects_per_company":     2,
            "icp_industries":                ICP_INDUSTRIES,
            "icp_job_titles":                ICP_JOB_TITLES,
            "icp_seniority_levels":          ICP_SENIORITY_LEVELS,
            "icp_countries":                 ICP_COUNTRIES,
            "icp_company_size_min":          51,
            "message_tone":                  "professional",
            "value_proposition":             VALUE_PROPOSITION,
            "pain_point":                    PAIN_POINT,
            "cta_type":                      "book_call",
            # discovery_mock_mode NOT set — real run
        },
    )
    campaign = body.get("campaign") or body
    campaign_id = campaign.get("_id") or campaign.get("id")
    if not campaign_id:
        raise StepFailed("create_campaign", 200, f"No campaign _id in response: {body}")
    chk.update_state(campaign_id=str(campaign_id))
    chk.mark_done("create_campaign", campaign_id=str(campaign_id))
    logger.info("[create_campaign] campaign_id=%s", campaign_id)


async def step_discovery_poll(
    client: LoggingClient, chk: Checkpoint, obs: ObsLog, cfg: dict
) -> None:
    if chk.done("discovery_poll"):
        logger.info("[skip] discovery_poll")
        return
    campaign_id = chk.state["campaign_id"]
    path = f"/api/campaigns/{campaign_id}/discovery-status"

    def _done(body):
        if not isinstance(body, dict):
            return False
        ds = body.get("discovery_status")
        mg = body.get("message_gen_status")
        if ds == "failed":
            return True
        if ds != "completed":
            return False
        # Discovery done — also wait for message gen to leave idle/running
        return mg not in ("idle", "running")

    result = await poll(
        client, "discovery_poll", path,
        done_fn=_done, interval=20, timeout=cfg["poll_timeout"],
    )

    if result.get("discovery_status") == "failed":
        err = result.get("discovery_error") or result.get("discovery_failure_reason") or "unknown"
        obs.log(
            "blocker", "Discovery FAILED",
            f"discovery_error: {err}\n\n```json\n{_json.dumps(result, indent=2, default=str)}\n```",
        )
        raise StepFailed("discovery_poll", 200, f"discovery_status=failed: {err}")

    logger.info(
        "[discovery_poll] DONE — companies=%d enrolled=%d msg_gen=%s",
        result.get("discovery_companies_found", 0),
        result.get("discovery_prospects_enrolled", 0),
        result.get("message_gen_status"),
    )

    if result.get("discovery_prospects_enrolled", 0) == 0:
        obs.log("major", "Discovery completed but 0 prospects enrolled", _json.dumps(result, indent=2, default=str))

    chk.update_state(discovery_result=result)
    chk.mark_done(
        "discovery_poll",
        companies_found=result.get("discovery_companies_found", 0),
        enrolled=result.get("discovery_prospects_enrolled", 0),
        msg_gen_status=result.get("message_gen_status"),
    )


async def step_approve_day1(client: LoggingClient, chk: Checkpoint, obs: ObsLog) -> None:
    if chk.done("approve_day1"):
        logger.info("[skip] approve_day1")
        return
    campaign_id = chk.state["campaign_id"]
    # Retry loop: on 400 trigger generate-messages?day=1 then wait and retry.
    # Handles cases where message gen failed (e.g. API credits exhausted) and
    # was subsequently fixed, or where messages are still generating after discovery.
    _retry_interval = 30
    _max_retries = 20  # 20 × 30s = 600s max
    status, body = 400, {}
    for _attempt in range(_max_retries):
        status, body = await client.call(
            "approve_day1", "POST", f"/api/campaigns/{campaign_id}/approve-day/1",
            ok_statuses={200, 400},
            fatal=False,
        )
        if status == 200:
            break
        if status >= 500:
            raise StepFailed("approve_day1", status, body)
        # 400 — trigger regeneration and wait
        _detail = (body.get("detail") if isinstance(body, dict) else str(body)) or ""
        logger.info("[approve_day1] 400 on attempt %d, triggering generate-messages?day=1", _attempt + 1)
        await client.call(
            f"approve_day1_regen_{_attempt}", "POST",
            f"/api/campaigns/{campaign_id}/generate-messages",
            params={"day": 1},
            ok_statuses={200, 400},
            fatal=False,
        )
        if _attempt < _max_retries - 1:
            await asyncio.sleep(_retry_interval)
    else:
        obs.log("blocker", "approve_day1 timed out — day-1 messages never ready", str(body))
        raise StepFailed("approve_day1", status, body)
    day1_date = (body.get("campaign") or {}).get("launch_day1_date") or body.get("day1_date")
    if day1_date:
        chk.update_state(day1_date=day1_date)
    chk.mark_done("approve_day1", day1_date=day1_date)
    logger.info("[approve_day1] day1_date=%s", day1_date)


async def step_approve_day2(client: LoggingClient, chk: Checkpoint, obs: ObsLog) -> None:
    if chk.done("approve_day2"):
        logger.info("[skip] approve_day2")
        return
    campaign_id = chk.state["campaign_id"]
    # Pre-check: fetch all enrollments (2 pages max) to count day-2 ones.
    _day2_count = 0
    _total_enrolled = 0
    for _pg in (1, 2):
        _, enr_body = await client.call(
            f"approve_day2_preflight_p{_pg}", "GET",
            f"/api/campaigns/{campaign_id}/enrollments",
            params={"page": _pg, "page_size": 200},
            ok_statuses={200, 404, 422},
            fatal=False,
        )
        if not isinstance(enr_body, dict):
            break
        _total_enrolled = enr_body.get("total", _total_enrolled)
        _page_enrs = enr_body.get("enrollments", [])
        _day2_count += sum(1 for e in _page_enrs if e.get("smart_campaign_send_day", 1) > 1)
        if len(_page_enrs) < 200:
            break  # last page
    logger.info("[approve_day2] day-2 enrollment count: %d / %d total", _day2_count, _total_enrolled)
    if _day2_count == 0 and _total_enrolled <= 400:
        obs.log("minor", "approve_day2 skipped — no day-2 enrollments", "all prospects on day 1")
        chk.mark_done("approve_day2", status="skipped_no_day2")
        return
    # Poll for day-2 message gen: approve_day1 triggers background gen for day 2.
    # Retry on 400 "may still be generating" up to ~10 minutes.
    _retry_interval = 30
    _max_retries = 20  # 20 × 30s = 600s max wait
    status, body = 400, {}
    for _attempt in range(_max_retries):
        await asyncio.sleep(_retry_interval)
        status, body = await client.call(
            "approve_day2", "POST", f"/api/campaigns/{campaign_id}/approve-day/2",
            ok_statuses={200, 400},
            fatal=False,
        )
        if status == 200:
            break
        if status >= 500:
            obs.log("major", "approve_day2 server error", f"HTTP {status}: {body}")
            raise StepFailed("approve_day2", status, body)
        if _attempt < _max_retries - 1:
            logger.info("[approve_day2] day-2 messages not ready, retry %d/%d in %ds",
                        _attempt + 1, _max_retries, _retry_interval)
    else:
        obs.log("minor", "approve_day2 timed out waiting for day-2 messages", str(body))
    chk.mark_done("approve_day2", status=status)


# ---------------------------------------------------------------------------
# Re-trigger helper (called by PM after a bug fix — NOT a step)
# ---------------------------------------------------------------------------

async def retrigger_discovery(client: LoggingClient, campaign_id: str) -> dict:
    """Re-trigger discovery on the existing campaign after a bug fix. Not a checkpointed step."""
    _, body = await client.call(
        "retrigger_discovery", "POST", f"/api/campaigns/{campaign_id}/discover-prospects",
    )
    logger.info("[retrigger_discovery] %s", body)
    return body


# ---------------------------------------------------------------------------
# Summary writer
# ---------------------------------------------------------------------------

def _write_summary(run_dir: Path, chk: Checkpoint, cfg: dict) -> None:
    s     = chk.summary()
    state = s["state"]
    steps = s["steps"]

    lines = [
        f"# QA Run Summary — {s['run_id']}",
        f"\n**Profile:** `{s['profile']}`  |  **Target:** {cfg['company_count']} co / {cfg['prospect_count']} prospects",
    ]

    lines.append("\n## Steps")
    for name in STEP_ORDER:
        info   = steps.get(name, {})
        status = info.get("status", "pending")
        icon   = "✓" if status == "done" else ("✗" if status == "failed" else "—")
        extra  = ""
        if name == "discovery_poll" and status == "done":
            extra = f"  _(companies={info.get('companies_found', 0)}, enrolled={info.get('enrolled', 0)}, msg_gen={info.get('msg_gen_status')})_"
        elif name == "connect_sender" and info.get("reused"):
            extra = " _(reused existing)_"
        err = info.get("error", "")
        err_str = f"\n  - error: `{err[:200]}`" if err else ""
        lines.append(f"- [{icon}] **{name}**{extra}{err_str}")

    dr = state.get("discovery_result") or {}
    if dr:
        lines += [
            "\n## Discovery Results",
            f"- Companies found: {dr.get('discovery_companies_found', 0)}",
            f"- Prospects enrolled: {dr.get('discovery_prospects_enrolled', 0)}",
            f"- From DB (reused): {dr.get('discovery_prospects_from_db', 0)}",
            f"- From Apify (new): {dr.get('discovery_prospects_from_apify', 0)}",
            f"- Message gen status: `{dr.get('message_gen_status', '?')}`",
            f"- Message gen done: {dr.get('message_gen_prospects_done', 0)}",
        ]

    if state.get("campaign_id"):
        lines.append(f"\n**Campaign ID:** `{state['campaign_id']}`")
    if state.get("day1_date"):
        lines.append(f"**Day-1 send date:** {state['day1_date']}")

    (run_dir / "summary.md").write_text("\n".join(lines) + "\n")
    logger.info("Summary → %s", run_dir / "summary.md")


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

async def run_flow(profile: str, run_dir: Path) -> None:
    run_id = run_dir.name
    cfg    = PROFILE_CONFIGS[profile]

    run_dir.mkdir(parents=True, exist_ok=True)
    logger.info("=== QA Harness | profile=%s | run_id=%s ===", profile, run_id)
    logger.info("Run dir: %s", run_dir)
    logger.info("Target: %d companies / %d prospects", cfg["company_count"], cfg["prospect_count"])

    chk    = Checkpoint(run_dir, run_id, profile)
    obs    = ObsLog(run_dir)
    client = LoggingClient(run_dir)

    # Rehydrate auth token when resuming
    if chk.state.get("token"):
        client.set_token(chk.state["token"])

    failed_step = None
    try:
        # Prerequisite: backend health
        status, hbody = await client.call("health", "GET", "/health", ok_statuses={200}, fatal=True)
        logger.info("Backend healthy: %s", hbody)

        await step_register(client, chk, obs)
        await step_login(client, chk, obs)
        await step_onboarding_start(client, chk, obs)
        await step_stage1_scrape(client, chk, obs)
        await step_stage1_poll(client, chk, obs)
        await step_stage1_save(client, chk, obs)
        await step_stage2_voice(client, chk, obs)
        await step_stage3_icp(client, chk, obs)
        await step_stage4_offer(client, chk, obs)
        await step_stage6_chat(client, chk, obs)
        await step_wizard_complete(client, chk, obs)
        await step_connect_sender(client, chk, obs)
        await step_create_campaign(client, chk, obs, cfg)
        await step_discovery_poll(client, chk, obs, cfg)
        await step_approve_day1(client, chk, obs)
        await step_approve_day2(client, chk, obs)

        logger.info("=== ALL STEPS COMPLETE ===")

    except (StepFailed, TimeoutError, httpx.RequestError) as exc:
        failed_step = getattr(exc, "step", "unknown")
        chk.mark_failed(failed_step, str(exc))
        logger.error("FAILED at step=%s: %s", failed_step, exc)
        obs.log(
            "blocker",
            f"Step `{failed_step}` failed",
            f"Error: `{exc}`\n\nCheckpoint: {run_dir / 'checkpoint.json'}\n\n"
            f"Resume with:\n```\npython3 scripts/e2e/run_full_flow.py --profile {profile} --run-dir {run_dir}\n```",
        )
        raise

    finally:
        await client.aclose()
        _write_summary(run_dir, chk, cfg)
        logger.info("api_log → %s", run_dir / "api_log.jsonl")
        logger.info("checkpoint → %s", run_dir / "checkpoint.json")
        if failed_step:
            logger.info("observations → %s", run_dir / "observations.md")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _latest_run_dir(profile: str) -> Path | None:
    if not RUNS_DIR.exists():
        return None
    candidates = sorted(
        [d for d in RUNS_DIR.iterdir() if d.is_dir() and profile in d.name],
        reverse=True,
    )
    return candidates[0] if candidates else None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="QA Flow Harness: Onboarding → Smart Campaign",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--profile", choices=["smoke", "delivery"], default="smoke",
        help="smoke=10co/20pr (fast QA), delivery=200co/400pr (full run)",
    )
    parser.add_argument("--run-dir", help="Specific run directory to resume")
    parser.add_argument("--resume", action="store_true", help="Resume the latest run for this profile")
    args = parser.parse_args()

    if args.run_dir:
        run_dir = Path(args.run_dir)
        if not run_dir.is_absolute():
            run_dir = Path.cwd() / run_dir
    elif args.resume:
        run_dir = _latest_run_dir(args.profile)
        if not run_dir:
            print(f"No previous run found for profile={args.profile}. Start a new one without --resume.")
            sys.exit(1)
        logger.info("Resuming: %s", run_dir)
    else:
        ts      = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        run_dir = RUNS_DIR / f"{ts}-{args.profile}"

    asyncio.run(run_flow(args.profile, run_dir))


if __name__ == "__main__":
    main()
