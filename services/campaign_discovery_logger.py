import asyncio
import json
import logging
import textwrap
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

_log = logging.getLogger(__name__)

_TXT_PHASE_WIDTH = 22
_TXT_LEVEL_WIDTH = 7


def _fmt_txt_value(value: Any, indent: int = 4) -> str:
    """Render a value for the human-readable txt log."""
    prefix = " " * indent
    if isinstance(value, dict):
        lines = ["{"]
        for k, v in value.items():
            lines.append(f"{prefix}  {k}: {_fmt_txt_value(v, indent + 2)}")
        lines.append(f"{prefix}}}")
        return "\n".join(lines)
    if isinstance(value, list):
        if not value:
            return "[]"
        if all(isinstance(v, (str, int, float, bool)) for v in value):
            return str(value)
        lines = ["["]
        for item in value:
            lines.append(f"{prefix}  {_fmt_txt_value(item, indent + 2)}")
        lines.append(f"{prefix}]")
        return "\n".join(lines)
    return str(value)


def _fmt_txt_entry(entry: dict) -> str:
    """Format a structured log entry as a human-readable text block."""
    ts = entry.get("ts", "")
    level = (entry.get("level") or "info").upper().ljust(_TXT_LEVEL_WIDTH)
    phase = (entry.get("phase") or "").ljust(_TXT_PHASE_WIDTH)
    event = entry.get("event", "")
    data = entry.get("data") or {}

    header = f"[{ts}] [{level}] [{phase}] {event}"
    lines = [header]
    for k, v in data.items():
        rendered = _fmt_txt_value(v)
        # Indent multiline values
        if "\n" in rendered:
            lines.append(f"    {k}:")
            for sub in rendered.split("\n"):
                lines.append(f"        {sub}")
        else:
            lines.append(f"    {k}: {rendered}")
    return "\n".join(lines)


class CampaignDiscoveryLogger:
    def __init__(self, campaign_id: str, account_id: str, log_dir: str = "logs/campaigns"):
        self.campaign_id = campaign_id
        self.account_id = account_id
        self._log_dir = Path(log_dir) / account_id / campaign_id
        self._log_path = self._log_dir / "discovery.jsonl"
        self._txt_path = self._log_dir / "campaign_log.txt"
        self._summary_path = self._log_dir / "summary.json"
        self._lock = asyncio.Lock()
        self._file = None
        self._txt_file = None
        self._start_time = datetime.now(timezone.utc)

    def _recover_start_time(self) -> None:
        """If the jsonl already exists, read the first line to get the original start time."""
        try:
            if self._log_path.exists():
                first_line = self._log_path.open("r", encoding="utf-8").readline().strip()
                if first_line:
                    first_entry = json.loads(first_line)
                    ts_str = first_entry.get("ts")
                    if ts_str:
                        self._start_time = datetime.fromisoformat(ts_str)
        except Exception:
            pass  # keep the default start time

    async def __aenter__(self) -> "CampaignDiscoveryLogger":
        self._log_dir.mkdir(parents=True, exist_ok=True)
        is_resume = self._log_path.exists()
        if is_resume:
            self._recover_start_time()
        self._file = open(self._log_path, "a", buffering=1, encoding="utf-8")
        self._txt_file = open(self._txt_path, "a", buffering=1, encoding="utf-8")
        if is_resume:
            await self.log(phase="init", event="discovery_resumed",
                           campaign_id=self.campaign_id, account_id=self.account_id)
        else:
            await self.log(phase="init", event="discovery_started",
                           campaign_id=self.campaign_id, account_id=self.account_id)
        return self

    async def __aexit__(self, *_):
        if self._file:
            self._file.close()
            self._file = None
        if self._txt_file:
            self._txt_file.close()
            self._txt_file = None

    async def log(self, *, phase: str, event: str, level: str = "info", **data: Any):
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "campaign_id": self.campaign_id,
            "account_id": self.account_id,
            "phase": phase,
            "level": level,
            "event": event,
            "data": data,
        }
        try:
            jsonl_line = json.dumps(entry, default=str)
        except Exception:
            jsonl_line = json.dumps({
                "ts": entry["ts"], "phase": phase, "event": event,
                "level": "error", "data": {"serialize_error": True},
            })
        try:
            txt_block = _fmt_txt_entry(entry)
        except Exception:
            txt_block = f"[{entry['ts']}] [{level.upper()}] [{phase}] {event} <format error>"

        async with self._lock:
            if self._file:
                self._file.write(jsonl_line + "\n")
            if self._txt_file:
                self._txt_file.write(txt_block + "\n\n")

    async def warning(self, phase: str, event: str, **data: Any):
        await self.log(phase=phase, event=event, level="warning", **data)

    async def error(self, phase: str, event: str, exc: Optional[Exception] = None, **data: Any):
        if exc:
            data["traceback"] = traceback.format_exc()
            data["exception_type"] = type(exc).__name__
            data["exception_msg"] = str(exc)
        await self.log(phase=phase, event=event, level="error", **data)

    async def finalize(self, summary: dict):
        elapsed = (datetime.now(timezone.utc) - self._start_time).total_seconds()
        summary.setdefault("campaign_id", self.campaign_id)
        summary.setdefault("account_id", self.account_id)
        summary["started_at"] = self._start_time.isoformat()
        summary["finished_at"] = datetime.now(timezone.utc).isoformat()
        summary["elapsed_seconds"] = elapsed
        async with self._lock:
            try:
                self._summary_path.write_text(
                    json.dumps(summary, indent=2, default=str), encoding="utf-8"
                )
            except Exception as e:
                _log.warning(f"Failed to write discovery summary: {e}")
        # Also emit a finalize entry to the running log so the txt file has a clear end marker
        await self.log(phase="finalize", event="summary_written", **summary)
