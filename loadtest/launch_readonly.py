#!/usr/bin/env python3
"""Read-only launch-load harness for local and staging environments.

The harness deliberately has no code path for POST, PUT, PATCH, or DELETE. It
models dashboard/API reads and optionally long-lived notification SSE streams;
it never launches campaigns, mutates data, or calls an outreach provider.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

import httpx


READ_ENDPOINTS = (
    "/api/prospects?page=1&page_size=20",
    "/api/campaigns?page=1&page_size=20",
    "/api/notifications/unread-count",
    "/api/email-accounts",
)
LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1"}
STAGING_CONFIRMATION = "READ_ONLY_STAGING_LOAD"


@dataclass
class Metrics:
    latencies_ms: dict[str, list[float]] = field(
        default_factory=lambda: defaultdict(list)
    )
    statuses: Counter = field(default_factory=Counter)
    errors: Counter = field(default_factory=Counter)
    sse_connected: int = 0
    sse_events: int = 0

    def record(self, endpoint: str, elapsed_ms: float, status: int) -> None:
        self.latencies_ms[endpoint].append(elapsed_ms)
        self.statuses[str(status)] += 1


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))]


def validate_target(base_url: str, allow_staging: bool) -> str:
    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("base URL must be an absolute http(s) URL")
    if parsed.hostname in LOCAL_HOSTS:
        return base_url.rstrip("/")
    if not allow_staging:
        raise ValueError("non-local targets require --allow-staging")
    if parsed.scheme != "https":
        raise ValueError("staging load tests require HTTPS")
    if os.environ.get("OUTFLO_LOAD_CONFIRM") != STAGING_CONFIRMATION:
        raise ValueError(
            f"set OUTFLO_LOAD_CONFIRM={STAGING_CONFIRMATION} for staging"
        )
    return base_url.rstrip("/")


def load_tokens(path: Path) -> list[list[str]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    accounts = payload.get("accounts")
    if not isinstance(accounts, list) or not accounts:
        raise ValueError("credentials file requires a non-empty accounts array")
    result: list[list[str]] = []
    for index, account in enumerate(accounts):
        tokens = account.get("seat_tokens") if isinstance(account, dict) else None
        if not isinstance(tokens, list) or not tokens or not all(
            isinstance(token, str) and token.strip() for token in tokens
        ):
            raise ValueError(f"accounts[{index}].seat_tokens is invalid")
        result.append([token.strip() for token in tokens])
    return result


def build_seats(
    account_tokens: list[list[str]], virtual_accounts: int, seats_per_account: int
) -> list[tuple[int, int, str]]:
    seats: list[tuple[int, int, str]] = []
    for account_index in range(virtual_accounts):
        tokens = account_tokens[account_index % len(account_tokens)]
        for seat_index in range(seats_per_account):
            seats.append((account_index, seat_index, tokens[seat_index % len(tokens)]))
    return seats


def plan(args: argparse.Namespace, credential_accounts: int = 0) -> dict:
    total_messages = args.accounts * args.messages_per_account_day
    average_messages_per_second = total_messages / 86_400
    active_window_seconds = args.active_hours * 3_600
    active_window_messages_per_second = total_messages / active_window_seconds
    return {
        "virtual_accounts": args.accounts,
        "seats_per_account": args.seats,
        "concurrent_seats": args.accounts * args.seats,
        "credential_accounts": credential_accounts,
        "messages_per_account_day_modelled_not_sent": args.messages_per_account_day,
        "messages_per_day_modelled_not_sent": total_messages,
        "average_message_rate_per_second": round(average_messages_per_second, 4),
        "active_window_message_rate_per_second": round(
            active_window_messages_per_second, 4
        ),
        "api_reads_per_seat_minute": args.reads_per_seat_minute,
        "steady_api_requests_per_minute": (
            args.accounts * args.seats * args.reads_per_seat_minute
        ),
        "sse_connections": args.accounts * args.seats if args.include_sse else 0,
        "ramp_seconds": args.ramp_seconds,
        "duration_seconds": args.duration,
        "read_only": True,
    }


async def read_loop(
    client: httpx.AsyncClient,
    token: str,
    seat_number: int,
    duration: int,
    start_delay: float,
    interval: float,
    metrics: Metrics,
) -> None:
    await asyncio.sleep(start_delay)
    deadline = time.monotonic() + duration
    headers = {"Authorization": f"Bearer {token}"}
    cursor = seat_number % len(READ_ENDPOINTS)
    while time.monotonic() < deadline:
        endpoint = READ_ENDPOINTS[cursor % len(READ_ENDPOINTS)]
        cursor += 1
        started = time.perf_counter()
        try:
            response = await client.get(endpoint, headers=headers)
            metrics.record(
                endpoint, (time.perf_counter() - started) * 1000, response.status_code
            )
            if response.status_code >= 400:
                metrics.errors[f"http_{response.status_code}"] += 1
        except Exception as exc:  # report transport class, never token/details
            metrics.errors[type(exc).__name__] += 1
        remaining = deadline - time.monotonic()
        if remaining > 0:
            await asyncio.sleep(min(interval, remaining))


async def sse_loop(
    client: httpx.AsyncClient,
    token: str,
    duration: int,
    start_delay: float,
    metrics: Metrics,
) -> None:
    await asyncio.sleep(start_delay)
    deadline = time.monotonic() + duration
    try:
        async with client.stream(
            "GET",
            "/api/notifications/stream",
            params={"token": token},
            timeout=httpx.Timeout(connect=10, read=None, write=10, pool=10),
        ) as response:
            metrics.statuses[f"sse_{response.status_code}"] += 1
            if response.status_code != 200:
                metrics.errors[f"sse_http_{response.status_code}"] += 1
                return
            metrics.sse_connected += 1
            async for line in response.aiter_lines():
                if line.startswith("event:"):
                    metrics.sse_events += 1
                if time.monotonic() >= deadline:
                    return
    except Exception as exc:
        metrics.errors[f"sse_{type(exc).__name__}"] += 1


async def execute(args: argparse.Namespace) -> dict:
    base_url = validate_target(args.base_url, args.allow_staging)
    account_tokens = load_tokens(args.credentials)
    seats = build_seats(account_tokens, args.accounts, args.seats)
    metrics = Metrics()
    interval = 60.0 / args.reads_per_seat_minute
    limits = httpx.Limits(
        max_connections=max(100, len(seats) * (2 if args.include_sse else 1)),
        max_keepalive_connections=max(50, len(seats)),
    )
    timeout = httpx.Timeout(connect=10, read=30, write=10, pool=10)
    async with httpx.AsyncClient(
        base_url=base_url,
        timeout=timeout,
        limits=limits,
        follow_redirects=False,
        headers={"User-Agent": "outflo-readonly-load/1.0"},
    ) as client:
        tasks = [
            asyncio.create_task(
                read_loop(
                    client,
                    token,
                    index,
                    args.duration,
                    (index / max(len(seats), 1)) * args.ramp_seconds,
                    interval,
                    metrics,
                )
            )
            for index, (_, _, token) in enumerate(seats)
        ]
        if args.include_sse:
            tasks.extend(
                asyncio.create_task(
                    sse_loop(
                        client,
                        token,
                        args.duration,
                        (index / max(len(seats), 1)) * args.ramp_seconds,
                        metrics,
                    )
                )
                for index, (_, _, token) in enumerate(seats)
            )
        await asyncio.gather(*tasks)

    endpoint_results = {}
    for endpoint, values in metrics.latencies_ms.items():
        endpoint_results[endpoint] = {
            "requests": len(values),
            "mean_ms": round(statistics.fmean(values), 2),
            "p50_ms": round(percentile(values, 0.50), 2),
            "p95_ms": round(percentile(values, 0.95), 2),
            "p99_ms": round(percentile(values, 0.99), 2),
        }
    total_requests = sum(len(values) for values in metrics.latencies_ms.values())
    total_errors = sum(metrics.errors.values())
    result = {
        "plan": plan(args, len(account_tokens)),
        "results": {
            "requests": total_requests,
            "statuses": dict(metrics.statuses),
            "errors": dict(metrics.errors),
            "error_rate": round(total_errors / max(total_requests, 1), 6),
            "sse_connected": metrics.sse_connected,
            "sse_events": metrics.sse_events,
            "endpoints": endpoint_results,
        },
        "limitations": [
            "No provider send or application mutation is performed.",
            "Message throughput is a capacity model, not a provider-send test.",
            "Repeated credential accounts measure capacity but not 100-tenant isolation.",
        ],
    }
    return result


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--base-url", default="http://127.0.0.1:8008")
    result.add_argument("--credentials", type=Path)
    result.add_argument("--accounts", type=int, default=100)
    result.add_argument("--seats", type=int, default=3)
    result.add_argument("--messages-per-account-day", type=int, default=50)
    result.add_argument("--active-hours", type=float, default=8.0)
    result.add_argument("--reads-per-seat-minute", type=float, default=2.0)
    result.add_argument("--duration", type=int, default=600)
    result.add_argument("--ramp-seconds", type=float, default=60.0)
    result.add_argument("--include-sse", action="store_true")
    result.add_argument("--allow-staging", action="store_true")
    result.add_argument("--plan-only", action="store_true")
    result.add_argument("--report", type=Path)
    return result


def main() -> int:
    args = parser().parse_args()
    positive = {
        "accounts": args.accounts,
        "seats": args.seats,
        "messages-per-account-day": args.messages_per_account_day,
        "active-hours": args.active_hours,
        "reads-per-seat-minute": args.reads_per_seat_minute,
        "duration": args.duration,
        "ramp-seconds": args.ramp_seconds,
    }
    if any(value <= 0 for value in positive.values()):
        print("all workload values must be greater than zero", file=sys.stderr)
        return 2
    if args.plan_only:
        output = plan(args)
    else:
        if args.credentials is None:
            print("--credentials is required unless --plan-only is used", file=sys.stderr)
            return 2
        try:
            output = asyncio.run(execute(args))
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            print(f"load test refused: {exc}", file=sys.stderr)
            return 2
    rendered = json.dumps(output, indent=2, sort_keys=True)
    print(rendered)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
