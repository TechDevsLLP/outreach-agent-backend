"""
Standalone scheduler process.

Run as: python -m scheduler_worker
or: uvicorn doesn't apply here — this is a plain asyncio process.

In production, run as a separate systemd unit / Docker container alongside
the web API (which runs with APP_ROLE=web and does NOT start the scheduler).
"""
import asyncio
import logging
import signal
from contextlib import suppress

from config import get_settings
from database import create_indexes

settings = get_settings()
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s",
)
logger = logging.getLogger("scheduler_worker")


async def main():
    logger.info("Starting OutFlo scheduler worker...")
    await create_indexes()
    logger.info("MongoDB indexes ready")

    from services.scheduler_service import start_scheduler, shutdown_scheduler
    from services.notification_service import shutdown_sse
    start_scheduler()
    logger.info("Scheduler worker running. Press Ctrl+C to stop.")

    stop_event = asyncio.Event()

    def _handle_stop(*_):
        logger.info("Shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, _handle_stop)

    try:
        await stop_event.wait()
    finally:
        logger.info("Stopping scheduler worker...")
        shutdown_scheduler()
        shutdown_sse()
        logger.info("Scheduler worker stopped")


if __name__ == "__main__":
    asyncio.run(main())
