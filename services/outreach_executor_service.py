"""
Outreach Executor Service - Background execution engine.
After admin approves the daily schedule, this service sends each message
at its scheduled time via SendGrid (email) or Unipile (LinkedIn).
"""

import asyncio
import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Optional

from bson import ObjectId

from database import outreach_schedules_collection, prospects_collection

logger = logging.getLogger(__name__)


async def resume_interrupted_schedules():
    """
    On startup, find any schedules that were interrupted (in_progress) or
    approved but never started, and resume/start them.
    """
    # Find schedules that need resuming
    interrupted = await outreach_schedules_collection.find(
        {"status": {"$in": ["in_progress", "approved"]}}
    ).to_list(length=None)

    if not interrupted:
        logger.info("No interrupted schedules to resume")
        return

    for schedule in interrupted:
        sid = str(schedule["_id"])
        status = schedule["status"]
        date = schedule.get("schedule_date", "unknown")

        if status == "approved":
            logger.info(f"Resuming approved schedule {sid} for {date} — starting execution")
            asyncio.create_task(execute_schedule(sid))
        elif status == "in_progress":
            logger.info(f"Resuming interrupted schedule {sid} for {date} — sending remaining items")
            asyncio.create_task(_resume_in_progress_schedule(sid))


async def _resume_in_progress_schedule(schedule_id: str):
    """Resume a schedule that was interrupted mid-execution. Only sends pending items."""
    try:
        oid = ObjectId(schedule_id)
    except Exception:
        logger.error(f"Invalid schedule ID for resume: {schedule_id}")
        return

    schedule = await outreach_schedules_collection.find_one({"_id": oid})
    if not schedule:
        logger.error(f"Schedule not found for resume: {schedule_id}")
        return

    if schedule["status"] != "in_progress":
        logger.warning(f"Schedule {schedule_id} status changed to '{schedule['status']}', skipping resume")
        return

    logger.info(f"Resuming in-progress schedule {schedule_id} for {schedule['schedule_date']}")

    # Collect only pending items
    all_items = []
    for batch_key in ["email_batch", "linkedin_connection_batch", "linkedin_inmail_batch"]:
        batch = schedule.get(batch_key, {})
        for item in batch.get("items", []):
            if item.get("send_status", "pending") == "pending":
                item["_batch_key"] = batch_key
                all_items.append(item)

    if not all_items:
        logger.info(f"No pending items to resume for schedule {schedule_id}, marking completed")
        await _finalize_schedule(schedule_id)
        return

    logger.info(f"Resuming {len(all_items)} pending items for schedule {schedule_id}")

    all_items.sort(key=lambda x: x.get("scheduled_send_time") or datetime.min)
    await _execute_items(schedule_id, all_items)
    await _finalize_schedule(schedule_id) 


async def retry_failed_items(schedule_id: str):
    """Retry all failed items in a completed schedule."""
    try:
        oid = ObjectId(schedule_id)
    except Exception:
        logger.error(f"Invalid schedule ID for retry: {schedule_id}")
        return

    schedule = await outreach_schedules_collection.find_one({"_id": oid})
    if not schedule:
        logger.error(f"Schedule not found for retry: {schedule_id}")
        return

    if schedule["status"] not in ("completed", "retrying"):
        logger.error(f"Schedule {schedule_id} status is '{schedule['status']}', expected 'completed'")
        return

    # Collect failed items from all batches
    failed_items = []
    for batch_key in ["email_batch", "linkedin_connection_batch", "linkedin_inmail_batch"]:
        batch = schedule.get(batch_key, {})
        for item in batch.get("items", []):
            if item.get("send_status") == "failed":
                item["_batch_key"] = batch_key
                failed_items.append(item)

    if not failed_items:
        logger.info(f"No failed items to retry for schedule {schedule_id}")
        return

    logger.info(f"Retrying {len(failed_items)} failed items for schedule {schedule_id}")

    # Mark schedule as retrying
    await outreach_schedules_collection.update_one(
        {"_id": oid},
        {"$set": {"status": "retrying"}},
    )

    # Reset each failed item to pending, stagger LinkedIn items 60-120s apart
    now = datetime.utcnow()
    offset_seconds = 0
    for item in failed_items:
        new_send_time = now + timedelta(seconds=offset_seconds)
        item["scheduled_send_time"] = new_send_time
        channel = item.get("channel", "")
        if channel in ("linkedin_connection", "linkedin_inmail"):
            offset_seconds += random.randint(60, 120)
        else:
            offset_seconds += 30

        # Reset to pending and clear error_message
        batch_key = item["_batch_key"]
        item_id = item["item_id"]
        await outreach_schedules_collection.update_one(
            {"_id": oid},
            {"$set": {
                f"{batch_key}.items.$[elem].send_status": "pending",
                f"{batch_key}.items.$[elem].error_message": None,
                f"{batch_key}.items.$[elem].scheduled_send_time": new_send_time,
            }},
            array_filters=[{"elem.item_id": item_id}],
        )

    try:
        await _execute_items(schedule_id, failed_items)
    except Exception as e:
        logger.error(f"Retry execution error for schedule {schedule_id}: {e}", exc_info=True)
    finally:
        await _finalize_schedule(schedule_id)


async def execute_schedule(schedule_id: str):
    """
    Execute an approved schedule as a background task.
    Sends each message at its scheduled time, records in conversations,
    updates prospect status, and initializes follow-up sequences.
    """
    try:
        oid = ObjectId(schedule_id)
    except Exception:
        logger.error(f"Invalid schedule ID: {schedule_id}")
        return

    schedule = await outreach_schedules_collection.find_one({"_id": oid})
    if not schedule:
        logger.error(f"Schedule not found: {schedule_id}")
        return

    if schedule["status"] != "approved":
        logger.error(f"Schedule {schedule_id} has status '{schedule['status']}', expected 'approved'")
        return

    # Mark as in_progress
    await outreach_schedules_collection.update_one(
        {"_id": oid},
        {"$set": {
            "status": "in_progress",
            "execution_started_at": datetime.utcnow(),
        }},
    )

    logger.info(f"Starting execution of schedule {schedule_id} for {schedule['schedule_date']}")

    # Collect all items from all batches, sort by scheduled_send_time
    all_items = []
    for batch_key in ["email_batch", "linkedin_connection_batch", "linkedin_inmail_batch"]:
        batch = schedule.get(batch_key, {})
        for item in batch.get("items", []):
            item["_batch_key"] = batch_key
            all_items.append(item)

    all_items.sort(key=lambda x: x.get("scheduled_send_time") or datetime.min)

    await _execute_items(schedule_id, all_items)
    await _finalize_schedule(schedule_id)


async def _execute_items(schedule_id: str, all_items: list):
    """Execute a list of schedule items, waiting for each item's scheduled time."""
    for item in all_items:
        item_id = item["item_id"]
        batch_key = item["_batch_key"]
        channel = item["channel"]

        # Wait until scheduled time (skip if already past → send immediately)
        scheduled_time = item.get("scheduled_send_time")
        if scheduled_time:
            if isinstance(scheduled_time, str):
                scheduled_time = datetime.fromisoformat(scheduled_time)
            now = datetime.utcnow()
            wait_seconds = (scheduled_time - now).total_seconds()
            if wait_seconds > 0:
                logger.info(
                    f"Waiting {wait_seconds:.0f}s until {scheduled_time.isoformat()} "
                    f"for {channel} to {item.get('prospect_name', 'unknown')}"
                )
                await asyncio.sleep(wait_seconds)

        # Send via appropriate channel
        try:
            if channel == "email":
                result = await _send_email_item(item)
            elif channel == "linkedin_connection":
                result = await _send_linkedin_connection_item(item)
            elif channel == "linkedin_inmail":
                result = await _send_inmail_item(item)
            else:
                logger.warning(f"Unknown channel '{channel}' for item {item_id}")
                result = {"status": "skipped", "error": f"Unknown channel: {channel}"}

            status = result.get("status", "failed")

            if status == "sent":
                await _post_send_actions(item, result)
            elif status != "skipped":
                pass  # failed

            # Update item in schedule document
            await _update_schedule_item(
                schedule_id=schedule_id,
                batch_key=batch_key,
                item_id=item_id,
                send_status=status,
                sent_at=datetime.utcnow() if status == "sent" else None,
                error_message=result.get("error"),
                conversation_id=result.get("conversation_id"),
            )

        except Exception as e:
            logger.error(f"Failed to send {channel} to {item.get('prospect_name')}: {e}", exc_info=True)
            await _update_schedule_item(
                schedule_id=schedule_id,
                batch_key=batch_key,
                item_id=item_id,
                send_status="failed",
                error_message=str(e),
            )


async def _finalize_schedule(schedule_id: str):
    """Mark schedule as completed and update per-batch counts."""
    try:
        oid = ObjectId(schedule_id)
    except Exception:
        return

    schedule = await outreach_schedules_collection.find_one({"_id": oid})
    if not schedule:
        return

    total_sent = 0
    total_failed = 0
    total_skipped = 0

    for batch_key in ["email_batch", "linkedin_connection_batch", "linkedin_inmail_batch"]:
        batch = schedule.get(batch_key, {})
        items = batch.get("items", [])

        # Mark any leftover pending items as failed (execution is over)
        for it in items:
            if it.get("send_status") == "pending":
                await _update_schedule_item(
                    schedule_id=schedule_id,
                    batch_key=batch_key,
                    item_id=it["item_id"],
                    send_status="failed",
                    error_message="Not executed (schedule finalized)",
                )

        sent = sum(1 for it in items if it.get("send_status") == "sent")
        pending_as_failed = sum(1 for it in items if it.get("send_status") == "pending")
        failed = sum(1 for it in items if it.get("send_status") == "failed") + pending_as_failed
        skipped = sum(1 for it in items if it.get("send_status") == "skipped")
        total_sent += sent
        total_failed += failed
        total_skipped += skipped
        await outreach_schedules_collection.update_one(
            {"_id": oid},
            {"$set": {
                f"{batch_key}.sent_count": sent,
                f"{batch_key}.failed_count": failed,
            }},
        )

    await outreach_schedules_collection.update_one(
        {"_id": oid},
        {"$set": {
            "status": "completed",
            "execution_completed_at": datetime.utcnow(),
            "total_sent": total_sent,
            "total_failed": total_failed,
            "total_skipped": total_skipped,
        }},
    )

    logger.info(
        f"Schedule {schedule_id} execution completed: "
        f"{total_sent} sent, {total_failed} failed, {total_skipped} skipped"
    )


async def _send_email_item(item: dict) -> dict:
    """Send a single email via SendGrid using the edited message from the schedule."""
    to_email = item.get("prospect_email")
    if not to_email:
        return {"status": "skipped", "error": "No email address"}

    subject = item.get("message_subject", "Let's connect")
    body = item.get("message_body", "")
    if not body:
        return {"status": "skipped", "error": "No message body"}

    prospect_id = item["prospect_id"]
    to_name = item.get("prospect_name", "")

    from services.email_sender_service import EmailSender
    sender = EmailSender()
    result = await sender.send_cold_email(
        to_email=to_email,
        to_name=to_name,
        subject=subject,
        body=body,
        custom_args={
            "prospect_id": prospect_id,
            "variant_id": item.get("subject_variant_id", "A"),
        },
    )

    if result.get("status") == "sent":
        # Record in conversations
        try:
            from services.conversation_service import record_outbound_email
            from services.email_sender_service import plain_text_to_html
            conv = await record_outbound_email(
                prospect_id=prospect_id,
                prospect_name=to_name,
                prospect_email=to_email,
                prospect_company=item.get("prospect_company"),
                subject=subject,
                body_text=body,
                body_html=plain_text_to_html(body),
                sendgrid_message_id=result.get("message_id"),
                email_message_id=result.get("email_message_id"),
                variant_id=item.get("subject_variant_id", "A"),
                account_id=item.get("account_id"),
            )
            result["conversation_id"] = str(conv["_id"]) if conv else None
        except Exception as e:
            logger.warning(f"Failed to record outbound email in conversations: {e}")

    return result


async def _send_linkedin_connection_item(item: dict) -> dict:
    """Send a LinkedIn connection request via Unipile."""
    profile_url = item.get("prospect_linkedin")
    if not profile_url:
        return {"status": "skipped", "error": "No LinkedIn URL"}

    message = item.get("message_body", "")
    prospect_id = item["prospect_id"]

    from services.unipile_service import UnipileClient
    client = UnipileClient()
    result = await client.send_connection_request(
        profile_url=profile_url,
        message=message if message else None,
    )

    # Record in conversations
    try:
        from services.conversation_service import record_outbound_linkedin_message
        conv = await record_outbound_linkedin_message(
            prospect_id=prospect_id,
            prospect_name=item.get("prospect_name"),
            prospect_company=item.get("prospect_company"),
            message_text=message,
            account_id=item.get("account_id"),
        )
        result["conversation_id"] = str(conv["_id"]) if conv else None
    except Exception as e:
        logger.warning(f"Failed to record LinkedIn connection in conversations: {e}")

    result["status"] = "sent"

    # Record the timestamp for connection tracking
    await prospects_collection.update_one(
        {"_id": ObjectId(item["prospect_id"])},
        {"$set": {"connection_request_sent_at": datetime.now(timezone.utc)}},
    )

    return result


async def _send_inmail_item(item: dict) -> dict:
    """Send a LinkedIn InMail via Unipile."""
    profile_url = item.get("prospect_linkedin")
    if not profile_url:
        return {"status": "skipped", "error": "No LinkedIn URL"}

    message_body = item.get("message_body", "")
    subject = item.get("message_subject")
    prospect_id = item["prospect_id"]

    if not message_body:
        return {"status": "skipped", "error": "No message body"}

    from services.unipile_service import UnipileClient
    client = UnipileClient()
    result = await client.send_inmail(
        profile_url=profile_url,
        text=message_body,
        subject=subject,
    )

    # Record in conversations
    try:
        from services.conversation_service import record_outbound_linkedin_message
        conv = await record_outbound_linkedin_message(
            prospect_id=prospect_id,
            prospect_name=item.get("prospect_name"),
            prospect_company=item.get("prospect_company"),
            message_text=message_body,
            account_id=item.get("account_id"),
        )
        result["conversation_id"] = str(conv["_id"]) if conv else None
    except Exception as e:
        logger.warning(f"Failed to record InMail in conversations: {e}")

    result["status"] = "sent"
    return result


async def _post_send_actions(item: dict, send_result: dict):
    """After successful send: update prospect status, record outreach history, init follow-up."""
    prospect_id = item["prospect_id"]

    try:
        prospect_oid = ObjectId(prospect_id)
    except Exception:
        logger.error(f"Invalid prospect ID: {prospect_id}")
        return

    now = datetime.utcnow()
    channel = item["channel"]

    # Build outreach history event
    history_event = {
        "event_type": "sent",
        "channel": channel,
        "timestamp": now,
        "subject": item.get("message_subject"),
        "message_preview": (item.get("message_body", "")[:100] if item.get("message_body") else ""),
        "conversation_id": send_result.get("conversation_id"),
    }

    # Update prospect: status → contacted, push to outreach_history
    await prospects_collection.update_one(
        {"_id": prospect_oid},
        {
            "$set": {
                "status": "contacted",
                "last_updated_at": now,
            },
            "$push": {"outreach_history": history_event},
        },
    )

    # Follow-up sequencing handled by flow_engine via campaign_engine
    pass


async def _update_schedule_item(
    schedule_id: str,
    batch_key: str,
    item_id: str,
    send_status: str,
    sent_at: Optional[datetime] = None,
    error_message: Optional[str] = None,
    conversation_id: Optional[str] = None,
):
    """Update a single item's status within the schedule document using array filters."""
    try:
        oid = ObjectId(schedule_id)
    except Exception:
        return

    update_fields = {f"{batch_key}.items.$[elem].send_status": send_status}
    if sent_at:
        update_fields[f"{batch_key}.items.$[elem].sent_at"] = sent_at
    if error_message:
        update_fields[f"{batch_key}.items.$[elem].error_message"] = error_message
    if conversation_id:
        update_fields[f"{batch_key}.items.$[elem].conversation_id"] = conversation_id

    await outreach_schedules_collection.update_one(
        {"_id": oid},
        {"$set": update_fields},
        array_filters=[{"elem.item_id": item_id}],
    )
