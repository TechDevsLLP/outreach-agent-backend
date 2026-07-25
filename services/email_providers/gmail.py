"""
Gmail API provider — thin adapter over services/gmail_service.py implementing
the common EmailProvider interface.
"""

import logging
from typing import Optional

from services import gmail_service
from services.email_providers.base import DraftResult, EmailProvider, ReplyMeta, SendResult

logger = logging.getLogger(__name__)


class GmailProvider(EmailProvider):
    async def send(
        self,
        to_email: str,
        subject: str,
        html_body: str,
        *,
        text_body: Optional[str] = None,
        in_reply_to: Optional[str] = None,
        thread_ref: Optional[str] = None,
    ) -> Optional[SendResult]:
        result = await gmail_service.send_gmail_email(
            self.account,
            to_email,
            subject,
            html_body,
            text_body=text_body,
            in_reply_to=in_reply_to,
            thread_id=thread_ref,
        )
        if not result:
            return None
        return SendResult(
            message_id=result["message_id"],
            provider="google",
            thread_ref=result.get("thread_id"),
            rfc_message_id=result.get("rfc_message_id"),
        )

    async def create_draft(
        self,
        to_email: str,
        subject: str,
        html_body: str,
        *,
        text_body: Optional[str] = None,
        in_reply_to: Optional[str] = None,
        thread_ref: Optional[str] = None,
    ) -> Optional[DraftResult]:
        result = await gmail_service.create_gmail_draft(
            self.account,
            to_email,
            subject,
            html_body,
            text_body=text_body,
            in_reply_to=in_reply_to,
            thread_id=thread_ref,
        )
        if not result:
            return None
        return DraftResult(
            draft_id=result["draft_id"],
            provider="google",
            thread_ref=result.get("thread_id"),
        )

    async def fetch_new_replies(self, thread_ref: str, sender_email: str) -> list[ReplyMeta]:
        access_token = await gmail_service.refresh_token_if_needed(self.account)
        if not access_token:
            return []
        messages = await gmail_service.get_thread_messages(access_token, thread_ref, sender_email)
        return [
            ReplyMeta(
                provider_message_id=m.get("id", ""),
                from_email=m.get("from_email", ""),
                subject=m.get("subject", ""),
                snippet=m.get("snippet", ""),
                date=m.get("date", ""),
                thread_ref=thread_ref,
            )
            for m in messages
            if m.get("is_reply")
        ]

    async def get_message_body(self, provider_message_id: str) -> str:
        access_token = await gmail_service.refresh_token_if_needed(self.account)
        if not access_token:
            return ""
        return await gmail_service.get_message_body(access_token, provider_message_id)

    async def verify(self) -> bool:
        access_token = await gmail_service.refresh_token_if_needed(self.account)
        return access_token is not None
