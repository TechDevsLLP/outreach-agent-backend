"""
Common interface implemented by every email sending channel
(Gmail API, Zoho Mail API, custom SMTP+IMAP).

All three concrete providers (`gmail.py`, `zoho.py`, `smtp_imap.py`) implement
`EmailProvider` so that `services/email_delivery_service.py` and
`services/email_reply_poller.py` can treat them uniformly.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class SendResult:
    """Result of a successful send/reply."""
    message_id: str            # provider-native message id (stored as provider_message_id)
    provider: str               # "google" | "zoho" | "smtp"
    thread_ref: Optional[str] = None       # provider thread handle (see module docstring below)
    rfc_message_id: Optional[str] = None   # RFC 2822 Message-ID header we generated/used


@dataclass
class DraftResult:
    """Result of a successful native mailbox draft creation."""
    draft_id: str
    provider: str
    thread_ref: Optional[str] = None


@dataclass
class ReplyMeta:
    """A single inbound message discovered while polling for replies."""
    provider_message_id: str
    from_email: str
    subject: str = ""
    snippet: str = ""
    date: str = ""
    thread_ref: Optional[str] = None
    # RFC 2822 Message-ID of the inbound message. Stored on the conversation so
    # a later reply can point In-Reply-To/References at the prospect's own
    # message rather than at our previous outbound one.
    rfc_message_id: Optional[str] = None


class EmailProvider(ABC):
    """
    One instance wraps one `email_accounts` document (already decrypted).

    `thread_ref` semantics per provider (opaque to callers — always round-tripped
    through `campaign_messages.provider_thread_id` / `conversations` messages):
      - Gmail:      Gmail `threadId`
      - Zoho:       Zoho `threadId`
      - SMTP/IMAP:  the RFC `Message-ID` of the first message in the thread
                     (used to build `References`/`In-Reply-To` and to IMAP-search)
    """

    def __init__(self, email_account: dict):
        self.account = email_account

    @abstractmethod
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
        """
        Send a new email (or, if `thread_ref`/`in_reply_to` given, a threaded reply).

        `text_body` is the text/plain alternative. Providers that build real MIME
        must emit it alongside the HTML part — without it, clients that prefer
        plain text get nothing and HTML clients are the only ones that render.
        """
        raise NotImplementedError

    async def reply(
        self,
        to_email: str,
        subject: str,
        html_body: str,
        *,
        text_body: Optional[str] = None,
        thread_ref: Optional[str] = None,
        provider_message_id: Optional[str] = None,
        in_reply_to: Optional[str] = None,
    ) -> Optional[SendResult]:
        """
        Send a threaded reply. Default implementation just delegates to `send()`
        with threading hints — providers that need a distinct reply endpoint
        (e.g. Zoho's POST .../messages/{messageId} with action=reply) override this.
        """
        return await self.send(
            to_email,
            subject,
            html_body,
            text_body=text_body,
            in_reply_to=in_reply_to or provider_message_id,
            thread_ref=thread_ref,
        )

    @abstractmethod
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
        """Create a real draft in the provider's mailbox (not just an in-app draft)."""
        raise NotImplementedError

    @abstractmethod
    async def fetch_new_replies(self, thread_ref: str, sender_email: str) -> list[ReplyMeta]:
        """Return any inbound messages in the given thread not sent by `sender_email`."""
        raise NotImplementedError

    @abstractmethod
    async def get_message_body(self, provider_message_id: str) -> str:
        """Fetch the plain-text/preview body of a single message, for storing in conversations."""
        raise NotImplementedError

    @abstractmethod
    async def verify(self) -> bool:
        """Lightweight connectivity/credential check, used by the account 'test' endpoint."""
        raise NotImplementedError
