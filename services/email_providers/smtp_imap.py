"""
Custom SMTP (send) + IMAP (reply-check, drafts) provider.

Send/reply use aiosmtplib (already a project dependency, async-native).
IMAP has no async library in the project's dependency set, so blocking
`imaplib` calls are wrapped in `asyncio.to_thread`.

Threading model: since a bare SMTP/IMAP account has no provider-native
thread id, `thread_ref` is the RFC `Message-ID` of the first message we sent
in the conversation. Replies are located by IMAP-searching INBOX for that id
in the `References` / `In-Reply-To` headers.
"""

import asyncio
import email
import email.utils
import imaplib
import logging
import uuid
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

from services.email_providers.base import DraftResult, EmailProvider, ReplyMeta, SendResult
from utils.email_html import html_to_plain_text

logger = logging.getLogger(__name__)

_DRAFT_FOLDER_CANDIDATES = ["Drafts", "INBOX.Drafts", "[Gmail]/Drafts", "Draft"]


def _build_mime(
    from_email: str,
    to_email: str,
    subject: str,
    html_body: str,
    *,
    message_id: str,
    text_body: Optional[str] = None,
    in_reply_to: Optional[str] = None,
    references: Optional[str] = None,
) -> MIMEMultipart:
    msg = MIMEMultipart("alternative")
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg["Message-ID"] = message_id
    msg["Date"] = email.utils.formatdate(localtime=True)
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
        msg["References"] = references or in_reply_to
    # RFC 2046: last part wins, so text/plain must precede text/html.
    msg.attach(MIMEText(text_body if text_body is not None else html_to_plain_text(html_body), "plain"))
    msg.attach(MIMEText(html_body, "html"))
    return msg


async def send_smtp_email(
    email_account: dict,
    to_email: str,
    subject: str,
    html_body: str,
    *,
    text_body: Optional[str] = None,
    in_reply_to: Optional[str] = None,
    references: Optional[str] = None,
) -> Optional[dict]:
    """Send via SMTP. Returns {message_id, thread_ref} using our own generated RFC id."""
    import aiosmtplib

    from_email = email_account.get("email") or email_account.get("smtp_username", "")
    rfc_message_id = f"<{uuid.uuid4()}@outflo.app>"
    msg = _build_mime(
        from_email, to_email, subject, html_body,
        message_id=rfc_message_id, text_body=text_body,
        in_reply_to=in_reply_to, references=references,
    )

    try:
        await aiosmtplib.send(
            msg,
            hostname=email_account["smtp_host"],
            port=email_account.get("smtp_port", 587),
            username=email_account["smtp_username"],
            password=email_account["smtp_password"],
            use_tls=(email_account.get("smtp_encryption") == "ssl"),
            start_tls=(email_account.get("smtp_encryption") == "tls"),
        )
        return {
            "message_id": rfc_message_id,
            "thread_ref": in_reply_to or rfc_message_id,
        }
    except Exception as e:
        logger.error(f"SMTP send failed for {from_email} -> {to_email}: {e}")
        return None


def _imap_connect(email_account: dict) -> imaplib.IMAP4:
    host = email_account["imap_host"]
    port = email_account.get("imap_port", 993)
    encryption = email_account.get("imap_encryption", "ssl")

    if encryption == "ssl":
        conn = imaplib.IMAP4_SSL(host, port)
    else:
        conn = imaplib.IMAP4(host, port)
        if encryption == "tls":
            conn.starttls()

    conn.login(
        email_account.get("imap_username") or email_account.get("smtp_username", ""),
        email_account["imap_password"],
    )
    return conn


def _sync_verify_imap(email_account: dict) -> bool:
    try:
        conn = _imap_connect(email_account)
        conn.logout()
        return True
    except Exception as e:
        logger.error(f"IMAP verify failed for {email_account.get('email')}: {e}")
        return False


def _find_draft_folder(conn: imaplib.IMAP4) -> str:
    try:
        typ, folders = conn.list()
        if typ == "OK" and folders:
            for raw in folders:
                name = raw.decode(errors="ignore") if isinstance(raw, bytes) else str(raw)
                if "draft" in name.lower():
                    # Folder name is the last quoted/unquoted token
                    parts = name.rsplit(" ", 1)
                    return parts[-1].strip('"')
    except Exception:
        pass
    return "Drafts"


def _sync_create_draft(email_account: dict, msg: MIMEMultipart) -> bool:
    conn = _imap_connect(email_account)
    try:
        folder = _find_draft_folder(conn)
        conn.append(folder, "\\Draft", imaplib.Time2Internaldate(__import__("time").time()), msg.as_bytes())
        return True
    finally:
        try:
            conn.logout()
        except Exception:
            pass


def _sync_search_replies(email_account: dict, thread_ref: str, sender_email: str) -> list[dict]:
    """Blocking IMAP search for replies to `thread_ref` in INBOX, excluding `sender_email`."""
    conn = _imap_connect(email_account)
    results: list[dict] = []
    try:
        conn.select("INBOX", readonly=True)
        uids: set[bytes] = set()
        for header in ("REFERENCES", "IN-REPLY-TO"):
            try:
                typ, data = conn.search(None, "HEADER", header, thread_ref)
                if typ == "OK" and data and data[0]:
                    uids.update(data[0].split())
            except Exception:
                continue

        sender_lower = sender_email.lower()
        for uid in uids:
            try:
                typ, msg_data = conn.fetch(uid, "(BODY.PEEK[HEADER] BODY.PEEK[TEXT])")
                if typ != "OK" or not msg_data:
                    continue
                header_bytes = b""
                text_bytes = b""
                for part in msg_data:
                    if isinstance(part, tuple):
                        section = part[0].decode(errors="ignore") if isinstance(part[0], bytes) else str(part[0])
                        if "TEXT" in section:
                            text_bytes = part[1]
                        else:
                            header_bytes = part[1]
                parsed = email.message_from_bytes(header_bytes)
                from_header = parsed.get("From", "")
                from_email = email.utils.parseaddr(from_header)[1].lower()
                if from_email == sender_lower or not from_email:
                    continue

                snippet = ""
                try:
                    snippet = text_bytes.decode(errors="ignore")[:300]
                except Exception:
                    pass

                results.append({
                    "message_id": parsed.get("Message-ID", "").strip(),
                    "from_email": from_email,
                    "subject": parsed.get("Subject", ""),
                    "date": parsed.get("Date", ""),
                    "snippet": snippet,
                })
            except Exception as e:
                logger.warning(f"IMAP fetch failed for uid {uid}: {e}")
                continue
    finally:
        try:
            conn.logout()
        except Exception:
            pass
    return results


def _sync_get_body(email_account: dict, message_id: str) -> str:
    conn = _imap_connect(email_account)
    try:
        conn.select("INBOX", readonly=True)
        typ, data = conn.search(None, "HEADER", "Message-ID", message_id)
        if typ != "OK" or not data or not data[0]:
            return ""
        uid = data[0].split()[0]
        typ, msg_data = conn.fetch(uid, "(BODY.PEEK[TEXT])")
        if typ != "OK" or not msg_data:
            return ""
        for part in msg_data:
            if isinstance(part, tuple):
                return part[1].decode(errors="ignore")
        return ""
    except Exception as e:
        logger.warning(f"IMAP body fetch failed for {message_id}: {e}")
        return ""
    finally:
        try:
            conn.logout()
        except Exception:
            pass


class SmtpImapProvider(EmailProvider):
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
        references = None
        if in_reply_to and thread_ref and thread_ref != in_reply_to:
            references = f"{thread_ref} {in_reply_to}"
        result = await send_smtp_email(
            self.account, to_email, subject, html_body,
            text_body=text_body, in_reply_to=in_reply_to, references=references,
        )
        if not result:
            return None
        return SendResult(
            message_id=result["message_id"],
            provider="smtp",
            thread_ref=thread_ref or result["thread_ref"],
            rfc_message_id=result["message_id"],
        )

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
        return await self.send(
            to_email, subject, html_body,
            text_body=text_body,
            in_reply_to=in_reply_to or provider_message_id,
            thread_ref=thread_ref,
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
        from_email = self.account.get("email") or self.account.get("smtp_username", "")
        rfc_message_id = f"<{uuid.uuid4()}@outflo.app>"
        msg = _build_mime(
            from_email, to_email, subject, html_body,
            message_id=rfc_message_id, text_body=text_body, in_reply_to=in_reply_to,
            references=thread_ref if thread_ref and thread_ref != in_reply_to else in_reply_to,
        )
        try:
            ok = await asyncio.to_thread(_sync_create_draft, self.account, msg)
        except Exception as e:
            logger.error(f"IMAP draft creation failed for {from_email}: {e}")
            return None
        if not ok:
            return None
        return DraftResult(draft_id=rfc_message_id, provider="smtp", thread_ref=thread_ref)

    async def fetch_new_replies(self, thread_ref: str, sender_email: str) -> list[ReplyMeta]:
        if not thread_ref:
            return []
        try:
            raw = await asyncio.to_thread(_sync_search_replies, self.account, thread_ref, sender_email)
        except Exception as e:
            logger.warning(f"IMAP reply search failed for thread {thread_ref}: {e}")
            return []
        return [
            ReplyMeta(
                provider_message_id=r["message_id"] or f"imap-{uuid.uuid4()}",
                from_email=r["from_email"],
                subject=r["subject"],
                snippet=r["snippet"],
                date=r["date"],
                thread_ref=thread_ref,
            )
            for r in raw
        ]

    async def get_message_body(self, provider_message_id: str) -> str:
        try:
            return await asyncio.to_thread(_sync_get_body, self.account, provider_message_id)
        except Exception as e:
            logger.warning(f"IMAP body fetch failed for {provider_message_id}: {e}")
            return ""

    async def verify(self) -> bool:
        try:
            return await asyncio.to_thread(_sync_verify_imap, self.account)
        except Exception:
            return False
