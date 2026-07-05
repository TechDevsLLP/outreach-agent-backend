"""
Email Sending Integration via SendGrid.
Supports manual single-email sending to prospects.
"""

import logging
import uuid
from typing import Optional

try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail, TrackingSettings, ClickTracking, OpenTracking, Header, ReplyTo
    SENDGRID_AVAILABLE = True
except ImportError:
    SENDGRID_AVAILABLE = False

from config import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()


def plain_text_to_html(text: str) -> str:
    """Convert plain text email body to HTML with paragraph and line break tags."""
    if not text or "<p>" in text or "<br" in text or "<div" in text:
        return text

    paragraphs = text.strip().split("\n\n")
    html_parts = []
    for p in paragraphs:
        p = p.strip()
        if p:
            p = p.replace("\n", "<br>")
            html_parts.append(f"<p>{p}</p>")
    return "\n".join(html_parts)


class EmailSender:
    """SendGrid email sender with tracking."""

    def __init__(self, api_key: Optional[str] = None):
        if not SENDGRID_AVAILABLE:
            raise ImportError("SendGrid not installed. Run: pip install sendgrid")

        self.api_key = api_key or settings.sendgrid_api_key
        if not self.api_key:
            raise ValueError("SendGrid API key not configured in settings")

        self.client = SendGridAPIClient(self.api_key)

    async def send_cold_email(
        self,
        to_email: str,
        to_name: str,
        subject: str,
        body: str,
        from_email: Optional[str] = None,
        from_name: Optional[str] = None,
        custom_args: Optional[dict] = None,
    ) -> dict:
        """
        Send a cold email via SendGrid with tracking enabled.

        Returns:
            Response dict with status and message_id
        """
        if not from_email:
            from_email = getattr(settings, "sender_email", "outreach@outflo.ai")
        if not from_name:
            from_name = getattr(settings, "sender_name", "OutFlo")

        html_body = plain_text_to_html(body)
        message = Mail(
            from_email=(from_email, from_name),
            to_emails=(to_email, to_name),
            subject=subject,
            html_content=html_body,
        )

        message.tracking_settings = TrackingSettings()
        message.tracking_settings.click_tracking = ClickTracking(enable=True, enable_text=False)
        message.tracking_settings.open_tracking = OpenTracking(enable=True)

        # Generate RFC Message-ID for threading
        rfc_message_id = f"<{uuid.uuid4()}@outflo.ai>"
        message.header = Header("Message-ID", rfc_message_id)

        # Add Reply-To pointing to inbound parse address
        reply_to_addr = getattr(settings, "reply_to_email", "")
        if reply_to_addr:
            message.reply_to = ReplyTo(reply_to_addr)

        if custom_args:
            for key, value in custom_args.items():
                message.custom_arg = {key: str(value)}

        try:
            response = self.client.send(message)

            logger.info(
                f"Email sent to {to_email}. "
                f"Status: {response.status_code}, MessageID: {response.headers.get('X-Message-Id')}"
            )

            return {
                "status": "sent",
                "status_code": response.status_code,
                "message_id": response.headers.get("X-Message-Id"),
                "email_message_id": rfc_message_id,
                "to_email": to_email,
                "subject": subject,
            }

        except Exception as e:
            logger.error(f"Failed to send email to {to_email}: {e}", exc_info=True)
            return {
                "status": "failed",
                "error": str(e),
                "to_email": to_email,
                "subject": subject,
            }

    async def send_prospect_outreach(
        self,
        prospect: dict,
        variant_id: str = "A",
    ) -> dict:
        """
        Send outreach email to a prospect using their generated outreach_messages.

        Args:
            prospect: Prospect dict (must have outreach_messages)
            variant_id: Subject line variant to use ("A", "B", or "C")

        Returns:
            Response dict with status
        """
        to_email = prospect.get("email")
        to_name = prospect.get("name") or prospect.get("first_name", "")
        prospect_id = str(prospect.get("_id"))

        if not to_email:
            return {"status": "failed", "error": "No email address"}

        outreach = prospect.get("outreach_messages", {})
        cold_email = outreach.get("cold_email", {})

        body = cold_email.get("body", "")
        if not body:
            return {"status": "failed", "error": "No outreach body generated"}

        # Get subject from selected variant
        subject_variants = cold_email.get("subject_variants", [])
        subject = None

        for variant in subject_variants:
            if variant.get("variant_id") == variant_id:
                subject = variant.get("subject")
                break

        if not subject:
            if subject_variants:
                subject = subject_variants[0].get("subject", "Let's connect")
            else:
                subject = cold_email.get("subject", "Let's connect")

        result = await self.send_cold_email(
            to_email=to_email,
            to_name=to_name,
            subject=subject,
            body=body,
            custom_args={
                "prospect_id": prospect_id,
                "variant_id": variant_id,
            },
        )

        # Record outbound email in conversations
        if result.get("status") == "sent":
            try:
                from services.conversation_service import record_outbound_email
                await record_outbound_email(
                    prospect_id=prospect_id,
                    prospect_name=to_name,
                    prospect_email=to_email,
                    prospect_company=prospect.get("company_name"),
                    subject=subject,
                    body_text=body,
                    body_html=plain_text_to_html(body),
                    sendgrid_message_id=result.get("message_id"),
                    email_message_id=result.get("email_message_id"),
                    variant_id=variant_id,
                )
            except Exception as e:
                logger.warning(f"Failed to record outbound email in conversations: {e}")

        return result
