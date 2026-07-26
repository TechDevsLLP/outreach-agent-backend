"""Inbound replies must record the prospect's RFC Message-ID.

Without it, a reply's In-Reply-To/References point at our own previous outbound
message rather than at the message being answered, so strict mail clients build
a broken reference chain.
"""
import inspect

from services.email_providers.base import ReplyMeta
from services import gmail_service, reply_ingest


def test_reply_meta_carries_rfc_message_id():
    meta = ReplyMeta(
        provider_message_id="19f9d949015b0d83",
        from_email="prospect@example.com",
        rfc_message_id="<abc@mail.example.com>",
    )
    assert meta.rfc_message_id == "<abc@mail.example.com>"
    # Optional: Zoho cannot supply one, and that must not break ingest.
    assert ReplyMeta(provider_message_id="x", from_email="y").rfc_message_id is None


def test_gmail_requests_the_message_id_header():
    """Gmail returns metadata headers only when explicitly asked for them."""
    src = inspect.getsource(gmail_service.get_thread_messages)
    assert '"Message-ID"' in src, "Message-ID must be in metadataHeaders"
    assert 'headers.get("message-id"' in src, "parsed header is lower-cased"


def test_ingest_stores_the_inbound_message_id():
    """_record_reply_in_conversation must persist it as email_message_id."""
    sig = inspect.signature(reply_ingest._record_reply_in_conversation)
    assert "rfc_message_id" in sig.parameters
    assert sig.parameters["rfc_message_id"].default is None

    src = inspect.getsource(reply_ingest._record_reply_in_conversation)
    assert "email_message_id=rfc_message_id" in src

    # And process_reply must actually pass it down from the ReplyMeta.
    assert "rfc_message_id=reply_meta.rfc_message_id" in inspect.getsource(
        reply_ingest.process_reply
    )
