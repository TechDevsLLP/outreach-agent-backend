"""
Plain-text → HTML conversion for outbound email bodies.

Every generated message in the system (campaign steps, AI replies, meeting
proposals) is authored as PLAIN TEXT with real newlines. Every email provider
we ship sends a `text/html` part. HTML collapses runs of whitespace, so handing
raw plain text to the HTML part turned every message into one run-on paragraph
in the recipient's inbox. These helpers are the single place that conversion
happens, so all send paths stay consistent.
"""

import html as _html
import re
from typing import Tuple

# Only treat a body as pre-rendered HTML when it opens with a real block/anchor
# tag. A conservative test matters because generated copy legitimately contains
# characters like "<" or "->" that a naive "does it contain a tag?" check would
# misread as markup, which would skip escaping and let the text through raw.
_HTML_DOCUMENT_RE = re.compile(
    r"<\s*(?:!doctype\b|html\b|body\b|div\b|p\s*[>/]|table\b|br\s*/?>|ul\b|ol\b|li\b"
    r"|h[1-6]\s*[>/]|a\s[^>]*href)",
    re.IGNORECASE,
)


def looks_like_html(body: str) -> bool:
    """True when `body` is already rendered HTML and must be passed through untouched."""
    if not body:
        return False
    return bool(_HTML_DOCUMENT_RE.search(body))


def plain_text_to_html(text: str) -> str:
    """
    Render a plain-text email body as HTML, preserving paragraphs and line breaks.

    Escaping happens FIRST so that any `<`, `>` or `&` the message generator
    produced is shown literally instead of being interpreted as markup (or used
    to inject it). Blank-line-separated blocks become `<p>`; single newlines
    inside a block become `<br>`.
    """
    if not text:
        return ""

    escaped = _html.escape(text.strip(), quote=False)
    # Normalise CRLF/CR so paragraph splitting works on Windows-authored copy.
    escaped = escaped.replace("\r\n", "\n").replace("\r", "\n")

    blocks = re.split(r"\n\s*\n", escaped)
    paragraphs = [
        "<p>" + block.strip().replace("\n", "<br>") + "</p>"
        for block in blocks
        if block.strip()
    ]
    return "\n".join(paragraphs)


def build_email_bodies(body: str) -> Tuple[str, str]:
    """
    Normalise an outbound body into the (text/plain, text/html) pair to send.

    Callers hand us either plain text or already-rendered HTML. For plain text
    we keep the original (newlines intact) as the plain part and generate the
    HTML part. For HTML we pass it through unchanged and derive a readable
    plain-text fallback, so the multipart/alternative message is never
    half-empty for text-only clients.
    """
    if not body:
        return "", ""
    if looks_like_html(body):
        return html_to_plain_text(body), body
    return body, plain_text_to_html(body)


def html_to_plain_text(html_body: str) -> str:
    """
    Best-effort plain-text fallback for a body that was supplied as HTML.

    Deliberately crude — it exists so the text/plain part of a multipart message
    is readable, not to round-trip arbitrary HTML.
    """
    if not html_body:
        return ""
    text = re.sub(r"(?is)<(script|style).*?</\1>", "", html_body)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</\s*(p|div|li|h[1-6]|tr)\s*>", "\n\n", text)
    text = re.sub(r"(?i)<li[^>]*>", "- ", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = _html.unescape(text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()
