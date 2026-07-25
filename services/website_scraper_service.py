"""
Website scraping service.

Two consumers:
- Onboarding analysis (`scrape_website`): returns a raw text dump for AI analysis.
- Company enrichment (`fetch_website_data`): returns a clean, structured
  WebsiteData dict (title, meta description, about text, social links, tech
  hints) for persistence on companies.website_data.

Both share the same httpx fetch internals (UA, timeout, redirect handling).
- Regular URLs: httpx GET + HTML parsing via stdlib html.parser
- LinkedIn company URLs: Apify COMPANY_SCRAPER actor via asyncio.run_in_executor

All failures are swallowed. `scrape_website` returns "" on failure;
`fetch_website_data` returns None (with the reason logged).
"""

import asyncio
import logging
from datetime import datetime
from html.parser import HTMLParser
from urllib.parse import urljoin

import httpx

logger = logging.getLogger(__name__)

_MAX_CHARS = 5000              # onboarding raw text dump cap
_ABOUT_MAX_CHARS = 4000        # structured about_text cap
_SCRAPE_TIMEOUT = 15.0         # seconds for httpx

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# host substring -> social platform key
_SOCIAL_HOSTS = {
    "linkedin.com": "linkedin",
    "twitter.com": "twitter",
    "x.com": "x",
    "facebook.com": "facebook",
    "instagram.com": "instagram",
    "youtube.com": "youtube",
    "youtu.be": "youtube",
    "github.com": "github",
}

# lowercase substring in a <script src> (or generator) -> tech label
_TECH_SCRIPT_HINTS = {
    "wp-content": "WordPress",
    "wp-includes": "WordPress",
    "shopify": "Shopify",
    "cdn.shopify": "Shopify",
    "wix.com": "Wix",
    "squarespace": "Squarespace",
    "hubspot": "HubSpot",
    "hs-scripts": "HubSpot",
    "webflow": "Webflow",
    "_next/": "Next.js",
    "gatsby": "Gatsby",
    "googletagmanager": "Google Tag Manager",
    "google-analytics": "Google Analytics",
    "cloudflare": "Cloudflare",
    "jquery": "jQuery",
    "react": "React",
    "vue": "Vue.js",
    "drupal": "Drupal",
    "intercom": "Intercom",
    "segment.com": "Segment",
    "stripe.com": "Stripe",
}


class _TextExtractor(HTMLParser):
    """Strips HTML tags and collects visible text content."""

    # Only tags with matching end tags — void tags like <meta>/<link> would
    # leak skip_depth (no end tag to decrement) and suppress all body text.
    SKIP_TAGS = {"script", "style", "head", "noscript", "svg", "iframe"}

    def __init__(self):
        super().__init__()
        self._parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag, attrs):
        if tag in self.SKIP_TAGS:
            self._skip_depth += 1

    def handle_endtag(self, tag):
        if tag in self.SKIP_TAGS:
            self._skip_depth = max(0, self._skip_depth - 1)

    def handle_data(self, data):
        if self._skip_depth == 0:
            stripped = data.strip()
            if stripped:
                self._parts.append(stripped)

    def get_text(self) -> str:
        return " ".join(self._parts)


class _StructuredExtractor(HTMLParser):
    """
    Single-pass extraction of title, meta description, generator/tech hints,
    social links, and visible body text.
    """

    SKIP_TAGS = {"script", "style", "head", "noscript", "svg", "iframe"}

    def __init__(self):
        super().__init__()
        self.title: str | None = None
        self.meta_desc: str | None = None
        self.socials: dict[str, str] = {}
        self.tech: list[str] = []
        self._text_parts: list[str] = []
        self._skip_depth = 0
        self._in_title = False
        self._title_parts: list[str] = []

    def handle_starttag(self, tag, attrs):
        attr = dict(attrs)
        if tag == "title":
            self._in_title = True
        elif tag == "meta":
            self._handle_meta(attr)
        elif tag == "a":
            self._handle_link(attr.get("href"))
        elif tag == "script":
            self._add_tech_from_src(attr.get("src"))

        if tag in self.SKIP_TAGS:
            self._skip_depth += 1

    def handle_endtag(self, tag):
        if tag == "title":
            self._in_title = False
        if tag in self.SKIP_TAGS:
            self._skip_depth = max(0, self._skip_depth - 1)

    def handle_data(self, data):
        if self._in_title:
            stripped = data.strip()
            if stripped:
                self._title_parts.append(stripped)
        if self._skip_depth == 0:
            stripped = data.strip()
            if stripped:
                self._text_parts.append(stripped)

    def _handle_meta(self, attr: dict):
        name = (attr.get("name") or attr.get("property") or "").lower()
        content = (attr.get("content") or "").strip()
        if not content:
            return
        if name in ("description", "og:description") and not self.meta_desc:
            self.meta_desc = content
        elif name == "generator":
            self._add_tech(content)

    def _handle_link(self, href: str | None):
        if not href:
            return
        low = href.lower()
        for host, key in _SOCIAL_HOSTS.items():
            if host in low and key not in self.socials:
                self.socials[key] = href.strip()
                break

    def _add_tech_from_src(self, src: str | None):
        if not src:
            return
        low = src.lower()
        for hint, label in _TECH_SCRIPT_HINTS.items():
            if hint in low:
                self._add_tech(label)

    def _add_tech(self, label: str):
        if label and label not in self.tech:
            self.tech.append(label)

    def finalize(self):
        if self._title_parts:
            self.title = " ".join(self._title_parts).strip() or None

    def get_text(self) -> str:
        return " ".join(self._text_parts)


def _is_linkedin_company_url(url: str) -> bool:
    return "linkedin.com/company/" in url.lower()


def _looks_html(content_type: str | None) -> bool:
    if not content_type:
        # No content-type header: optimistically try to parse as HTML.
        return True
    ct = content_type.lower()
    return "html" in ct or "xml" in ct or ct.startswith("text/")


async def _http_get(url: str) -> tuple[httpx.Response | None, str | None]:
    """
    Perform a GET with the shared UA/timeout/redirect settings.
    Returns (response, None) on success or (None, reason) on failure.
    Never raises.
    """
    if not url:
        return None, "empty_url"
    try:
        async with httpx.AsyncClient(
            timeout=_SCRAPE_TIMEOUT,
            follow_redirects=True,
            headers={"User-Agent": _USER_AGENT},
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp, None
    except httpx.TimeoutException:
        logger.warning(f"Timeout scraping {url}")
        return None, "timeout"
    except httpx.HTTPStatusError as e:
        code = e.response.status_code
        logger.warning(f"HTTP {code} scraping {url}")
        return None, f"http_{code}"
    except Exception as e:
        logger.warning(f"Failed to scrape {url}: {e}")
        return None, f"error:{type(e).__name__}"


# ══════════════════════════════════════════════════════════════════
# Structured website data (company enrichment)
# ══════════════════════════════════════════════════════════════════


async def fetch_website_data(url: str) -> dict | None:
    """
    Fetch a company website and return a structured WebsiteData dict:
        {url, title, meta_desc, about_text, tech, socials, scraped_at}
    Returns None on any failure (non-HTML, redirect-to-error, timeout, etc.)
    with the reason logged. Never raises into the caller.
    """
    if not url:
        return None

    fetch_url = url if url.startswith("http") else f"https://{url}"
    resp, reason = await _http_get(fetch_url)
    if resp is None:
        return None

    content_type = resp.headers.get("content-type")
    if not _looks_html(content_type):
        logger.warning(f"Non-HTML content-type '{content_type}' for {fetch_url}")
        return None

    try:
        parser = _StructuredExtractor()
        parser.feed(resp.text)
        parser.finalize()
    except Exception as e:
        logger.warning(f"Failed to parse HTML for {fetch_url}: {e}")
        return None

    # Resolve relative social hrefs against the final (post-redirect) URL.
    base = str(resp.url)
    socials = {}
    for key, href in parser.socials.items():
        try:
            socials[key] = urljoin(base, href)
        except Exception:
            socials[key] = href

    about_text = " ".join(parser.get_text().split())[:_ABOUT_MAX_CHARS]

    return {
        "url": base,
        "title": parser.title,
        "meta_desc": parser.meta_desc,
        "about_text": about_text or None,
        "tech": parser.tech,
        "socials": socials,
        "scraped_at": datetime.utcnow(),
    }


def website_data_failure_marker(url: str, reason: str | None = None) -> dict:
    """A distinguishable 'we tried and failed' marker (has scraped_at + error)."""
    return {
        "url": url,
        "scraped_at": datetime.utcnow(),
        "error": reason or "unknown",
    }


def is_website_data_stale(website_data: dict | None, max_age_days: int = 90) -> bool:
    """
    True when website_data is missing/never-tried or older than max_age_days.
    A failure marker (has scraped_at) counts as tried, so it is only refreshed
    once it goes stale — never re-fetched on every upsert.
    """
    if not website_data:
        return True
    scraped_at = website_data.get("scraped_at")
    if not isinstance(scraped_at, datetime):
        return True
    age = datetime.utcnow() - scraped_at
    return age.days >= max_age_days


# ══════════════════════════════════════════════════════════════════
# Raw text dump (onboarding analysis)
# ══════════════════════════════════════════════════════════════════


async def scrape_website(url: str) -> str:
    """
    Fetch and extract text content from a URL.
    Returns text truncated to _MAX_CHARS, or "" on any failure.
    """
    if not url:
        return ""
    if _is_linkedin_company_url(url):
        return await _scrape_linkedin_company(url)
    return await _scrape_regular_website(url)


async def _scrape_regular_website(url: str) -> str:
    resp, reason = await _http_get(url)
    if resp is None:
        return ""
    try:
        parser = _TextExtractor()
        parser.feed(resp.text)
        text = parser.get_text()
        logger.info(f"Scraped {len(text)} chars from {url}")
        return text[:_MAX_CHARS]
    except Exception as e:
        logger.warning(f"Failed to parse {url}: {e}")
        return ""


async def _scrape_linkedin_company(url: str) -> str:
    """Calls the async Apify scrape_company_pages with a 90s wall-clock cap."""
    from services.company_scraper_service import scrape_company_pages

    try:
        _, results = await asyncio.wait_for(
            scrape_company_pages([url]),
            timeout=90.0,
        )
        if not results:
            logger.warning(f"No LinkedIn company results for {url}")
            return ""

        company = results[0]
        parts = []
        for field in ["description", "tagline", "about", "specialities", "industries", "name"]:
            val = company.get(field)
            if isinstance(val, str) and val.strip():
                parts.append(val.strip())
            elif isinstance(val, list):
                parts.append(", ".join(str(v) for v in val if v))

        text = " ".join(parts)
        logger.info(f"Scraped {len(text)} chars from LinkedIn company {url}")
        return text[:_MAX_CHARS]

    except asyncio.TimeoutError:
        logger.warning(f"Apify LinkedIn scrape timed out for {url}")
        return ""
    except Exception as e:
        logger.warning(f"Apify LinkedIn scrape failed for {url}: {e}")
        return ""
