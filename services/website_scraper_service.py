"""
Website scraping service for onboarding analysis.
- Regular URLs: httpx GET + HTML stripping via stdlib html.parser
- LinkedIn company URLs: Apify COMPANY_SCRAPER actor via asyncio.run_in_executor
Errors are swallowed and return empty string (graceful degradation).
"""

import asyncio
import logging
from html.parser import HTMLParser

import httpx

logger = logging.getLogger(__name__)

_MAX_CHARS = 5000
_SCRAPE_TIMEOUT = 15.0  # seconds for httpx


class _TextExtractor(HTMLParser):
    """Strips HTML tags and collects visible text content."""

    SKIP_TAGS = {"script", "style", "head", "noscript", "svg", "iframe", "meta", "link"}

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


def _is_linkedin_company_url(url: str) -> bool:
    return "linkedin.com/company/" in url.lower()


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
    try:
        async with httpx.AsyncClient(
            timeout=_SCRAPE_TIMEOUT,
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            },
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()

            parser = _TextExtractor()
            parser.feed(resp.text)
            text = parser.get_text()
            logger.info(f"Scraped {len(text)} chars from {url}")
            return text[:_MAX_CHARS]

    except httpx.TimeoutException:
        logger.warning(f"Timeout scraping {url}")
        return ""
    except httpx.HTTPStatusError as e:
        logger.warning(f"HTTP {e.response.status_code} scraping {url}")
        return ""
    except Exception as e:
        logger.warning(f"Failed to scrape {url}: {e}")
        return ""


async def _scrape_linkedin_company(url: str) -> str:
    """Wraps the blocking Apify scrape_company_pages in the default thread pool."""
    from services.company_scraper_service import scrape_company_pages

    loop = asyncio.get_event_loop()
    try:
        _, results = await asyncio.wait_for(
            loop.run_in_executor(None, scrape_company_pages, [url]),
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
