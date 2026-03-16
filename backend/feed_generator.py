"""
Feed generation engine — multi-phase news pipeline.

Phase 0 : Filter RSS to items from the last 36 hours.
Phase 1 : Pre-scrape top ~150 articles using trafilatura for high-quality body text.
Phase 2 : Single fast-model call to simultaneously filter by interests AND cluster into groups.
Phase 3 : Main model synthesises each group into one digest post (concurrent, reuses pre-scraped text).

Videos  : Fast model picks the best N from YouTube feeds.
Stocks  : yfinance.
"""
import os
import re
import hashlib
import secrets
import asyncio
import logging
import time as _time
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone, timedelta
from typing import Optional

import feedparser
import httpx
import yfinance as yf
from bs4 import BeautifulSoup
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from models import User, Interest, FeedItem

log = logging.getLogger("feed_generator")

# Thread pool for CPU-bound sync work (feedparser, BeautifulSoup)
# so they never block the async event loop
_executor = ThreadPoolExecutor(max_workers=8)

# ---------------------------------------------------------------------------
# Generation status tracking (in-memory, global)
# ---------------------------------------------------------------------------

# Approximate seconds each phase takes — used for remaining-time estimates
_PHASE_DURATIONS = {
    "fetching":     ("Fetching news from 60+ sources…",          15),
    "scraping":     ("Reading article content…",                  25),
    "filtering":    ("Finding stories for your interests…",       12),
    "synthesizing": ("Writing your digest posts…",                30),
    "facts_videos": ("Selecting videos…",                         10),
    "saving":       ("Saving your feed…",                          4),
}
_TOTAL_ESTIMATE_SECONDS = sum(d for _, d in _PHASE_DURATIONS.values())  # ~80 s

_gen_status: dict = {
    "active": False,
    "phase": "",
    "label": "",
    "started_at": None,   # float (time.monotonic)
    "phase_started_at": None,
}


def _set_phase(phase: str) -> None:
    """Update the global generation status to the given phase."""
    label = _PHASE_DURATIONS.get(phase, ("Working…", 0))[0]
    _gen_status.update({
        "active": True,
        "phase": phase,
        "label": label,
        "phase_started_at": _time.monotonic(),
    })
    if _gen_status.get("started_at") is None:
        _gen_status["started_at"] = _time.monotonic()
    log.info("Generation phase: %s", phase)


def _clear_phase() -> None:
    _gen_status.update({"active": False, "phase": "done", "label": "Done!", "started_at": None})


def get_generation_status() -> dict:
    """Return a snapshot of the current generation status for the API."""
    st = dict(_gen_status)
    elapsed = (_time.monotonic() - st["started_at"]) if st.get("started_at") else 0
    remaining = max(0, _TOTAL_ESTIMATE_SECONDS - int(elapsed))
    return {
        "active": st["active"],
        "phase": st["phase"],
        "label": st["label"],
        "elapsed_seconds": int(elapsed),
        "remaining_seconds": remaining,
        "total_estimate_seconds": _TOTAL_ESTIMATE_SECONDS,
    }


def _in_thread(fn, *args):
    """Run a synchronous callable in the thread pool and await the result."""
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(_executor, fn, *args)


_HTML_TAG_RE = re.compile(r"<[^>]+>")

def strip_html(text: str) -> str:
    """Fast HTML-tag stripper for short strings (RSS summaries, etc.)."""
    return " ".join(_HTML_TAG_RE.sub(" ", text).split())

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ANTHROPIC_API_KEY    = os.environ.get("ANTHROPIC_API_KEY", "")
OPENROUTER_API_KEY   = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL     = os.environ.get("OPENROUTER_MODEL",      "anthropic/claude-opus-4")
FAST_MODEL_OPENROUTER = os.environ.get("FAST_MODEL_OPENROUTER", "google/gemini-flash-1.5")
FAST_MODEL_ANTHROPIC  = os.environ.get("FAST_MODEL_ANTHROPIC",  "claude-haiku-4-5-20251001")

ITEMS_PER_USER = int(os.environ.get("ITEMS_PER_USER", "12"))
NEWS_RECENCY_HOURS = int(os.environ.get("NEWS_RECENCY_HOURS", "36"))
PRE_SCRAPE_LIMIT = int(os.environ.get("PRE_SCRAPE_LIMIT", "150"))

# ---------------------------------------------------------------------------
# RSS sources
# ---------------------------------------------------------------------------
NEWS_FEEDS = [
    # --- General / International (kept lean) ---
    ("BBC News",          "http://feeds.bbci.co.uk/news/rss.xml"),
    ("Reuters",           "https://feeds.reuters.com/reuters/topNews"),
    ("NPR News",          "https://feeds.npr.org/1001/rss.xml"),
    ("AP News",           "https://feeds.apnews.com/rss/topnews"),

    # --- Technology & Science ---
    ("Ars Technica",      "http://feeds.arstechnica.com/arstechnica/index"),
    ("Hacker News",       "https://news.ycombinator.com/rss"),
    ("Wired",             "https://www.wired.com/feed/rss"),
    ("MIT Tech Review",   "https://www.technologyreview.com/feed/"),
    ("The Verge",         "https://www.theverge.com/rss/index.xml"),
    ("Science Daily",     "https://www.sciencedaily.com/rss/all.xml"),
    ("New Scientist",     "https://www.newscientist.com/feed/home/"),
    ("NASA",              "https://www.nasa.gov/rss/dyn/breaking_news.rss"),
    ("TechCrunch",        "https://techcrunch.com/feed/"),

    # --- Finance & Economics ---
    ("MarketWatch",       "https://feeds.marketwatch.com/marketwatch/topstories/"),
    ("Reuters Business",  "https://feeds.reuters.com/reuters/businessNews"),
    ("Investopedia",      "https://www.investopedia.com/feedbuilder/feed/getfeed/?feedName=rss_headline"),
    ("Seeking Alpha",     "https://seekingalpha.com/market_currents.xml"),

    # --- Food & Cooking ---
    ("Serious Eats",      "https://www.seriouseats.com/feeds/all"),
    ("Bon Appétit",       "https://www.bonappetit.com/feed/rss"),
    ("The Kitchn",        "https://www.thekitchn.com/main.rss"),
    ("Food52",            "https://food52.com/blog/feed"),
    ("BBC Good Food",     "https://www.bbcgoodfood.com/api/json/rss/homepage-feed.rss"),

    # --- Sports ---
    ("BBC Sport",         "http://feeds.bbci.co.uk/sport/rss.xml"),
    ("ESPN",              "https://www.espn.com/espn/rss/news"),
    ("Sky Sports",        "https://www.skysports.com/rss/12040"),

    # --- Gaming ---
    ("Eurogamer",         "https://www.eurogamer.net/?format=rss"),
    ("Rock Paper Shotgun","https://www.rockpapershotgun.com/feed"),
    ("IGN",               "https://feeds.ign.com/ign/games-articles"),

    # --- Health & Wellbeing ---
    ("Harvard Health",    "https://www.health.harvard.edu/blog/feed"),
    ("WebMD",             "https://rssfeeds.webmd.com/rss/rss.aspx?RSSSource=RSS_PUBLIC"),

    # --- Environment & Sustainability ---
    ("Yale E360",         "https://e360.yale.edu/feed"),
    ("Carbon Brief",      "https://www.carbonbrief.org/feed"),
    ("Guardian Environment", "https://www.theguardian.com/environment/rss"),

    # --- Arts, Culture & Books ---
    ("The Guardian Culture", "https://www.theguardian.com/culture/rss"),
    ("Pitchfork",         "https://pitchfork.com/rss/news/"),
    ("Literary Hub",      "https://lithub.com/feed/"),

    # --- Travel ---
    ("Lonely Planet",     "https://www.lonelyplanet.com/news/feed"),
    ("Atlas Obscura",     "https://www.atlasobscura.com/feeds/latest"),

    # --- Austria ---
    ("ORF News",          "https://rss.orf.at/news.xml"),
    ("ORF Österreich",    "https://rss.orf.at/oesterreich.xml"),
    ("ORF Wissenschaft",  "https://rss.orf.at/science.xml"),
    ("Der Standard",      "https://www.derstandard.at/rss"),
    ("Die Presse",        "https://diepresse.com/rss"),
    ("Heute",             "https://www.heute.at/feed/"),
    ("Vienna Online",     "https://www.vienna.at/feed"),

    # --- Reddit ---
    ("Reddit r/worldnews",      "https://www.reddit.com/r/worldnews/hot.rss?limit=25"),
    ("Reddit r/technology",     "https://www.reddit.com/r/technology/hot.rss?limit=25"),
    ("Reddit r/science",        "https://www.reddit.com/r/science/hot.rss?limit=25"),
    ("Reddit r/programming",    "https://www.reddit.com/r/programming/hot.rss?limit=25"),
    ("Reddit r/cooking",        "https://www.reddit.com/r/cooking/hot.rss?limit=25"),
    ("Reddit r/investing",      "https://www.reddit.com/r/investing/hot.rss?limit=25"),
    ("Reddit r/personalfinance","https://www.reddit.com/r/personalfinance/hot.rss?limit=25"),
    ("Reddit r/sports",         "https://www.reddit.com/r/sports/hot.rss?limit=25"),
    ("Reddit r/gaming",         "https://www.reddit.com/r/gaming/hot.rss?limit=25"),
    ("Reddit r/environment",    "https://www.reddit.com/r/environment/hot.rss?limit=25"),
    ("Reddit r/books",          "https://www.reddit.com/r/books/hot.rss?limit=25"),
    ("Reddit r/travel",         "https://www.reddit.com/r/travel/hot.rss?limit=25"),
    ("Reddit r/Austria",        "https://www.reddit.com/r/Austria/hot.rss?limit=25"),
    ("Reddit r/todayilearned",  "https://www.reddit.com/r/todayilearned/hot.rss?limit=25"),
    ("Reddit r/health",         "https://www.reddit.com/r/health/hot.rss?limit=25"),
    ("Reddit r/space",          "https://www.reddit.com/r/space/hot.rss?limit=25"),
    ("Reddit r/food",           "https://www.reddit.com/r/food/hot.rss?limit=25"),
    ("Reddit r/MachineLearning","https://www.reddit.com/r/MachineLearning/hot.rss?limit=25"),
    ("Reddit r/ArtificialIntelligence","https://www.reddit.com/r/ArtificialIntelligence/hot.rss?limit=25"),
    ("Reddit r/soccer",         "https://www.reddit.com/r/soccer/hot.rss?limit=25"),
    ("Reddit r/formula1",       "https://www.reddit.com/r/formula1/hot.rss?limit=25"),
    ("Reddit r/cycling",        "https://www.reddit.com/r/cycling/hot.rss?limit=25"),
]

YOUTUBE_FEEDS = [
    # Science & space
    ("Kurzgesagt",        "https://www.youtube.com/feeds/videos.xml?channel_id=UCsXVk37bltHxD1rDPwtNM8Q"),
    ("Veritasium",        "https://www.youtube.com/feeds/videos.xml?channel_id=UCHnyfMqiRRG1u-2MsSQLbXA"),
    ("Smarter Every Day", "https://www.youtube.com/feeds/videos.xml?channel_id=UC6107grRI4m0o2-emgoDnAA"),
    ("3Blue1Brown",       "https://www.youtube.com/feeds/videos.xml?channel_id=UCYO_jab_esuFRV4b17AJtAg"),
    ("SciShow",           "https://www.youtube.com/feeds/videos.xml?channel_id=UCZYTClx2T1of7BRZ86-8fow"),
    ("NASA",              "https://www.youtube.com/feeds/videos.xml?channel_id=UCLA_DiR1FfKNvjuUpBHmylQ"),
    # Tech & engineering
    ("Linus Tech Tips",   "https://www.youtube.com/feeds/videos.xml?channel_id=UCXuqSBlHAE6Xw-yeJA0Tunw"),
    ("Mark Rober",        "https://www.youtube.com/feeds/videos.xml?channel_id=UCY1kMZp36IQSyNx_9h4mpCg"),
    ("CGP Grey",          "https://www.youtube.com/feeds/videos.xml?channel_id=UC2C_jShtL725hvbm1arSV9w"),
    ("Fireship",          "https://www.youtube.com/feeds/videos.xml?channel_id=UCsBjURrPoezykLs9EqgamOA"),
    ("Two Minute Papers", "https://www.youtube.com/feeds/videos.xml?channel_id=UCbfYPyITQ-7l4upoX8nvctg"),
    # Cooking & food
    ("Binging with Babish",   "https://www.youtube.com/feeds/videos.xml?channel_id=UCJHA_jMfCvEnv-3kRjTCQXw"),
    ("Joshua Weissman",       "https://www.youtube.com/feeds/videos.xml?channel_id=UChBEbMKI1eCcejTtmI32UEw"),
    ("Internet Shaquille",    "https://www.youtube.com/feeds/videos.xml?channel_id=UCEIKfkyGFD1EMwkiQqmYhkA"),
    ("Pro Home Cooks",        "https://www.youtube.com/feeds/videos.xml?channel_id=UCCMxHHciWRBBouzk-PGzmtQ"),
    # Finance & economics
    ("Plain Bagel",       "https://www.youtube.com/feeds/videos.xml?channel_id=UCFCEuCsyWP0YkP3CZ3Mr01Q"),
    ("Patrick Boyle",     "https://www.youtube.com/feeds/videos.xml?channel_id=UCASM_PTnGsVnLeEccID6HFw"),
    ("Andrei Jikh",       "https://www.youtube.com/feeds/videos.xml?channel_id=UCGy7SkBjcIAgTiwkXEtPnYg"),
    # Sports & fitness
    ("GQ Sports",         "https://www.youtube.com/feeds/videos.xml?channel_id=UCIRYBXDze5krPDzAEOxFGVA"),
    ("Global Cycling Network","https://www.youtube.com/feeds/videos.xml?channel_id=UCuTaETsuCOkJ0H_kqoAinad"),
    # History & culture
    ("Oversimplified",    "https://www.youtube.com/feeds/videos.xml?channel_id=UCNIuvl7V8zACPpTmmNIqioA"),
    ("Toldinstone",       "https://www.youtube.com/feeds/videos.xml?channel_id=UCjA5GZDEsGMKCjPSDBL4bSg"),
    # Gaming
    ("Noclip",            "https://www.youtube.com/feeds/videos.xml?channel_id=UC0fDG3byEcMtbOqPMymDNbw"),
    ("GMTK",              "https://www.youtube.com/feeds/videos.xml?channel_id=UCqJ-Xo29CKyLTjn6z2XwYAw"),
    # Environment & travel
    ("Real Engineering",  "https://www.youtube.com/feeds/videos.xml?channel_id=UCR1IuLEqb6UEA_zQ81kwXfg"),
    ("Wendover Productions","https://www.youtube.com/feeds/videos.xml?channel_id=UC9RM-iSvTu1uPJb8X5yp3EQ"),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_share_token(user_id: int, feed_date: date, position: int) -> str:
    raw = f"{user_id}:{feed_date.isoformat()}:{position}:{secrets.token_hex(8)}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def is_recent(entry: dict, hours: int = NEWS_RECENCY_HOURS) -> bool:
    """Return True if the entry was published within the last `hours` hours."""
    pp = entry.get("published_parsed")
    if not pp:
        return True  # no date → include to be safe
    try:
        pub_dt = datetime.fromtimestamp(_time.mktime(pp), tz=timezone.utc)
        return pub_dt >= datetime.now(timezone.utc) - timedelta(hours=hours)
    except Exception:
        return True


def parse_json_safely(text: str) -> dict | list | None:
    """Strip markdown fences and parse JSON, returning None on failure."""
    text = text.strip()
    if text.startswith("```"):
        parts = text.split("```")
        text = parts[1] if len(parts) > 1 else text
        if text.startswith("json"):
            text = text[4:]
    try:
        return json.loads(text.strip())
    except Exception:
        # Try to find a JSON object/array anywhere in the response
        for start, end in [('{', '}'), ('[', ']')]:
            s = text.find(start)
            e = text.rfind(end)
            if s != -1 and e != -1 and e > s:
                try:
                    return json.loads(text[s:e+1])
                except Exception:
                    pass
        return None


# ---------------------------------------------------------------------------
# RSS fetching
# ---------------------------------------------------------------------------

def _parse_feed(text: str) -> list[dict]:
    """Synchronous feedparser call — run in thread pool."""
    parsed = feedparser.parse(text)
    entries = []
    for e in parsed.entries[:30]:
        entries.append({
            "title": e.get("title", ""),
            "summary": e.get("summary", e.get("description", "")),
            "link": e.get("link", ""),
            "published": e.get("published", ""),
            "published_parsed": e.get("published_parsed"),
            "media_thumbnail": (
                e.get("media_thumbnail", [{}])[0].get("url")
                if e.get("media_thumbnail") else None
            ),
        })
    return entries


async def fetch_feed(url: str) -> list[dict]:
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0 (compatible; BoonScroll/1.0; +https://boonscroll.app)"})
        # feedparser is CPU-bound — run off the event loop
        return await _in_thread(_parse_feed, resp.text)
    except Exception as exc:
        log.warning("Failed to fetch %s: %s", url, exc)
        return []


async def fetch_all_news() -> list[tuple[str, dict]]:
    results = await asyncio.gather(*[fetch_feed(url) for _, url in NEWS_FEEDS])
    pairs = []
    for (name, _), entries in zip(NEWS_FEEDS, results):
        for entry in entries:
            pairs.append((name, entry))
    return pairs


async def fetch_all_videos() -> list[tuple[str, dict]]:
    results = await asyncio.gather(*[fetch_feed(url) for _, url in YOUTUBE_FEEDS])
    pairs = []
    for (name, _), entries in zip(YOUTUBE_FEEDS, results):
        for entry in entries:
            pairs.append((name, entry))
    return pairs


def fetch_stock_data(tickers: list[str]) -> list[dict]:
    results = []
    for ticker in tickers:
        try:
            t = yf.Ticker(ticker)
            info = t.fast_info
            price = getattr(info, "last_price", None)
            prev_close = getattr(info, "previous_close", None)
            if price and prev_close:
                change = price - prev_close
                change_pct = (change / prev_close) * 100
            else:
                change = change_pct = 0.0
            results.append({
                "ticker": ticker,
                "price": round(price, 2) if price else 0.0,
                "change": round(change, 2),
                "change_pct": round(change_pct, 2),
            })
        except Exception as exc:
            log.warning("Stock fetch failed for %s: %s", ticker, exc)
    return results


async def extract_tickers_from_interests(interests: list[str]) -> list[str]:
    """Use the fast model to find US stock tickers relevant to the user's interests.
    Returns an empty list if no clear match exists — we skip stocks rather than show irrelevant ones."""
    if not (OPENROUTER_API_KEY or ANTHROPIC_API_KEY):
        return []

    interests_text = "\n".join(f"- {i}" for i in interests)
    prompt = f"""Given these personal interests, list up to 5 relevant US stock tickers to display in a news feed.
Only include tickers with a clear, direct connection to an interest.
If there is no natural connection (e.g. interests are cooking, history, or sport with no financial angle), return an empty list — do NOT force generic picks like SPY or AAPL.

Interests:
{interests_text}

Examples:
- "electric vehicles, Tesla" → ["TSLA"]
- "gaming, video games" → ["ATVI", "MSFT", "NTDOY"]
- "cooking, food" → []
- "AI, machine learning" → ["NVDA", "MSFT", "GOOGL"]
- "cycling, running" → []
- "investing, stock market" → ["SPY", "QQQ", "BRK-B"]

Return JSON only: {{"tickers": ["TICK1", "TICK2"]}}"""

    try:
        raw = await call_ai_fast(prompt, max_tokens=80)
        data = parse_json_safely(raw)
        if isinstance(data, dict) and "tickers" in data:
            tickers = [t.upper().strip() for t in data["tickers"] if isinstance(t, str)]
            log.info("AI extracted tickers: %s", tickers)
            return tickers[:5]
    except Exception as exc:
        log.warning("Ticker extraction failed: %s", exc)
    return []


# ---------------------------------------------------------------------------
# AI helpers — main model and fast/cheap model
# ---------------------------------------------------------------------------

async def call_ai(prompt: str, max_tokens: int = 600) -> str:
    """Call the main (capable) AI model."""
    if OPENROUTER_API_KEY:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(
            api_key=OPENROUTER_API_KEY,
            base_url="https://openrouter.ai/api/v1",
            default_headers={"HTTP-Referer": "http://localhost:8080", "X-Title": "BoonScroll"},
        )
        resp = await client.chat.completions.create(
            model=OPENROUTER_MODEL, max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.choices[0].message.content
    elif ANTHROPIC_API_KEY:
        from anthropic import AsyncAnthropic
        resp = await AsyncAnthropic(api_key=ANTHROPIC_API_KEY).messages.create(
            model="claude-opus-4-6", max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text
    raise ValueError("No AI API key configured")


async def call_ai_fast(prompt: str, max_tokens: int = 512) -> str:
    """Call the cheap/fast AI model (for filtering and grouping tasks)."""
    if OPENROUTER_API_KEY:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(
            api_key=OPENROUTER_API_KEY,
            base_url="https://openrouter.ai/api/v1",
            default_headers={"HTTP-Referer": "http://localhost:8080", "X-Title": "BoonScroll"},
        )
        resp = await client.chat.completions.create(
            model=FAST_MODEL_OPENROUTER, max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.choices[0].message.content
    elif ANTHROPIC_API_KEY:
        from anthropic import AsyncAnthropic
        resp = await AsyncAnthropic(api_key=ANTHROPIC_API_KEY).messages.create(
            model=FAST_MODEL_ANTHROPIC, max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text
    raise ValueError("No AI API key configured")


# ---------------------------------------------------------------------------
# Article content fetching
# ---------------------------------------------------------------------------

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def _extract_with_trafilatura(html: str, max_chars: int) -> str:
    """Extract article body text using trafilatura (Mozilla Readability algorithm).
    Falls back to BeautifulSoup if trafilatura yields nothing useful."""
    try:
        import trafilatura
        text = trafilatura.extract(html, include_comments=False, include_tables=False)
        if text and len(text.strip()) > 100:
            return text.strip()[:max_chars]
    except Exception:
        pass
    # BeautifulSoup fallback
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "nav", "header", "footer", "aside",
                      "form", "figure", "figcaption", "iframe", "noscript"]):
        tag.decompose()
    body = soup.find("article") or soup.find("main") or soup.body
    if not body:
        return ""
    return " ".join(body.get_text(separator=" ").split())[:max_chars]


def _fetch_with_cloudscraper(url: str, max_chars: int) -> str:
    """Synchronous fallback fetch for Cloudflare/bot-protected sites."""
    try:
        import cloudscraper
        scraper = cloudscraper.create_scraper()
        resp = scraper.get(url, timeout=15)
        if resp.status_code == 200:
            return _extract_with_trafilatura(resp.text, max_chars)
    except Exception:
        pass
    return ""


async def fetch_article_text(url: str, max_chars: int = 4000) -> str:
    if not url:
        return ""
    try:
        async with httpx.AsyncClient(timeout=12, follow_redirects=True) as client:
            resp = await client.get(url, headers=_BROWSER_HEADERS)
        if resp.status_code == 200:
            text = await _in_thread(_extract_with_trafilatura, resp.text, max_chars)
            if text:
                return text
        if resp.status_code in (403, 429, 503):
            # Try cloudscraper for Cloudflare/bot-protected sites
            text = await _in_thread(_fetch_with_cloudscraper, url, max_chars)
            if text:
                return text
    except Exception as exc:
        log.debug("Article text fetch failed %s: %s", url, exc)
    return ""


def _extract_og_image(html: str) -> str | None:
    """Synchronous og:image extraction — run in thread pool."""
    soup = BeautifulSoup(html, "lxml")
    tag = (soup.find("meta", property="og:image") or
           soup.find("meta", attrs={"name": "twitter:image"}))
    return tag.get("content") if tag else None


async def fetch_og_image(url: str) -> str | None:
    """Try to find og:image or twitter:image meta tag at a URL."""
    if not url:
        return None
    try:
        async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
            resp = await client.get(url, headers=_BROWSER_HEADERS)
            if resp.status_code != 200:
                return None
        return await _in_thread(_extract_og_image, resp.text)
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Phase 1 — Pre-scrape article content
# ---------------------------------------------------------------------------

async def pre_scrape_articles(
    articles: list[tuple[str, dict]],
    limit: int = PRE_SCRAPE_LIMIT,
) -> dict[str, str]:
    """Scrape article bodies for the most recent articles before AI processing.

    Returns a dict mapping url -> extracted text (empty string if scraping failed).
    This runs before any AI call so the filter+group step has real article content
    instead of RSS teasers.
    """
    candidates = articles[:limit]
    urls = [e.get("link", "") for _, e in candidates]
    texts = await asyncio.gather(*[fetch_article_text(url) for url in urls])
    result: dict[str, str] = {}
    successes = 0
    for url, text in zip(urls, texts):
        if url:
            result[url] = text
            if text:
                successes += 1
    log.info("Pre-scrape: %d/%d articles extracted successfully", successes, len(candidates))
    return result


# ---------------------------------------------------------------------------
# Phase 2 — Filter by interests AND group by topic (single fast-model call)
# ---------------------------------------------------------------------------

async def filter_and_group(
    articles: list[tuple[str, dict]],
    scraped_texts: dict[str, str],
    interests: list[str],
    n_groups: int = 6,
) -> list[dict]:
    """Single fast-model call: filter relevant articles AND cluster into topic groups.

    Uses pre-scraped article body text for rich context — far better signal than
    RSS teasers. Returns groups with at least 2 articles each.
    """
    if not articles:
        return []
    if not (OPENROUTER_API_KEY or ANTHROPIC_API_KEY):
        # Fallback: pair consecutive articles without AI
        groups = []
        for i in range(0, min(len(articles), n_groups * 2), 2):
            pair = articles[i:i+2]
            if len(pair) >= 2:
                groups.append({"topic": pair[0][1]["title"], "angle": "", "tags": interests[:1], "articles": pair})
        return groups[:n_groups]

    interests_text = "; ".join(interests)

    def _snippet(i: int, src: str, e: dict) -> str:
        url = e.get("link", "")
        text = scraped_texts.get(url, "")
        if not text:
            text = strip_html(e.get("summary", ""))
        excerpt = " ".join(text.split())[:250]
        line = f"{i}: [{src}] {e.get('title', '').strip()}"
        if excerpt:
            line += f"\n   {excerpt}"
        return line

    lines = [_snippet(i, src, e) for i, (src, e) in enumerate(articles)]

    prompt = f"""You are curating a personalised news digest.

User interests: {interests_text}

Below are {len(articles)} recent news articles. In one step:
1. Identify which articles match the user's interests (mentally score 0–10; keep only ≥6).
2. Cluster the relevant articles into up to {n_groups} groups where each group covers the same story or a closely related theme.

RULES:
- Every group must contain AT LEAST 2 articles. Never create single-article groups.
- Groups should have 2–5 articles covering the same story or closely related theme.
- Only create groups that clearly relate to the user's interests.
- Every group must have at least one tag using exact wording from the user's interest list.
- Discard articles that don't match the user's interests.

Articles (index: [source] title + body excerpt):
{chr(10).join(lines)}

Return JSON only:
{{"groups": [{{"topic": "concise headline (≤10 words)", "angle": "why this matters to this reader", "tags": ["exact interest label"], "indices": [0, 3, 7]}}]}}"""

    try:
        raw = await call_ai_fast(prompt, max_tokens=1500)
        data = parse_json_safely(raw)
        if isinstance(data, dict) and "groups" in data:
            groups = []
            for g in data["groups"]:
                group_articles = [articles[i] for i in g.get("indices", []) if i < len(articles)]
                if len(group_articles) >= 2:
                    tags = g.get("tags") or []
                    if not tags:
                        topic_lower = g.get("topic", "").lower()
                        tags = [interest for interest in interests
                                if any(w in topic_lower for w in interest.lower().split())]
                    if not tags:
                        tags = interests[:1]
                    groups.append({
                        "topic": g.get("topic", group_articles[0][1]["title"]),
                        "angle": g.get("angle", ""),
                        "tags": tags,
                        "articles": group_articles,
                    })
            log.info("filter_and_group: %d articles → %d topic groups", len(articles), len(groups))
            return groups[:n_groups]
    except Exception as exc:
        log.warning("filter_and_group failed: %s", exc)
    # Fallback: pair consecutive articles
    groups = []
    for i in range(0, min(len(articles), n_groups * 2), 2):
        pair = articles[i:i+2]
        if len(pair) >= 2:
            groups.append({"topic": pair[0][1]["title"], "angle": "", "tags": interests[:1], "articles": pair})
    return groups[:n_groups]


def _apply_interest_cap(groups: list[dict], interests: list[str], max_per_interest: int = 2) -> list[dict]:
    """Limit to max_per_interest groups per interest. Merge overflow into the last kept group."""
    from collections import defaultdict

    kept: list[dict] = []
    interest_count: dict[str, int] = defaultdict(int)
    overflow_by_interest: dict[str, list[dict]] = defaultdict(list)

    for group in groups:
        primary = (group.get("tags") or [""])[0]
        if not primary or interest_count[primary] < max_per_interest:
            kept.append(group)
            if primary:
                interest_count[primary] += 1
        else:
            overflow_by_interest[primary].append(group)

    # Merge overflow articles into the last kept group for that interest
    for interest, overflow_groups in overflow_by_interest.items():
        for g in reversed(kept):
            if (g.get("tags") or [""])[0] == interest:
                for og in overflow_groups:
                    g["articles"].extend(og["articles"])
                g["topic"] = f"{interest.title()} — daily digest"
                g["angle"] = (g.get("angle") or "") + " Covers multiple stories from today."
                break

    log.info("Interest cap: %d groups -> %d (max %d per interest)", len(groups), len(kept), max_per_interest)
    return kept


# ---------------------------------------------------------------------------
# Phase 3 — Synthesise topic group into one post (main model)
# ---------------------------------------------------------------------------

async def synthesize_topic_group(
    group: dict,
    interests: list[str] | None = None,
    scraped_texts: dict[str, str] | None = None,
) -> dict | None:
    """Write a digest post for one topic group.

    If scraped_texts is provided (pre-scraped dict mapping url->text), uses that
    directly and skips re-fetching. Returns None if source content is too thin.
    """
    topic    = group["topic"]
    angle    = group["angle"]
    tags     = group.get("tags", [])
    articles = group["articles"]

    # Use pre-scraped texts if available, otherwise fetch now
    if scraped_texts is not None:
        texts = [scraped_texts.get(e.get("link", ""), "") for _, e in articles]
    else:
        texts = await asyncio.gather(*[fetch_article_text(e.get("link", "")) for _, e in articles])

    # Find best thumbnail
    thumbnail = next((e.get("media_thumbnail") for _, e in articles if e.get("media_thumbnail")), None)
    if not thumbnail:
        # Try og:image from the first article that has a link
        first_link = next((e.get("link", "") for _, e in articles if e.get("link")), "")
        thumbnail = await fetch_og_image(first_link)

    # Build source context — prefer scraped article text, fall back to RSS summary
    source_blocks = []
    scrape_successes = 0
    for (src, e), text in zip(articles, texts):
        if text:
            body = text[:2000]
            scrape_successes += 1
        else:
            # Strip HTML from RSS summary and use it as fallback
            raw_summary = e.get("summary", "")
            body = strip_html(raw_summary)[:600]
        if body:
            source_blocks.append(f"Source: {src}\nTitle: {e['title']}\n{body}")

    if scrape_successes == 0 and source_blocks:
        log.debug("Topic '%s': all %d sources used RSS summaries (scraping blocked)", topic, len(articles))
    elif scrape_successes < len(articles):
        log.debug("Topic '%s': %d/%d sources scraped, rest used RSS summaries", topic, scrape_successes, len(articles))

    # Skip post if no content at all, or if total content is too thin to synthesise reliably
    if not source_blocks:
        log.info("Skipping topic '%s' — no source content available", topic)
        return None
    total_content_chars = sum(len(b) for b in source_blocks)
    if total_content_chars < 300:
        log.info("Skipping topic '%s' — source content too thin (%d chars)", topic, total_content_chars)
        return None

    sources_text = "\n\n---\n\n".join(source_blocks)
    source_names = list(dict.fromkeys(src for src, _ in articles))
    # Collect all valid source URLs for the post
    all_source_urls = [e.get("link") for _, e in articles if e.get("link")]
    source_url = all_source_urls[0] if all_source_urls else None

    reader_context = ""
    if interests:
        reader_context = f"\nThe reader's interests: {'; '.join(interests)}\nFrame and emphasise the parts of this story most relevant to those interests.\n"

    depth_note = (
        "Note: source material is from article excerpts — use all details available."
        if scrape_successes > 0 else
        "Note: source material is from brief RSS summaries — write what you can confidently say; do not invent specifics not present in the text."
    )

    prompt = f"""You are writing a digest post for a personal news app.

Topic: {topic}
Why it matters to this reader: {angle}{reader_context}
{depth_note}

Source material:
{sources_text}

Return JSON only with two fields:
{{
  "title": "A specific, informative headline (max 12 words). Include a key detail — a name, number, country, or concrete outcome. No clickbait. No vague phrases like 'everything you need to know'.",
  "content": "A digest of 150–220 words (6–9 sentences) that:\\n- Opens with the most important or surprising fact — not 'According to' or 'This article'\\n- Synthesises details from ALL provided sources, not just one\\n- Uses specific numbers, names, and details where available\\n- Frames the story from the angle most relevant to the reader's interests\\n- Ends with one sentence of forward-looking context\\n- Reads like a well-informed friend explaining the story, not a press release\\n- Flowing prose. No bullet points. No subheadings."
}}"""

    try:
        raw = (await call_ai(prompt, max_tokens=700)).strip()
        parsed = parse_json_safely(raw)
        if isinstance(parsed, dict) and parsed.get("title") and parsed.get("content"):
            synth_title   = parsed["title"].strip()
            synth_content = parsed["content"].strip()
        else:
            # Fallback: treat entire response as content, keep topic as title
            synth_title   = topic
            synth_content = raw
    except Exception as exc:
        log.warning("Synthesis failed for '%s': %s", topic, exc)
        synth_title   = topic
        synth_content = " ".join(
            strip_html(e.get("summary", ""))[:300] for _, e in articles if e.get("summary")
        ).strip()
        if not synth_content:
            return None

    return {
        "title": synth_title,
        "content": synth_content,
        "thumbnail_url": thumbnail,
        "source_url": source_url,
        "source_urls": all_source_urls,
        "source_name": " · ".join(source_names[:3]),
        "tags": tags,
    }


# ---------------------------------------------------------------------------
# Videos — select best N with fast model
# ---------------------------------------------------------------------------

async def select_videos(
    all_videos: list[tuple[str, dict]],
    interests: list[str],
    n: int = 2,
) -> list[tuple[str, dict]]:
    if not all_videos or not (OPENROUTER_API_KEY or ANTHROPIC_API_KEY):
        return all_videos[:n]

    interests_text = "; ".join(interests)
    lines = [f"{i}: [{src}] {e['title']}" for i, (src, e) in enumerate(all_videos[:40])]

    prompt = f"""You are picking YouTube videos for a personal feed.

User interests: {interests_text}

Select exactly {n} videos that best match the user's specific interests above.
Prefer videos that directly relate to an interest over generic "fascinating" picks.
Only fall back to broadly interesting videos if nothing matches an interest.

Videos:
{chr(10).join(lines)}

Return JSON only: {{"selected": [list of {n} integer indices]}}"""

    try:
        text = await call_ai_fast(prompt, max_tokens=100)
        data = parse_json_safely(text)
        if isinstance(data, dict) and "selected" in data:
            indices = [i for i in data["selected"] if i < len(all_videos)]
            return [all_videos[i] for i in indices[:n]]
    except Exception as exc:
        log.warning("Video selection failed: %s", exc)
    return all_videos[:n]


# ---------------------------------------------------------------------------
# Facts — generated by main model
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Main generation function
# ---------------------------------------------------------------------------

async def generate_feed_for_user(
    session: AsyncSession,
    user: User,
    feed_date: date,
    all_news: list[tuple[str, dict]],
    all_videos: list[tuple[str, dict]],
    interests: list[str],
):
    await session.execute(
        delete(FeedItem).where(FeedItem.user_id == user.id, FeedItem.feed_date == feed_date)
    )

    if not interests:
        interests = ["general news", "science", "technology"]

    n_videos = 2
    n_stocks = 2
    n_news   = max(3, ITEMS_PER_USER - n_videos - n_stocks)

    # --- Phase 0: filter to recent news only ---
    recent_news = [(s, e) for s, e in all_news if is_recent(e)]
    if not recent_news:
        log.warning("No recent news for user %s, using all items", user.name)
        recent_news = all_news
    log.info("User %s: %d recent news items (from %d total)", user.name, len(recent_news), len(all_news))

    # --- Phases 1-3 and video selection run concurrently ---
    async def _build_news_items():
        # Phase 1: Pre-scrape article content before any AI call
        _set_phase("scraping")
        candidates = recent_news[:PRE_SCRAPE_LIMIT]
        scraped_texts = await pre_scrape_articles(candidates)

        # Phase 2: Single AI call to filter relevant articles AND cluster into groups
        _set_phase("filtering")
        # Request more groups than needed since interest cap may reduce them
        topic_groups = await filter_and_group(candidates, scraped_texts, interests, n_groups=n_news * 3)
        # Enforce max 2 posts per interest (merge overflow into 2nd group)
        topic_groups = _apply_interest_cap(topic_groups, interests, max_per_interest=2)
        topic_groups = topic_groups[:n_news]

        # Phase 3: Synthesise groups — text already fetched, no re-scraping needed
        _set_phase("synthesizing")
        results = await asyncio.gather(*[
            synthesize_topic_group(g, interests, scraped_texts=scraped_texts)
            for g in topic_groups
        ])
        return [r for r in results if r is not None]

    synthesized_news, selected_vids = await asyncio.gather(
        _build_news_items(),
        select_videos(all_videos, interests, n=n_videos),
    )

    items_to_save: list[FeedItem] = []
    pos = 0

    # --- NEWS ---
    for synth in synthesized_news:
        raw_tags = synth.get("tags") or []
        src_urls = synth.get("source_urls") or []
        items_to_save.append(FeedItem(
            user_id=user.id, feed_date=feed_date, item_type="news",
            title=synth["title"][:499],
            content=synth.get("content") or None,
            source_url=synth.get("source_url"),
            source_urls=json.dumps(src_urls) if src_urls else None,
            thumbnail_url=synth.get("thumbnail_url"),
            source_name=synth.get("source_name"),
            tags=", ".join(raw_tags) if raw_tags else None,
            position=pos,
            share_token=make_share_token(user.id, feed_date, pos),
        ))
        pos += 1

    # --- VIDEOS ---
    for src_name, entry in selected_vids:
        link = entry.get("link", "")
        yt_embed = None
        if "youtube.com/watch?v=" in link:
            yt_embed = "https://www.youtube.com/embed/" + link.split("v=")[1].split("&")[0]
        elif "youtu.be/" in link:
            yt_embed = "https://www.youtube.com/embed/" + link.split("youtu.be/")[1].split("?")[0]
        # Tag: first interest whose keywords appear in the title, else first interest
        vid_title_lower = entry["title"].lower()
        vid_tag = next(
            (i for i in interests if any(w in vid_title_lower for w in i.lower().split())),
            interests[0] if interests else None,
        )
        items_to_save.append(FeedItem(
            user_id=user.id, feed_date=feed_date, item_type="video",
            title=entry["title"][:499],
            summary=entry.get("summary", "")[:2000],
            source_url=link, media_url=yt_embed,
            source_name=src_name, position=pos,
            tags=vid_tag,
            share_token=make_share_token(user.id, feed_date, pos),
        ))
        pos += 1

    # --- STOCKS ---
    tickers = await extract_tickers_from_interests(interests)
    if not tickers:
        log.info("No relevant tickers for user %s — skipping stocks", user.name)
    stock_data = await asyncio.get_event_loop().run_in_executor(None, fetch_stock_data, tickers[:n_stocks]) if tickers else []
    for stock in stock_data:
        direction = "up" if stock["change"] >= 0 else "down"
        items_to_save.append(FeedItem(
            user_id=user.id, feed_date=feed_date, item_type="stock",
            title=f"{stock['ticker']} — ${stock['price']:.2f}",
            summary=f"{stock['ticker']} is {direction} {abs(stock['change_pct']):.2f}% today.",
            ticker=stock["ticker"], stock_price=stock["price"],
            stock_change=stock["change"], stock_change_pct=stock["change_pct"],
            source_url=f"https://finance.yahoo.com/quote/{stock['ticker']}",
            source_name="Yahoo Finance", position=pos,
            share_token=make_share_token(user.id, feed_date, pos),
        ))
        pos += 1

    # Interleave: news, fact, video, stock, news, ...
    type_order = {"news": 0, "fact": 1, "video": 2, "stock": 3}
    items_to_save.sort(key=lambda x: (type_order.get(x.item_type, 9), x.position))
    for i, item in enumerate(items_to_save):
        item.position = i

    session.add_all(items_to_save)
    await session.commit()
    log.info("Generated %d items for user %s on %s", len(items_to_save), user.name, feed_date)


async def generate_all_feeds(session: AsyncSession, feed_date: Optional[date] = None):
    if feed_date is None:
        feed_date = date.today()

    _gen_status["started_at"] = _time.monotonic()
    _set_phase("fetching")
    log.info("Starting feed generation for %s", feed_date)
    all_news, all_videos = await asyncio.gather(fetch_all_news(), fetch_all_videos())
    log.info("Fetched %d news items, %d videos", len(all_news), len(all_videos))

    result = await session.execute(select(User))
    users = result.scalars().all()

    for user in users:
        result2 = await session.execute(select(Interest).where(Interest.user_id == user.id))
        interests = [i.description for i in result2.scalars().all()]
        try:
            await generate_feed_for_user(session, user, feed_date, all_news, all_videos, interests)
        except Exception as exc:
            log.error("Failed to generate feed for %s: %s", user.name, exc)

    _set_phase("saving")
    log.info("Feed generation complete for %s", feed_date)
    _clear_phase()
