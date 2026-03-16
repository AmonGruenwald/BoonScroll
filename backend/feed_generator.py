"""
Feed generation engine — multi-phase news pipeline.

Phase 0 : Filter RSS to items from the last 36 hours.
Phase 1 : Fast model scores articles by relevance to user interests.
Phase 2 : Fast model clusters relevant articles into N topic groups.
Phase 3 : Main model synthesises each group into one long digest post (concurrent).
Phase 4 : Extract the best image for each group (og:image scrape if RSS has none).

Videos  : Fast model picks the best N from YouTube feeds.
Facts   : Main model generates interest-tailored facts.
Stocks  : yfinance.
"""
import os
import hashlib
import secrets
import asyncio
import logging
import time as _time
import json
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

async def fetch_feed(url: str) -> list[dict]:
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            resp = await client.get(url, headers={"User-Agent": "BoonScroll/1.0"})
            parsed = feedparser.parse(resp.text)
            entries = []
            for e in parsed.entries[:30]:
                entries.append({
                    "title": e.get("title", ""),
                    "summary": e.get("summary", e.get("description", "")),
                    "link": e.get("link", ""),
                    "published": e.get("published", ""),
                    "published_parsed": e.get("published_parsed"),  # time.struct_time
                    "media_thumbnail": (
                        e.get("media_thumbnail", [{}])[0].get("url")
                        if e.get("media_thumbnail") else None
                    ),
                })
            return entries
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

async def fetch_article_text(url: str, max_chars: int = 4000) -> str:
    if not url:
        return ""
    try:
        async with httpx.AsyncClient(timeout=12, follow_redirects=True) as client:
            headers = {"User-Agent": "Mozilla/5.0 (compatible; BoonScroll/1.0)"}
            resp = await client.get(url, headers=headers)
            if resp.status_code != 200:
                return ""
        soup = BeautifulSoup(resp.text, "lxml")
        for tag in soup(["script", "style", "nav", "header", "footer", "aside",
                          "form", "figure", "figcaption", "iframe", "noscript"]):
            tag.decompose()
        body = soup.find("article") or soup.find("main") or soup.body
        if not body:
            return ""
        return " ".join(body.get_text(separator=" ").split())[:max_chars]
    except Exception as exc:
        log.debug("Article text fetch failed %s: %s", url, exc)
        return ""


async def fetch_og_image(url: str) -> str | None:
    """Try to find og:image or twitter:image meta tag at a URL."""
    if not url:
        return None
    try:
        async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0 (compatible; BoonScroll/1.0)"})
            if resp.status_code != 200:
                return None
        soup = BeautifulSoup(resp.text, "lxml")
        tag = (soup.find("meta", property="og:image") or
               soup.find("meta", attrs={"name": "twitter:image"}))
        if tag:
            return tag.get("content")
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Phase 1 — Filter by interests (fast model)
# ---------------------------------------------------------------------------

async def filter_by_interests(
    news_items: list[tuple[str, dict]],
    interests: list[str],
    max_relevant: int = 35,
) -> list[tuple[str, dict]]:
    """Use a cheap model to strictly filter articles to only those matching user interests."""
    if not news_items or not (OPENROUTER_API_KEY or ANTHROPIC_API_KEY):
        return news_items[:max_relevant]

    interests_text = "\n".join(f"- {i}" for i in interests)

    def _entry_line(i: int, src: str, e: dict) -> str:
        title = e.get("title", "").strip()
        snippet = BeautifulSoup(e.get("summary", ""), "lxml").get_text(separator=" ")
        snippet = " ".join(snippet.split())[:160]
        return f"{i}: [{src}] {title}" + (f" — {snippet}" if snippet else "")

    lines = [_entry_line(i, src, e) for i, (src, e) in enumerate(news_items[:120])]

    prompt = f"""You are a STRICT relevance filter for a personal news feed.

The user ONLY wants news about their specific interests — nothing else.

USER'S INTERESTS:
{interests_text}

Score each article 0–10 using its title AND the short description:
- 8–10: Directly and clearly about one of the user's interests
- 6–7: Has a meaningful, specific connection to an interest
- 0–5: Off-topic, generic, or only tangentially related → EXCLUDE

STRICT RULES:
- A user interested in "cooking" gets 0 for politics, sports, business, science (unless food science), crime, war, celebrity gossip.
- A user interested in "cooking" gets 8+ for: recipes, restaurants, food trends, ingredients, chefs, kitchen tools, cuisine.
- When in doubt, score 0. Missing a relevant article is better than including an irrelevant one.
- Only return indices that score 6 or higher, ordered best-first.

ARTICLES (index: [source] title — description):
{chr(10).join(lines)}

Return JSON only: {{"relevant": [list of integer indices, best-first]}}"""

    try:
        raw = await call_ai_fast(prompt, max_tokens=400)
        data = parse_json_safely(raw)
        if isinstance(data, dict) and "relevant" in data:
            indices = [i for i in data["relevant"] if isinstance(i, int) and i < len(news_items)]
            result = [news_items[i] for i in indices[:max_relevant]]
            log.info("Phase 1: %d → %d relevant articles", len(news_items), len(result))
            return result
    except Exception as exc:
        log.warning("Interest filtering failed: %s", exc)
    return news_items[:max_relevant]


# ---------------------------------------------------------------------------
# Phase 2 — Group by topic (fast model)
# ---------------------------------------------------------------------------

async def group_by_topic(
    relevant: list[tuple[str, dict]],
    interests: list[str],
    n_groups: int = 6,
) -> list[dict]:
    """Cluster relevant articles into topic groups."""
    if not relevant:
        return []
    if not (OPENROUTER_API_KEY or ANTHROPIC_API_KEY):
        return [{"topic": e["title"], "angle": "", "articles": [(s, e)]} for s, e in relevant[:n_groups]]

    interests_text = "; ".join(interests)
    lines = [f"{i}: [{src}] {e['title']}" for i, (src, e) in enumerate(relevant)]

    prompt = f"""User interests: {interests_text}

Group these news articles into up to {n_groups} topic clusters.
Each cluster = one cohesive news story or theme that relates to the user's interests.
Combine 2–4 articles covering the same story. Single-article clusters are fine for standalone stories.
ONLY create clusters that genuinely relate to the user's interests above.

For each group also list which of the user's interests it covers (use the exact wording from the interest list).

Articles:
{chr(10).join(lines)}

Return JSON only:
{{"groups": [{{"topic": "brief title", "angle": "why interesting for this user", "tags": ["exact interest label"], "indices": [0, 3, 7]}}]}}"""

    try:
        raw = await call_ai_fast(prompt, max_tokens=900)
        data = parse_json_safely(raw)
        if isinstance(data, dict) and "groups" in data:
            groups = []
            for g in data["groups"]:
                articles = [relevant[i] for i in g.get("indices", []) if i < len(relevant)]
                if articles:
                    groups.append({
                        "topic": g.get("topic", articles[0][1]["title"]),
                        "angle": g.get("angle", ""),
                        "tags": g.get("tags", []),
                        "articles": articles,
                    })
            log.info("Phase 2: grouped into %d topics", len(groups))
            return groups[:n_groups]
    except Exception as exc:
        log.warning("Topic grouping failed: %s", exc)
    return [{"topic": e["title"], "angle": "", "tags": [], "articles": [(s, e)]} for s, e in relevant[:n_groups]]


# ---------------------------------------------------------------------------
# Phase 3 — Synthesise topic group into one post (main model)
# ---------------------------------------------------------------------------

async def synthesize_topic_group(group: dict, interests: list[str] | None = None) -> dict:
    """Fetch article texts and write a comprehensive digest for one topic group."""
    topic    = group["topic"]
    angle    = group["angle"]
    tags     = group.get("tags", [])
    articles = group["articles"]

    # Fetch article texts concurrently
    texts = await asyncio.gather(*[fetch_article_text(e.get("link", "")) for _, e in articles])

    # Find best thumbnail
    thumbnail = next((e.get("media_thumbnail") for _, e in articles if e.get("media_thumbnail")), None)
    if not thumbnail:
        # Try og:image from the first article
        thumbnail = await fetch_og_image(articles[0][1].get("link", ""))

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
            body = BeautifulSoup(raw_summary, "lxml").get_text(separator=" ")
            body = " ".join(body.split())[:600]
        if body:
            source_blocks.append(f"Source: {src}\nTitle: {e['title']}\n{body}")

    if scrape_successes == 0 and source_blocks:
        log.debug("Topic '%s': all %d sources used RSS summaries (scraping blocked)", topic, len(articles))
    elif scrape_successes < len(articles):
        log.debug("Topic '%s': %d/%d sources scraped, rest used RSS summaries", topic, scrape_successes, len(articles))

    if not source_blocks:
        src_name, entry = articles[0]
        return {
            "title": topic,
            "content": BeautifulSoup(entry.get("summary", ""), "lxml").get_text(separator=" ")[:500],
            "thumbnail_url": thumbnail,
            "source_url": entry.get("link"),
            "source_name": src_name,
            "tags": tags,
        }

    sources_text = "\n\n---\n\n".join(source_blocks)
    source_names = list(dict.fromkeys(src for src, _ in articles))
    source_url   = articles[0][1].get("link")

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

Write a digest of 150–220 words (6–9 sentences) that:
- Opens with the most important or surprising fact — not "According to" or "This article"
- Synthesises details from ALL provided sources, not just one
- Uses specific numbers, names, and details where available (avoid vague generalities)
- Frames the story from the angle most relevant to the reader's interests
- Ends with one sentence of forward-looking context (what happens next, why it matters long-term)
- Reads like a well-informed friend explaining the story, not a press release

Write in flowing prose. No bullet points. No subheadings."""

    try:
        content = (await call_ai(prompt, max_tokens=600)).strip()
    except Exception as exc:
        log.warning("Synthesis failed for '%s': %s", topic, exc)
        content = articles[0][1].get("summary", "")

    return {
        "title": topic,
        "content": content,
        "thumbnail_url": thumbnail,
        "source_url": source_url,
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

async def generate_facts(interests: list[str], n: int = 1) -> list[dict]:
    if not (OPENROUTER_API_KEY or ANTHROPIC_API_KEY):
        return []

    interests_text = "\n".join(f"- {i}" for i in interests)
    prompt = f"""Write {n} fascinating, surprising fact{"s" if n > 1 else ""} that someone with these interests would love:
{interests_text}

Each fact should be genuinely surprising and educational — something worth sharing.
Write 3–4 sentences per fact.

Return JSON only:
{{"facts": [{{"title": "short catchy title", "content": "the fact in 3–4 sentences"}}]}}"""

    try:
        text = await call_ai(prompt, max_tokens=400)
        data = parse_json_safely(text)
        if isinstance(data, dict) and "facts" in data:
            return data["facts"][:n]
    except Exception as exc:
        log.warning("Fact generation failed: %s", exc)
    return []


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
    n_facts  = 1
    n_news   = max(3, ITEMS_PER_USER - n_videos - n_stocks - n_facts)

    # --- Phase 0: filter to recent news only ---
    recent_news = [(s, e) for s, e in all_news if is_recent(e)]
    if not recent_news:
        log.warning("No recent news for user %s, using all items", user.name)
        recent_news = all_news
    log.info("User %s: %d recent news items (from %d total)", user.name, len(recent_news), len(all_news))

    # --- Phases 1–3 and video selection run concurrently ---
    async def _build_news_items():
        relevant     = await filter_by_interests(recent_news, interests, max_relevant=35)
        topic_groups = await group_by_topic(relevant, interests, n_groups=n_news)
        return await asyncio.gather(*[synthesize_topic_group(g, interests) for g in topic_groups])

    synthesized_news, selected_vids, facts = await asyncio.gather(
        _build_news_items(),
        select_videos(all_videos, interests, n=n_videos),
        generate_facts(interests, n=n_facts),
    )

    items_to_save: list[FeedItem] = []
    pos = 0

    # --- NEWS ---
    for synth in synthesized_news:
        raw_tags = synth.get("tags") or []
        items_to_save.append(FeedItem(
            user_id=user.id, feed_date=feed_date, item_type="news",
            title=synth["title"][:499],
            content=synth.get("content") or None,
            source_url=synth.get("source_url"),
            thumbnail_url=synth.get("thumbnail_url"),
            source_name=synth.get("source_name"),
            tags=", ".join(raw_tags) if raw_tags else None,
            position=pos,
            share_token=make_share_token(user.id, feed_date, pos),
        ))
        pos += 1

    # --- FACTS ---
    for fact in facts:
        items_to_save.append(FeedItem(
            user_id=user.id, feed_date=feed_date, item_type="fact",
            title=fact.get("title", "Did you know?")[:499],
            content=fact.get("content", ""),
            tags=fact.get("tag") or None,
            source_name="BoonScroll",
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
        items_to_save.append(FeedItem(
            user_id=user.id, feed_date=feed_date, item_type="video",
            title=entry["title"][:499],
            summary=entry.get("summary", "")[:2000],
            source_url=link, media_url=yt_embed,
            source_name=src_name, position=pos,
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

    log.info("Feed generation complete for %s", feed_date)
