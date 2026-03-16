"""
Feed generation engine. Called daily at 5am to populate each user's feed.
Content types: news, fact, video, stock, image
"""
import os
import hashlib
import secrets
import asyncio
import logging
from datetime import date, datetime, timezone
from typing import Optional

import json

import feedparser
import httpx
import yfinance as yf
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from models import User, Interest, FeedItem

log = logging.getLogger("feed_generator")

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
# Model to use via OpenRouter (default: Claude Opus via OpenRouter)
OPENROUTER_MODEL = os.environ.get("OPENROUTER_MODEL", "anthropic/claude-opus-4")

# ---------------------------------------------------------------------------
# RSS sources (public, no key required)
# ---------------------------------------------------------------------------
NEWS_FEEDS = [
    # International
    ("BBC News", "http://feeds.bbci.co.uk/news/rss.xml"),
    ("Reuters", "https://feeds.reuters.com/reuters/topNews"),
    ("The Guardian", "https://www.theguardian.com/world/rss"),
    ("Hacker News", "https://news.ycombinator.com/rss"),
    ("NASA Breaking News", "https://www.nasa.gov/rss/dyn/breaking_news.rss"),
    ("Science Daily", "https://www.sciencedaily.com/rss/all.xml"),
    ("TechCrunch", "https://techcrunch.com/feed/"),
    ("Ars Technica", "http://feeds.arstechnica.com/arstechnica/index"),
    ("NPR News", "https://feeds.npr.org/1001/rss.xml"),
    # Austria
    ("ORF News", "https://rss.orf.at/news.xml"),
    ("ORF Österreich", "https://rss.orf.at/oesterreich.xml"),
    ("ORF Wissenschaft", "https://rss.orf.at/science.xml"),
    ("Der Standard", "https://www.derstandard.at/rss"),
    ("Die Presse", "https://diepresse.com/rss"),
    ("Kurier", "https://kurier.at/xml/rssfeed"),
    ("Heute", "https://www.heute.at/feed/"),
    ("Vienna Online", "https://www.vienna.at/feed"),
]

YOUTUBE_FEEDS = [
    ("Kurzgesagt", "https://www.youtube.com/feeds/videos.xml?channel_id=UCsXVk37bltHxD1rDPwtNM8Q"),
    ("Veritasium", "https://www.youtube.com/feeds/videos.xml?channel_id=UCHnyfMqiRRG1u-2MsSQLbXA"),
    ("Mark Rober", "https://www.youtube.com/feeds/videos.xml?channel_id=UCY1kMZp36IQSyNx_9h4mpCg"),
    ("Smarter Every Day", "https://www.youtube.com/feeds/videos.xml?channel_id=UC6107grRI4m0o2-emgoDnAA"),
    ("3Blue1Brown", "https://www.youtube.com/feeds/videos.xml?channel_id=UCYO_jab_esuFRV4b17AJtAg"),
    ("CGP Grey", "https://www.youtube.com/feeds/videos.xml?channel_id=UC2C_jShtL725hvbm1arSV9w"),
]

DEFAULT_STOCKS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "SPY"]

ITEMS_PER_USER = int(os.environ.get("ITEMS_PER_USER", "12"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_share_token(user_id: int, feed_date: date, position: int) -> str:
    raw = f"{user_id}:{feed_date.isoformat()}:{position}:{secrets.token_hex(8)}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


async def fetch_feed(url: str) -> list[dict]:
    """Fetch and parse a single RSS feed, return list of entry dicts."""
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
    """Fetch all RSS news feeds concurrently, return (source_name, entry) pairs."""
    tasks = [fetch_feed(url) for _, url in NEWS_FEEDS]
    results = await asyncio.gather(*tasks)
    pairs = []
    for (name, _), entries in zip(NEWS_FEEDS, results):
        for entry in entries:
            pairs.append((name, entry))
    return pairs


async def fetch_all_videos() -> list[tuple[str, dict]]:
    tasks = [fetch_feed(url) for _, url in YOUTUBE_FEEDS]
    results = await asyncio.gather(*tasks)
    pairs = []
    for (name, _), entries in zip(YOUTUBE_FEEDS, results):
        for entry in entries:
            # YouTube RSS has yt_videoid
            pairs.append((name, entry))
    return pairs


def fetch_stock_data(tickers: list[str]) -> list[dict]:
    """Fetch current stock data using yfinance."""
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
                "name": getattr(info, "quote_type", ticker),
            })
        except Exception as exc:
            log.warning("Stock fetch failed for %s: %s", ticker, exc)
    return results


# ---------------------------------------------------------------------------
# AI-powered curation — Anthropic or OpenRouter
# ---------------------------------------------------------------------------

def _build_curation_prompt(
    interests: list[str],
    news_items: list[tuple[str, dict]],
    video_items: list[tuple[str, dict]],
    n_news: int,
    n_videos: int,
) -> str:
    news_lines = [f"{i}: [{src}] {e['title']}" for i, (src, e) in enumerate(news_items[:80])]
    video_lines = [f"{i}: [{src}] {e['title']}" for i, (src, e) in enumerate(video_items[:40])]
    interests_text = "\n".join(f"- {i}" for i in interests)
    n_facts = 1 if n_news <= 4 else 2
    return f"""You are curating a personal news feed. The user has the following interests:
{interests_text}

Available news articles (index: title):
{chr(10).join(news_lines)}

Available videos (index: title):
{chr(10).join(video_lines)}

Your tasks:
1. Select the {n_news} most relevant and interesting news article indices for this user. Prefer variety — avoid duplicate topics.
2. Select the {n_videos} most relevant video indices.
3. Write {n_facts} fascinating, brief facts (2-3 sentences each) that this user would find genuinely interesting, based on their interests. Make them surprising and educational — things worth sharing.

Respond with valid JSON only, in this exact format:
{{
  "selected_news": [list of integer indices],
  "selected_videos": [list of integer indices],
  "facts": [
    {{"title": "short catchy title", "content": "the interesting fact in 2-3 sentences"}}
  ]
}}"""


def _parse_curation_json(text: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    return json.loads(text.strip())


async def call_ai(prompt: str, max_tokens: int = 1024) -> str:
    """Call the configured AI and return the text response. Raises if no key set."""
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
        client = AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
        resp = await client.messages.create(
            model="claude-opus-4-6", max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text
    else:
        raise ValueError("No AI API key configured")


# ---------------------------------------------------------------------------
# Article fetching & summarisation
# ---------------------------------------------------------------------------

async def fetch_article_text(url: str, max_chars: int = 4000) -> str:
    """Download a news article and extract its plain text."""
    if not url:
        return ""
    try:
        async with httpx.AsyncClient(timeout=12, follow_redirects=True) as client:
            headers = {"User-Agent": "Mozilla/5.0 (compatible; BoonScroll/1.0; +http://localhost)"}
            resp = await client.get(url, headers=headers)
            if resp.status_code != 200:
                return ""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "lxml")
        for tag in soup(["script", "style", "nav", "header", "footer", "aside",
                          "form", "figure", "figcaption", "iframe", "noscript"]):
            tag.decompose()
        # Prefer <article> or <main>, fall back to <body>
        body = soup.find("article") or soup.find("main") or soup.body
        if not body:
            return ""
        text = " ".join(body.get_text(separator=" ").split())
        return text[:max_chars]
    except Exception as exc:
        log.debug("Article fetch failed for %s: %s", url, exc)
        return ""


async def summarize_article(title: str, source: str, article_text: str, rss_summary: str) -> str:
    """
    Use AI to write a 3–4 sentence digest of the article.
    Falls back to the RSS summary if AI is unavailable or article text is empty.
    """
    text = article_text or rss_summary or ""
    if not text or (not OPENROUTER_API_KEY and not ANTHROPIC_API_KEY):
        return ""

    prompt = f"""Write a clear, engaging 3–4 sentence summary of this news article for a personal news digest.
Be factual and informative. Write in plain prose — no bullet points, no "The article says".
Keep it under 80 words.

Source: {source}
Title: {title}
Article text: {text[:3000]}

Summary:"""

    try:
        return (await call_ai(prompt, max_tokens=200)).strip()
    except Exception as exc:
        log.warning("Summarisation failed for '%s': %s", title, exc)
        return ""


async def fetch_and_summarize_news(
    selected: list[tuple[str, dict]],
) -> list[str]:
    """
    Concurrently fetch article text and summarise each selected news item.
    Returns a list of summary strings (empty string if failed).
    """
    async def _one(src_name: str, entry: dict) -> str:
        article_text = await fetch_article_text(entry.get("link", ""))
        return await summarize_article(
            title=entry.get("title", ""),
            source=src_name,
            article_text=article_text,
            rss_summary=entry.get("summary", ""),
        )

    return list(await asyncio.gather(*[_one(s, e) for s, e in selected]))


async def curate_with_claude(
    interests: list[str],
    news_items: list[tuple[str, dict]],
    video_items: list[tuple[str, dict]],
    n_news: int = 5,
    n_videos: int = 2,
) -> dict:
    """
    Curate feed using an AI model. Prefers OpenRouter if OPENROUTER_API_KEY is
    set, otherwise falls back to Anthropic. If neither key is present, returns
    a simple first-N fallback with no facts.
    """
    fallback = {
        "selected_news": list(range(min(n_news, len(news_items)))),
        "selected_videos": list(range(min(n_videos, len(video_items)))),
        "facts": [],
    }

    if not OPENROUTER_API_KEY and not ANTHROPIC_API_KEY:
        log.warning("No AI API key set (ANTHROPIC_API_KEY or OPENROUTER_API_KEY). Skipping curation.")
        return fallback

    prompt = _build_curation_prompt(interests, news_items, video_items, n_news, n_videos)

    try:
        log.info("Curating via %s", "OpenRouter" if OPENROUTER_API_KEY else "Anthropic")
        text = await call_ai(prompt)
        return _parse_curation_json(text)
    except Exception as exc:
        log.error("AI curation failed: %s", exc)
        return fallback


# ---------------------------------------------------------------------------
# Stock ticker extraction from interests
# ---------------------------------------------------------------------------

async def extract_tickers_from_interests(interests: list[str]) -> list[str]:
    """Try to find stock tickers mentioned in interests, fall back to defaults."""
    mentioned = []
    interest_text = " ".join(interests).upper()
    for ticker in ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "NFLX", "AMD", "INTC"]:
        if ticker in interest_text or ticker.lower() in " ".join(interests).lower():
            mentioned.append(ticker)

    # Always include SPY and a few blue chips for context
    base = ["SPY", "AAPL", "MSFT"]
    combined = list(dict.fromkeys(mentioned + base))
    return combined[:5]


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
    """Generate and store a full feed for one user for a given date."""
    # Delete any existing feed for this user+date
    await session.execute(
        delete(FeedItem).where(
            FeedItem.user_id == user.id,
            FeedItem.feed_date == feed_date,
        )
    )

    if not interests:
        interests = ["general news", "science", "technology"]

    # Determine allocation
    n_news = max(3, ITEMS_PER_USER - 4)
    n_videos = 2
    n_stocks = 2
    n_facts = ITEMS_PER_USER - n_news - n_videos - n_stocks

    # Curate with Claude
    curation = await curate_with_claude(
        interests, all_news, all_videos, n_news=n_news, n_videos=n_videos
    )

    items_to_save: list[FeedItem] = []
    pos = 0

    # --- NEWS — fetch full articles and summarise concurrently ---
    selected_news = [
        all_news[idx]
        for idx in curation.get("selected_news", [])[:n_news]
        if idx < len(all_news)
    ]
    log.info("Fetching and summarising %d articles for user %s", len(selected_news), user.name)
    summaries = await fetch_and_summarize_news(selected_news)

    for (src_name, entry), ai_summary in zip(selected_news, summaries):
        item = FeedItem(
            user_id=user.id,
            feed_date=feed_date,
            item_type="news",
            title=entry["title"][:499],
            # AI digest goes in content; raw RSS excerpt kept in summary as fallback
            content=ai_summary or None,
            summary=entry.get("summary", "")[:2000] or None,
            source_url=entry.get("link"),
            thumbnail_url=entry.get("media_thumbnail"),
            source_name=src_name,
            position=pos,
            share_token=make_share_token(user.id, feed_date, pos),
        )
        items_to_save.append(item)
        pos += 1

    # --- FACTS ---
    for fact in curation.get("facts", [])[:n_facts]:
        item = FeedItem(
            user_id=user.id,
            feed_date=feed_date,
            item_type="fact",
            title=fact.get("title", "Did you know?")[:499],
            content=fact.get("content", ""),
            source_name="BoonScroll AI",
            position=pos,
            share_token=make_share_token(user.id, feed_date, pos),
        )
        items_to_save.append(item)
        pos += 1

    # --- VIDEOS ---
    for idx in curation.get("selected_videos", [])[:n_videos]:
        if idx >= len(all_videos):
            continue
        src_name, entry = all_videos[idx]
        link = entry.get("link", "")
        # Convert YouTube watch link to embed
        yt_embed = None
        if "youtube.com/watch?v=" in link:
            vid_id = link.split("v=")[1].split("&")[0]
            yt_embed = f"https://www.youtube.com/embed/{vid_id}"
        elif "youtu.be/" in link:
            vid_id = link.split("youtu.be/")[1].split("?")[0]
            yt_embed = f"https://www.youtube.com/embed/{vid_id}"

        item = FeedItem(
            user_id=user.id,
            feed_date=feed_date,
            item_type="video",
            title=entry["title"][:499],
            summary=entry.get("summary", "")[:2000],
            source_url=link,
            media_url=yt_embed,
            source_name=src_name,
            position=pos,
            share_token=make_share_token(user.id, feed_date, pos),
        )
        items_to_save.append(item)
        pos += 1

    # --- STOCKS ---
    tickers = await extract_tickers_from_interests(interests)
    stock_data = await asyncio.get_event_loop().run_in_executor(
        None, fetch_stock_data, tickers[:n_stocks]
    )
    for stock in stock_data[:n_stocks]:
        direction = "up" if stock["change"] >= 0 else "down"
        item = FeedItem(
            user_id=user.id,
            feed_date=feed_date,
            item_type="stock",
            title=f"{stock['ticker']} — ${stock['price']:.2f}",
            summary=f"{stock['ticker']} is {direction} {abs(stock['change_pct']):.2f}% today.",
            ticker=stock["ticker"],
            stock_price=stock["price"],
            stock_change=stock["change"],
            stock_change_pct=stock["change_pct"],
            source_url=f"https://finance.yahoo.com/quote/{stock['ticker']}",
            source_name="Yahoo Finance",
            position=pos,
            share_token=make_share_token(user.id, feed_date, pos),
        )
        items_to_save.append(item)
        pos += 1

    # Shuffle items to mix types (interleave)
    # Order: news, fact, video, stock, news, fact, ...
    type_order = {"news": 0, "fact": 1, "video": 2, "stock": 3}
    items_to_save.sort(key=lambda x: (type_order.get(x.item_type, 9), x.position))
    for i, item in enumerate(items_to_save):
        item.position = i

    session.add_all(items_to_save)
    await session.commit()
    log.info("Generated %d items for user %s on %s", len(items_to_save), user.name, feed_date)


async def generate_all_feeds(session: AsyncSession, feed_date: Optional[date] = None):
    """Generate feeds for all users."""
    if feed_date is None:
        feed_date = date.today()

    log.info("Starting feed generation for %s", feed_date)

    # Fetch all content sources concurrently
    news_task = fetch_all_news()
    videos_task = fetch_all_videos()
    all_news, all_videos = await asyncio.gather(news_task, videos_task)
    log.info("Fetched %d news items, %d videos", len(all_news), len(all_videos))

    # Load all users with their interests
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
