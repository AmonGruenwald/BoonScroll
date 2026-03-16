import os
import json
import logging
from datetime import date, datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pydantic import BaseModel

from database import get_db, init_db
from models import User, Interest, FeedItem
from feed_generator import generate_all_feeds

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("main")

app = FastAPI(title="BoonScroll", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------
scheduler = AsyncIOScheduler(timezone="UTC")


async def scheduled_feed_job():
    from database import AsyncSessionLocal
    async with AsyncSessionLocal() as session:
        await generate_all_feeds(session)


@app.on_event("startup")
async def startup():
    await init_db()
    scheduler.add_job(
        scheduled_feed_job,
        trigger="cron",
        hour=5,
        minute=0,
        id="daily_feed",
        replace_existing=True,
    )
    scheduler.start()
    log.info("BoonScroll started. Daily feed scheduled at 05:00 UTC.")


@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class UserCreate(BaseModel):
    name: str
    display_name: str
    avatar_color: Optional[str] = "#6366f1"


class UserUpdate(BaseModel):
    display_name: Optional[str] = None
    avatar_color: Optional[str] = None


class InterestCreate(BaseModel):
    description: str


class FeedTriggerRequest(BaseModel):
    feed_date: Optional[str] = None  # ISO date string, defaults to today


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def item_to_dict(item: FeedItem) -> dict:
    return {
        "id": item.id,
        "user_id": item.user_id,
        "feed_date": item.feed_date.isoformat(),
        "item_type": item.item_type,
        "title": item.title,
        "summary": item.summary,
        "content": item.content,
        "source_url": item.source_url,
        "media_url": item.media_url,
        "thumbnail_url": item.thumbnail_url,
        "source_name": item.source_name,
        "ticker": item.ticker,
        "stock_price": item.stock_price,
        "stock_change": item.stock_change,
        "stock_change_pct": item.stock_change_pct,
        "tags": [t.strip() for t in item.tags.split(",")] if item.tags else [],
        "source_urls": json.loads(item.source_urls) if item.source_urls else [],
        "position": item.position,
        "share_token": item.share_token,
        "share_url": f"/shared/{item.share_token}",
        "created_at": item.created_at.isoformat(),
    }


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

@app.get("/api/users")
async def list_users(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).order_by(User.created_at))
    users = result.scalars().all()
    return [
        {
            "id": u.id,
            "name": u.name,
            "display_name": u.display_name,
            "avatar_color": u.avatar_color,
            "created_at": u.created_at.isoformat(),
        }
        for u in users
    ]


@app.post("/api/users", status_code=201)
async def create_user(body: UserCreate, db: AsyncSession = Depends(get_db)):
    existing = await db.execute(select(User).where(User.name == body.name))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Username already taken")
    user = User(
        name=body.name,
        display_name=body.display_name,
        avatar_color=body.avatar_color or "#6366f1",
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return {"id": user.id, "name": user.name, "display_name": user.display_name}


@app.get("/api/users/{user_id}")
async def get_user(user_id: int, db: AsyncSession = Depends(get_db)):
    user = await db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    result = await db.execute(select(Interest).where(Interest.user_id == user_id).order_by(Interest.created_at))
    interests = result.scalars().all()
    return {
        "id": user.id,
        "name": user.name,
        "display_name": user.display_name,
        "avatar_color": user.avatar_color,
        "created_at": user.created_at.isoformat(),
        "interests": [{"id": i.id, "description": i.description} for i in interests],
    }


@app.patch("/api/users/{user_id}")
async def update_user(user_id: int, body: UserUpdate, db: AsyncSession = Depends(get_db)):
    user = await db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if body.display_name is not None:
        user.display_name = body.display_name
    if body.avatar_color is not None:
        user.avatar_color = body.avatar_color
    await db.commit()
    return {"ok": True}


@app.delete("/api/users/{user_id}", status_code=204)
async def delete_user(user_id: int, db: AsyncSession = Depends(get_db)):
    user = await db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    await db.delete(user)
    await db.commit()


# ---------------------------------------------------------------------------
# Interests
# ---------------------------------------------------------------------------

@app.post("/api/users/{user_id}/interests", status_code=201)
async def add_interest(user_id: int, body: InterestCreate, db: AsyncSession = Depends(get_db)):
    user = await db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    interest = Interest(user_id=user_id, description=body.description)
    db.add(interest)
    await db.commit()
    await db.refresh(interest)
    return {"id": interest.id, "description": interest.description}


@app.delete("/api/users/{user_id}/interests/{interest_id}", status_code=204)
async def delete_interest(user_id: int, interest_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Interest).where(Interest.id == interest_id, Interest.user_id == user_id)
    )
    interest = result.scalar_one_or_none()
    if not interest:
        raise HTTPException(status_code=404, detail="Interest not found")
    await db.delete(interest)
    await db.commit()


# ---------------------------------------------------------------------------
# Feed
# ---------------------------------------------------------------------------

@app.get("/api/users/{user_id}/feed")
async def get_feed(
    user_id: int,
    feed_date: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    user = await db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    target_date = date.today()
    if feed_date:
        try:
            target_date = date.fromisoformat(feed_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format")

    result = await db.execute(
        select(FeedItem)
        .where(FeedItem.user_id == user_id, FeedItem.feed_date == target_date)
        .order_by(FeedItem.position)
    )
    items = result.scalars().all()
    return {
        "user_id": user_id,
        "feed_date": target_date.isoformat(),
        "items": [item_to_dict(i) for i in items],
    }


@app.get("/api/users/{user_id}/feed/dates")
async def get_feed_dates(user_id: int, db: AsyncSession = Depends(get_db)):
    """Return all dates for which this user has a feed."""
    result = await db.execute(
        select(FeedItem.feed_date)
        .where(FeedItem.user_id == user_id)
        .group_by(FeedItem.feed_date)
        .order_by(FeedItem.feed_date.desc())
    )
    dates = [row[0].isoformat() for row in result.all()]
    return {"dates": dates}


@app.post("/api/feed/generate")
async def trigger_feed_generation(body: FeedTriggerRequest):
    """Manually trigger feed generation (for testing or re-runs)."""
    target_date = None
    if body.feed_date:
        try:
            target_date = date.fromisoformat(body.feed_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format")

    async def _run():
        from database import AsyncSessionLocal
        async with AsyncSessionLocal() as session:
            await generate_all_feeds(session, target_date)

    import asyncio
    asyncio.create_task(_run())
    return {"ok": True, "message": f"Feed generation started for {target_date or date.today()}"}


# ---------------------------------------------------------------------------
# Shared items (public, no auth needed)
# ---------------------------------------------------------------------------

@app.get("/api/shared/{share_token}")
async def get_shared_item(share_token: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(FeedItem).where(FeedItem.share_token == share_token)
    )
    item = result.scalar_one_or_none()
    if not item:
        raise HTTPException(status_code=404, detail="Shared item not found")
    user = await db.get(User, item.user_id)
    data = item_to_dict(item)
    data["shared_by"] = user.display_name if user else "Unknown"
    return data


# ---------------------------------------------------------------------------
# Static files & SPA fallback
# ---------------------------------------------------------------------------

FRONTEND_DIR = os.environ.get("FRONTEND_DIR", "/app/frontend")

app.mount("/static", StaticFiles(directory=f"{FRONTEND_DIR}/static"), name="static")


@app.get("/shared/{share_token}")
async def shared_page(share_token: str):
    return FileResponse(f"{FRONTEND_DIR}/templates/index.html")


@app.get("/{full_path:path}")
async def spa_fallback(full_path: str):
    return FileResponse(f"{FRONTEND_DIR}/templates/index.html")
