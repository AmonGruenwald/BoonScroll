#!/usr/bin/env python3
"""
Manually trigger feed generation for a specific date (or today).
Usage: python scripts/generate_feed.py [YYYY-MM-DD]
"""
import asyncio
import sys
import os
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

from database import AsyncSessionLocal, init_db
from feed_generator import generate_all_feeds


async def main():
    target_date = None
    if len(sys.argv) > 1:
        target_date = date.fromisoformat(sys.argv[1])

    await init_db()
    async with AsyncSessionLocal() as session:
        await generate_all_feeds(session, target_date)


if __name__ == "__main__":
    asyncio.run(main())
