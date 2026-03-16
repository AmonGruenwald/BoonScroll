from sqlalchemy import (
    Column, Integer, String, Text, DateTime, Date, ForeignKey, Boolean, Float
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime, timezone

Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    display_name = Column(String(100), nullable=False)
    avatar_color = Column(String(7), default="#6366f1")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    interests = relationship("Interest", back_populates="user", cascade="all, delete-orphan")
    feed_items = relationship("FeedItem", back_populates="user", cascade="all, delete-orphan")


class Interest(Base):
    __tablename__ = "interests"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    description = Column(Text, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    user = relationship("User", back_populates="interests")


class FeedItem(Base):
    __tablename__ = "feed_items"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    feed_date = Column(Date, nullable=False)
    item_type = Column(String(20), nullable=False)  # news, fact, video, stock, image
    title = Column(String(500), nullable=False)
    summary = Column(Text)
    content = Column(Text)
    source_url = Column(String(2000))
    media_url = Column(String(2000))
    thumbnail_url = Column(String(2000))
    source_name = Column(String(200))
    # Stock-specific
    ticker = Column(String(20))
    stock_price = Column(Float)
    stock_change = Column(Float)
    stock_change_pct = Column(Float)
    # Tags — comma-separated interest labels this post covers
    tags = Column(Text)
    # Order in the feed
    position = Column(Integer, default=0)
    share_token = Column(String(64), unique=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    user = relationship("User", back_populates="feed_items")
