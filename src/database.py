"""
Database operations for the Bluesky domain-based feed generator.

This module provides async database operations using SQLAlchemy with SQLite.
It manages three tables:
- posts: Stores Bluesky post metadata
- urls: Stores unique URLs with share counts
- post_urls: Junction table linking posts to URLs
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Text,
    DateTime,
    ForeignKey,
    Index,
    func,
    event,
    text,
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger(__name__)

Base = declarative_base()


class Post(Base):
    """Represents a Bluesky post containing a whitelisted URL."""
    
    __tablename__ = "posts"
    
    uri = Column(String, primary_key=True)
    cid = Column(String, nullable=False)
    author_did = Column(String, nullable=False)
    text = Column(Text)
    created_at = Column(DateTime, nullable=False)
    indexed_at = Column(DateTime, default=datetime.utcnow)
    repost_count = Column(Integer, default=0)
    
    # Relationship to URLs through junction table
    post_urls = relationship("PostURL", back_populates="post", cascade="all, delete-orphan")
    
    # Index for performance
    __table_args__ = (
        Index("idx_posts_repost_count", "repost_count"),
    )
    
    def __repr__(self):
        return f"<Post(uri={self.uri}, author={self.author_did})>"


class URL(Base):
    """Represents a unique URL with share tracking."""
    
    __tablename__ = "urls"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String, unique=True, nullable=False)
    domain = Column(String, nullable=False)
    first_seen = Column(DateTime, default=datetime.utcnow)
    share_count = Column(Integer, default=1)
    
    # Relationship to posts through junction table
    post_urls = relationship("PostURL", back_populates="url", cascade="all, delete-orphan")
    
    # Indexes for performance
    __table_args__ = (
        Index("idx_domain", "domain"),
        Index("idx_share_count", "share_count"),
    )
    
    def __repr__(self):
        return f"<URL(id={self.id}, url={self.url}, shares={self.share_count})>"


class PostURL(Base):
    """Junction table linking posts to URLs."""
    
    __tablename__ = "post_urls"
    
    post_uri = Column(String, ForeignKey("posts.uri", ondelete="CASCADE"), primary_key=True)
    url_id = Column(Integer, ForeignKey("urls.id", ondelete="CASCADE"), primary_key=True)
    shared_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    post = relationship("Post", back_populates="post_urls")
    url = relationship("URL", back_populates="post_urls")
    
    # Index for time-based queries
    __table_args__ = (
        Index("idx_shared_at", "shared_at"),
    )
    
    def __repr__(self):
        return f"<PostURL(post={self.post_uri}, url_id={self.url_id})>"


class Database:
    """
    Async database manager for the feed generator.
    
    Provides methods for:
    - Initializing database schema
    - Storing posts and URLs
    - Tracking URL share counts
    - Querying posts for feed generation
    """
    
    def __init__(self, db_path: str = "data/feed.db"):
        """
        Initialize database connection.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        
        # Ensure data directory exists
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Create async engine
        self.engine = create_async_engine(
            f"sqlite+aiosqlite:///{db_path}",
            echo=False,  # Set to True for SQL debugging
        )
        
        # Enable foreign keys and optimize SQLite for each connection
        @event.listens_for(self.engine.sync_engine, "connect")
        def set_sqlite_pragma(dbapi_conn, connection_record):
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA journal_mode=WAL")  # Write-Ahead Logging for better concurrency
            cursor.execute("PRAGMA synchronous=NORMAL")  # Faster than FULL, still safe
            cursor.execute("PRAGMA cache_size=-64000")  # 64MB cache
            cursor.execute("PRAGMA temp_store=MEMORY")  # Store temp tables in memory
            cursor.close()
        
        # Create session factory
        self.async_session = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        
        logger.info(f"Database initialized at {db_path}")
    
    async def initialize(self):
        """Create all database tables and indexes."""
        from sqlalchemy import text
        
        # Enable foreign key support for SQLite
        async with self.engine.begin() as conn:
            await conn.execute(text("PRAGMA foreign_keys = ON"))
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database schema created")
    
    async def close(self):
        """Close database connection."""
        await self.engine.dispose()
        logger.info("Database connection closed")
    
    async def add_post(
        self,
        uri: str,
        cid: str,
        author_did: str,
        url: str,
        domain: str,
        text: Optional[str] = None,
        created_at: Optional[datetime] = None,
    ) -> bool:
        """
        Add a post and its URL to the database.
        
        This method:
        1. Creates or updates the URL record
        2. Increments share count if URL exists
        3. Creates the post record
        4. Links post to URL in junction table
        
        Args:
            uri: AT Protocol URI of the post
            cid: Content ID of the post
            author_did: DID of the post author
            url: Normalized URL from the post
            domain: Domain extracted from URL
            text: Post text content (optional)
            created_at: Post creation timestamp (defaults to now)
        
        Returns:
            True if post was added successfully, False if it already exists
        """
        if created_at is None:
            created_at = datetime.utcnow()
        
        async with self.async_session() as session:
            try:
                # Check if URL exists, create or update
                url_record = await self._get_or_create_url(session, url, domain)
                
                # Create post record
                post = Post(
                    uri=uri,
                    cid=cid,
                    author_did=author_did,
                    text=text,
                    created_at=created_at,
                )
                session.add(post)
                
                # Create junction table entry
                post_url = PostURL(
                    post_uri=uri,
                    url_id=url_record.id,
                )
                session.add(post_url)
                
                await session.commit()
                logger.debug(f"Added post {uri} with URL {url}")
                return True
                
            except IntegrityError:
                await session.rollback()
                logger.debug(f"Post {uri} already exists")
                return False
            except Exception as e:
                await session.rollback()
                logger.error(f"Error adding post {uri}: {e}")
                raise
    
    async def _get_or_create_url(self, session: AsyncSession, url: str, domain: str) -> URL:
        """
        Get existing URL or create new one, incrementing share count.
        
        Args:
            session: Active database session
            url: Normalized URL
            domain: Domain extracted from URL
        
        Returns:
            URL record (existing or newly created)
        """
        from sqlalchemy import select
        
        # Try to get existing URL
        result = await session.execute(
            select(URL).where(URL.url == url)
        )
        url_record = result.scalar_one_or_none()
        
        if url_record:
            # URL exists, increment share count
            url_record.share_count += 1
            logger.debug(f"Incremented share count for {url} to {url_record.share_count}")
        else:
            # Create new URL record
            url_record = URL(
                url=url,
                domain=domain,
                share_count=1,
            )
            session.add(url_record)
            logger.debug(f"Created new URL record for {url}")
        
        await session.flush()  # Ensure ID is generated
        return url_record
    
    async def add_posts_batch(
        self,
        posts: List[Dict[str, Any]],
    ) -> int:
        """
        Add multiple posts in a single batch transaction.
        
        This is much more efficient than adding posts one at a time,
        as it reduces database round-trips and transaction overhead.
        
        Args:
            posts: List of post dictionaries, each containing:
                - uri: AT Protocol URI of the post
                - cid: Content ID of the post
                - author_did: DID of the post author
                - url: Normalized URL from the post
                - domain: Domain extracted from URL
                - text: Post text content (optional)
                - created_at: Post creation timestamp
        
        Returns:
            Number of posts successfully added (excludes duplicates)
        """
        if not posts:
            return 0
        
        added_count = 0
        
        async with self.async_session() as session:
            try:
                # Process each post in the batch
                for post_data in posts:
                    try:
                        # Check if URL exists, create or update
                        url_record = await self._get_or_create_url(
                            session,
                            post_data['url'],
                            post_data['domain']
                        )
                        
                        # Create post record
                        post = Post(
                            uri=post_data['uri'],
                            cid=post_data['cid'],
                            author_did=post_data['author_did'],
                            text=post_data.get('text'),
                            created_at=post_data['created_at'],
                        )
                        session.add(post)
                        
                        # Create junction table entry
                        post_url = PostURL(
                            post_uri=post_data['uri'],
                            url_id=url_record.id,
                        )
                        session.add(post_url)
                        
                        added_count += 1
                        
                    except IntegrityError:
                        # Post already exists, skip it
                        await session.rollback()
                        logger.debug(f"Post {post_data['uri']} already exists, skipping")
                        continue
                
                # Commit all posts in one transaction
                await session.commit()
                logger.debug(f"Batch added {added_count} posts out of {len(posts)}")
                return added_count
                
            except Exception as e:
                await session.rollback()
                logger.error(f"Error in batch add: {e}", exc_info=True)
                raise
    
    async def increment_repost_count(self, post_uri: str) -> bool:
        """
        Increment the repost count for a post.
        
        Args:
            post_uri: AT Protocol URI of the post being reposted
        
        Returns:
            True if post exists and was updated, False if post not found
        """
        from sqlalchemy import select
        
        async with self.async_session() as session:
            result = await session.execute(
                select(Post).where(Post.uri == post_uri)
            )
            post = result.scalar_one_or_none()
            
            if post:
                post.repost_count += 1
                await session.commit()
                logger.debug(f"Incremented repost count for {post_uri} to {post.repost_count}")
                return True
            else:
                logger.debug(f"Post {post_uri} not found for repost increment")
                return False
    
    async def get_post(self, uri: str) -> Optional[Dict[str, Any]]:
        """
        Get a post by URI.
        
        Args:
            uri: AT Protocol URI of the post
        
        Returns:
            Dictionary with post data, or None if not found
        """
        from sqlalchemy import select
        
        async with self.async_session() as session:
            result = await session.execute(
                select(Post).where(Post.uri == uri)
            )
            post = result.scalar_one_or_none()
            
            if post:
                return {
                    "uri": post.uri,
                    "cid": post.cid,
                    "author_did": post.author_did,
                    "text": post.text,
                    "created_at": post.created_at,
                    "indexed_at": post.indexed_at,
                    "repost_count": post.repost_count,
                }
            return None
    
    async def get_url(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Get URL information by URL string.
        
        Args:
            url: Normalized URL
        
        Returns:
            Dictionary with URL data, or None if not found
        """
        from sqlalchemy import select
        
        async with self.async_session() as session:
            result = await session.execute(
                select(URL).where(URL.url == url)
            )
            url_record = result.scalar_one_or_none()
            
            if url_record:
                return {
                    "id": url_record.id,
                    "url": url_record.url,
                    "domain": url_record.domain,
                    "first_seen": url_record.first_seen,
                    "share_count": url_record.share_count,
                }
            return None
    
    async def get_url_share_count(self, url: str) -> int:
        """
        Get the share count for a URL.
        
        Args:
            url: Normalized URL
        
        Returns:
            Share count (0 if URL not found)
        """
        url_data = await self.get_url(url)
        return url_data["share_count"] if url_data else 0
    
    async def get_posts_by_domain(
        self,
        domain: str,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Get posts containing URLs from a specific domain.
        
        Args:
            domain: Domain to filter by
            limit: Maximum number of posts to return
            offset: Number of posts to skip
        
        Returns:
            List of post dictionaries with URL information
        """
        from sqlalchemy import select
        
        async with self.async_session() as session:
            query = (
                select(Post, URL, PostURL)
                .join(PostURL, Post.uri == PostURL.post_uri)
                .join(URL, PostURL.url_id == URL.id)
                .where(URL.domain == domain)
                .order_by(Post.created_at.desc())
                .limit(limit)
                .offset(offset)
            )
            
            result = await session.execute(query)
            rows = result.all()
            
            posts = []
            for post, url, post_url in rows:
                posts.append({
                    "uri": post.uri,
                    "cid": post.cid,
                    "author_did": post.author_did,
                    "text": post.text,
                    "created_at": post.created_at,
                    "indexed_at": post.indexed_at,
                    "url": url.url,
                    "domain": url.domain,
                    "share_count": url.share_count,
                    "shared_at": post_url.shared_at,
                    "repost_count": post.repost_count,
                })
            
            return posts
    
    async def get_recent_posts(
        self,
        hours: int = 168,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get recent posts within specified time window.
        
        Args:
            hours: Number of hours to look back
            limit: Maximum number of posts to return
        
        Returns:
            List of post dictionaries with URL information
        """
        from sqlalchemy import select
        from datetime import timedelta
        
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        
        async with self.async_session() as session:
            query = (
                select(Post, URL, PostURL)
                .join(PostURL, Post.uri == PostURL.post_uri)
                .join(URL, PostURL.url_id == URL.id)
                .where(Post.created_at >= cutoff_time)
                .order_by(Post.created_at.desc())
                .limit(limit)
            )
            
            result = await session.execute(query)
            rows = result.all()
            
            posts = []
            for post, url, post_url in rows:
                posts.append({
                    "uri": post.uri,
                    "cid": post.cid,
                    "author_did": post.author_did,
                    "text": post.text,
                    "created_at": post.created_at,
                    "indexed_at": post.indexed_at,
                    "url": url.url,
                    "domain": url.domain,
                    "share_count": url.share_count,
                    "shared_at": post_url.shared_at,
                    "repost_count": post.repost_count,
                })
            
            return posts
    
    async def get_stats(self) -> Dict[str, int]:
        """
        Get database statistics.
        
        Returns:
            Dictionary with counts of posts, URLs, and total shares
        """
        from sqlalchemy import select
        
        async with self.async_session() as session:
            # Count posts
            post_count = await session.execute(select(func.count(Post.uri)))
            posts = post_count.scalar()
            
            # Count URLs
            url_count = await session.execute(select(func.count(URL.id)))
            urls = url_count.scalar()
            
            # Sum share counts
            share_sum = await session.execute(select(func.sum(URL.share_count)))
            shares = share_sum.scalar() or 0
            
            return {
                "total_posts": posts,
                "unique_urls": urls,
                "total_shares": shares,
            }
    
    async def delete_posts_in_period(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> int:
        """
        Delete posts within a specific time period.
        
        This allows for precise control over which posts to delete.
        URLs are preserved with their share counts.
        
        Args:
            start_date: Delete posts created on or after this date (inclusive)
            end_date: Delete posts created before this date (exclusive)
        
        Returns:
            Number of posts deleted
        """
        from sqlalchemy import delete, and_
        
        if start_date is None and end_date is None:
            logger.warning("No date range specified, no posts deleted")
            return 0
        
        async with self.async_session() as session:
            # Build query conditions
            conditions = []
            if start_date is not None:
                conditions.append(Post.created_at >= start_date)
            if end_date is not None:
                conditions.append(Post.created_at < end_date)
            
            # Delete posts in the specified period (cascade will handle post_urls)
            if len(conditions) == 1:
                result = await session.execute(
                    delete(Post).where(conditions[0])
                )
            else:
                result = await session.execute(
                    delete(Post).where(and_(*conditions))
                )
            
            deleted = result.rowcount
            await session.commit()
            
            period_desc = []
            if start_date:
                period_desc.append(f"from {start_date.isoformat()}")
            if end_date:
                period_desc.append(f"to {end_date.isoformat()}")
            
            logger.info(f"Deleted {deleted} posts {' '.join(period_desc)}")
            return deleted
    
    async def delete_old_posts(self, days: int = 30) -> int:
        """
        Delete posts older than specified days.
        
        This helps manage database size by removing old posts.
        URLs are preserved with their share counts.
        
        Args:
            days: Delete posts older than this many days
        
        Returns:
            Number of posts deleted
        """
        from datetime import timedelta
        
        cutoff_time = datetime.utcnow() - timedelta(days=days)
        return await self.delete_posts_in_period(end_date=cutoff_time)
    
    async def cleanup_orphaned_urls(self) -> int:
        """
        Remove URLs that are no longer linked to any posts.
        
        Returns:
            Number of URLs deleted
        """
        from sqlalchemy import delete, select
        
        async with self.async_session() as session:
            # Get all URL IDs that are still referenced in post_urls
            result = await session.execute(
                select(PostURL.url_id).distinct()
            )
            active_url_ids = {row[0] for row in result.all()}
            
            # Get all URL IDs
            result = await session.execute(select(URL.id))
            all_url_ids = {row[0] for row in result.all()}
            
            # Find orphaned URL IDs
            orphaned_ids = all_url_ids - active_url_ids
            
            if orphaned_ids:
                # Delete orphaned URLs
                result = await session.execute(
                    delete(URL).where(URL.id.in_(orphaned_ids))
                )
                deleted = result.rowcount
            else:
                deleted = 0
            
            await session.commit()
            
            logger.info(f"Deleted {deleted} orphaned URLs")
            return deleted


# Convenience function for creating database instance
def create_database(db_path: str = "data/feed.db") -> Database:
    """
    Create and return a Database instance.
    
    Args:
        db_path: Path to SQLite database file
    
    Returns:
        Database instance
    """
    return Database(db_path)
