"""
FastAPI server for AT Protocol Feed Generator.

This module implements the feed server endpoints required by the AT Protocol
to serve a custom Bluesky feed. It provides endpoints for feed discovery,
description, and skeleton generation.
"""

import os
import logging
import asyncio
import json
from typing import Optional
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from atproto import Client, AtUri

from src.database import Database
from src.ranking import RankingEngine, RankingConfig

logger = logging.getLogger(__name__)


# Load environment variables
load_dotenv()

# Configuration
FEED_HOSTNAME = os.getenv("FEED_HOSTNAME", "http://localhost:8000")
DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/feed.db")
BSKY_HANDLE = os.getenv("BSKY_HANDLE", "")

# For feed generators, we use did:web for the service DID
SERVICE_DID = f"did:web:{FEED_HOSTNAME.replace('http://', '').replace('https://', '').split(':')[0]}"

# The feed URI will be determined by where the feed record is published
# (typically at://<user-did>/app.bsky.feed.generator/<feed-name>)
# We'll accept any feed URI with the correct feed name
FEED_NAME = "trending-news"


# Pydantic models for request/response validation
class FeedSkeletonPost(BaseModel):
    """A single post in the feed skeleton."""
    post: str = Field(..., description="AT URI of the post")


class FeedSkeletonResponse(BaseModel):
    """Response for getFeedSkeleton endpoint."""
    feed: list[FeedSkeletonPost] = Field(default_factory=list, description="List of posts")
    cursor: Optional[str] = Field(None, description="Pagination cursor")


class FeedDescription(BaseModel):
    """Feed metadata for describeFeedGenerator endpoint."""
    uri: str = Field(..., description="AT URI of the feed")
    cid: str = Field(..., description="Content ID")


class FeedGeneratorDescription(BaseModel):
    """Response for describeFeedGenerator endpoint."""
    did: str = Field(..., description="DID of the feed generator")
    feeds: list[FeedDescription] = Field(..., description="List of feeds provided")


class DIDDocument(BaseModel):
    """DID document for .well-known/did.json endpoint."""
    context: list[str] = Field(alias="@context", default_factory=lambda: ["https://www.w3.org/ns/did/v1"])
    id: str = Field(..., description="DID identifier")
    service: list[dict] = Field(default_factory=list, description="Service endpoints")


# Global database and ranking engine instances
db: Optional[Database] = None
ranking_engine: Optional[RankingEngine] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for FastAPI application.
    
    Handles startup and shutdown events using the modern lifespan pattern.
    Code before yield runs at startup, code after yield runs at shutdown.
    """
    global db, ranking_engine
    
    # Startup: Initialize database and ranking engine
    db = Database(DATABASE_PATH)
    await db.initialize()
    
    config = RankingConfig.from_file("config/ranking.json")
    ranking_engine = RankingEngine(db, config)
    
    print(f"Feed server started: {FEED_HOSTNAME}")
    print(f"Service DID: {SERVICE_DID}")
    print(f"Feed name: {FEED_NAME}")
    print(f"Database: {DATABASE_PATH}")
    
    yield  # Application runs here
    
    # Shutdown: Cleanup resources
    if db:
        await db.close()
    print("Feed server stopped")


# Initialize FastAPI app with lifespan
app = FastAPI(
    title="Bluesky Domain Feed Generator",
    description="A custom Bluesky feed that displays posts from whitelisted news domains, ranked by share count with time decay",
    version="1.0.0",
    lifespan=lifespan
)

# Mount static files directory
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def root():
    """Root endpoint with basic information."""
    return {
        "name": "Bluesky Domain Feed Generator",
        "description": "A custom feed displaying posts from whitelisted news domains",
        "service_did": SERVICE_DID,
        "feed_name": FEED_NAME,
        "version": "1.0.0"
    }


@app.get("/.well-known/did.json")
async def did_document():
    """
    Serve DID document for feed generator discovery.
    
    This endpoint is required by the AT Protocol for feed generator
    identification and service endpoint discovery.
    """
    doc = {
        "@context": ["https://www.w3.org/ns/did/v1"],
        "id": SERVICE_DID,
        "service": [
            {
                "id": "#bsky_fg",
                "type": "BskyFeedGenerator",
                "serviceEndpoint": FEED_HOSTNAME
            }
        ]
    }
    return JSONResponse(content=doc)


@app.get("/xrpc/app.bsky.feed.describeFeedGenerator")
async def describe_feed_generator():
    """
    Describe the feed generator and its feeds.
    
    This endpoint returns metadata about the feed generator,
    including the list of feeds it provides.
    
    Returns:
        FeedGeneratorDescription: Feed generator metadata
    """
    response = {
        "did": SERVICE_DID,
        "feeds": []
    }
    
    return JSONResponse(content=response)


@app.get("/xrpc/app.bsky.feed.getFeedSkeleton")
async def get_feed_skeleton(
    feed: str = Query(..., description="AT URI of the feed"),
    limit: int = Query(50, ge=1, le=100, description="Maximum number of posts to return"),
    cursor: Optional[str] = Query(None, description="Pagination cursor")
):
    """
    Get the feed skeleton (list of post URIs).
    
    This is the main endpoint that returns the ranked list of posts
    for the feed. The client will then fetch full post details from Bluesky.
    
    Args:
        feed: AT URI of the requested feed
        limit: Maximum number of posts to return (1-100)
        cursor: Optional pagination cursor for fetching next page
        
    Returns:
        FeedSkeletonResponse: List of post URIs and optional cursor
        
    Raises:
        HTTPException: If feed URI is invalid or database error occurs
    """
    global ranking_engine
    
    # Validate feed URI - check that it ends with our feed name
    # The feed URI format is: at://<user-did>/app.bsky.feed.generator/<feed-name>
    if not feed.endswith(f"/app.bsky.feed.generator/{FEED_NAME}"):
        raise HTTPException(
            status_code=400,
            detail=f"Unknown feed: {feed}. This server only serves feeds named '{FEED_NAME}'"
        )
    
    if not ranking_engine:
        raise HTTPException(
            status_code=503,
            detail="Ranking engine not initialized"
        )
    
    try:
        # Get feed skeleton from ranking engine with cursor-based pagination
        feed_data = await ranking_engine.get_feed_skeleton(
            limit=limit,
            cursor=cursor
        )
        
        return JSONResponse(content=feed_data)
        
    except Exception as e:
        logger.error(f"Error generating feed for {feed}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error generating feed: {str(e)}"
        )


@app.get("/health")
async def health_check():
    """
    Health check endpoint.
    
    Returns the health status of the feed server and its dependencies.
    """
    global db, ranking_engine
    
    health = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "components": {
            "database": "unknown",
            "ranking_engine": "unknown"
        }
    }
    
    # Check database
    if db:
        try:
            stats = await db.get_stats()
            health["components"]["database"] = "healthy"
            health["database_stats"] = stats
        except Exception as e:
            health["status"] = "degraded"
            health["components"]["database"] = f"unhealthy: {str(e)}"
    else:
        health["status"] = "degraded"
        health["components"]["database"] = "not initialized"
    
    # Check ranking engine
    if ranking_engine:
        health["components"]["ranking_engine"] = "healthy"
    else:
        health["status"] = "degraded"
        health["components"]["ranking_engine"] = "not initialized"
    
    status_code = 200 if health["status"] == "healthy" else 503
    return JSONResponse(content=health, status_code=status_code)


@app.get("/stats")
async def get_stats():
    """
    Get feed statistics.
    
    Returns statistics about the feed, including post counts,
    URL counts, and ranking information.
    """
    global db, ranking_engine
    
    if not db or not ranking_engine:
        raise HTTPException(
            status_code=503,
            detail="Server not fully initialized"
        )
    
    try:
        # Get database stats
        db_stats = await db.get_stats()
        
        # Get ranking stats
        ranking_stats = await ranking_engine.get_ranking_stats()
        
        return {
            "database": db_stats,
            "ranking": ranking_stats,
            "service_did": SERVICE_DID,
            "feed_name": FEED_NAME
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving stats: {str(e)}"
        )


async def _hydrate_post(post_uri: str) -> Optional[dict]:
    """
    Fetch full post details from Bluesky API, including author information.
    
    This also serves as a visibility check - posts restricted to authenticated
    users will fail to fetch, and we'll return None. Additionally, posts with
    the !no-unauthenticated label are filtered out.
    
    Args:
        post_uri: AT Protocol URI of the post
        
    Returns:
        Dictionary with hydrated post data, or None if post is not publicly visible
    """
    try:
        # Create an unauthenticated client with the public Bluesky API endpoint
        client = Client(base_url='https://public.api.bsky.app')
        
        # Fetch the post without authentication
        response = client.app.bsky.feed.get_posts({'uris': [post_uri]})
        
        # Check if we got the post back
        if not response or not response.posts or len(response.posts) == 0:
            logger.debug(f"Post {post_uri} not found or not accessible")
            return None
        
        post = response.posts[0]
        
        # Check for !no-unauthenticated label
        # Posts with this label should not be shown to unauthenticated users
        if hasattr(post, 'labels') and post.labels:
            for label in post.labels:
                # Check if label has a 'val' attribute (label value)
                label_val = label.val if hasattr(label, 'val') else str(label)
                if label_val == '!no-unauthenticated':
                    logger.debug(f"Post {post_uri} has !no-unauthenticated label, filtering out")
                    return None
        
        # Extract author information
        author = post.author
        hydrated_data = {
            'author_handle': author.handle,
            'author_display_name': author.display_name or author.handle,
            'author_avatar': author.avatar if hasattr(author, 'avatar') else None,
            'post_text': post.record.text if hasattr(post.record, 'text') else '',
            'like_count': post.like_count if hasattr(post, 'like_count') else 0,
            'repost_count': post.repost_count if hasattr(post, 'repost_count') else 0,
            'reply_count': post.reply_count if hasattr(post, 'reply_count') else 0,
        }
        
        return hydrated_data
        
    except Exception as e:
        # If we get an error (e.g., authentication required, not found),
        # the post is not publicly visible
        logger.debug(f"Failed to hydrate post {post_uri}: {e}")
        return None


@app.get("/preview", response_class=HTMLResponse)
async def preview_feed(
    limit: int = Query(50, ge=1, le=100, description="Maximum number of posts to display")
):
    """
    Preview the feed in a human-readable HTML format.
    
    This endpoint displays the ranked feed posts with their content,
    URLs, share counts, and timestamps in a nice web interface.
    Posts are hydrated with author information from the Bluesky API,
    and only publicly visible posts are displayed.
    
    Args:
        limit: Maximum number of posts to display (1-100)
        
    Returns:
        HTML page with formatted feed posts
    """
    global db, ranking_engine
    
    if not db or not ranking_engine:
        return HTMLResponse(
            content="<html><body><h1>Error</h1><p>Server not fully initialized</p></body></html>",
            status_code=503
        )
    
    try:
        # Get ranked posts - fetch more than needed since some may be filtered out
        ranked_posts = await ranking_engine.rank_posts(limit=int(limit * 1.5))
        
        # Hydrate posts concurrently for much better performance
        # Run them all concurrently with a semaphore to limit concurrent requests
        # This prevents overwhelming the Bluesky API
        semaphore = asyncio.Semaphore(20)  # Max 20 concurrent requests
        
        async def hydrate_with_limit(post_uri: str):
            async with semaphore:
                return await _hydrate_post(post_uri)
        
        # Execute all hydration tasks concurrently
        hydration_results = await asyncio.gather(
            *[hydrate_with_limit(post['uri']) for post in ranked_posts],
            return_exceptions=True
        )
        
        # Filter and combine results
        hydrated_posts = []
        for post, hydrated_data in zip(ranked_posts, hydration_results):
            if len(hydrated_posts) >= limit:
                break
            
            # Check if hydration was successful (not None and not an exception)
            if hydrated_data and not isinstance(hydrated_data, Exception):
                # Merge hydrated data with original post data
                post.update(hydrated_data)
                hydrated_posts.append(post)
            else:
                if isinstance(hydrated_data, Exception):
                    logger.debug(f"Exception hydrating post {post['uri']}: {hydrated_data}")
                else:
                    logger.info(f"Filtered out non-public post: {post['uri']}")
        
        # Get database stats
        stats = await db.get_stats()
        
        # Build HTML
        html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Bluesky Domain Feed Preview</title>
    <link rel="stylesheet" href="/static/styles.css">
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Bluesky Domain Feed</h1>
            <p>News posts from trusted sources, ranked by popularity and freshness</p>
            
            <div class="stats">
                <div class="stat">
                    <div class="stat-label">Total Posts</div>
                    <div class="stat-value">{stats['total_posts']}</div>
                </div>
                <div class="stat">
                    <div class="stat-label">Unique URLs</div>
                    <div class="stat-value">{stats['unique_urls']}</div>
                </div>
                <div class="stat">
                    <div class="stat-label">Total Shares</div>
                    <div class="stat-value">{stats['total_shares']}</div>
                </div>
            </div>
            
            <button class="refresh-btn" onclick="location.reload()">Refresh Feed</button>
        </div>
"""
        
        if not hydrated_posts:
            html_content += """
        <div class="empty-state">
            <h2>No posts yet</h2>
            <p>The feed is empty. Make sure the firehose listener is running to collect posts.</p>
        </div>
"""
        else:
            for i, post in enumerate(hydrated_posts, 1):
                # Format timestamp
                created_at = post['created_at']
                if isinstance(created_at, str):
                    from dateutil import parser
                    created_at = parser.parse(created_at)
                
                time_ago = _format_time_ago(created_at)
                
                # Use hydrated author information
                author_handle = post.get('author_handle', 'unknown')
                author_display_name = post.get('author_display_name', author_handle)
                author_avatar = post.get('author_avatar')
                
                # Get post text from hydrated data (more accurate) or fallback to stored text
                post_text = post.get('post_text', post.get('text', '')).strip()
                if not post_text:
                    post_text = '<em>No text content</em>'
                
                # Format score
                score = post.get('score', 0)
                
                # Get engagement metrics
                like_count = post.get('like_count', 0)
                repost_count = post.get('repost_count', 0)
                reply_count = post.get('reply_count', 0)
                
                # Build author display with avatar if available
                author_html = f'<img src="{author_avatar}" alt="{author_display_name}" style="width: 20px; height: 20px; border-radius: 50%; vertical-align: middle; margin-right: 5px;">' if author_avatar else ''
                author_html += f'<strong>{author_display_name}</strong> @{author_handle}'
                
                html_content += f"""
        <div class="post">
            <div class="post-header">
                <div class="post-meta">
                    <div class="post-author">{author_html}</div>
                    <div class="post-time">{time_ago}</div>
                </div>
                <div class="post-score">{score:.2f}</div>
            </div>
            
            <div class="post-text">{post_text}</div>
            
            <div class="post-url">
                <a href="{post['url']}" target="_blank" rel="noopener noreferrer">
                    {post['url']}
                </a>
            </div>
            
            <div class="post-footer">
                <span class="badge badge-domain">{post['domain']}</span>
                <span class="badge badge-shares">{post['share_count']} share{'s' if post['share_count'] != 1 else ''}</span>
                <span class="badge badge-engagement">{like_count} likes</span>
                <span class="badge badge-engagement">{repost_count} reposts</span>
                <span class="badge badge-engagement">{reply_count} replies</span>
            </div>
        </div>
"""
        
        html_content += """
    </div>
</body>
</html>
"""
        
        return HTMLResponse(content=html_content)
        
    except Exception as e:
        error_html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Error</title>
    <style>
        body {{ font-family: sans-serif; padding: 40px; background: #f3f4f6; }}
        .error {{ background: white; padding: 30px; border-radius: 8px; max-width: 600px; margin: 0 auto; }}
        h1 {{ color: #dc2626; }}
        pre {{ background: #f3f4f6; padding: 15px; border-radius: 4px; overflow-x: auto; }}
    </style>
</head>
<body>
    <div class="error">
        <h1>Error Loading Feed</h1>
        <p>An error occurred while loading the feed preview:</p>
        <pre>{str(e)}</pre>
    </div>
</body>
</html>
"""
        return HTMLResponse(content=error_html, status_code=500)


def _format_time_ago(dt: datetime) -> str:
    """Format a datetime as a human-readable 'time ago' string."""
    now = datetime.utcnow()
    diff = now - dt
    
    seconds = diff.total_seconds()
    
    if seconds < 60:
        return "just now"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    elif seconds < 86400:
        hours = int(seconds / 3600)
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    else:
        days = int(seconds / 86400)
        return f"{days} day{'s' if days != 1 else ''} ago"


@app.get("/about", response_class=HTMLResponse)
async def about_page():
    """
    About page explaining the feed and listing whitelisted domains.
    
    Returns:
        HTML page with feed information and alphabetized domain list
    """
    global ranking_engine
    
    # Load domains from config file
    try:
        with open("config/domains.json", "r") as f:
            config = json.load(f)
            domains = sorted(config.get("domains", []))
    except Exception as e:
        logger.error(f"Error loading domains: {e}")
        domains = []
    
    # Get ranking config values
    if ranking_engine:
        ranking_config = ranking_engine.config
        decay_rate = ranking_config.decay_rate
        repost_weight = ranking_config.repost_weight
        max_age_hours = ranking_config.max_age_hours
        max_posts_per_url = ranking_config.max_posts_per_url
    else:
        # Fallback to defaults if engine not initialized
        decay_rate = 0.05
        repost_weight = 1.0
        max_age_hours = 72
        max_posts_per_url = None
    
    # Build domain list HTML
    domain_list_html = ""
    for domain in domains:
        domain_list_html += f'                <li class="domain-item">{domain}</li>\n'
    
    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>About - Bluesky Domain Feed</title>
    <link rel="stylesheet" href="/static/styles.css">
</head>
<body>
    <div class="container">
        <div class="content">
            <h1>About This Feed</h1>
            
            <p>
                This feed displays posts from Bluesky that link to specific news domains.
            </p>
            
            <div class="highlight">
                <p><strong>How it works:</strong> The feed monitors the Bluesky firehose for posts containing links
                to the whitelisted domains listed below. Posts are ranked by a score calculated from the number of times
                the URL has been shared, with a time decay factor that reduces the score of older urls. Posts older than
                {max_age_hours} hours are excluded.{f' The feed shows up to {max_posts_per_url} posts per unique URL.' if max_posts_per_url else ''}</p>
            </div>
            
            <h2>Features</h2>
            <ul class="feature-list">
                <li>Filters posts to only include links from whitelisted domains</li>
                <li>Ranks posts by share count with time decay (decay rate: {decay_rate})</li>
                <li>Monitors the Bluesky firehose in real-time</li>
                <li>Tracks URL shares and reposts (repost weight: {repost_weight})</li>
                <li>Excludes posts older than {max_age_hours} hours</li>
                <li>Shows only publicly visible posts</li>
            </ul>
            
            <h2>Whitelisted Domains</h2>
            <p>
                This feed includes posts linking to the following {len(domains)} trusted news sources
                (listed alphabetically):
            </p>
            
            <ul class="domain-list">
{domain_list_html}            </ul>
            
            <div class="highlight" style="margin-top: 30px;">
                <p><strong>Note:</strong> The feed matches both the main domain and all subdomains.
                For example, "nytimes.com" will match posts from "www.nytimes.com", "cooking.nytimes.com", etc.</p>
            </div>
            
            <div class="nav-buttons">
                <a href="/preview" class="btn">View Feed Preview</a>
                <a href="/stats" class="btn btn-secondary">View Statistics</a>
                <a href="/" class="btn btn-secondary">Home</a>
            </div>
        </div>
    </div>
</body>
</html>
"""
    
    return HTMLResponse(content=html_content)


# For running with uvicorn directly
if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(
        "src.server:app",
        host="0.0.0.0",
        port=port,
        reload=True,
        log_level="info"
    )
