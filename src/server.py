"""
FastAPI server for AT Protocol Feed Generator.

This module implements the feed server endpoints required by the AT Protocol
to serve a custom Bluesky feed. It provides endpoints for feed discovery,
description, and skeleton generation.
"""

import os
from typing import Optional
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel, Field
from dotenv import load_dotenv

from src.database import Database
from src.ranking import RankingEngine, RankingConfig


# Load environment variables
load_dotenv()

# Configuration
FEED_HOSTNAME = os.getenv("FEED_HOSTNAME", "http://localhost:8000")
DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/feed.db")
BSKY_HANDLE = os.getenv("BSKY_HANDLE", "")

# Extract DID from hostname for feed URI
# In production, this would be a proper DID
FEED_DID = f"did:web:{FEED_HOSTNAME.replace('http://', '').replace('https://', '').split(':')[0]}"
FEED_URI = f"at://{FEED_DID}/app.bsky.feed.generator/domain-news"


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
    print(f"Feed URI: {FEED_URI}")
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


@app.get("/")
async def root():
    """Root endpoint with basic information."""
    return {
        "name": "Bluesky Domain Feed Generator",
        "description": "A custom feed displaying posts from whitelisted news domains",
        "feed_uri": FEED_URI,
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
        "id": FEED_DID,
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
    # In a real implementation, the CID would be the content hash
    # For now, we use a placeholder
    feed_cid = "bafyreihqhqklffqkwtpn6wtjzz7d5lqzx7obed4qkbvvzqyqkqkqkqkqkq"
    
    response = {
        "did": FEED_DID,
        "feeds": [
            {
                "uri": FEED_URI,
                "cid": feed_cid
            }
        ]
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
    
    # Validate feed URI
    if feed != FEED_URI:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown feed: {feed}. Expected: {FEED_URI}"
        )
    
    if not ranking_engine:
        raise HTTPException(
            status_code=503,
            detail="Ranking engine not initialized"
        )
    
    try:
        # Get feed skeleton from ranking engine
        # Note: Cursor-based pagination not yet fully implemented in ranking engine
        # For now, we ignore the cursor and return top results
        feed_data = await ranking_engine.get_feed_skeleton(
            limit=limit,
            cursor=cursor
        )
        
        return JSONResponse(content=feed_data)
        
    except Exception as e:
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
            "feed_uri": FEED_URI
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving stats: {str(e)}"
        )


@app.get("/preview", response_class=HTMLResponse)
async def preview_feed(
    limit: int = Query(50, ge=1, le=100, description="Maximum number of posts to display")
):
    """
    Preview the feed in a human-readable HTML format.
    
    This endpoint displays the ranked feed posts with their content,
    URLs, share counts, and timestamps in a nice web interface.
    
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
        # Get ranked posts
        ranked_posts = await ranking_engine.rank_posts(limit=limit)
        
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
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        
        .container {{
            max-width: 800px;
            margin: 0 auto;
        }}
        
        .header {{
            background: white;
            border-radius: 12px;
            padding: 30px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }}
        
        .header h1 {{
            color: #1d4ed8;
            margin-bottom: 10px;
            font-size: 2em;
        }}
        
        .header p {{
            color: #6b7280;
            font-size: 1.1em;
        }}
        
        .stats {{
            display: flex;
            gap: 20px;
            margin-top: 20px;
            flex-wrap: wrap;
        }}
        
        .stat {{
            background: #f3f4f6;
            padding: 15px 20px;
            border-radius: 8px;
            flex: 1;
            min-width: 150px;
        }}
        
        .stat-label {{
            color: #6b7280;
            font-size: 0.9em;
            margin-bottom: 5px;
        }}
        
        .stat-value {{
            color: #1f2937;
            font-size: 1.8em;
            font-weight: bold;
        }}
        
        .post {{
            background: white;
            border-radius: 12px;
            padding: 25px;
            margin-bottom: 15px;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            transition: transform 0.2s, box-shadow 0.2s;
        }}
        
        .post:hover {{
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15);
        }}
        
        .post-header {{
            display: flex;
            justify-content: space-between;
            align-items: start;
            margin-bottom: 15px;
            gap: 15px;
        }}
        
        .post-meta {{
            flex: 1;
        }}
        
        .post-author {{
            color: #6b7280;
            font-size: 0.9em;
            margin-bottom: 5px;
        }}
        
        .post-time {{
            color: #9ca3af;
            font-size: 0.85em;
        }}
        
        .post-score {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 8px 15px;
            border-radius: 20px;
            font-weight: bold;
            font-size: 0.9em;
            white-space: nowrap;
        }}
        
        .post-text {{
            color: #1f2937;
            line-height: 1.6;
            margin-bottom: 15px;
            font-size: 1.05em;
        }}
        
        .post-url {{
            background: #f3f4f6;
            padding: 12px 15px;
            border-radius: 8px;
            margin-bottom: 10px;
        }}
        
        .post-url a {{
            color: #1d4ed8;
            text-decoration: none;
            word-break: break-all;
            font-size: 0.95em;
        }}
        
        .post-url a:hover {{
            text-decoration: underline;
        }}
        
        .post-footer {{
            display: flex;
            gap: 15px;
            flex-wrap: wrap;
            margin-top: 12px;
        }}
        
        .badge {{
            display: inline-flex;
            align-items: center;
            gap: 5px;
            padding: 5px 12px;
            border-radius: 15px;
            font-size: 0.85em;
            font-weight: 500;
        }}
        
        .badge-domain {{
            background: #dbeafe;
            color: #1e40af;
        }}
        
        .badge-shares {{
            background: #fef3c7;
            color: #92400e;
        }}
        
        .empty-state {{
            background: white;
            border-radius: 12px;
            padding: 60px 30px;
            text-align: center;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        }}
        
        .empty-state h2 {{
            color: #6b7280;
            margin-bottom: 10px;
        }}
        
        .empty-state p {{
            color: #9ca3af;
        }}
        
        .refresh-btn {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            padding: 12px 24px;
            border-radius: 8px;
            font-size: 1em;
            font-weight: 600;
            cursor: pointer;
            margin-top: 20px;
            transition: opacity 0.2s;
        }}
        
        .refresh-btn:hover {{
            opacity: 0.9;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📰 Bluesky Domain Feed</h1>
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
            
            <button class="refresh-btn" onclick="location.reload()">🔄 Refresh Feed</button>
        </div>
"""
        
        if not ranked_posts:
            html_content += """
        <div class="empty-state">
            <h2>No posts yet</h2>
            <p>The feed is empty. Make sure the firehose listener is running to collect posts.</p>
        </div>
"""
        else:
            for i, post in enumerate(ranked_posts, 1):
                # Format timestamp
                created_at = post['created_at']
                if isinstance(created_at, str):
                    from dateutil import parser
                    created_at = parser.parse(created_at)
                
                time_ago = _format_time_ago(created_at)
                
                # Truncate author DID for display
                author_short = post['author_did'][:20] + '...' if len(post['author_did']) > 20 else post['author_did']
                
                # Get post text or use placeholder
                post_text = post.get('text', '').strip()
                if not post_text:
                    post_text = '<em>No text content</em>'
                
                # Format score
                score = post.get('score', 0)
                
                html_content += f"""
        <div class="post">
            <div class="post-header">
                <div class="post-meta">
                    <div class="post-author">👤 {author_short}</div>
                    <div class="post-time">🕐 {time_ago}</div>
                </div>
                <div class="post-score">⭐ {score:.2f}</div>
            </div>
            
            <div class="post-text">{post_text}</div>
            
            <div class="post-url">
                <a href="{post['url']}" target="_blank" rel="noopener noreferrer">
                    🔗 {post['url']}
                </a>
            </div>
            
            <div class="post-footer">
                <span class="badge badge-domain">📰 {post['domain']}</span>
                <span class="badge badge-shares">🔄 {post['share_count']} share{'s' if post['share_count'] != 1 else ''}</span>
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
