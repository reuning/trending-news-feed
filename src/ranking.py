"""
Ranking engine for the Bluesky domain-based feed generator.

This module implements a time-decay ranking algorithm that balances
URL share count, post repost count, and post freshness. Posts are scored using:
    score = repost_count * share_count * exp(-decay_rate * age_in_hours)

This ensures:
- Popular URLs (high share count) rank higher
- Popular posts (high repost count) rank higher
- Recent posts get boosted
- Very old posts decay even if popular
"""

import json
import logging
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

from .database import Database

logger = logging.getLogger(__name__)


class RankingConfig:
    """Configuration for the ranking algorithm."""
    
    def __init__(
        self,
        decay_rate: float = 0.05,
        max_age_hours: int = 168,
        min_share_count: int = 1,
        min_repost_count: int = 0,
        repost_weight: float = 1.0,
        results_limit: int = 50,
    ):
        """
        Initialize ranking configuration.
        
        Args:
            decay_rate: Exponential decay rate (lambda). Higher = faster decay.
                       Default 0.05 means ~60% score after 10 hours.
            max_age_hours: Maximum age of posts to include (in hours).
                          Default 168 = 1 week.
            min_share_count: Minimum share count to include in results.
                            Default 1 = include all posts.
            min_repost_count: Minimum repost count to include in results.
                             Default 0 = include all posts.
            repost_weight: Multiplier for repost influence on score.
                          Default 1.0 = normal influence.
                          Higher values increase repost importance.
            results_limit: Maximum number of posts to return.
                          Default 50.
        """
        self.decay_rate = decay_rate
        self.max_age_hours = max_age_hours
        self.min_share_count = min_share_count
        self.min_repost_count = min_repost_count
        self.repost_weight = repost_weight
        self.results_limit = results_limit
    
    @classmethod
    def from_file(cls, config_path: str = "config/ranking.json") -> "RankingConfig":
        """
        Load configuration from JSON file.
        
        Args:
            config_path: Path to ranking configuration file
        
        Returns:
            RankingConfig instance
        
        Raises:
            FileNotFoundError: If config file doesn't exist
            json.JSONDecodeError: If config file is invalid JSON
        """
        path = Path(config_path)
        if not path.exists():
            logger.warning(f"Config file {config_path} not found, using defaults")
            return cls()
        
        with open(path, 'r') as f:
            config_data = json.load(f)
        
        logger.info(f"Loaded ranking config from {config_path}")
        return cls(
            decay_rate=config_data.get("decay_rate", 0.05),
            max_age_hours=config_data.get("max_age_hours", 168),
            min_share_count=config_data.get("min_share_count", 1),
            min_repost_count=config_data.get("min_repost_count", 0),
            repost_weight=config_data.get("repost_weight", 1.0),
            results_limit=config_data.get("results_limit", 50),
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "decay_rate": self.decay_rate,
            "max_age_hours": self.max_age_hours,
            "min_share_count": self.min_share_count,
            "min_repost_count": self.min_repost_count,
            "repost_weight": self.repost_weight,
            "results_limit": self.results_limit,
        }


class RankingEngine:
    """
    Ranking engine that scores posts using time-decay algorithm with repost multiplier.
    
    The algorithm balances URL popularity, post popularity, and freshness:
        score = repost_count * share_count * exp(-decay_rate * age_in_hours)
    
    This ensures:
    - Popular URLs (high share count) rank higher
    - Popular posts (high repost count) rank higher
    - Recent posts get boosted
    - Very old posts decay even if popular
    - Balance between URL virality, post virality, and freshness
    """
    
    def __init__(
        self,
        database: Database,
        config: Optional[RankingConfig] = None,
    ):
        """
        Initialize ranking engine.
        
        Args:
            database: Database instance for querying posts
            config: Ranking configuration (loads from file if not provided)
        """
        self.database = database
        self.config = config or RankingConfig.from_file()
        logger.info(f"Ranking engine initialized with config: {self.config.to_dict()}")
    
    def calculate_score(
        self,
        share_count: int,
        age_hours: float,
        repost_count: int = 0,
    ) -> float:
        """
        Calculate time-decay score for a post with repost multiplier.
        
        Formula: score = (repost_count ^ repost_weight) * share_count * exp(-decay_rate * age_hours)
        
        The repost_weight parameter allows tuning the influence of reposts:
        - repost_weight = 1.0: Linear influence (default)
        - repost_weight > 1.0: Amplifies repost influence (e.g., 2.0 squares the count)
        - repost_weight < 1.0: Dampens repost influence (e.g., 0.5 takes square root)
        
        Args:
            share_count: Number of times the URL has been shared
            age_hours: Age of the post in hours
            repost_count: Number of times this specific post has been reposted
        
        Returns:
            Calculated score (float)
        
        Examples:
            >>> engine = RankingEngine(db)  # default repost_weight=1.0
            >>> engine.calculate_score(10, 1, 5)   # 10 shares, 1 hour old, 5 reposts
            47.55  # 5^1.0 * 10 * exp(-0.05 * 1)
            >>> engine.calculate_score(10, 24, 1)  # 10 shares, 24 hours old, 1 repost
            3.01   # 1^1.0 * 10 * exp(-0.05 * 24)
            >>> engine.calculate_score(5, 1, 0)    # 5 shares, 1 hour old, 0 reposts
            4.76   # max(1, 0)^1.0 * 5 * exp(-0.05 * 1)
        """
        # Exponential decay: e^(-λt)
        decay_factor = math.exp(-self.config.decay_rate * age_hours)
        
        # Use max(1, repost_count) to ensure posts with 0 reposts still get scored
        # This way a post with 0 reposts acts as if it has 1 repost (neutral multiplier)
        effective_repost_count = max(1, repost_count)
        
        # Apply repost weight to adjust influence
        # repost_weight = 1.0 means linear, > 1.0 amplifies, < 1.0 dampens
        weighted_repost_count = math.pow(effective_repost_count, self.config.repost_weight)
        
        score = weighted_repost_count * share_count * decay_factor
        return score
    
    def _calculate_age_hours(self, created_at: datetime) -> float:
        """
        Calculate age of a post in hours.
        
        Args:
            created_at: Post creation timestamp
        
        Returns:
            Age in hours (float)
        """
        now = datetime.utcnow()
        age = now - created_at
        return age.total_seconds() / 3600  # Convert to hours
    
    async def rank_posts(
        self,
        limit: Optional[int] = None,
        domain: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get ranked posts using time-decay algorithm with repost multiplier.
        
        This method:
        1. Queries recent posts from database
        2. Filters by age and minimum share count
        3. Calculates score for each post (including repost multiplier)
        4. Sorts by score (highest first)
        5. Returns top N posts
        
        Args:
            limit: Maximum number of posts to return (uses config default if None)
            domain: Optional domain filter (returns posts from specific domain only)
        
        Returns:
            List of post dictionaries with scores, sorted by score descending.
            Each dict contains:
                - uri: Post URI
                - cid: Content ID
                - author_did: Author DID
                - text: Post text
                - created_at: Creation timestamp
                - url: URL from post
                - domain: URL domain
                - share_count: Number of shares
                - repost_count: Number of reposts
                - age_hours: Age in hours
                - score: Calculated ranking score
        """
        if limit is None:
            limit = self.config.results_limit
        
        # Query recent posts from database
        if domain:
            posts = await self.database.get_posts_by_domain(
                domain=domain,
                limit=1000,  # Get more than needed for filtering
            )
        else:
            posts = await self.database.get_recent_posts(
                hours=self.config.max_age_hours,
                limit=1000,  # Get more than needed for filtering
            )
        
        logger.debug(f"Retrieved {len(posts)} posts for ranking")
        
        # Calculate scores and filter
        scored_posts = []
        for post in posts:
            # Calculate age
            age_hours = self._calculate_age_hours(post["created_at"])
            
            # Filter by max age
            if age_hours > self.config.max_age_hours:
                continue
            
            # Filter by minimum share count
            if post["share_count"] < self.config.min_share_count:
                continue
            
            # Filter by minimum repost count
            if post.get("repost_count", 0) < self.config.min_repost_count:
                continue
            
            # Calculate score
            score = self.calculate_score(
                post["share_count"],
                age_hours,
                post.get("repost_count", 0)
            )
            
            # Add score and age to post data
            scored_post = {
                **post,
                "age_hours": age_hours,
                "score": score,
            }
            scored_posts.append(scored_post)
        
        # Sort by score (highest first)
        scored_posts.sort(key=lambda p: p["score"], reverse=True)
        
        # Limit results
        ranked_posts = scored_posts[:limit]
        
        logger.info(
            f"Ranked {len(scored_posts)} posts, returning top {len(ranked_posts)}"
        )
        
        return ranked_posts
    
    async def get_feed_skeleton(
        self,
        limit: int = 50,
        cursor: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get feed skeleton for AT Protocol feed endpoint.
        
        This is the main method called by the feed server to generate
        the feed response. It returns post URIs in ranked order.
        
        Args:
            limit: Maximum number of posts to return
            cursor: Pagination cursor (not yet implemented)
        
        Returns:
            Dictionary with:
                - feed: List of dicts with 'post' key containing URI
                - cursor: Pagination cursor (optional)
        
        Example response:
            {
                "feed": [
                    {"post": "at://did:plc:abc/app.bsky.feed.post/123"},
                    {"post": "at://did:plc:def/app.bsky.feed.post/456"},
                ],
                "cursor": None
            }
        """
        # Get ranked posts
        ranked_posts = await self.rank_posts(limit=limit)
        
        # Format for AT Protocol
        feed = [{"post": post["uri"]} for post in ranked_posts]
        
        # TODO: Implement cursor-based pagination
        # For now, return None cursor
        response = {
            "feed": feed,
        }
        
        # Only include cursor if there might be more results
        if len(feed) >= limit:
            # In future, encode last post's score/timestamp as cursor
            response["cursor"] = None
        
        logger.info(f"Generated feed skeleton with {len(feed)} posts")
        return response
    
    async def get_ranking_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the ranking engine.
        
        Returns:
            Dictionary with ranking statistics:
                - config: Current configuration
                - total_posts: Total posts in database
                - ranked_posts: Number of posts that would be ranked
                - top_score: Highest score in current ranking
                - avg_score: Average score of ranked posts
        """
        # Get all ranked posts
        ranked_posts = await self.rank_posts(limit=None)
        
        # Calculate statistics
        scores = [p["score"] for p in ranked_posts]
        
        stats = {
            "config": self.config.to_dict(),
            "total_posts": len(ranked_posts),
            "ranked_posts": len(ranked_posts),
        }
        
        if scores:
            stats["top_score"] = max(scores)
            stats["avg_score"] = sum(scores) / len(scores)
            stats["min_score"] = min(scores)
        else:
            stats["top_score"] = 0
            stats["avg_score"] = 0
            stats["min_score"] = 0
        
        return stats
    
    def reload_config(self, config_path: str = "config/ranking.json"):
        """
        Reload configuration from file.
        
        Args:
            config_path: Path to ranking configuration file
        """
        self.config = RankingConfig.from_file(config_path)
        logger.info(f"Reloaded ranking config: {self.config.to_dict()}")


# Convenience function for creating ranking engine
async def create_ranking_engine(
    db_path: str = "data/feed.db",
    config_path: str = "config/ranking.json",
) -> RankingEngine:
    """
    Create and return a RankingEngine instance.
    
    Args:
        db_path: Path to SQLite database file
        config_path: Path to ranking configuration file
    
    Returns:
        RankingEngine instance
    """
    from .database import Database
    
    database = Database(db_path)
    await database.initialize()
    
    config = RankingConfig.from_file(config_path)
    engine = RankingEngine(database, config)
    
    return engine
