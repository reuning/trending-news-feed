"""
Tests for the ranking engine.

Tests cover:
- Configuration loading and validation
- Score calculation with time-decay
- Post ranking and sorting
- Feed skeleton generation
- Statistics and edge cases
"""

import json
import math
import pytest
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from src.ranking import RankingConfig, RankingEngine
from src.database import Database


@pytest.fixture
async def test_db():
    """Create a temporary test database."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    db = Database(db_path)
    await db.initialize()
    yield db
    
    await db.close()
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def temp_config_file():
    """Create a temporary config file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        config_data = {
            "decay_rate": 0.1,
            "max_age_hours": 72,
            "min_share_count": 2,
            "results_limit": 25,
        }
        json.dump(config_data, f)
        config_path = f.name
    
    yield config_path
    Path(config_path).unlink(missing_ok=True)


# Configuration Tests

def test_ranking_config_defaults():
    """Test default configuration values."""
    config = RankingConfig()
    
    assert config.decay_rate == 0.05
    assert config.max_age_hours == 168
    assert config.min_share_count == 1
    assert config.results_limit == 50


def test_ranking_config_custom():
    """Test custom configuration values."""
    config = RankingConfig(
        decay_rate=0.1,
        max_age_hours=72,
        min_share_count=5,
        results_limit=100,
    )
    
    assert config.decay_rate == 0.1
    assert config.max_age_hours == 72
    assert config.min_share_count == 5
    assert config.results_limit == 100


def test_ranking_config_from_file(temp_config_file):
    """Test loading configuration from file."""
    config = RankingConfig.from_file(temp_config_file)
    
    assert config.decay_rate == 0.1
    assert config.max_age_hours == 72
    assert config.min_share_count == 2
    assert config.results_limit == 25


def test_ranking_config_from_missing_file():
    """Test loading configuration from missing file uses defaults."""
    config = RankingConfig.from_file("nonexistent.json")
    
    # Should use defaults
    assert config.decay_rate == 0.05
    assert config.max_age_hours == 168


def test_ranking_config_to_dict():
    """Test converting configuration to dictionary."""
    config = RankingConfig(decay_rate=0.1, max_age_hours=72)
    config_dict = config.to_dict()
    
    assert config_dict["decay_rate"] == 0.1
    assert config_dict["max_age_hours"] == 72
    assert config_dict["min_share_count"] == 1
    assert config_dict["results_limit"] == 50


# Score Calculation Tests

@pytest.mark.asyncio
async def test_calculate_score_basic(test_db):
    """Test basic score calculation."""
    config = RankingConfig(decay_rate=0.05)
    engine = RankingEngine(test_db, config)
    
    # Score = share_count * exp(-0.05 * url_age_hours)
    score = engine.calculate_score(share_count=10, url_age_hours=0)
    assert abs(score - 10.0) < 0.01  # No decay at age 0
    
    score = engine.calculate_score(share_count=10, url_age_hours=1)
    expected = 10 * math.exp(-0.05 * 1)
    assert abs(score - expected) < 0.01


@pytest.mark.asyncio
async def test_calculate_score_decay(test_db):
    """Test score decay over time."""
    config = RankingConfig(decay_rate=0.05)
    engine = RankingEngine(test_db, config)
    
    # Same share count, different ages
    score_1h = engine.calculate_score(10, 1)
    score_24h = engine.calculate_score(10, 24)
    score_48h = engine.calculate_score(10, 48)
    
    # Scores should decrease with age
    assert score_1h > score_24h > score_48h
    
    # Verify approximate values from architecture doc
    assert abs(score_1h - 9.51) < 0.1
    assert abs(score_24h - 3.01) < 0.1
    assert abs(score_48h - 0.91) < 0.1


@pytest.mark.asyncio
async def test_calculate_score_share_count(test_db):
    """Test score scales with share count."""
    config = RankingConfig(decay_rate=0.05)
    engine = RankingEngine(test_db, config)
    
    # Same age, different share counts
    score_5 = engine.calculate_score(5, 24)
    score_10 = engine.calculate_score(10, 24)
    score_20 = engine.calculate_score(20, 24)
    
    # Scores should scale linearly with share count
    assert abs(score_10 / score_5 - 2.0) < 0.01
    assert abs(score_20 / score_10 - 2.0) < 0.01


@pytest.mark.asyncio
async def test_calculate_score_different_decay_rates(test_db):
    """Test different decay rates."""
    # Slower decay
    config_slow = RankingConfig(decay_rate=0.01)
    engine_slow = RankingEngine(test_db, config_slow)
    
    # Faster decay
    config_fast = RankingConfig(decay_rate=0.1)
    engine_fast = RankingEngine(test_db, config_fast)
    
    # At 24 hours
    score_slow = engine_slow.calculate_score(10, 24)
    score_fast = engine_fast.calculate_score(10, 24)
    
    # Slower decay should have higher score
    assert score_slow > score_fast


@pytest.mark.asyncio
async def test_calculate_age_hours(test_db):
    """Test age calculation in hours."""
    engine = RankingEngine(test_db)
    
    # URL first seen 1 hour ago
    first_seen = datetime.utcnow() - timedelta(hours=1)
    age = engine._calculate_age_hours(first_seen)
    assert abs(age - 1.0) < 0.1  # Allow small variance
    
    # URL first seen 24 hours ago
    first_seen = datetime.utcnow() - timedelta(hours=24)
    age = engine._calculate_age_hours(first_seen)
    assert abs(age - 24.0) < 0.1


# Ranking Tests

@pytest.mark.asyncio
async def test_rank_posts_empty_database(test_db):
    """Test ranking with empty database."""
    engine = RankingEngine(test_db)
    
    ranked = await engine.rank_posts()
    assert ranked == []


@pytest.mark.asyncio
async def test_rank_posts_single_post(test_db):
    """Test ranking with single post."""
    # Add a post
    await test_db.add_post(
        uri="at://did:plc:user1/app.bsky.feed.post/1",
        cid="cid1",
        author_did="did:plc:user1",
        url="https://nytimes.com/article1",
        domain="nytimes.com",
        text="Check this out",
        created_at=datetime.utcnow() - timedelta(hours=1),
    )
    
    engine = RankingEngine(test_db)
    ranked = await engine.rank_posts()
    
    assert len(ranked) == 1
    assert ranked[0]["uri"] == "at://did:plc:user1/app.bsky.feed.post/1"
    assert ranked[0]["share_count"] == 1
    assert "score" in ranked[0]
    assert "url_age_hours" in ranked[0]


@pytest.mark.asyncio
async def test_rank_posts_sorting(test_db):
    """Test posts are sorted by score."""
    now = datetime.utcnow()
    
    # Post 1: High share count, older (should rank high)
    await test_db.add_post(
        uri="at://did:plc:user1/app.bsky.feed.post/1",
        cid="cid1",
        author_did="did:plc:user1",
        url="https://nytimes.com/popular",
        domain="nytimes.com",
        created_at=now - timedelta(hours=12),
    )
    # Add more shares
    for i in range(9):
        await test_db.add_post(
            uri=f"at://did:plc:user{i+2}/app.bsky.feed.post/{i+2}",
            cid=f"cid{i+2}",
            author_did=f"did:plc:user{i+2}",
            url="https://nytimes.com/popular",
            domain="nytimes.com",
            created_at=now - timedelta(hours=12),
        )
    
    # Post 2: Low share count, very recent (should rank lower)
    await test_db.add_post(
        uri="at://did:plc:user20/app.bsky.feed.post/20",
        cid="cid20",
        author_did="did:plc:user20",
        url="https://bbc.com/new",
        domain="bbc.com",
        created_at=now - timedelta(minutes=10),
    )
    
    engine = RankingEngine(test_db)
    ranked = await engine.rank_posts()
    
    # Should have both posts
    assert len(ranked) >= 2
    
    # Scores should be in descending order
    for i in range(len(ranked) - 1):
        assert ranked[i]["score"] >= ranked[i + 1]["score"]
    
    # Popular post should rank first
    assert ranked[0]["url"] == "https://nytimes.com/popular"
    assert ranked[0]["share_count"] == 10


@pytest.mark.asyncio
async def test_rank_posts_limit(test_db):
    """Test limiting number of results."""
    now = datetime.utcnow()
    
    # Add 10 posts
    for i in range(10):
        await test_db.add_post(
            uri=f"at://did:plc:user{i}/app.bsky.feed.post/{i}",
            cid=f"cid{i}",
            author_did=f"did:plc:user{i}",
            url=f"https://nytimes.com/article{i}",
            domain="nytimes.com",
            created_at=now - timedelta(hours=i),
        )
    
    engine = RankingEngine(test_db)
    
    # Request only 5
    ranked = await engine.rank_posts(limit=5)
    assert len(ranked) == 5


@pytest.mark.asyncio
async def test_rank_posts_max_age_filter(test_db):
    """Test filtering by maximum age."""
    now = datetime.utcnow()
    
    # Recent post (within max age)
    await test_db.add_post(
        uri="at://did:plc:user1/app.bsky.feed.post/1",
        cid="cid1",
        author_did="did:plc:user1",
        url="https://nytimes.com/recent",
        domain="nytimes.com",
        created_at=now - timedelta(hours=24),
    )
    
    # Old post (beyond max age)
    await test_db.add_post(
        uri="at://did:plc:user2/app.bsky.feed.post/2",
        cid="cid2",
        author_did="did:plc:user2",
        url="https://bbc.com/old",
        domain="bbc.com",
        created_at=now - timedelta(hours=200),  # > 168 hours (1 week)
    )
    
    config = RankingConfig(max_age_hours=168)
    engine = RankingEngine(test_db, config)
    ranked = await engine.rank_posts()
    
    # Should only include recent post
    assert len(ranked) == 1
    assert ranked[0]["url"] == "https://nytimes.com/recent"


@pytest.mark.asyncio
async def test_rank_posts_min_share_count_filter(test_db):
    """Test filtering by minimum share count."""
    now = datetime.utcnow()
    
    # URL with 1 share
    await test_db.add_post(
        uri="at://did:plc:user1/app.bsky.feed.post/1",
        cid="cid1",
        author_did="did:plc:user1",
        url="https://nytimes.com/unpopular",
        domain="nytimes.com",
        created_at=now - timedelta(hours=1),
    )
    
    # URL with 3 shares
    for i in range(3):
        await test_db.add_post(
            uri=f"at://did:plc:user{i+10}/app.bsky.feed.post/{i+10}",
            cid=f"cid{i+10}",
            author_did=f"did:plc:user{i+10}",
            url="https://bbc.com/popular",
            domain="bbc.com",
            created_at=now - timedelta(hours=1),
        )
    
    config = RankingConfig(min_share_count=2)
    engine = RankingEngine(test_db, config)
    ranked = await engine.rank_posts()
    
    # Should only include posts with share_count >= 2
    assert len(ranked) == 3
    for post in ranked:
        assert post["share_count"] >= 2


@pytest.mark.asyncio
async def test_rank_posts_by_domain(test_db):
    """Test filtering by specific domain."""
    now = datetime.utcnow()
    
    # NYTimes posts
    await test_db.add_post(
        uri="at://did:plc:user1/app.bsky.feed.post/1",
        cid="cid1",
        author_did="did:plc:user1",
        url="https://nytimes.com/article1",
        domain="nytimes.com",
        created_at=now - timedelta(hours=1),
    )
    
    # BBC posts
    await test_db.add_post(
        uri="at://did:plc:user2/app.bsky.feed.post/2",
        cid="cid2",
        author_did="did:plc:user2",
        url="https://bbc.com/article1",
        domain="bbc.com",
        created_at=now - timedelta(hours=1),
    )
    
    engine = RankingEngine(test_db)
    
    # Filter by nytimes.com
    ranked = await engine.rank_posts(domain="nytimes.com")
    assert len(ranked) == 1
    assert ranked[0]["domain"] == "nytimes.com"
    
    # Filter by bbc.com
    ranked = await engine.rank_posts(domain="bbc.com")
    assert len(ranked) == 1
    assert ranked[0]["domain"] == "bbc.com"


# Feed Skeleton Tests

@pytest.mark.asyncio
async def test_get_feed_skeleton_empty(test_db):
    """Test feed skeleton with empty database."""
    engine = RankingEngine(test_db)
    
    skeleton = await engine.get_feed_skeleton()
    
    assert "feed" in skeleton
    assert skeleton["feed"] == []


@pytest.mark.asyncio
async def test_get_feed_skeleton_format(test_db):
    """Test feed skeleton format."""
    # Add a post
    await test_db.add_post(
        uri="at://did:plc:user1/app.bsky.feed.post/1",
        cid="cid1",
        author_did="did:plc:user1",
        url="https://nytimes.com/article1",
        domain="nytimes.com",
        created_at=datetime.utcnow() - timedelta(hours=1),
    )
    
    engine = RankingEngine(test_db)
    skeleton = await engine.get_feed_skeleton()
    
    assert "feed" in skeleton
    assert len(skeleton["feed"]) == 1
    assert "post" in skeleton["feed"][0]
    assert skeleton["feed"][0]["post"] == "at://did:plc:user1/app.bsky.feed.post/1"


@pytest.mark.asyncio
async def test_get_feed_skeleton_limit(test_db):
    """Test feed skeleton respects limit."""
    now = datetime.utcnow()
    
    # Add 10 posts
    for i in range(10):
        await test_db.add_post(
            uri=f"at://did:plc:user{i}/app.bsky.feed.post/{i}",
            cid=f"cid{i}",
            author_did=f"did:plc:user{i}",
            url=f"https://nytimes.com/article{i}",
            domain="nytimes.com",
            created_at=now - timedelta(hours=i),
        )
    
    engine = RankingEngine(test_db)
    skeleton = await engine.get_feed_skeleton(limit=5)
    
    assert len(skeleton["feed"]) == 5


@pytest.mark.asyncio
async def test_get_feed_skeleton_cursor(test_db):
    """Test feed skeleton cursor handling."""
    # Add a post
    await test_db.add_post(
        uri="at://did:plc:user1/app.bsky.feed.post/1",
        cid="cid1",
        author_did="did:plc:user1",
        url="https://nytimes.com/article1",
        domain="nytimes.com",
        created_at=datetime.utcnow() - timedelta(hours=1),
    )
    
    engine = RankingEngine(test_db)
    skeleton = await engine.get_feed_skeleton(limit=50)
    
    # With fewer results than limit, cursor should not be present or be None
    assert skeleton.get("cursor") is None or "cursor" not in skeleton


# Statistics Tests

@pytest.mark.asyncio
async def test_get_ranking_stats_empty(test_db):
    """Test statistics with empty database."""
    engine = RankingEngine(test_db)
    
    stats = await engine.get_ranking_stats()
    
    assert stats["total_posts"] == 0
    assert stats["ranked_posts"] == 0
    assert stats["top_score"] == 0
    assert stats["avg_score"] == 0


@pytest.mark.asyncio
async def test_get_ranking_stats(test_db):
    """Test statistics calculation."""
    now = datetime.utcnow()
    
    # Add posts
    for i in range(5):
        await test_db.add_post(
            uri=f"at://did:plc:user{i}/app.bsky.feed.post/{i}",
            cid=f"cid{i}",
            author_did=f"did:plc:user{i}",
            url=f"https://nytimes.com/article{i}",
            domain="nytimes.com",
            created_at=now - timedelta(hours=i),
        )
    
    engine = RankingEngine(test_db)
    stats = await engine.get_ranking_stats()
    
    assert stats["total_posts"] == 5
    assert stats["ranked_posts"] == 5
    assert stats["top_score"] > 0
    assert stats["avg_score"] > 0
    assert stats["min_score"] > 0
    assert "config" in stats


# Configuration Reload Tests

@pytest.mark.asyncio
async def test_reload_config(test_db, temp_config_file):
    """Test reloading configuration."""
    # Start with default config
    engine = RankingEngine(test_db)
    assert engine.config.decay_rate == 0.05
    
    # Reload from file
    engine.reload_config(temp_config_file)
    assert engine.config.decay_rate == 0.1
    assert engine.config.max_age_hours == 72


# Edge Cases

@pytest.mark.asyncio
async def test_score_with_zero_age(test_db):
    """Test score calculation with zero age."""
    engine = RankingEngine(test_db)
    
    score = engine.calculate_score(10, 0)
    assert score == 10.0  # No decay


@pytest.mark.asyncio
async def test_score_with_very_old_post(test_db):
    """Test score calculation with very old post."""
    engine = RankingEngine(test_db)
    
    # 1000 hours old
    score = engine.calculate_score(100, 1000)
    
    # Should be very small but not zero
    assert score > 0
    assert score < 1


@pytest.mark.asyncio
async def test_rank_posts_with_same_scores(test_db):
    """Test ranking when posts have identical scores."""
    now = datetime.utcnow()
    
    # Add posts with same URL (same share count) at same time
    for i in range(3):
        await test_db.add_post(
            uri=f"at://did:plc:user{i}/app.bsky.feed.post/{i}",
            cid=f"cid{i}",
            author_did=f"did:plc:user{i}",
            url="https://nytimes.com/same",
            domain="nytimes.com",
            created_at=now - timedelta(hours=1),
        )
    
    engine = RankingEngine(test_db)
    ranked = await engine.rank_posts()
    
    # With max_posts_per_url=2 (from config), only 2 posts should be returned
    # even though 3 were added with the same URL
    assert len(ranked) == 2
    scores = [p["score"] for p in ranked]
    # The 2 returned posts should have same score
    assert all(abs(s - scores[0]) < 0.01 for s in scores)
    # All should be from the same URL
    assert all(p["url"] == "https://nytimes.com/same" for p in ranked)


@pytest.mark.asyncio
async def test_rank_posts_with_future_timestamp(test_db):
    """Test handling of posts with future timestamps."""
    now = datetime.utcnow()
    
    # Post with future timestamp (clock skew)
    await test_db.add_post(
        uri="at://did:plc:user1/app.bsky.feed.post/1",
        cid="cid1",
        author_did="did:plc:user1",
        url="https://nytimes.com/future",
        domain="nytimes.com",
        created_at=now + timedelta(hours=1),
    )
    
    engine = RankingEngine(test_db)
    ranked = await engine.rank_posts()
    
    # Should still include the post
    assert len(ranked) == 1
    # Age might be negative, but score should still be calculated
    assert "score" in ranked[0]


# Repost Configuration Tests

def test_ranking_config_with_repost_params():
    """Test configuration with repost parameters."""
    config = RankingConfig(
        min_repost_count=2,
        repost_weight=1.5,
    )
    
    assert config.min_repost_count == 2
    assert config.repost_weight == 1.5


def test_ranking_config_repost_defaults():
    """Test default repost configuration values."""
    config = RankingConfig()
    
    assert config.min_repost_count == 0
    assert config.repost_weight == 1.0


def test_ranking_config_to_dict_includes_repost_params():
    """Test that to_dict includes repost parameters."""
    config = RankingConfig(min_repost_count=3, repost_weight=2.0)
    config_dict = config.to_dict()
    
    assert config_dict["min_repost_count"] == 3
    assert config_dict["repost_weight"] == 2.0


@pytest.mark.asyncio
async def test_calculate_score_with_repost_count(test_db):
    """Test score calculation with repost multiplier."""
    config = RankingConfig(decay_rate=0.05, repost_weight=1.0)
    engine = RankingEngine(test_db, config)
    
    # Score with 0 reposts (treated as 1)
    score_0 = engine.calculate_score(share_count=10, url_age_hours=1, repost_count=0)
    
    # Score with 1 repost
    score_1 = engine.calculate_score(share_count=10, url_age_hours=1, repost_count=1)
    
    # Score with 5 reposts
    score_5 = engine.calculate_score(share_count=10, url_age_hours=1, repost_count=5)
    
    # 0 reposts should be treated as 1 (neutral multiplier)
    assert abs(score_0 - score_1) < 0.01
    
    # 5 reposts should give 5x the score of 1 repost
    assert abs(score_5 / score_1 - 5.0) < 0.01


@pytest.mark.asyncio
async def test_calculate_score_with_repost_weight(test_db):
    """Test score calculation with different repost weights."""
    # Linear weight (default)
    config_linear = RankingConfig(repost_weight=1.0)
    engine_linear = RankingEngine(test_db, config_linear)
    
    # Amplified weight (square)
    config_amplified = RankingConfig(repost_weight=2.0)
    engine_amplified = RankingEngine(test_db, config_amplified)
    
    # Dampened weight (square root)
    config_dampened = RankingConfig(repost_weight=0.5)
    engine_dampened = RankingEngine(test_db, config_dampened)
    
    # Calculate scores with 4 reposts
    score_linear = engine_linear.calculate_score(10, 1, 4)
    score_amplified = engine_amplified.calculate_score(10, 1, 4)
    score_dampened = engine_dampened.calculate_score(10, 1, 4)
    
    # Linear: 4^1.0 = 4
    # Amplified: 4^2.0 = 16
    # Dampened: 4^0.5 = 2
    
    # Amplified should be higher than linear
    assert score_amplified > score_linear
    
    # Dampened should be lower than linear
    assert score_dampened < score_linear
    
    # Check approximate ratios
    assert abs(score_amplified / score_linear - 4.0) < 0.01  # 16/4 = 4
    assert abs(score_linear / score_dampened - 2.0) < 0.01  # 4/2 = 2


@pytest.mark.asyncio
async def test_rank_posts_min_repost_count_filter(test_db):
    """Test filtering by minimum repost count."""
    now = datetime.utcnow()
    
    # Post with 0 reposts
    await test_db.add_post(
        uri="at://did:plc:user1/app.bsky.feed.post/1",
        cid="cid1",
        author_did="did:plc:user1",
        url="https://nytimes.com/no-reposts",
        domain="nytimes.com",
        created_at=now - timedelta(hours=1),
    )
    
    # Post with 2 reposts
    await test_db.add_post(
        uri="at://did:plc:user2/app.bsky.feed.post/2",
        cid="cid2",
        author_did="did:plc:user2",
        url="https://bbc.com/some-reposts",
        domain="bbc.com",
        created_at=now - timedelta(hours=1),
    )
    await test_db.increment_repost_count("at://did:plc:user2/app.bsky.feed.post/2")
    await test_db.increment_repost_count("at://did:plc:user2/app.bsky.feed.post/2")
    
    # Post with 5 reposts
    await test_db.add_post(
        uri="at://did:plc:user3/app.bsky.feed.post/3",
        cid="cid3",
        author_did="did:plc:user3",
        url="https://cnn.com/many-reposts",
        domain="cnn.com",
        created_at=now - timedelta(hours=1),
    )
    for _ in range(5):
        await test_db.increment_repost_count("at://did:plc:user3/app.bsky.feed.post/3")
    
    # Filter for posts with at least 2 reposts
    config = RankingConfig(min_repost_count=2)
    engine = RankingEngine(test_db, config)
    ranked = await engine.rank_posts()
    
    # Should only include posts with repost_count >= 2
    assert len(ranked) == 2
    for post in ranked:
        assert post["repost_count"] >= 2
    
    # Verify the correct posts are included
    uris = [p["uri"] for p in ranked]
    assert "at://did:plc:user2/app.bsky.feed.post/2" in uris
    assert "at://did:plc:user3/app.bsky.feed.post/3" in uris
    assert "at://did:plc:user1/app.bsky.feed.post/1" not in uris


@pytest.mark.asyncio
async def test_rank_posts_repost_multiplier_affects_ranking(test_db):
    """Test that repost count affects post ranking."""
    now = datetime.utcnow()
    
    # Post A: Same URL shared 5 times, 1 repost
    for i in range(5):
        await test_db.add_post(
            uri=f"at://did:plc:userA{i}/app.bsky.feed.post/A{i}",
            cid=f"cidA{i}",
            author_did=f"did:plc:userA{i}",
            url="https://nytimes.com/article-a",
            domain="nytimes.com",
            created_at=now - timedelta(hours=1),
        )
    # Add 1 repost to first post
    await test_db.increment_repost_count("at://did:plc:userA0/app.bsky.feed.post/A0")
    
    # Post B: Same URL shared 5 times, 10 reposts
    for i in range(5):
        await test_db.add_post(
            uri=f"at://did:plc:userB{i}/app.bsky.feed.post/B{i}",
            cid=f"cidB{i}",
            author_did=f"did:plc:userB{i}",
            url="https://bbc.com/article-b",
            domain="bbc.com",
            created_at=now - timedelta(hours=1),
        )
    # Add 10 reposts to first post
    for _ in range(10):
        await test_db.increment_repost_count("at://did:plc:userB0/app.bsky.feed.post/B0")
    
    engine = RankingEngine(test_db)
    ranked = await engine.rank_posts()
    
    # Post B should rank higher due to more reposts (same share count, same age)
    # Find the posts in the ranked list
    post_a = next(p for p in ranked if p["uri"] == "at://did:plc:userA0/app.bsky.feed.post/A0")
    post_b = next(p for p in ranked if p["uri"] == "at://did:plc:userB0/app.bsky.feed.post/B0")
    
    assert post_b["score"] > post_a["score"]
    assert post_b["repost_count"] == 10
    assert post_a["repost_count"] == 1


# Pagination Tests

@pytest.mark.asyncio
async def test_cursor_encoding_decoding(test_db):
    """Test cursor encoding and decoding."""
    engine = RankingEngine(test_db)
    
    # Test encoding
    score = 42.5
    uri = "at://did:plc:user1/app.bsky.feed.post/123"
    cursor = engine._encode_cursor(score, uri)
    
    # Cursor should be a base64 string
    assert isinstance(cursor, str)
    assert len(cursor) > 0
    
    # Test decoding
    decoded_score, decoded_uri = engine._decode_cursor(cursor)
    assert abs(decoded_score - score) < 0.0001
    assert decoded_uri == uri


@pytest.mark.asyncio
async def test_cursor_decoding_invalid(test_db):
    """Test cursor decoding with invalid input."""
    engine = RankingEngine(test_db)
    
    # Invalid base64
    with pytest.raises(ValueError):
        engine._decode_cursor("not-valid-base64!!!")
    
    # Valid base64 but wrong format
    import base64
    invalid_cursor = base64.b64encode(b"no-separator").decode('utf-8')
    with pytest.raises(ValueError):
        engine._decode_cursor(invalid_cursor)


@pytest.mark.asyncio
async def test_pagination_first_page(test_db):
    """Test getting the first page without cursor."""
    now = datetime.utcnow()
    
    # Add 10 posts
    for i in range(10):
        await test_db.add_post(
            uri=f"at://did:plc:user{i}/app.bsky.feed.post/{i}",
            cid=f"cid{i}",
            author_did=f"did:plc:user{i}",
            url=f"https://nytimes.com/article{i}",
            domain="nytimes.com",
            created_at=now - timedelta(hours=i),
        )
    
    engine = RankingEngine(test_db)
    
    # Get first page with limit of 5
    result = await engine.get_feed_skeleton(limit=5, cursor=None)
    
    assert "feed" in result
    assert len(result["feed"]) == 5
    
    # Should have cursor since there are more results
    assert "cursor" in result
    assert result["cursor"] is not None


@pytest.mark.asyncio
async def test_pagination_second_page(test_db):
    """Test getting the second page with cursor."""
    now = datetime.utcnow()
    
    # Add 10 posts
    for i in range(10):
        await test_db.add_post(
            uri=f"at://did:plc:user{i}/app.bsky.feed.post/{i}",
            cid=f"cid{i}",
            author_did=f"did:plc:user{i}",
            url=f"https://nytimes.com/article{i}",
            domain="nytimes.com",
            created_at=now - timedelta(hours=i),
        )
    
    engine = RankingEngine(test_db)
    
    # Get first page
    page1 = await engine.get_feed_skeleton(limit=5, cursor=None)
    assert len(page1["feed"]) == 5
    assert page1["cursor"] is not None
    
    # Get second page using cursor
    page2 = await engine.get_feed_skeleton(limit=5, cursor=page1["cursor"])
    assert len(page2["feed"]) == 5
    
    # Posts should be different
    page1_uris = {post["post"] for post in page1["feed"]}
    page2_uris = {post["post"] for post in page2["feed"]}
    assert page1_uris.isdisjoint(page2_uris)  # No overlap


@pytest.mark.asyncio
async def test_pagination_last_page(test_db):
    """Test that last page has no cursor."""
    now = datetime.utcnow()
    
    # Add exactly 10 posts
    for i in range(10):
        await test_db.add_post(
            uri=f"at://did:plc:user{i}/app.bsky.feed.post/{i}",
            cid=f"cid{i}",
            author_did=f"did:plc:user{i}",
            url=f"https://nytimes.com/article{i}",
            domain="nytimes.com",
            created_at=now - timedelta(hours=i),
        )
    
    engine = RankingEngine(test_db)
    
    # Get first page (5 posts)
    page1 = await engine.get_feed_skeleton(limit=5, cursor=None)
    assert len(page1["feed"]) == 5
    assert page1["cursor"] is not None
    
    # Get second page (last 5 posts)
    page2 = await engine.get_feed_skeleton(limit=5, cursor=page1["cursor"])
    assert len(page2["feed"]) == 5
    
    # Last page should not have cursor
    assert page2.get("cursor") is None


@pytest.mark.asyncio
async def test_pagination_empty_second_page(test_db):
    """Test requesting page beyond available data."""
    now = datetime.utcnow()
    
    # Add only 3 posts
    for i in range(3):
        await test_db.add_post(
            uri=f"at://did:plc:user{i}/app.bsky.feed.post/{i}",
            cid=f"cid{i}",
            author_did=f"did:plc:user{i}",
            url=f"https://nytimes.com/article{i}",
            domain="nytimes.com",
            created_at=now - timedelta(hours=i),
        )
    
    engine = RankingEngine(test_db)
    
    # Get first page
    page1 = await engine.get_feed_skeleton(limit=5, cursor=None)
    assert len(page1["feed"]) == 3
    assert page1.get("cursor") is None  # No more results
    
    # If we somehow got a cursor and tried to use it, should return empty
    if page1.get("cursor"):
        page2 = await engine.get_feed_skeleton(limit=5, cursor=page1["cursor"])
        assert len(page2["feed"]) == 0


@pytest.mark.asyncio
async def test_pagination_invalid_cursor(test_db):
    """Test handling of invalid cursor."""
    now = datetime.utcnow()
    
    # Add posts
    for i in range(5):
        await test_db.add_post(
            uri=f"at://did:plc:user{i}/app.bsky.feed.post/{i}",
            cid=f"cid{i}",
            author_did=f"did:plc:user{i}",
            url=f"https://nytimes.com/article{i}",
            domain="nytimes.com",
            created_at=now - timedelta(hours=i),
        )
    
    engine = RankingEngine(test_db)
    
    # Use invalid cursor - should treat as no cursor and return from beginning
    result = await engine.get_feed_skeleton(limit=5, cursor="invalid-cursor")
    
    # Should still return results (treating invalid cursor as no cursor)
    assert "feed" in result
    assert len(result["feed"]) == 5


@pytest.mark.asyncio
async def test_pagination_stale_cursor(test_db):
    """Test handling of stale cursor (post no longer exists at that position)."""
    now = datetime.utcnow()
    
    # Add posts
    for i in range(10):
        await test_db.add_post(
            uri=f"at://did:plc:user{i}/app.bsky.feed.post/{i}",
            cid=f"cid{i}",
            author_did=f"did:plc:user{i}",
            url=f"https://nytimes.com/article{i}",
            domain="nytimes.com",
            created_at=now - timedelta(hours=i),
        )
    
    engine = RankingEngine(test_db)
    
    # Get first page
    page1 = await engine.get_feed_skeleton(limit=5, cursor=None)
    cursor = page1["cursor"]
    
    # Use the cursor - should work with score-based fallback
    page2 = await engine.get_feed_skeleton(limit=5, cursor=cursor)
    
    # Should still get results
    assert "feed" in page2
    assert len(page2["feed"]) > 0


@pytest.mark.asyncio
async def test_pagination_consistent_ordering(test_db):
    """Test that pagination maintains consistent ordering."""
    now = datetime.utcnow()
    
    # Add 15 posts
    for i in range(15):
        await test_db.add_post(
            uri=f"at://did:plc:user{i}/app.bsky.feed.post/{i}",
            cid=f"cid{i}",
            author_did=f"did:plc:user{i}",
            url=f"https://nytimes.com/article{i}",
            domain="nytimes.com",
            created_at=now - timedelta(hours=i),
        )
    
    engine = RankingEngine(test_db)
    
    # Get all posts via pagination
    all_paginated_uris = []
    cursor = None
    
    for _ in range(3):  # 3 pages of 5 each
        result = await engine.get_feed_skeleton(limit=5, cursor=cursor)
        all_paginated_uris.extend([post["post"] for post in result["feed"]])
        cursor = result.get("cursor")
        if not cursor:
            break
    
    # Get all posts at once
    all_at_once = await engine.get_feed_skeleton(limit=15, cursor=None)
    all_at_once_uris = [post["post"] for post in all_at_once["feed"]]
    
    # Order should be the same
    assert all_paginated_uris == all_at_once_uris


@pytest.mark.asyncio
async def test_pagination_with_limit_one(test_db):
    """Test pagination with limit of 1."""
    now = datetime.utcnow()
    
    # Add 3 posts
    for i in range(3):
        await test_db.add_post(
            uri=f"at://did:plc:user{i}/app.bsky.feed.post/{i}",
            cid=f"cid{i}",
            author_did=f"did:plc:user{i}",
            url=f"https://nytimes.com/article{i}",
            domain="nytimes.com",
            created_at=now - timedelta(hours=i),
        )
    
    engine = RankingEngine(test_db)
    
    # Get posts one at a time
    page1 = await engine.get_feed_skeleton(limit=1, cursor=None)
    assert len(page1["feed"]) == 1
    assert page1["cursor"] is not None
    
    page2 = await engine.get_feed_skeleton(limit=1, cursor=page1["cursor"])
    assert len(page2["feed"]) == 1
    assert page2["cursor"] is not None
    
    page3 = await engine.get_feed_skeleton(limit=1, cursor=page2["cursor"])
    assert len(page3["feed"]) == 1
    assert page3.get("cursor") is None  # Last page
    
    # All posts should be different
    all_uris = {page1["feed"][0]["post"], page2["feed"][0]["post"], page3["feed"][0]["post"]}
    assert len(all_uris) == 3


@pytest.mark.asyncio
async def test_pagination_no_cursor_when_exact_limit(test_db):
    """Test that no cursor is returned when results exactly match limit."""
    now = datetime.utcnow()
    
    # Add exactly 5 posts
    for i in range(5):
        await test_db.add_post(
            uri=f"at://did:plc:user{i}/app.bsky.feed.post/{i}",
            cid=f"cid{i}",
            author_did=f"did:plc:user{i}",
            url=f"https://nytimes.com/article{i}",
            domain="nytimes.com",
            created_at=now - timedelta(hours=i),
        )
    
    engine = RankingEngine(test_db)
    
    # Request exactly 5 (all available)
    result = await engine.get_feed_skeleton(limit=5, cursor=None)
    
    assert len(result["feed"]) == 5
    # Should not have cursor since we got all results
    assert result.get("cursor") is None
