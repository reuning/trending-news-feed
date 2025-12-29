"""
Tests for database operations.

Tests cover:
- Database initialization
- Post and URL creation
- Share count tracking
- Querying operations
- Statistics and cleanup
"""

import pytest
import tempfile
import os
from datetime import datetime, timedelta
from pathlib import Path

from src.database import Database, create_database, Post, URL, PostURL


@pytest.fixture
async def temp_db():
    """Create a temporary database for testing."""
    # Create temporary file
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    
    # Create database instance
    db = Database(path)
    await db.initialize()
    
    yield db
    
    # Cleanup
    await db.close()
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def sample_post_data():
    """Sample post data for testing."""
    return {
        "uri": "at://did:plc:test123/app.bsky.feed.post/abc123",
        "cid": "bafytest123",
        "author_did": "did:plc:test123",
        "url": "https://nytimes.com/article/test",
        "domain": "nytimes.com",
        "text": "Check out this article!",
        "created_at": datetime.utcnow(),
    }


class TestDatabaseInitialization:
    """Test database initialization and setup."""
    
    @pytest.mark.asyncio
    async def test_database_creation(self):
        """Test that database file is created."""
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(path)  # Remove it so we can test creation
        
        db = Database(path)
        await db.initialize()
        
        assert os.path.exists(path)
        
        await db.close()
        os.unlink(path)
    
    @pytest.mark.asyncio
    async def test_database_directory_creation(self):
        """Test that database directory is created if it doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "subdir", "test.db")
            
            db = Database(db_path)
            await db.initialize()
            
            assert os.path.exists(db_path)
            
            await db.close()
    
    @pytest.mark.asyncio
    async def test_create_database_function(self):
        """Test the create_database convenience function."""
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        
        db = create_database(path)
        assert isinstance(db, Database)
        assert db.db_path == path
        
        await db.close()
        os.unlink(path)


class TestPostOperations:
    """Test post-related database operations."""
    
    @pytest.mark.asyncio
    async def test_add_post(self, temp_db, sample_post_data):
        """Test adding a post to the database."""
        result = await temp_db.add_post(**sample_post_data)
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_add_duplicate_post(self, temp_db, sample_post_data):
        """Test that adding duplicate post returns False."""
        await temp_db.add_post(**sample_post_data)
        result = await temp_db.add_post(**sample_post_data)
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_get_post(self, temp_db, sample_post_data):
        """Test retrieving a post by URI."""
        await temp_db.add_post(**sample_post_data)
        
        post = await temp_db.get_post(sample_post_data["uri"])
        
        assert post is not None
        assert post["uri"] == sample_post_data["uri"]
        assert post["cid"] == sample_post_data["cid"]
        assert post["author_did"] == sample_post_data["author_did"]
        assert post["text"] == sample_post_data["text"]
    
    @pytest.mark.asyncio
    async def test_get_nonexistent_post(self, temp_db):
        """Test getting a post that doesn't exist."""
        post = await temp_db.get_post("at://nonexistent/post")
        
        assert post is None
    
    @pytest.mark.asyncio
    async def test_add_post_without_text(self, temp_db, sample_post_data):
        """Test adding a post without text content."""
        data = sample_post_data.copy()
        data["text"] = None
        
        result = await temp_db.add_post(**data)
        assert result is True
        
        post = await temp_db.get_post(data["uri"])
        assert post["text"] is None
    
    @pytest.mark.asyncio
    async def test_add_post_default_timestamp(self, temp_db, sample_post_data):
        """Test that created_at defaults to current time if not provided."""
        data = sample_post_data.copy()
        del data["created_at"]
        
        before = datetime.utcnow()
        await temp_db.add_post(**data)
        after = datetime.utcnow()
        
        post = await temp_db.get_post(data["uri"])
        assert before <= post["created_at"] <= after


class TestURLOperations:
    """Test URL-related database operations."""
    
    @pytest.mark.asyncio
    async def test_url_created_with_post(self, temp_db, sample_post_data):
        """Test that URL is created when adding a post."""
        await temp_db.add_post(**sample_post_data)
        
        url = await temp_db.get_url(sample_post_data["url"])
        
        assert url is not None
        assert url["url"] == sample_post_data["url"]
        assert url["domain"] == sample_post_data["domain"]
        assert url["share_count"] == 1
    
    @pytest.mark.asyncio
    async def test_url_share_count_increments(self, temp_db, sample_post_data):
        """Test that share count increments when same URL is posted again."""
        # Add first post
        await temp_db.add_post(**sample_post_data)
        
        # Add second post with same URL
        data2 = sample_post_data.copy()
        data2["uri"] = "at://did:plc:test456/app.bsky.feed.post/xyz789"
        data2["cid"] = "bafytest456"
        data2["author_did"] = "did:plc:test456"
        
        await temp_db.add_post(**data2)
        
        url = await temp_db.get_url(sample_post_data["url"])
        assert url["share_count"] == 2
    
    @pytest.mark.asyncio
    async def test_get_url_share_count(self, temp_db, sample_post_data):
        """Test getting share count for a URL."""
        await temp_db.add_post(**sample_post_data)
        
        count = await temp_db.get_url_share_count(sample_post_data["url"])
        assert count == 1
    
    @pytest.mark.asyncio
    async def test_get_url_share_count_nonexistent(self, temp_db):
        """Test getting share count for non-existent URL returns 0."""
        count = await temp_db.get_url_share_count("https://nonexistent.com")
        assert count == 0
    
    @pytest.mark.asyncio
    async def test_multiple_urls_different_share_counts(self, temp_db, sample_post_data):
        """Test that different URLs maintain separate share counts."""
        # Add first URL twice
        await temp_db.add_post(**sample_post_data)
        
        data2 = sample_post_data.copy()
        data2["uri"] = "at://did:plc:test456/app.bsky.feed.post/xyz789"
        data2["cid"] = "bafytest456"
        await temp_db.add_post(**data2)
        
        # Add different URL once
        data3 = sample_post_data.copy()
        data3["uri"] = "at://did:plc:test789/app.bsky.feed.post/def456"
        data3["cid"] = "bafytest789"
        data3["url"] = "https://bbc.com/news/article"
        data3["domain"] = "bbc.com"
        await temp_db.add_post(**data3)
        
        count1 = await temp_db.get_url_share_count(sample_post_data["url"])
        count2 = await temp_db.get_url_share_count("https://bbc.com/news/article")
        
        assert count1 == 2
        assert count2 == 1


class TestQueryOperations:
    """Test database query operations."""
    
    @pytest.mark.asyncio
    async def test_get_posts_by_domain(self, temp_db, sample_post_data):
        """Test querying posts by domain."""
        # Add posts from different domains
        await temp_db.add_post(**sample_post_data)
        
        data2 = sample_post_data.copy()
        data2["uri"] = "at://did:plc:test456/app.bsky.feed.post/xyz789"
        data2["cid"] = "bafytest456"
        data2["url"] = "https://bbc.com/news/article"
        data2["domain"] = "bbc.com"
        await temp_db.add_post(**data2)
        
        # Query for nytimes.com posts
        posts = await temp_db.get_posts_by_domain("nytimes.com")
        
        assert len(posts) == 1
        assert posts[0]["domain"] == "nytimes.com"
    
    @pytest.mark.asyncio
    async def test_get_posts_by_domain_limit(self, temp_db, sample_post_data):
        """Test limit parameter in get_posts_by_domain."""
        # Add 3 posts from same domain
        for i in range(3):
            data = sample_post_data.copy()
            data["uri"] = f"at://did:plc:test{i}/app.bsky.feed.post/post{i}"
            data["cid"] = f"bafytest{i}"
            await temp_db.add_post(**data)
        
        posts = await temp_db.get_posts_by_domain("nytimes.com", limit=2)
        
        assert len(posts) == 2
    
    @pytest.mark.asyncio
    async def test_get_posts_by_domain_offset(self, temp_db, sample_post_data):
        """Test offset parameter in get_posts_by_domain."""
        # Add 3 posts from same domain
        for i in range(3):
            data = sample_post_data.copy()
            data["uri"] = f"at://did:plc:test{i}/app.bsky.feed.post/post{i}"
            data["cid"] = f"bafytest{i}"
            await temp_db.add_post(**data)
        
        posts = await temp_db.get_posts_by_domain("nytimes.com", limit=10, offset=1)
        
        assert len(posts) == 2
    
    @pytest.mark.asyncio
    async def test_get_recent_posts(self, temp_db, sample_post_data):
        """Test getting recent posts within time window."""
        # Add recent post
        await temp_db.add_post(**sample_post_data)
        
        # Add old post
        data2 = sample_post_data.copy()
        data2["uri"] = "at://did:plc:old/app.bsky.feed.post/old123"
        data2["cid"] = "bafyold123"
        data2["created_at"] = datetime.utcnow() - timedelta(days=10)
        await temp_db.add_post(**data2)
        
        # Query for posts from last 7 days
        posts = await temp_db.get_recent_posts(hours=168)  # 7 days
        
        assert len(posts) == 1
        assert posts[0]["uri"] == sample_post_data["uri"]
    
    @pytest.mark.asyncio
    async def test_get_recent_posts_limit(self, temp_db, sample_post_data):
        """Test limit parameter in get_recent_posts."""
        # Add 3 recent posts
        for i in range(3):
            data = sample_post_data.copy()
            data["uri"] = f"at://did:plc:test{i}/app.bsky.feed.post/post{i}"
            data["cid"] = f"bafytest{i}"
            await temp_db.add_post(**data)
        
        posts = await temp_db.get_recent_posts(hours=24, limit=2)
        
        assert len(posts) == 2
    
    @pytest.mark.asyncio
    async def test_posts_include_url_info(self, temp_db, sample_post_data):
        """Test that query results include URL information."""
        await temp_db.add_post(**sample_post_data)
        
        posts = await temp_db.get_posts_by_domain("nytimes.com")
        
        assert len(posts) == 1
        post = posts[0]
        assert "url" in post
        assert "domain" in post
        assert "share_count" in post
        assert "shared_at" in post
        assert post["url"] == sample_post_data["url"]


class TestStatistics:
    """Test database statistics operations."""
    
    @pytest.mark.asyncio
    async def test_get_stats_empty_database(self, temp_db):
        """Test statistics on empty database."""
        stats = await temp_db.get_stats()
        
        assert stats["total_posts"] == 0
        assert stats["unique_urls"] == 0
        assert stats["total_shares"] == 0
    
    @pytest.mark.asyncio
    async def test_get_stats_with_data(self, temp_db, sample_post_data):
        """Test statistics with data in database."""
        # Add 2 posts with same URL
        await temp_db.add_post(**sample_post_data)
        
        data2 = sample_post_data.copy()
        data2["uri"] = "at://did:plc:test456/app.bsky.feed.post/xyz789"
        data2["cid"] = "bafytest456"
        await temp_db.add_post(**data2)
        
        # Add 1 post with different URL
        data3 = sample_post_data.copy()
        data3["uri"] = "at://did:plc:test789/app.bsky.feed.post/def456"
        data3["cid"] = "bafytest789"
        data3["url"] = "https://bbc.com/news/article"
        data3["domain"] = "bbc.com"
        await temp_db.add_post(**data3)
        
        stats = await temp_db.get_stats()
        
        assert stats["total_posts"] == 3
        assert stats["unique_urls"] == 2
        assert stats["total_shares"] == 3  # 2 + 1


class TestCleanupOperations:
    """Test database cleanup operations."""
    
    @pytest.mark.asyncio
    async def test_delete_old_posts(self, temp_db, sample_post_data):
        """Test deleting old posts."""
        # Add recent post
        await temp_db.add_post(**sample_post_data)
        
        # Add old post
        data2 = sample_post_data.copy()
        data2["uri"] = "at://did:plc:old/app.bsky.feed.post/old123"
        data2["cid"] = "bafyold123"
        data2["created_at"] = datetime.utcnow() - timedelta(days=40)
        await temp_db.add_post(**data2)
        
        # Delete posts older than 30 days
        deleted = await temp_db.delete_old_posts(days=30)
        
        assert deleted == 1
        
        # Verify recent post still exists
        post = await temp_db.get_post(sample_post_data["uri"])
        assert post is not None
        
        # Verify old post is gone
        old_post = await temp_db.get_post(data2["uri"])
        assert old_post is None
    
    @pytest.mark.asyncio
    async def test_delete_old_posts_preserves_urls(self, temp_db, sample_post_data):
        """Test that deleting old posts preserves URL records."""
        # Add old post
        data = sample_post_data.copy()
        data["created_at"] = datetime.utcnow() - timedelta(days=40)
        await temp_db.add_post(**data)
        
        # Delete old posts
        await temp_db.delete_old_posts(days=30)
        
        # URL should still exist
        url = await temp_db.get_url(sample_post_data["url"])
        assert url is not None
    
    @pytest.mark.asyncio
    async def test_cleanup_orphaned_urls(self, temp_db, sample_post_data):
        """Test cleaning up URLs with no associated posts."""
        # Add post
        await temp_db.add_post(**sample_post_data)
        
        # Verify URL exists
        url_before = await temp_db.get_url(sample_post_data["url"])
        assert url_before is not None
        
        # Delete the post (simulating old post cleanup)
        # This will cascade delete the PostURL entries
        deleted_posts = await temp_db.delete_old_posts(days=0)
        assert deleted_posts == 1
        
        # URL should still exist (not automatically deleted)
        url_after_post_delete = await temp_db.get_url(sample_post_data["url"])
        assert url_after_post_delete is not None
        
        # Clean up orphaned URLs (URLs with no PostURL entries)
        deleted = await temp_db.cleanup_orphaned_urls()
        
        assert deleted == 1
        
        # URL should now be gone
        url = await temp_db.get_url(sample_post_data["url"])
        assert url is None
    
    @pytest.mark.asyncio
    async def test_cleanup_preserves_urls_with_posts(self, temp_db, sample_post_data):
        """Test that cleanup doesn't delete URLs still linked to posts."""
        # Add 2 posts with same URL
        await temp_db.add_post(**sample_post_data)
        
        data2 = sample_post_data.copy()
        data2["uri"] = "at://did:plc:test456/app.bsky.feed.post/xyz789"
        data2["cid"] = "bafytest456"
        await temp_db.add_post(**data2)
        
        # Delete one post
        data2["created_at"] = datetime.utcnow() - timedelta(days=40)
        await temp_db.delete_old_posts(days=30)
        
        # Clean up orphaned URLs
        deleted = await temp_db.cleanup_orphaned_urls()
        
        assert deleted == 0
        
        # URL should still exist
        url = await temp_db.get_url(sample_post_data["url"])
        assert url is not None


class TestEdgeCases:
    """Test edge cases and error handling."""
    
    @pytest.mark.asyncio
    async def test_empty_domain_query(self, temp_db):
        """Test querying for domain with no posts."""
        posts = await temp_db.get_posts_by_domain("nonexistent.com")
        assert posts == []
    
    @pytest.mark.asyncio
    async def test_very_long_url(self, temp_db, sample_post_data):
        """Test handling very long URLs."""
        data = sample_post_data.copy()
        data["url"] = "https://example.com/" + "a" * 1000
        
        result = await temp_db.add_post(**data)
        assert result is True
        
        url = await temp_db.get_url(data["url"])
        assert url is not None
    
    @pytest.mark.asyncio
    async def test_special_characters_in_text(self, temp_db, sample_post_data):
        """Test handling special characters in post text."""
        data = sample_post_data.copy()
        data["text"] = "Test with émojis 🎉 and spëcial çharacters"
        
        result = await temp_db.add_post(**data)
        assert result is True
        
        post = await temp_db.get_post(data["uri"])
        assert post["text"] == data["text"]
    
    @pytest.mark.asyncio
    async def test_concurrent_url_increments(self, temp_db, sample_post_data):
        """Test that concurrent posts with same URL increment correctly."""
        # Add multiple posts with same URL in sequence
        tasks = []
        for i in range(5):
            data = sample_post_data.copy()
            data["uri"] = f"at://did:plc:test{i}/app.bsky.feed.post/post{i}"
            data["cid"] = f"bafytest{i}"
            await temp_db.add_post(**data)
        
        # Check final share count
        count = await temp_db.get_url_share_count(sample_post_data["url"])
        assert count == 5


class TestRepostTracking:
    """Test repost count tracking operations."""
    
    @pytest.mark.asyncio
    async def test_increment_repost_count(self, temp_db, sample_post_data):
        """Test incrementing repost count for a post."""
        # Add a post
        await temp_db.add_post(**sample_post_data)
        
        # Increment repost count
        result = await temp_db.increment_repost_count(sample_post_data["uri"])
        assert result is True
        
        # Verify count increased
        post = await temp_db.get_post(sample_post_data["uri"])
        assert post["repost_count"] == 1
    
    @pytest.mark.asyncio
    async def test_increment_repost_count_multiple_times(self, temp_db, sample_post_data):
        """Test incrementing repost count multiple times."""
        # Add a post
        await temp_db.add_post(**sample_post_data)
        
        # Increment repost count multiple times
        for i in range(5):
            result = await temp_db.increment_repost_count(sample_post_data["uri"])
            assert result is True
        
        # Verify final count
        post = await temp_db.get_post(sample_post_data["uri"])
        assert post["repost_count"] == 5
    
    @pytest.mark.asyncio
    async def test_increment_repost_count_nonexistent_post(self, temp_db):
        """Test incrementing repost count for non-existent post."""
        result = await temp_db.increment_repost_count("at://did:plc:fake/app.bsky.feed.post/999")
        assert result is False
    
    @pytest.mark.asyncio
    async def test_new_post_has_zero_reposts(self, temp_db, sample_post_data):
        """Test that newly added posts have repost_count of 0."""
        await temp_db.add_post(**sample_post_data)
        
        post = await temp_db.get_post(sample_post_data["uri"])
        assert post["repost_count"] == 0
    
    @pytest.mark.asyncio
    async def test_repost_count_in_query_results(self, temp_db, sample_post_data):
        """Test that repost_count is included in query results."""
        # Add post and increment repost count
        await temp_db.add_post(**sample_post_data)
        await temp_db.increment_repost_count(sample_post_data["uri"])
        await temp_db.increment_repost_count(sample_post_data["uri"])
        
        # Test get_posts_by_domain
        posts = await temp_db.get_posts_by_domain(sample_post_data["domain"])
        assert len(posts) == 1
        assert posts[0]["repost_count"] == 2
        
        # Test get_recent_posts
        recent_posts = await temp_db.get_recent_posts(hours=24)
        assert len(recent_posts) == 1
        assert recent_posts[0]["repost_count"] == 2
    
    @pytest.mark.asyncio
    async def test_different_posts_different_repost_counts(self, temp_db, sample_post_data):
        """Test that different posts maintain separate repost counts."""
        # Add first post
        await temp_db.add_post(**sample_post_data)
        
        # Add second post with same URL
        data2 = sample_post_data.copy()
        data2["uri"] = "at://did:plc:test456/app.bsky.feed.post/xyz789"
        data2["cid"] = "bafytest456"
        await temp_db.add_post(**data2)
        
        # Increment first post 3 times
        for _ in range(3):
            await temp_db.increment_repost_count(sample_post_data["uri"])
        
        # Increment second post 1 time
        await temp_db.increment_repost_count(data2["uri"])
        
        # Verify counts are different
        post1 = await temp_db.get_post(sample_post_data["uri"])
        post2 = await temp_db.get_post(data2["uri"])
        
        assert post1["repost_count"] == 3
        assert post2["repost_count"] == 1
