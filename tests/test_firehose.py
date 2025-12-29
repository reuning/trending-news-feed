"""Tests for the Firehose Listener component"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from src.firehose import FirehoseListener


class TestFirehoseListener:
    """Test suite for FirehoseListener class"""

    @pytest.fixture
    def mock_callback(self):
        """Create a mock callback function"""
        return AsyncMock()

    @pytest.fixture
    def mock_repost_callback(self):
        """Create a mock repost callback function"""
        return AsyncMock()

    @pytest.fixture
    def listener(self, mock_callback):
        """Create a FirehoseListener instance"""
        return FirehoseListener(on_post_callback=mock_callback)

    @pytest.fixture
    def listener_with_repost(self, mock_callback, mock_repost_callback):
        """Create a FirehoseListener instance with repost callback"""
        return FirehoseListener(
            on_post_callback=mock_callback,
            on_repost_callback=mock_repost_callback
        )

    def test_initialization(self, listener, mock_callback):
        """Test that listener initializes correctly"""
        assert listener.on_post_callback == mock_callback
        assert listener.firehose_url == "wss://bsky.network"
        assert listener.is_running is False
        assert listener.stats['posts_processed'] == 0
        assert listener.stats['errors'] == 0

    def test_custom_firehose_url(self, mock_callback):
        """Test initialization with custom firehose URL"""
        custom_url = "wss://custom.firehose.url"
        listener = FirehoseListener(
            on_post_callback=mock_callback,
            firehose_url=custom_url
        )
        assert listener.firehose_url == custom_url

    def test_has_links_with_http(self, listener):
        """Test _has_links detects http URLs in text"""
        record = {'text': 'Check out http://example.com'}
        assert listener._has_links(record) is True

    def test_has_links_with_https(self, listener):
        """Test _has_links detects https URLs in text"""
        record = {'text': 'Check out https://example.com'}
        assert listener._has_links(record) is True

    def test_has_links_with_external_embed(self, listener):
        """Test _has_links detects external embeds"""
        record = {
            'text': 'Check this out',
            'embed': {
                '$type': 'app.bsky.embed.external'
            }
        }
        assert listener._has_links(record) is True

    def test_has_links_with_record_with_media(self, listener):
        """Test _has_links detects external links in recordWithMedia"""
        record = {
            'text': 'Check this out',
            'embed': {
                '$type': 'app.bsky.embed.recordWithMedia',
                'media': {
                    '$type': 'app.bsky.embed.external'
                }
            }
        }
        assert listener._has_links(record) is True

    def test_has_links_no_links(self, listener):
        """Test _has_links returns False for posts without links"""
        record = {'text': 'Just a regular post'}
        assert listener._has_links(record) is False

    def test_has_links_empty_record(self, listener):
        """Test _has_links handles empty records"""
        record = {}
        assert listener._has_links(record) is False

    @pytest.mark.asyncio
    async def test_handle_post_with_links(self, listener, mock_callback):
        """Test that posts with links are passed to callback"""
        record = {'text': 'Check out https://example.com'}
        
        await listener._handle_post(
            uri='at://did:plc:test/app.bsky.feed.post/123',
            cid='bafytest',
            author_did='did:plc:test',
            record=record,
            timestamp='2024-01-01T00:00:00Z'
        )
        
        # Callback should be called
        mock_callback.assert_called_once()
        call_args = mock_callback.call_args[1]
        assert call_args['uri'] == 'at://did:plc:test/app.bsky.feed.post/123'
        assert call_args['cid'] == 'bafytest'
        assert call_args['author_did'] == 'did:plc:test'
        assert call_args['record'] == record

    @pytest.mark.asyncio
    async def test_handle_post_without_links(self, listener, mock_callback):
        """Test that posts without links are ignored"""
        record = {'text': 'Just a regular post'}
        
        await listener._handle_post(
            uri='at://did:plc:test/app.bsky.feed.post/123',
            cid='bafytest',
            author_did='did:plc:test',
            record=record,
            timestamp='2024-01-01T00:00:00Z'
        )
        
        # Callback should NOT be called
        mock_callback.assert_not_called()

    @pytest.mark.asyncio
    async def test_stop(self, listener):
        """Test that stop() sets running flag to False"""
        listener._running = True
        assert listener.is_running is True
        
        await listener.stop()
        
        assert listener.is_running is False

    def test_stats(self, listener):
        """Test stats property returns correct information"""
        listener._posts_processed = 42
        listener._errors = 3
        listener._running = True
        
        stats = listener.stats
        
        assert stats['posts_processed'] == 42
        assert stats['errors'] == 3
        assert stats['is_running'] is True

    @pytest.mark.asyncio
    async def test_handle_repost_with_callback(self, listener_with_repost, mock_repost_callback):
        """Test that reposts are handled when callback is provided"""
        # Mock callback returns True (post is tracked)
        mock_repost_callback.return_value = True
        
        record = {
            'subject': {
                'uri': 'at://did:plc:original/app.bsky.feed.post/456'
            }
        }
        
        await listener_with_repost._handle_repost(
            repost_uri='at://did:plc:test/app.bsky.feed.repost/123',
            author_did='did:plc:test',
            record=record,
            timestamp='2024-01-01T00:00:00Z'
        )
        
        # Callback should be called with correct arguments
        mock_repost_callback.assert_called_once()
        call_args = mock_repost_callback.call_args[1]
        assert call_args['repost_uri'] == 'at://did:plc:test/app.bsky.feed.repost/123'
        assert call_args['original_post_uri'] == 'at://did:plc:original/app.bsky.feed.post/456'
        assert call_args['author_did'] == 'did:plc:test'
        assert call_args['timestamp'] == '2024-01-01T00:00:00Z'
        
        # Stats should be updated
        assert listener_with_repost.stats['reposts_tracked'] == 1

    @pytest.mark.asyncio
    async def test_handle_repost_without_callback(self, listener):
        """Test that reposts are handled gracefully when no callback is provided"""
        record = {
            'subject': {
                'uri': 'at://did:plc:original/app.bsky.feed.post/456'
            }
        }
        
        # Should not raise an error
        await listener._handle_repost(
            repost_uri='at://did:plc:test/app.bsky.feed.repost/123',
            author_did='did:plc:test',
            record=record,
            timestamp='2024-01-01T00:00:00Z'
        )
        
        # Stats should not be updated since no callback
        assert listener.stats['reposts_tracked'] == 0

    @pytest.mark.asyncio
    async def test_handle_repost_untracked_post(self, listener_with_repost, mock_repost_callback):
        """Test that reposts of untracked posts don't increment tracked count"""
        # Mock callback returns False (post is not tracked)
        mock_repost_callback.return_value = False
        
        record = {
            'subject': {
                'uri': 'at://did:plc:original/app.bsky.feed.post/456'
            }
        }
        
        await listener_with_repost._handle_repost(
            repost_uri='at://did:plc:test/app.bsky.feed.repost/123',
            author_did='did:plc:test',
            record=record,
            timestamp='2024-01-01T00:00:00Z'
        )
        
        # Callback should be called
        mock_repost_callback.assert_called_once()
        
        # Stats should NOT be incremented for untracked posts
        assert listener_with_repost.stats['reposts_tracked'] == 0

    @pytest.mark.asyncio
    async def test_handle_repost_missing_subject(self, listener_with_repost, mock_repost_callback):
        """Test that reposts without subject are handled gracefully"""
        record = {}  # No subject
        
        await listener_with_repost._handle_repost(
            repost_uri='at://did:plc:test/app.bsky.feed.repost/123',
            author_did='did:plc:test',
            record=record,
            timestamp='2024-01-01T00:00:00Z'
        )
        
        # Callback should NOT be called
        mock_repost_callback.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_repost_missing_uri(self, listener_with_repost, mock_repost_callback):
        """Test that reposts without URI in subject are handled gracefully"""
        record = {
            'subject': {}  # No URI
        }
        
        await listener_with_repost._handle_repost(
            repost_uri='at://did:plc:test/app.bsky.feed.repost/123',
            author_did='did:plc:test',
            record=record,
            timestamp='2024-01-01T00:00:00Z'
        )
        
        # Callback should NOT be called
        mock_repost_callback.assert_not_called()

    def test_stats_includes_repost_counts(self, listener_with_repost):
        """Test that stats include repost-related counts"""
        listener_with_repost._reposts_processed = 100
        listener_with_repost._reposts_tracked = 25
        
        stats = listener_with_repost.stats
        
        assert stats['reposts_processed'] == 100
        assert stats['reposts_tracked'] == 25


