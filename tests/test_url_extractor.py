"""Tests for the URL Extractor component"""

import pytest
from src.url_extractor import URLExtractor


class TestURLExtractor:
    """Test suite for URLExtractor class"""

    @pytest.fixture
    def extractor(self):
        """Create a URLExtractor instance"""
        return URLExtractor()

    @pytest.fixture
    def extractor_keep_params(self):
        """Create a URLExtractor that keeps tracking params"""
        return URLExtractor(remove_tracking_params=False)

    def test_initialization(self, extractor):
        """Test that extractor initializes correctly"""
        assert extractor.remove_tracking_params is True

    def test_initialization_keep_params(self, extractor_keep_params):
        """Test initialization with tracking params kept"""
        assert extractor_keep_params.remove_tracking_params is False

    def test_extract_url_from_external_embed(self, extractor):
        """Test extracting URL from external embed"""
        record = {
            'text': 'Check this out',
            'embed': {
                '$type': 'app.bsky.embed.external',
                'external': {
                    'uri': 'https://www.nytimes.com/article',
                    'title': 'Article',
                    'description': 'Description'
                }
            }
        }
        
        url = extractor.extract_url(record)
        assert url == 'https://nytimes.com/article'

    def test_extract_url_from_record_with_media(self, extractor):
        """Test extracting URL from recordWithMedia embed"""
        record = {
            'text': 'Story with image',
            'embed': {
                '$type': 'app.bsky.embed.recordWithMedia',
                'media': {
                    '$type': 'app.bsky.embed.external',
                    'external': {
                        'uri': 'https://bbc.com/news/article-123',
                        'title': 'News',
                        'description': 'Breaking news'
                    }
                }
            }
        }
        
        url = extractor.extract_url(record)
        assert url == 'https://bbc.com/news/article-123'

    def test_extract_url_no_embed(self, extractor):
        """Test that None is returned when no embed present"""
        record = {'text': 'Just text, no links'}
        
        url = extractor.extract_url(record)
        assert url is None

    def test_extract_url_images_embed(self, extractor):
        """Test that None is returned for image embeds"""
        record = {
            'text': 'Photos',
            'embed': {
                '$type': 'app.bsky.embed.images',
                'images': []
            }
        }
        
        url = extractor.extract_url(record)
        assert url is None

    def test_extract_url_video_embed(self, extractor):
        """Test that None is returned for video embeds"""
        record = {
            'text': 'Video',
            'embed': {
                '$type': 'app.bsky.embed.video',
                'video': {}
            }
        }
        
        url = extractor.extract_url(record)
        assert url is None

    def test_normalize_url_removes_www(self, extractor):
        """Test that www. is removed from domain"""
        url = extractor.normalize_url('https://www.example.com/path')
        assert url == 'https://example.com/path'

    def test_normalize_url_removes_tracking_params(self, extractor):
        """Test that tracking parameters are removed"""
        url = extractor.normalize_url(
            'https://example.com/article?utm_source=twitter&utm_campaign=test&id=123'
        )
        assert url == 'https://example.com/article?id=123'

    def test_normalize_url_keeps_params_when_configured(self, extractor_keep_params):
        """Test that tracking params are kept when configured"""
        url = extractor_keep_params.normalize_url(
            'https://example.com/article?utm_source=twitter&id=123'
        )
        assert 'utm_source=twitter' in url
        assert 'id=123' in url

    def test_normalize_url_removes_fragment(self, extractor):
        """Test that URL fragments are removed"""
        url = extractor.normalize_url('https://example.com/article#section')
        assert url == 'https://example.com/article'

    def test_normalize_url_converts_http_to_https(self, extractor):
        """Test that http is converted to https"""
        url = extractor.normalize_url('http://example.com/article')
        assert url == 'https://example.com/article'

    def test_normalize_url_lowercases_domain(self, extractor):
        """Test that domain is lowercased"""
        url = extractor.normalize_url('https://EXAMPLE.COM/Path')
        assert url == 'https://example.com/Path'

    def test_normalize_url_adds_slash_if_missing(self, extractor):
        """Test that trailing slash is added if path is empty"""
        url = extractor.normalize_url('https://example.com')
        assert url == 'https://example.com/'

    def test_normalize_url_invalid_url(self, extractor):
        """Test that invalid URLs return None"""
        assert extractor.normalize_url('not-a-url') is None
        assert extractor.normalize_url('') is None
        assert extractor.normalize_url(None) is None

    def test_normalize_url_no_scheme(self, extractor):
        """Test that URLs without scheme return None"""
        url = extractor.normalize_url('example.com/article')
        assert url is None

    def test_extract_domain(self, extractor):
        """Test domain extraction"""
        assert extractor.extract_domain('https://example.com/path') == 'example.com'
        assert extractor.extract_domain('https://www.example.com/path') == 'example.com'
        assert extractor.extract_domain('https://sub.example.com/path') == 'sub.example.com'

    def test_extract_domain_with_port(self, extractor):
        """Test domain extraction with port number"""
        assert extractor.extract_domain('https://example.com:8080/path') == 'example.com'

    def test_extract_domain_invalid_url(self, extractor):
        """Test domain extraction from invalid URL"""
        assert extractor.extract_domain('not-a-url') is None
        assert extractor.extract_domain('') is None

    def test_extract_url_complex_tracking_params(self, extractor):
        """Test removal of multiple tracking parameters"""
        record = {
            'embed': {
                '$type': 'app.bsky.embed.external',
                'external': {
                    'uri': 'https://example.com/article?utm_source=fb&utm_medium=social&fbclid=123&gclid=456&article_id=789'
                }
            }
        }
        
        url = extractor.extract_url(record)
        # Should keep article_id but remove tracking params
        assert 'article_id=789' in url
        assert 'utm_source' not in url
        assert 'fbclid' not in url
        assert 'gclid' not in url

    def test_extract_url_real_world_nytimes(self, extractor):
        """Test with real-world NYTimes URL"""
        record = {
            'embed': {
                '$type': 'app.bsky.embed.external',
                'external': {
                    'uri': 'https://www.nytimes.com/2024/01/15/world/article.html?utm_source=twitter'
                }
            }
        }
        
        url = extractor.extract_url(record)
        assert url == 'https://nytimes.com/2024/01/15/world/article.html'

    def test_extract_url_real_world_bbc(self, extractor):
        """Test with real-world BBC URL"""
        record = {
            'embed': {
                '$type': 'app.bsky.embed.external',
                'external': {
                    'uri': 'https://www.bbc.com/news/world-us-canada-12345678?ref=social'
                }
            }
        }
        
        url = extractor.extract_url(record)
        assert url == 'https://bbc.com/news/world-us-canada-12345678'

    def test_extract_url_empty_record(self, extractor):
        """Test with empty record"""
        url = extractor.extract_url({})
        assert url is None

    def test_extract_url_malformed_embed(self, extractor):
        """Test with malformed embed structure"""
        record = {
            'embed': {
                '$type': 'app.bsky.embed.external',
                # Missing 'external' key
            }
        }
        
        url = extractor.extract_url(record)
        assert url is None
