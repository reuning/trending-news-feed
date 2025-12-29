"""Tests for the Domain Filter component"""

import pytest
import json
import tempfile
from pathlib import Path
from src.domain_filter import DomainFilter


class TestDomainFilter:
    """Test suite for DomainFilter class"""

    @pytest.fixture
    def temp_config(self, tmp_path):
        """Create a temporary config file"""
        config_file = tmp_path / "domains.json"
        config = {
            "domains": [
                "nytimes.com",
                "bbc.com",
                "reuters.com"
            ],
            "match_subdomains": True
        }
        with open(config_file, 'w') as f:
            json.dump(config, f)
        return str(config_file)

    @pytest.fixture
    def temp_config_no_subdomains(self, tmp_path):
        """Create a temporary config file with subdomain matching disabled"""
        config_file = tmp_path / "domains_no_sub.json"
        config = {
            "domains": ["example.com"],
            "match_subdomains": False
        }
        with open(config_file, 'w') as f:
            json.dump(config, f)
        return str(config_file)

    @pytest.fixture
    def filter(self, temp_config):
        """Create a DomainFilter instance with temp config"""
        return DomainFilter(config_path=temp_config)

    def test_initialization(self, filter):
        """Test that filter initializes correctly"""
        assert len(filter) == 3
        assert filter.match_subdomains is True

    def test_load_domains(self, filter):
        """Test that domains are loaded correctly"""
        domains = filter.get_whitelisted_domains()
        assert "nytimes.com" in domains
        assert "bbc.com" in domains
        assert "reuters.com" in domains

    def test_is_allowed_exact_match(self, filter):
        """Test exact domain matching"""
        assert filter.is_allowed("nytimes.com") is True
        assert filter.is_allowed("bbc.com") is True
        assert filter.is_allowed("reuters.com") is True

    def test_is_allowed_with_www(self, filter):
        """Test that www. prefix is handled correctly"""
        assert filter.is_allowed("www.nytimes.com") is True
        assert filter.is_allowed("www.bbc.com") is True

    def test_is_allowed_subdomain(self, filter):
        """Test subdomain matching"""
        assert filter.is_allowed("mobile.nytimes.com") is True
        assert filter.is_allowed("www.mobile.nytimes.com") is True
        assert filter.is_allowed("api.reuters.com") is True

    def test_is_allowed_case_insensitive(self, filter):
        """Test case-insensitive matching"""
        assert filter.is_allowed("NYTIMES.COM") is True
        assert filter.is_allowed("NYTimes.Com") is True
        assert filter.is_allowed("bbc.COM") is True

    def test_is_not_allowed(self, filter):
        """Test that non-whitelisted domains are rejected"""
        assert filter.is_allowed("example.com") is False
        assert filter.is_allowed("google.com") is False
        assert filter.is_allowed("notwhitelisted.com") is False

    def test_is_not_allowed_similar_domain(self, filter):
        """Test that similar but different domains are rejected"""
        assert filter.is_allowed("nytimes.org") is False
        assert filter.is_allowed("bbc.co.uk") is False
        assert filter.is_allowed("fakereuters.com") is False

    def test_is_allowed_empty_domain(self, filter):
        """Test that empty domain returns False"""
        assert filter.is_allowed("") is False
        assert filter.is_allowed(None) is False

    def test_subdomain_matching_disabled(self, temp_config_no_subdomains):
        """Test behavior when subdomain matching is disabled"""
        filter = DomainFilter(config_path=temp_config_no_subdomains)
        
        assert filter.is_allowed("example.com") is True
        assert filter.is_allowed("www.example.com") is True  # www is always removed
        assert filter.is_allowed("sub.example.com") is False  # subdomain not allowed

    def test_filter_url(self, filter):
        """Test the filter_url method"""
        assert filter.filter_url("https://nytimes.com/article", "nytimes.com") is True
        assert filter.filter_url("https://example.com/page", "example.com") is False

    def test_contains_operator(self, filter):
        """Test using 'in' operator"""
        assert "nytimes.com" in filter
        assert "bbc.com" in filter
        assert "example.com" not in filter

    def test_len_operator(self, filter):
        """Test len() operator"""
        assert len(filter) == 3

    def test_get_whitelisted_domains(self, filter):
        """Test getting whitelisted domains"""
        domains = filter.get_whitelisted_domains()
        assert isinstance(domains, set)
        assert len(domains) == 3
        
        # Ensure it's a copy, not the original
        domains.add("test.com")
        assert len(filter) == 3

    def test_add_domain(self, filter):
        """Test adding a domain at runtime"""
        assert len(filter) == 3
        
        filter.add_domain("newdomain.com")
        assert len(filter) == 4
        assert filter.is_allowed("newdomain.com") is True

    def test_add_domain_with_www(self, filter):
        """Test adding a domain with www. prefix"""
        filter.add_domain("www.newdomain.com")
        assert filter.is_allowed("newdomain.com") is True
        assert filter.is_allowed("www.newdomain.com") is True

    def test_remove_domain(self, filter):
        """Test removing a domain at runtime"""
        assert len(filter) == 3
        assert filter.is_allowed("nytimes.com") is True
        
        filter.remove_domain("nytimes.com")
        assert len(filter) == 2
        assert filter.is_allowed("nytimes.com") is False

    def test_remove_nonexistent_domain(self, filter):
        """Test removing a domain that doesn't exist"""
        initial_len = len(filter)
        filter.remove_domain("nonexistent.com")
        assert len(filter) == initial_len

    def test_reload_config(self, temp_config, filter):
        """Test reloading configuration"""
        # Modify the config file
        config = {
            "domains": ["newdomain.com"],
            "match_subdomains": False
        }
        with open(temp_config, 'w') as f:
            json.dump(config, f)
        
        # Reload
        filter.reload_config()
        
        assert len(filter) == 1
        assert filter.is_allowed("newdomain.com") is True
        assert filter.is_allowed("nytimes.com") is False
        assert filter.match_subdomains is False

    def test_missing_config_file(self, tmp_path):
        """Test behavior with missing config file"""
        nonexistent = str(tmp_path / "nonexistent.json")
        filter = DomainFilter(config_path=nonexistent)
        
        # Should initialize with empty domain list
        assert len(filter) == 0

    def test_invalid_json_config(self, tmp_path):
        """Test behavior with invalid JSON config"""
        config_file = tmp_path / "invalid.json"
        with open(config_file, 'w') as f:
            f.write("{ invalid json }")
        
        filter = DomainFilter(config_path=str(config_file))
        
        # Should initialize with empty domain list
        assert len(filter) == 0

    def test_config_missing_domains_key(self, tmp_path):
        """Test config file without 'domains' key"""
        config_file = tmp_path / "no_domains.json"
        config = {"match_subdomains": True}
        with open(config_file, 'w') as f:
            json.dump(config, f)
        
        filter = DomainFilter(config_path=str(config_file))
        
        # Should initialize with empty domain list
        assert len(filter) == 0
        assert filter.match_subdomains is True

    def test_default_config_path(self):
        """Test using default config path"""
        # This should load from config/domains.json
        filter = DomainFilter()
        
        # Should have the domains from the actual config file
        assert "nytimes.com" in filter
        assert "washingtonpost.com" in filter
        assert "bbc.com" in filter
        assert "reuters.com" in filter
        assert "apnews.com" in filter

    def test_complex_subdomain(self, filter):
        """Test complex subdomain structures"""
        assert filter.is_allowed("a.b.c.nytimes.com") is True
        assert filter.is_allowed("very.long.subdomain.bbc.com") is True

    def test_partial_domain_match(self, filter):
        """Test that partial matches don't work"""
        # "nytimes.com" should not match "fakenytimes.com"
        assert filter.is_allowed("fakenytimes.com") is False
        assert filter.is_allowed("nytimes.com.fake.com") is False
