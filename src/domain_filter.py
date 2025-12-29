"""Domain Filter - Check if URLs match whitelisted domains"""

import json
import logging
from pathlib import Path
from typing import Set, Optional

logger = logging.getLogger(__name__)


class DomainFilter:
    """
    Filters URLs based on a whitelist of allowed domains.
    
    This component loads a domain whitelist from configuration and
    checks whether URLs belong to approved domains.
    """

    def __init__(self, config_path: str = "config/domains.json"):
        """
        Initialize the domain filter.
        
        Args:
            config_path: Path to the domains configuration file
        """
        self.config_path = config_path
        self.domains: Set[str] = set()
        self.match_subdomains: bool = True
        self._load_config()

    def _load_config(self):
        """Load domain whitelist from configuration file."""
        try:
            config_file = Path(self.config_path)
            
            if not config_file.exists():
                logger.warning(f"Config file not found: {self.config_path}")
                return

            with open(config_file, 'r') as f:
                config = json.load(f)

            # Load domains and convert to lowercase for case-insensitive matching
            domains = config.get('domains', [])
            self.domains = {domain.lower() for domain in domains}
            
            # Load subdomain matching setting
            self.match_subdomains = config.get('match_subdomains', True)

            logger.info(
                f"Loaded {len(self.domains)} domains from {self.config_path}. "
                f"Subdomain matching: {self.match_subdomains}"
            )
            logger.debug(f"Whitelisted domains: {sorted(self.domains)}")

        except json.JSONDecodeError as e:
            logger.error(f"Error parsing config file {self.config_path}: {e}")
        except Exception as e:
            logger.error(f"Error loading config file {self.config_path}: {e}")

    def reload_config(self):
        """Reload the domain whitelist from configuration file."""
        logger.info("Reloading domain configuration...")
        self._load_config()

    def is_allowed(self, domain: str) -> bool:
        """
        Check if a domain is in the whitelist.
        
        Args:
            domain: Domain name to check (e.g., "nytimes.com" or "www.nytimes.com")
            
        Returns:
            True if domain is whitelisted, False otherwise
        """
        if not domain:
            return False

        # Normalize domain to lowercase
        domain = domain.lower()

        # Remove www. prefix if present
        if domain.startswith('www.'):
            domain = domain[4:]

        # Direct match
        if domain in self.domains:
            return True

        # Subdomain matching if enabled
        if self.match_subdomains:
            # Check if domain is a subdomain of any whitelisted domain
            # e.g., "mobile.nytimes.com" should match "nytimes.com"
            for whitelisted_domain in self.domains:
                if domain.endswith('.' + whitelisted_domain):
                    return True

        return False

    def filter_url(self, url: str, domain: str) -> bool:
        """
        Check if a URL should be included based on its domain.
        
        Args:
            url: The full URL (for logging purposes)
            domain: The extracted domain from the URL
            
        Returns:
            True if URL should be included, False otherwise
        """
        allowed = self.is_allowed(domain)
        
        if allowed:
            logger.debug(f"URL allowed: {url} (domain: {domain})")
        else:
            logger.debug(f"URL filtered out: {url} (domain: {domain})")
        
        return allowed

    def get_whitelisted_domains(self) -> Set[str]:
        """
        Get the set of whitelisted domains.
        
        Returns:
            Set of whitelisted domain names
        """
        return self.domains.copy()

    def add_domain(self, domain: str):
        """
        Add a domain to the whitelist (runtime only, not persisted).
        
        Args:
            domain: Domain to add
        """
        domain = domain.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        
        self.domains.add(domain)
        logger.info(f"Added domain to whitelist: {domain}")

    def remove_domain(self, domain: str):
        """
        Remove a domain from the whitelist (runtime only, not persisted).
        
        Args:
            domain: Domain to remove
        """
        domain = domain.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        
        if domain in self.domains:
            self.domains.remove(domain)
            logger.info(f"Removed domain from whitelist: {domain}")
        else:
            logger.warning(f"Domain not in whitelist: {domain}")

    def __len__(self) -> int:
        """Return the number of whitelisted domains."""
        return len(self.domains)

    def __contains__(self, domain: str) -> bool:
        """Check if a domain is whitelisted using 'in' operator."""
        return self.is_allowed(domain)

