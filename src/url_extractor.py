"""URL Extractor - Extract and normalize URLs from post embeds"""

import logging
from typing import List, Optional
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

logger = logging.getLogger(__name__)


class URLExtractor:
    """
    Extracts and normalizes URLs from Bluesky post embeds.
    
    This component focuses on extracting URLs from embed objects,
    specifically external link embeds (app.bsky.embed.external).
    """

    # Common tracking parameters to remove during normalization
    TRACKING_PARAMS = {
        'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
        'fbclid', 'gclid', 'msclkid', 'mc_cid', 'mc_eid',
        '_ga', '_gl', 'ref', 'source', 'campaign',
        'link_source', 'taid', 'user_email',
    }

    def __init__(self, remove_tracking_params: bool = True):
        """
        Initialize the URL extractor.
        
        Args:
            remove_tracking_params: Whether to remove tracking parameters during normalization
        """
        self.remove_tracking_params = remove_tracking_params

    def extract_url(self, record: dict) -> Optional[str]:
        """
        Extract the primary URL from a post record's embed.
        
        Args:
            record: Post record dictionary from Bluesky
            
        Returns:
            Normalized URL or None if no URL found
        """
        try:
            embed = record.get('embed')
            if not embed:
                return None

            # Get the raw URL from embed
            raw_url = self._extract_from_embed(embed)
            
            if not raw_url:
                return None

            # Normalize and return
            return self.normalize_url(raw_url)

        except Exception as e:
            logger.error(f"Error extracting URL from record: {e}", exc_info=True)
            return None

    def _extract_from_embed(self, embed: dict) -> Optional[str]:
        """
        Extract URL from embed object based on its type.
        
        Args:
            embed: Embed object from post record
            
        Returns:
            Raw URL string or None
        """
        embed_type = embed.get('$type', '')

        try:
            # External link embed - this is the main case we care about
            if embed_type == 'app.bsky.embed.external':
                external = embed.get('external', {})
                return external.get('uri')

            # Record with media (contains external link + media like images)
            elif embed_type == 'app.bsky.embed.recordWithMedia':
                media = embed.get('media', {})
                if media.get('$type') == 'app.bsky.embed.external':
                    external = media.get('external', {})
                    return external.get('uri')

            # Other embed types (images, videos, records) don't have external URLs
            # that we want to track for this feed
            
        except Exception as e:
            logger.error(f"Error extracting from embed type {embed_type}: {e}")

        return None

    def normalize_url(self, url: str) -> Optional[str]:
        """
        Normalize a URL by:
        - Converting to lowercase domain
        - Removing www. prefix
        - Removing tracking parameters
        - Removing fragments (#)
        - Standardizing scheme to https
        
        Args:
            url: Raw URL string
            
        Returns:
            Normalized URL or None if invalid
        """
        if not url:
            return None

        try:
            # Parse the URL
            parsed = urlparse(url)

            # Ensure we have a scheme and netloc
            if not parsed.scheme or not parsed.netloc:
                return None

            # Normalize scheme to https if http
            scheme = 'https' if parsed.scheme in ('http', 'https') else parsed.scheme

            # Lowercase the domain
            netloc = parsed.netloc.lower()

            # Remove www. prefix for consistency
            if netloc.startswith('www.'):
                netloc = netloc[4:]

            # Handle query parameters
            if self.remove_tracking_params and parsed.query:
                # Parse query string
                params = parse_qs(parsed.query, keep_blank_values=True)
                
                # Remove tracking parameters
                cleaned_params = {
                    k: v for k, v in params.items()
                    if k.lower() not in self.TRACKING_PARAMS
                }
                
                # Rebuild query string
                query = urlencode(cleaned_params, doseq=True) if cleaned_params else ''
            else:
                query = parsed.query

            # Remove fragment (everything after #)
            fragment = ''

            # Ensure path is at least /
            path = parsed.path or '/'

            # Rebuild URL
            normalized = urlunparse((
                scheme,
                netloc,
                path,
                parsed.params,
                query,
                fragment
            ))

            return normalized

        except Exception as e:
            logger.error(f"Error normalizing URL {url}: {e}")
            return None

    def extract_domain(self, url: str) -> Optional[str]:
        """
        Extract the domain from a URL.
        
        Args:
            url: URL string
            
        Returns:
            Domain name (without www.) or None if invalid
        """
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            # Remove www. prefix
            if domain.startswith('www.'):
                domain = domain[4:]
            
            # Remove port if present
            if ':' in domain:
                domain = domain.split(':')[0]
            
            return domain if domain else None

        except Exception as e:
            logger.error(f"Error extracting domain from {url}: {e}")
            return None


def example_usage():
    """Example usage of URLExtractor."""
    extractor = URLExtractor()

    # Example 1: External link embed
    record1 = {
        'text': 'Check out this article',
        'embed': {
            '$type': 'app.bsky.embed.external',
            'external': {
                'uri': 'https://www.nytimes.com/2024/article?utm_source=twitter',
                'title': 'News Article',
                'description': 'An interesting article'
            }
        }
    }

    # Example 2: Record with media (external link + image)
    record2 = {
        'text': 'Great story with image',
        'embed': {
            '$type': 'app.bsky.embed.recordWithMedia',
            'media': {
                '$type': 'app.bsky.embed.external',
                'external': {
                    'uri': 'https://www.bbc.com/news/article-123?ref=social',
                    'title': 'BBC News',
                    'description': 'Breaking news'
                }
            }
        }
    }

    # Example 3: No external link (just images)
    record3 = {
        'text': 'Just some photos',
        'embed': {
            '$type': 'app.bsky.embed.images',
            'images': []
        }
    }

    print("Example 1 - External link:")
    url1 = extractor.extract_url(record1)
    if url1:
        print(f"  URL: {url1}")
        print(f"  Domain: {extractor.extract_domain(url1)}")
    else:
        print("  No URL found")

    print("\nExample 2 - Record with media:")
    url2 = extractor.extract_url(record2)
    if url2:
        print(f"  URL: {url2}")
        print(f"  Domain: {extractor.extract_domain(url2)}")
    else:
        print("  No URL found")

    print("\nExample 3 - No external link:")
    url3 = extractor.extract_url(record3)
    if url3:
        print(f"  URL: {url3}")
        print(f"  Domain: {extractor.extract_domain(url3)}")
    else:
        print("  No URL found")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    example_usage()
