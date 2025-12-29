#!/usr/bin/env python3
"""
Main entry point for the Bluesky Domain Feed Generator.

This module integrates all components and provides different operational modes:
- firehose: Listen to Bluesky firehose and store posts with whitelisted URLs
- server: Run the feed server to serve the ranked feed
- both: Run both firehose listener and server concurrently
- clear: Delete posts from the database based on time period criteria
"""

import asyncio
import argparse
import logging
import sys
from typing import Optional
from datetime import datetime

from dotenv import load_dotenv

from src.firehose import FirehoseListener
from src.url_extractor import URLExtractor
from src.domain_filter import DomainFilter
from src.database import Database
from src.ranking import RankingEngine, RankingConfig


# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('feed_generator.log')
    ]
)

logger = logging.getLogger(__name__)


class FeedGenerator:
    """
    Main feed generator application that integrates all components.
    
    This class coordinates the firehose listener, URL extraction, domain filtering,
    and database storage to build a feed of posts from whitelisted domains.
    """
    
    def __init__(
        self,
        db_path: str = "data/feed.db",
        domains_config: str = "config/domains.json",
        ranking_config: str = "config/ranking.json",
    ):
        """
        Initialize the feed generator.
        
        Args:
            db_path: Path to SQLite database
            domains_config: Path to domains configuration file
            ranking_config: Path to ranking configuration file
        """
        self.db_path = db_path
        self.domains_config = domains_config
        self.ranking_config = ranking_config
        
        # Components
        self.db: Optional[Database] = None
        self.url_extractor: Optional[URLExtractor] = None
        self.domain_filter: Optional[DomainFilter] = None
        self.firehose_listener: Optional[FirehoseListener] = None
        self.ranking_engine: Optional[RankingEngine] = None
        
        # Shutdown flag
        self._shutdown = False
        
    async def initialize(self):
        """Initialize all components."""
        logger.info("Initializing feed generator components...")
        
        # Initialize database
        self.db = Database(self.db_path)
        await self.db.initialize()
        logger.info(f"Database initialized at {self.db_path}")
        
        # Initialize URL extractor
        self.url_extractor = URLExtractor(remove_tracking_params=True)
        logger.info("URL extractor initialized")
        
        # Initialize domain filter
        self.domain_filter = DomainFilter(self.domains_config)
        logger.info(f"Domain filter initialized with {len(self.domain_filter)} domains")
        
        # Initialize ranking engine
        config = RankingConfig.from_file(self.ranking_config)
        self.ranking_engine = RankingEngine(self.db, config)
        logger.info("Ranking engine initialized")
        
        # Initialize firehose listener with callbacks
        self.firehose_listener = FirehoseListener(
            on_post_callback=self._handle_post,
            on_repost_callback=self._handle_repost
        )
        logger.info("Firehose listener initialized")
        
        logger.info("All components initialized successfully")
    
    async def _handle_post(
        self,
        uri: str,
        cid: str,
        author_did: str,
        record: dict,
        timestamp: str,
    ) -> bool:
        """
        Handle a post from the firehose.
        
        This callback is called by the firehose listener for each post with links.
        It extracts URLs, filters by domain, and stores in the database.
        
        Args:
            uri: AT Protocol URI of the post
            cid: Content ID of the post
            author_did: DID of the post author
            record: Post record data
            timestamp: Timestamp of the post
            
        Returns:
            True if post has whitelisted domain and was processed, False otherwise
        """
        try:
            # Extract URL from post
            url = self.url_extractor.extract_url(record)
            if not url:
                logger.debug(f"No URL found in post {uri}")
                return False
            
            # Extract domain
            domain = self.url_extractor.extract_domain(url)
            if not domain:
                logger.debug(f"Could not extract domain from URL {url}")
                return False
            
            # Check if domain is whitelisted
            if not self.domain_filter.is_allowed(domain):
                logger.debug(f"Domain {domain} not whitelisted, skipping post {uri}")
                return False
            
            # Store post in database
            text = record.get('text', '')
            created_at = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            
            success = await self.db.add_post(
                uri=uri,
                cid=cid,
                author_did=author_did,
                url=url,
                domain=domain,
                text=text,
                created_at=created_at,
            )
            
            if success:
                logger.info(f"Stored post {uri} with URL {url} (domain: {domain})")
            else:
                logger.debug(f"Post {uri} already exists in database")
            
            # Return True to indicate this post had a whitelisted domain
            return True
                
        except Exception as e:
            logger.error(f"Error handling post {uri}: {e}", exc_info=True)
            return False
    
    async def _handle_repost(
        self,
        repost_uri: str,
        original_post_uri: str,
        author_did: str,
        timestamp: str,
    ) -> bool:
        """
        Handle a repost event from the firehose.
        
        This callback is called by the firehose listener for each repost.
        It increments the repost count for the original post if we're tracking it.
        
        Args:
            repost_uri: AT Protocol URI of the repost itself
            original_post_uri: AT Protocol URI of the original post being reposted
            author_did: DID of the user who reposted
            timestamp: Timestamp of the repost
            
        Returns:
            True if the original post is tracked and repost count was incremented, False otherwise
        """
        try:
            # Try to increment the repost count for the original post
            # This will only succeed if we're tracking that post (it has a whitelisted URL)
            success = await self.db.increment_repost_count(original_post_uri)
            
            if success:
                logger.debug(f"Incremented repost count for tracked post {original_post_uri}")
            else:
                logger.debug(f"Repost of non-tracked post {original_post_uri}, skipping")
            
            return success
                
        except Exception as e:
            logger.error(f"Error handling repost {repost_uri}: {e}", exc_info=True)
            return False
    
    async def run_firehose(self):
        """Run the firehose listener to collect posts."""
        logger.info("Starting firehose listener...")
        
        try:
            await self.firehose_listener.start()
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Error in firehose listener: {e}", exc_info=True)
            raise
        finally:
            await self.cleanup()
    
    async def run_server(self):
        """Run the feed server."""
        logger.info("Starting feed server...")
        
        # Import here to avoid circular dependency
        import uvicorn
        import os
        
        port = int(os.getenv("PORT", "8000"))
        
        # Create server config
        config = uvicorn.Config(
            "src.server:app",
            host="0.0.0.0",
            port=port,
            log_level="info",
        )
        
        server = uvicorn.Server(config)
        
        try:
            await server.serve()
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Error in feed server: {e}", exc_info=True)
            raise
        finally:
            await self.cleanup()
    
    async def run_both(self):
        """Run both firehose listener and feed server concurrently."""
        logger.info("Starting both firehose listener and feed server...")
        
        # Import here to avoid circular dependency
        import uvicorn
        import os
        
        port = int(os.getenv("PORT", "8000"))
        
        # Create server config
        config = uvicorn.Config(
            "src.server:app",
            host="0.0.0.0",
            port=port,
            log_level="info",
        )
        
        server = uvicorn.Server(config)
        
        # Create tasks for both services
        firehose_task = asyncio.create_task(self.firehose_listener.start())
        server_task = asyncio.create_task(server.serve())
        
        # Run both tasks concurrently
        try:
            await asyncio.gather(firehose_task, server_task)
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("Received interrupt signal, stopping services...")
            # Cancel both tasks
            firehose_task.cancel()
            server_task.cancel()
            # Wait for them to finish cancelling
            await asyncio.gather(firehose_task, server_task, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error running services: {e}", exc_info=True)
            raise
        finally:
            await self.cleanup()
    
    async def run_clear(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        days: Optional[int] = None,
        cleanup_urls: bool = False,
    ):
        """
        Clear posts from the database based on date criteria.
        
        Args:
            start_date: Start date for deletion (inclusive)
            end_date: End date for deletion (exclusive)
            days: Delete posts older than this many days
            cleanup_urls: Whether to cleanup orphaned URLs after deletion
        """
        logger.info("Starting post cleanup operation...")
        
        try:
            # Determine which deletion method to use
            if days is not None:
                logger.info(f"Deleting posts older than {days} days...")
                deleted = await self.db.delete_old_posts(days=days)
            elif start_date is not None or end_date is not None:
                period_desc = []
                if start_date:
                    period_desc.append(f"from {start_date.isoformat()}")
                if end_date:
                    period_desc.append(f"to {end_date.isoformat()}")
                logger.info(f"Deleting posts {' '.join(period_desc)}...")
                deleted = await self.db.delete_posts_in_period(
                    start_date=start_date,
                    end_date=end_date,
                )
            else:
                logger.error("No deletion criteria specified. Use --days, --start-date, or --end-date")
                return
            
            logger.info(f"Successfully deleted {deleted} posts")
            
            # Optionally cleanup orphaned URLs
            if cleanup_urls:
                logger.info("Cleaning up orphaned URLs...")
                urls_deleted = await self.db.cleanup_orphaned_urls()
                logger.info(f"Successfully deleted {urls_deleted} orphaned URLs")
            
            # Show final statistics
            stats = await self.db.get_stats()
            logger.info(
                f"Database statistics - Posts: {stats['total_posts']}, "
                f"URLs: {stats['unique_urls']}, Shares: {stats['total_shares']}"
            )
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}", exc_info=True)
            raise
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """Cleanup resources on shutdown."""
        if self._shutdown:
            return
        
        self._shutdown = True
        logger.info("Cleaning up resources...")
        
        # Stop firehose listener
        if self.firehose_listener and self.firehose_listener.is_running:
            await self.firehose_listener.stop()
            logger.info("Firehose listener stopped")
        
        # Close database
        if self.db:
            await self.db.close()
            logger.info("Database connection closed")
        
        # Print final statistics
        if self.firehose_listener:
            stats = self.firehose_listener.stats
            logger.info(
                f"Final statistics - Posts processed: {stats['posts_processed']}, "
                f"Errors: {stats['errors']}"
            )
        
        logger.info("Cleanup complete")


async def main():
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(
        description="Bluesky Domain Feed Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run firehose listener only
  python main.py firehose
  
  # Run feed server only
  python main.py server
  
  # Run both firehose and server
  python main.py both
  
  # Clear posts older than 30 days
  python main.py clear --days 30
  
  # Clear posts from a specific date range
  python main.py clear --start-date 2024-01-01 --end-date 2024-02-01
  
  # Clear posts and cleanup orphaned URLs
  python main.py clear --days 60 --cleanup-urls
  
  # Use custom configuration
  python main.py firehose --db data/custom.db --domains config/custom_domains.json
        """
    )
    
    parser.add_argument(
        "mode",
        choices=["firehose", "server", "both", "clear"],
        help="Operation mode: firehose (collect posts), server (serve feed), both, or clear (delete posts)"
    )
    
    parser.add_argument(
        "--db",
        default="data/feed.db",
        help="Path to SQLite database (default: data/feed.db)"
    )
    
    parser.add_argument(
        "--domains",
        default="config/domains.json",
        help="Path to domains configuration file (default: config/domains.json)"
    )
    
    parser.add_argument(
        "--ranking",
        default="config/ranking.json",
        help="Path to ranking configuration file (default: config/ranking.json)"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    
    # Arguments for clear mode
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for clearing posts (ISO format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)"
    )
    
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for clearing posts (ISO format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)"
    )
    
    parser.add_argument(
        "--days",
        type=int,
        help="Delete posts older than this many days (alternative to date range)"
    )
    
    parser.add_argument(
        "--cleanup-urls",
        action="store_true",
        help="Also cleanup orphaned URLs after deleting posts"
    )
    
    args = parser.parse_args()
    
    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Create feed generator
    generator = FeedGenerator(
        db_path=args.db,
        domains_config=args.domains,
        ranking_config=args.ranking,
    )
    
    # Initialize components
    await generator.initialize()
    
    # Run in selected mode
    try:
        if args.mode == "firehose":
            await generator.run_firehose()
        elif args.mode == "server":
            await generator.run_server()
        elif args.mode == "both":
            await generator.run_both()
        elif args.mode == "clear":
            # Parse date arguments if provided
            start_date = None
            end_date = None
            
            if args.start_date:
                try:
                    start_date = datetime.fromisoformat(args.start_date)
                except ValueError:
                    logger.error(f"Invalid start date format: {args.start_date}. Use ISO format (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)")
                    return
            
            if args.end_date:
                try:
                    end_date = datetime.fromisoformat(args.end_date)
                except ValueError:
                    logger.error(f"Invalid end date format: {args.end_date}. Use ISO format (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)")
                    return
            
            await generator.run_clear(
                start_date=start_date,
                end_date=end_date,
                days=args.days,
                cleanup_urls=args.cleanup_urls,
            )
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        await generator.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
