"""Firehose Listener - Connects to Bluesky's firehose and processes incoming posts"""

import asyncio
import logging
from typing import Callable, Optional
from datetime import datetime

from atproto import (
    CAR,
    AtUri,
    firehose_models,
    models,
    AsyncFirehoseSubscribeReposClient,
    parse_subscribe_repos_message,
)

logger = logging.getLogger(__name__)


class FirehoseListener:
    """
    Listens to the Bluesky firehose and processes posts in real-time.
    
    This component establishes a WebSocket connection to Bluesky's firehose,
    parses incoming post records, and passes them to a callback function for
    further processing.
    """

    def __init__(
        self,
        on_post_callback: Callable,
        on_repost_callback: Optional[Callable] = None,
        firehose_url: str = "wss://bsky.network",
    ):
        """
        Initialize the Firehose Listener.
        
        Args:
            on_post_callback: Async function to call when a new post is received.
                             Should accept (uri, cid, author_did, record, timestamp)
            on_repost_callback: Optional async function to call when a repost is received.
                               Should accept (repost_uri, original_post_uri, author_did, timestamp)
            firehose_url: WebSocket URL for the Bluesky firehose
        """
        self.on_post_callback = on_post_callback
        self.on_repost_callback = on_repost_callback
        self.firehose_url = firehose_url
        self.client: Optional[AsyncFirehoseSubscribeReposClient] = None
        self._running = False
        self._posts_processed = 0
        self._posts_with_links = 0
        self._posts_with_whitelisted_links = 0
        self._reposts_processed = 0
        self._reposts_tracked = 0
        self._errors = 0
        self._processing_tasks = set()  # Track background tasks
        self._max_concurrent_tasks = 100  # Limit concurrent processing

    async def start(self):
        """
        Start listening to the firehose.
        
        This method establishes a connection and begins processing messages.
        It will run indefinitely until stop() is called or an error occurs.
        """
        self._running = True
        logger.info(f"Starting firehose listener on {self.firehose_url}")

        try:
            # Create async firehose client
            self.client = AsyncFirehoseSubscribeReposClient()
            
            # Define the async message handler
            async def on_message_handler(message):
                if not self._running:
                    return
                
                logger.debug(f"Received message from firehose")
                
                # Process message in background to avoid blocking the firehose
                # This prevents ConsumerTooSlow errors
                task = asyncio.create_task(self._process_message_wrapper(message))
                self._processing_tasks.add(task)
                task.add_done_callback(self._processing_tasks.discard)
                
                # If we have too many concurrent tasks, wait for some to complete
                if len(self._processing_tasks) >= self._max_concurrent_tasks:
                    logger.warning(f"Processing queue full ({len(self._processing_tasks)} tasks), waiting...")
                    # Wait for at least half to complete before continuing
                    while len(self._processing_tasks) >= self._max_concurrent_tasks // 2:
                        await asyncio.sleep(0.1)
            
            # Start listening to the firehose with async callback
            logger.info("Connecting to firehose...")
            await self.client.start(on_message_handler)

        except Exception as e:
            logger.error(f"Fatal error in firehose listener: {e}", exc_info=True)
            raise
        finally:
            self._running = False
            logger.info(
                f"Firehose listener stopped. "
                f"Processed: {self._posts_processed}, Errors: {self._errors}"
            )

    async def _process_message_wrapper(self, message):
        """
        Wrapper for processing messages that handles errors.
        
        Args:
            message: Raw message from the firehose
        """
        try:
            await self._process_message(message)
        except Exception as e:
            self._errors += 1
            logger.error(f"Error processing message: {e}", exc_info=True)
            # Continue processing despite errors

    async def _process_message(self, message):
        """
        Process a single message from the firehose.
        
        Args:
            message: Raw message from the firehose
        """

        # Parse the message
        commit = parse_subscribe_repos_message(message)

        # We only care about commit messages (new posts, likes, etc.)
        # Check if this is actually a Commit type (not other message types like Info, etc.)
        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            logger.debug(f"Skipping non-commit message: {type(commit)}")
            return

        # Check if commit has blocks (data)
        if not commit.blocks:
            logger.debug("Commit has no blocks")
            return

        # Parse the commit data
        try:
            # Decode the CAR file containing the operations
            car = CAR.from_bytes(commit.blocks)
            logger.debug(f"Processing commit with {len(commit.ops)} operations")

            # Process each operation in the commit
            for op in commit.ops:
                # We only care about creates (new posts and reposts)
                if op.action != 'create':
                    continue
                
                # Handle posts (app.bsky.feed.post)
                if op.path.startswith('app.bsky.feed.post/'):
                    # Get the record from the CAR file
                    if op.cid is None:
                        continue
                        
                    record = car.blocks.get(op.cid)
                    if record is None:
                        continue

                    # Extract post information
                    uri = AtUri.from_str(
                        f"at://{commit.repo}/{op.path}"
                    )
                    
                    # Call the callback with post data
                    await self._handle_post(
                        uri=str(uri),
                        cid=str(op.cid),
                        author_did=commit.repo,
                        record=record,
                        timestamp=commit.time,
                    )
                    
                    self._posts_processed += 1
                    
                    # Log progress every 100 posts with link statistics
                    if self._posts_processed % 100 == 0:
                        logger.info(
                            f"Processed {self._posts_processed} posts | "
                            f"Whitelisted: {self._posts_with_whitelisted_links} | "
                            f"Reposts: {self._reposts_processed} (tracked: {self._reposts_tracked}) | "
                            f"Errors: {self._errors}"
                        )
                
                # Handle reposts (app.bsky.feed.repost)
                elif op.path.startswith('app.bsky.feed.repost/'):
                    # Get the record from the CAR file
                    if op.cid is None:
                        continue
                        
                    record = car.blocks.get(op.cid)
                    if record is None:
                        continue

                    # Extract repost information
                    uri = AtUri.from_str(
                        f"at://{commit.repo}/{op.path}"
                    )
                    
                    # Call the repost handler
                    await self._handle_repost(
                        repost_uri=str(uri),
                        author_did=commit.repo,
                        record=record,
                        timestamp=commit.time,
                    )
                    
                    self._reposts_processed += 1

        except Exception as e:
            logger.error(f"Error parsing commit: {e}", exc_info=True)
            raise

    async def _handle_post(
        self,
        uri: str,
        cid: str,
        author_did: str,
        record: dict,
        timestamp: str,
    ):
        """
        Handle a single post by calling the callback.
        
        Args:
            uri: AT Protocol URI of the post
            cid: Content ID of the post
            author_did: DID of the post author
            record: Post record data
            timestamp: Timestamp of the post
        """
        try:
            # Extract text and other relevant fields from record
            text = record.get('text', '')
            
            # Check if post has any links (either in text or embeds)
            has_links = self._has_links(record)
            
            # Only process posts with links to save resources
            if not has_links:
                return

            # Track posts with links
            self._posts_with_links += 1

            # Call the callback function (which will filter by whitelist)
            # The callback should return True if the post was accepted (has whitelisted links)
            result = await self.on_post_callback(
                uri=uri,
                cid=cid,
                author_did=author_did,
                record=record,
                timestamp=timestamp,
            )
            
            # Track posts with whitelisted links if callback indicates success
            if result:
                self._posts_with_whitelisted_links += 1

        except Exception as e:
            logger.error(f"Error handling post {uri}: {e}", exc_info=True)
            raise

    async def _handle_repost(
        self,
        repost_uri: str,
        author_did: str,
        record: dict,
        timestamp: str,
    ):
        """
        Handle a repost event from the firehose.
        
        This method extracts the original post URI from the repost record
        and calls the repost callback if one is registered.
        
        Args:
            repost_uri: AT Protocol URI of the repost itself
            author_did: DID of the user who reposted
            record: Repost record data
            timestamp: Timestamp of the repost
        """
        try:
            # Extract the subject (original post) URI from the repost record
            subject = record.get('subject')
            if not subject:
                logger.debug(f"Repost {repost_uri} has no subject")
                return
            
            original_post_uri = subject.get('uri')
            if not original_post_uri:
                logger.debug(f"Repost {repost_uri} subject has no URI")
                return
            
            logger.debug(f"Repost detected: {repost_uri} -> {original_post_uri}")
            
            # Call the repost callback if registered
            if self.on_repost_callback:
                result = await self.on_repost_callback(
                    repost_uri=repost_uri,
                    original_post_uri=original_post_uri,
                    author_did=author_did,
                    timestamp=timestamp,
                )
                
                # Track if the repost was for a post we're tracking
                if result:
                    self._reposts_tracked += 1
                    logger.debug(f"Tracked repost for {original_post_uri}")
            
        except Exception as e:
            logger.error(f"Error handling repost {repost_uri}: {e}", exc_info=True)
            raise

    def _has_links(self, record: dict) -> bool:
        """
        Check if a post record contains any links.
        
        This checks multiple sources where URLs can appear in Bluesky posts:
        1. Facets (richtext annotations) - the primary way URLs are marked
        2. Embeds (external link cards)
        3. Deprecated entities field (for backwards compatibility)
        4. Raw text (as a fallback heuristic)
        
        Args:
            record: Post record data
            
        Returns:
            True if the post contains links, False otherwise
        """
        # 1. Check facets for link annotations (primary method)
        # Facets are the standard way URLs are annotated in posts
        facets = record.get('facets', [])
        if facets:
            for facet in facets:
                features = facet.get('features', [])
                for feature in features:
                    # Check if this feature is a link
                    if feature.get('$type') == 'app.bsky.richtext.facet#link':
                        return True
        
        # 2. Check deprecated entities field (for backwards compatibility)
        # Some older posts may still use this deprecated field
        entities = record.get('entities', [])
        if entities:
            for entity in entities:
                if entity.get('type') == 'link':
                    return True
        
        # 3. Check for link embeds
        embed = record.get('embed')
        if embed:
            # Check for external link embed (link cards)
            if embed.get('$type') == 'app.bsky.embed.external':
                return True
            
            # Check for record with media (which might have external links)
            if embed.get('$type') == 'app.bsky.embed.recordWithMedia':
                media = embed.get('media', {})
                if media.get('$type') == 'app.bsky.embed.external':
                    return True
            
            # Check for record embed (quoted posts might contain URLs)
            if embed.get('$type') == 'app.bsky.embed.record':
                # Note: We don't recursively check the embedded record's content
                # as that would require additional processing. The embedded record
                # itself will be processed separately by the firehose.
                pass

        # 4. Fallback: Check for URLs in raw text (simple heuristic)
        # This catches cases where URLs might not be properly annotated
        text = record.get('text', '')
        if 'http://' in text or 'https://' in text:
            return True

        return False

    async def stop(self):
        """Stop the firehose listener gracefully."""
        logger.info("Stopping firehose listener...")
        self._running = False
        
        # Wait for all background tasks to complete
        if self._processing_tasks:
            logger.info(f"Waiting for {len(self._processing_tasks)} background tasks to complete...")
            await asyncio.gather(*self._processing_tasks, return_exceptions=True)
            logger.info("All background tasks completed")
        
        # Give it a moment to finish current message
        await asyncio.sleep(1)

    @property
    def is_running(self) -> bool:
        """Check if the listener is currently running."""
        return self._running

    @property
    def stats(self) -> dict:
        """Get statistics about the listener."""
        return {
            'posts_processed': self._posts_processed,
            'posts_with_links': self._posts_with_links,
            'posts_with_whitelisted_links': self._posts_with_whitelisted_links,
            'reposts_processed': self._reposts_processed,
            'reposts_tracked': self._reposts_tracked,
            'errors': self._errors,
            'is_running': self._running,
        }


