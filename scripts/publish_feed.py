#!/usr/bin/env python3
"""
Script to publish the feed generator to Bluesky.

This script creates a feed generator record in your Bluesky account,
making the feed discoverable and subscribable by other users.
"""

import os
import sys
from pathlib import Path

# Add parent directory to path to import from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from atproto import Client
from dotenv import load_dotenv


def publish_feed():
    """Publish the feed generator to Bluesky."""
    
    # Load environment variables
    load_dotenv()
    
    handle = os.getenv("BSKY_HANDLE")
    password = os.getenv("BSKY_PASSWORD")
    hostname = os.getenv("FEED_HOSTNAME", "http://localhost:8000")
    
    if not handle or not password:
        print("❌ Error: BSKY_HANDLE and BSKY_PASSWORD must be set in .env file")
        sys.exit(1)
    
    # Extract hostname without protocol
    clean_hostname = hostname.replace('http://', '').replace('https://', '').split(':')[0]
    
    # Create DID (using did:web format)
    feed_did = f"did:web:{clean_hostname}"
    
    print(f"🔐 Authenticating as {handle}...")
    
    try:
        # Create client and login
        client = Client()
        profile = client.login(handle, password)
        
        print(f"✅ Authenticated successfully!")
        print(f"   DID: {profile.did}")
        print(f"   Handle: {profile.handle}")
        
        # Feed configuration
        feed_name = "trending-news"
        display_name = "Trending News Stories"
        description = "Feed tracks most shared news stories from a limited set of news domains. Displays the most popular posts sharing the most popular stories."
        
        print(f"\n📝 Creating feed generator record...")
        print(f"   Feed Name: {feed_name}")
        print(f"   Display Name: {display_name}")
        print(f"   Service DID: {feed_did}")
        print(f"   Service Endpoint: {hostname}")
        
        # Create the feed generator record
        # The record key (rkey) is the feed name
        record = {
            "$type": "app.bsky.feed.generator",
            "did": feed_did,
            "displayName": display_name,
            "description": description,
            "createdAt": client.get_current_time_iso(),
        }
        
        print(f"\n🔍 DEBUG: Feed generator record:")
        print(f"   Record: {record}")
        print(f"   Repo: {profile.did}")
        print(f"   Collection: app.bsky.feed.generator")
        print(f"   RKey: {feed_name}")
        
        # Put the record in the user's repository
        try:
            response = client.com.atproto.repo.put_record(
                {
                    "repo": profile.did,
                    "collection": "app.bsky.feed.generator",
                    "rkey": feed_name,
                    "record": record,
                }
            )
        except Exception as put_error:
            print(f"\n❌ Error details from put_record:")
            print(f"   Error type: {type(put_error).__name__}")
            print(f"   Error message: {str(put_error)}")
            if hasattr(put_error, 'response'):
                print(f"   Response: {put_error.response}")
            raise
        
        # Construct the feed URI
        feed_uri = f"at://{profile.did}/app.bsky.feed.generator/{feed_name}"
        
        print(f"\n✅ Feed published successfully!")
        print(f"\n📋 Feed Details:")
        print(f"   Feed URI: {feed_uri}")
        print(f"   Record CID: {response.cid}")
        print(f"   Record URI: {response.uri}")
        
        print(f"\n🔗 Share this URI with users to subscribe:")
        print(f"   {feed_uri}")
        
        print(f"\n📱 To add this feed in the Bluesky app:")
        print(f"   1. Open Bluesky app")
        print(f"   2. Go to Feeds")
        print(f"   3. Tap the '+' button")
        print(f"   4. Search for '{display_name}' or paste the feed URI")
        print(f"   5. Tap 'Add to my feeds'")
        
        print(f"\n⚠️  Important: Make sure your feed server is running at:")
        print(f"   {hostname}")
        print(f"   And that the .well-known/did.json endpoint is accessible.")
        
        return feed_uri
        
    except Exception as e:
        print(f"\n❌ Error publishing feed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def verify_feed(feed_uri: str):
    """Verify that the feed is accessible."""
    
    load_dotenv()
    
    handle = os.getenv("BSKY_HANDLE")
    password = os.getenv("BSKY_PASSWORD")
    
    print(f"\n🔍 Verifying feed accessibility...")
    
    try:
        client = Client()
        client.login(handle, password)
        
        # Try to get the feed skeleton
        response = client.app.bsky.feed.get_feed(
            {
                "feed": feed_uri,
                "limit": 10,
            }
        )
        
        post_count = len(response.feed) if hasattr(response, 'feed') else 0
        
        print(f"✅ Feed is accessible!")
        print(f"   Retrieved {post_count} posts")
        
        if post_count == 0:
            print(f"\n⚠️  Warning: Feed returned 0 posts.")
            print(f"   Make sure the firehose listener is running and collecting posts.")
        
    except Exception as e:
        print(f"❌ Error verifying feed: {e}")
        print(f"\n💡 Troubleshooting:")
        print(f"   1. Check that your feed server is running")
        print(f"   2. Verify the .well-known/did.json endpoint is accessible")
        print(f"   3. Check server logs for errors")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Publish Bluesky feed generator")
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify feed after publishing"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("  Bluesky Feed Generator Publisher")
    print("=" * 60)
    
    feed_uri = publish_feed()
    
    if args.verify and feed_uri:
        verify_feed(feed_uri)
    
    print("\n" + "=" * 60)
    print("  Done!")
    print("=" * 60)
