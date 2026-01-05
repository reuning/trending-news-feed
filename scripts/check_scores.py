#!/usr/bin/env python3
"""
Manual score checker for debugging post rankings.

This script allows you to check the scores of posts in the database
while the server is running. It's useful for understanding why some
posts have different scores even when they appear similar.

Usage:
    python scripts/check_scores.py [options]

Examples:
    # Check top 10 posts with scores
    python scripts/check_scores.py

    # Check specific post by URI
    python scripts/check_scores.py --uri "at://did:plc:abc/app.bsky.feed.post/123"

    # Check all posts from a specific domain
    python scripts/check_scores.py --domain "arxiv.org"

    # Compare two posts side-by-side
    python scripts/check_scores.py --compare "uri1" "uri2"

    # Show detailed breakdown of score calculation
    python scripts/check_scores.py --detailed
"""

import asyncio
import argparse
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database import Database
from src.ranking import RankingEngine, RankingConfig


def format_post_info(post: Dict[str, Any], detailed: bool = False, config: Optional[RankingConfig] = None) -> str:
    """Format post information for display."""
    lines = []
    lines.append(f"\n{'='*80}")
    lines.append(f"URI: {post['uri']}")
    lines.append(f"Author: {post['author_did']}")
    lines.append(f"Post Created: {post['created_at']}")
    lines.append(f"URL: {post['url']}")
    lines.append(f"Domain: {post['domain']}")
    
    if post.get('text'):
        text = post['text'][:100] + "..." if len(post['text']) > 100 else post['text']
        lines.append(f"Text: {text}")
    
    lines.append(f"\n--- Scoring Factors ---")
    lines.append(f"Share Count: {post['share_count']}")
    lines.append(f"Repost Count: {post.get('repost_count', 0)}")
    lines.append(f"URL Age (hours): {post.get('url_age_hours', 0):.2f}")
    if 'url_first_seen' in post:
        lines.append(f"URL First Seen: {post['url_first_seen']}")
    lines.append(f"SCORE: {post.get('score', 0):.4f}")
    
    if detailed and 'score' in post:
        lines.append(f"\n--- Score Breakdown ---")
        # Recreate the calculation for transparency
        import math
        url_age_hours = post.get('url_age_hours', 0)
        share_count = post['share_count']
        repost_count = post.get('repost_count', 0)
        
        # Use config values if provided, otherwise use defaults
        if config:
            decay_rate = config.decay_rate
            repost_weight = config.repost_weight
        else:
            # Fallback to defaults (should match RankingConfig defaults)
            decay_rate = 0.05
            repost_weight = 1.0
        
        decay_factor = math.exp(-decay_rate * url_age_hours)
        effective_repost = max(1, repost_count)
        weighted_repost = math.pow(effective_repost, repost_weight)
        
        lines.append(f"  Decay rate: {decay_rate}")
        lines.append(f"  Repost weight: {repost_weight}")
        lines.append(f"  Decay factor: exp(-{decay_rate} * {url_age_hours:.2f}) = {decay_factor:.4f}")
        lines.append(f"  Effective repost: max(1, {repost_count}) = {effective_repost}")
        lines.append(f"  Weighted repost: {effective_repost}^{repost_weight} = {weighted_repost:.4f}")
        lines.append(f"  Formula: {weighted_repost:.4f} * {share_count} * {decay_factor:.4f}")
        lines.append(f"  Result: {post['score']:.4f}")
    
    lines.append(f"{'='*80}")
    return "\n".join(lines)


def compare_posts(post1: Dict[str, Any], post2: Dict[str, Any]) -> str:
    """Compare two posts side-by-side."""
    lines = []
    lines.append(f"\n{'='*80}")
    lines.append("POST COMPARISON")
    lines.append(f"{'='*80}")
    
    lines.append(f"\n{'Post 1':<40} | {'Post 2':<40}")
    lines.append(f"{'-'*40} | {'-'*40}")
    
    # URIs
    uri1 = post1['uri'][:38] + "..." if len(post1['uri']) > 38 else post1['uri']
    uri2 = post2['uri'][:38] + "..." if len(post2['uri']) > 38 else post2['uri']
    lines.append(f"{uri1:<40} | {uri2:<40}")
    
    # Scores
    lines.append(f"\n{'SCORES':<40} | {'SCORES':<40}")
    lines.append(f"{post1.get('score', 0):<40.4f} | {post2.get('score', 0):<40.4f}")
    
    # Factors
    lines.append(f"\n{'FACTORS':<40} | {'FACTORS':<40}")
    lines.append(f"Share Count: {post1['share_count']:<28} | Share Count: {post2['share_count']:<28}")
    lines.append(f"Repost Count: {post1.get('repost_count', 0):<27} | Repost Count: {post2.get('repost_count', 0):<27}")
    lines.append(f"URL Age (hours): {post1.get('url_age_hours', 0):<24.2f} | URL Age (hours): {post2.get('url_age_hours', 0):<24.2f}")
    
    # Difference analysis
    lines.append(f"\n{'DIFFERENCES':<40}")
    score_diff = post1.get('score', 0) - post2.get('score', 0)
    share_diff = post1['share_count'] - post2['share_count']
    repost_diff = post1.get('repost_count', 0) - post2.get('repost_count', 0)
    url_age_diff = post1.get('url_age_hours', 0) - post2.get('url_age_hours', 0)
    
    lines.append(f"Score difference: {score_diff:+.4f}")
    lines.append(f"Share count difference: {share_diff:+d}")
    lines.append(f"Repost count difference: {repost_diff:+d}")
    lines.append(f"URL age difference: {url_age_diff:+.2f} hours")
    
    # Analysis
    lines.append(f"\n{'ANALYSIS':<40}")
    if abs(score_diff) < 0.01:
        lines.append("Posts have nearly identical scores")
    elif score_diff > 0:
        lines.append("Post 1 scores higher because:")
        if share_diff > 0:
            lines.append(f"  - Higher share count (+{share_diff})")
        if repost_diff > 0:
            lines.append(f"  - More reposts (+{repost_diff})")
        if url_age_diff < 0:
            lines.append(f"  - URL is newer by {abs(url_age_diff):.2f} hours")
    else:
        lines.append("Post 2 scores higher because:")
        if share_diff < 0:
            lines.append(f"  - Higher share count (+{abs(share_diff)})")
        if repost_diff < 0:
            lines.append(f"  - More reposts (+{abs(repost_diff)})")
        if url_age_diff > 0:
            lines.append(f"  - URL is newer by {url_age_diff:.2f} hours")
    
    lines.append(f"{'='*80}")
    return "\n".join(lines)


async def check_top_posts(engine: RankingEngine, limit: int = 10, detailed: bool = False):
    """Check top N posts with their scores."""
    print(f"\nFetching top {limit} posts...")
    posts = await engine.rank_posts(limit=limit)
    
    if not posts:
        print("No posts found in database")
        return
    
    print(f"\nFound {len(posts)} posts")
    for i, post in enumerate(posts, 1):
        print(f"\n--- Rank #{i} ---")
        print(format_post_info(post, detailed, engine.config))


async def check_post_by_uri(db: Database, engine: RankingEngine, uri: str, detailed: bool = False):
    """Check a specific post by URI."""
    print(f"\nLooking up post: {uri}")
    
    # Get post from database
    post_data = await db.get_post(uri)
    if not post_data:
        print(f"Post not found: {uri}")
        return None
    
    # Get all ranked posts to find this one with its score
    all_posts = await engine.rank_posts(limit=None)
    scored_post = None
    for p in all_posts:
        if p['uri'] == uri:
            scored_post = p
            break
    
    if not scored_post:
        print(f"Post found in database but not in ranked results (may be filtered out)")
        print(f"Post data: {post_data}")
        return None
    
    print(format_post_info(scored_post, detailed, engine.config))
    return scored_post


async def check_domain_posts(engine: RankingEngine, domain: str, limit: int = 10, detailed: bool = False):
    """Check posts from a specific domain."""
    print(f"\nFetching posts from domain: {domain}")
    
    # Get all posts and filter by domain
    all_posts = await engine.rank_posts(limit=None)
    domain_posts = [p for p in all_posts if p['domain'] == domain]
    
    if not domain_posts:
        print(f"No posts found for domain: {domain}")
        return
    
    print(f"\nFound {len(domain_posts)} posts from {domain}")
    for i, post in enumerate(domain_posts[:limit], 1):
        print(f"\n--- Rank #{i} (Score: {post['score']:.4f}) ---")
        print(format_post_info(post, detailed, engine.config))


async def compare_two_posts(db: Database, engine: RankingEngine, uri1: str, uri2: str):
    """Compare two posts side-by-side."""
    print(f"\nComparing posts...")
    
    post1 = await check_post_by_uri(db, engine, uri1, detailed=False)
    post2 = await check_post_by_uri(db, engine, uri2, detailed=False)
    
    if post1 and post2:
        print(compare_posts(post1, post2))


async def show_stats(db: Database, engine: RankingEngine):
    """Show database and ranking statistics."""
    print("\n" + "="*80)
    print("DATABASE & RANKING STATISTICS")
    print("="*80)
    
    # Database stats
    db_stats = await db.get_stats()
    print("\nDatabase Stats:")
    print(f"  Total posts: {db_stats['total_posts']}")
    print(f"  Unique URLs: {db_stats['unique_urls']}")
    print(f"  Total shares: {db_stats['total_shares']}")
    
    # Ranking stats
    rank_stats = await engine.get_ranking_stats()
    print("\nRanking Stats:")
    print(f"  Ranked posts: {rank_stats['ranked_posts']}")
    print(f"  Top score: {rank_stats['top_score']:.4f}")
    print(f"  Average score: {rank_stats['avg_score']:.4f}")
    print(f"  Min score: {rank_stats['min_score']:.4f}")
    
    print("\nRanking Config:")
    config = rank_stats['config']
    print(f"  Decay rate: {config['decay_rate']}")
    print(f"  Max age (hours): {config['max_age_hours']}")
    print(f"  Min share count: {config['min_share_count']}")
    print(f"  Min repost count: {config['min_repost_count']}")
    print(f"  Repost weight: {config['repost_weight']}")
    print(f"  Results limit: {config['results_limit']}")
    
    print("="*80)


async def main():
    parser = argparse.ArgumentParser(
        description="Check database scores for debugging post rankings",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "--db",
        default="data/feed.db",
        help="Path to database file (default: data/feed.db)"
    )
    
    parser.add_argument(
        "--config",
        default="config/ranking.json",
        help="Path to ranking config (default: config/ranking.json)"
    )
    
    parser.add_argument(
        "--uri",
        help="Check specific post by URI"
    )
    
    parser.add_argument(
        "--domain",
        help="Check posts from specific domain"
    )
    
    parser.add_argument(
        "--compare",
        nargs=2,
        metavar=("URI1", "URI2"),
        help="Compare two posts side-by-side"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of posts to show (default: 10)"
    )
    
    parser.add_argument(
        "--detailed",
        action="store_true",
        help="Show detailed score breakdown"
    )
    
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show database and ranking statistics"
    )
    
    args = parser.parse_args()
    
    # Initialize database and ranking engine
    print(f"Connecting to database: {args.db}")
    db = Database(args.db)
    await db.initialize()
    
    print(f"Loading ranking config: {args.config}")
    config = RankingConfig.from_file(args.config)
    engine = RankingEngine(db, config)
    
    try:
        # Execute requested operation
        if args.stats:
            await show_stats(db, engine)
        elif args.compare:
            await compare_two_posts(db, engine, args.compare[0], args.compare[1])
        elif args.uri:
            await check_post_by_uri(db, engine, args.uri, args.detailed)
        elif args.domain:
            await check_domain_posts(engine, args.domain, args.limit, args.detailed)
        else:
            # Default: show top posts
            await check_top_posts(engine, args.limit, args.detailed)
    
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
