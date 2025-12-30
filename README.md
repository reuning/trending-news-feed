
> [!WARNING] 
> This was a mostly vibe coded project as I was curious how powerful it really is. I used [Roo Code](https://roocode.com/) in VS with Claude Sonnet 4.5 through [OpenRouter](https://openrouter.ai/). The project cost about $30 in total. 

# Bluesky Domain-Based Feed Generator

A custom Bluesky feed that displays posts containing links from trusted news domains, ranked by URL popularity, individual post engagement, and freshness.

## What It Does

This feed generator monitors Bluesky's real-time firehose, filters posts containing URLs from whitelisted news domains (like nytimes.com, washingtonpost.com, etc.), and ranks them using a three-factor algorithm:

1. **URL Share Count** - How many times the URL has been shared
2. **Repost Count** - How many times this specific post has been reposted  
3. **Time Decay** - Exponential decay to prioritize fresh content

The result: trending news stories surface quickly, with the most engaging posts about each story ranked highest.

## Quick Start

### Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) package manager

### Installation

```bash
# Install dependencies
uv sync

# Create environment file
cp .env.example .env
# Edit .env with your Bluesky credentials
```

### Running

```bash
# Run both firehose listener and feed server
uv run python main.py both
```

### Viewing the Feed

Once running, access:
- **Web Preview**: http://localhost:8000/preview
- **Statistics**: http://localhost:8000/stats
- **API Docs**: http://localhost:8000/docs

## Ranking Algorithm

Posts are scored using:

```
score = repost_count × share_count × e^(-decay_rate × age_in_hours)
```

Where:
- `repost_count`: Number of reposts for this post (minimum 1)
- `share_count`: Total posts sharing this URL
- `decay_rate`: Configurable parameter (default: 0.05)
- `age_in_hours`: Time since post creation

**Example:** A post with 10 reposts about a URL shared 50 times, posted 2 hours ago:
- Score = 10 × 50 × e^(-0.05 × 2) = **452.5**

After 24 hours, the same post would score **150.5**, allowing fresher content to surface.

## Configuration

### Domain Whitelist

Edit [`config/domains.json`](config/domains.json):

```json
{
  "domains": [
    "nytimes.com",
    "washingtonpost.com",
    "bbc.com",
    "reuters.com"
  ],
  "match_subdomains": true
}
```

### Ranking Parameters

Edit [`config/ranking.json`](config/ranking.json):

```json
{
  "decay_rate": 0.05,
  "max_age_hours": 168,
  "min_share_count": 1,
  "results_limit": 50
}
```

## Project Structure

```
bskyfeed/
├── config/           # Domain whitelist and ranking parameters
├── src/
│   ├── firehose.py   # Real-time Bluesky firehose listener
│   ├── database.py   # SQLite database for posts and URLs
│   ├── ranking.py    # Three-factor ranking algorithm
│   ├── server.py     # FastAPI feed server
│   └── ...
├── tests/            # Comprehensive test suite (154 tests)
└── main.py           # Entry point
```

## Architecture

The system consists of two main components:

1. **Firehose Listener** - Connects to Bluesky's WebSocket firehose, extracts URLs from posts, filters by domain whitelist, and stores in SQLite database
2. **Feed Server** - FastAPI server implementing AT Protocol Feed Generator API, serving ranked posts based on the three-factor algorithm

See [`plans/bluesky-domain-feed-architecture.md`](plans/bluesky-domain-feed-architecture.md) for detailed architecture documentation.

## Development

Built with:
- Python 3.11+ with async/await
- FastAPI for the feed server
- SQLite for data storage
- WebSocket for real-time firehose connection
- Comprehensive test coverage (154 tests)

## License

MIT

## Contributing

This is a personal project built with AI assistance, but suggestions and improvements are welcome!
