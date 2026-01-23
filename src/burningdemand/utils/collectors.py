import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple


from ..utils.keywords import (
    build_stackoverflow_tags,
    matches_keywords,
)
from ..utils.url import iso_date_to_utc_bounds

# ============================================================================
# Collection Configuration (hardcoded)
# ============================================================================

BODY_MAX_LENGTH = 500  # Max length for body text
REDDIT_SUBREDDITS = ["entrepreneur", "SaaS", "startups"]  # Subreddits to monitor


async def collect_source_async(
    source: str,
    date: str,
    apis,
    http,
    context,
) -> Tuple[List[Dict], Dict]:
    """
    Collect items for a single (source, date) partition.
    Supports: github, stackoverflow, reddit, hackernews

    Returns: (items, metadata)

    Item schema:
      {
        "url": str,
        "title": str,
        "body": str,
        "created_at": str (iso8601)
      }
    """
    if source == "github":
        return await _collect_github(
            date=date,
            apis=apis,
            http=http,
            context=context,
        )

    if source == "stackoverflow":
        return await _collect_stackoverflow(
            date=date,
            apis=apis,
            http=http,
            context=context,
        )

    if source == "reddit":
        return await _collect_reddit(
            date=date,
            apis=apis,
            http=http,
            context=context,
        )

    if source == "hackernews":
        return await _collect_hackernews(
            date=date, apis=apis, http=http, context=context
        )

    return [], {"requests": 0, "note": f"unknown source={source}"}


# -----------------------------------------------------------------------------
# GitHub (token required)
# -----------------------------------------------------------------------------


async def _collect_github(
    date: str,
    apis,
    http,
    context,
) -> Tuple[List[Dict], Dict]:
    from ..utils.keywords import get_source_keywords

    headers = {"Authorization": f"token {apis.github_token}"}
    req_count = 0

    # Get all keywords and split into chunks to avoid GitHub's 5 operator limit
    # GitHub allows max 5 AND/OR/NOT operators, so we can use up to 6 keywords per query (5 ORs)
    source_keywords = get_source_keywords("github")
    chunk_size = 6  # 6 keywords = 5 OR operators
    keyword_chunks = [
        source_keywords[i : i + chunk_size]
        for i in range(0, len(source_keywords), chunk_size)
    ]

    # Build multiple queries, one per chunk
    queries = []
    for chunk in keyword_chunks:
        keyword_query = " OR ".join([f'"{kw}"' for kw in chunk])
        queries.append(f"is:issue created:{date} ({keyword_query})")

    # Calculate estimated requests: up to 10 pages per query
    max_pages_per_query = 10
    estimated_requests = len(queries) * max_pages_per_query
    context.log.info(
        f"GitHub collection plan: {len(queries)} queries, "
        f"up to {max_pages_per_query} pages each = ~{estimated_requests} requests max"
    )

    # Track seen URLs to deduplicate across queries
    seen_urls = set()
    all_items = []

    async def fetch_query_pages(query: str):
        """Fetch all pages for a single query."""
        nonlocal req_count
        query_items = []
        for page in range(1, 11):  # GitHub allows up to 10 pages (1000 results max)
            req_count += 1
            resp = await http.request_with_retry_async(
                "GET",
                "https://api.github.com/search/issues",
                params={"q": query, "per_page": 100, "page": page},
                headers=headers,
            )
            page_items = resp.json().get("items", [])
            if not page_items:  # No more results
                break
            query_items.extend(page_items)

        return query_items

    # Fetch all queries in parallel - rate limiting is handled in request_with_retry_async
    all_query_results = await asyncio.gather(*[fetch_query_pages(q) for q in queries])

    # Combine and deduplicate results
    for query_results in all_query_results:
        for it in query_results:
            url = it.get("html_url")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            title = it.get("title") or ""
            body = it.get("body") or ""
            combined_text = f"{title} {body}".lower()

            # Filter by keywords (double-check)
            if not matches_keywords(combined_text, source="github"):
                continue

            all_items.append(
                {
                    "url": url,
                    "title": title,
                    "body": body[:BODY_MAX_LENGTH],
                    "created_at": it.get("created_at") or "",
                }
            )

    context.log.info(
        f"GitHub collection completed: {req_count} requests made, {len(all_items)} items collected"
    )
    return all_items, {"requests": req_count, "queries": len(queries)}


# -----------------------------------------------------------------------------
# StackOverflow / StackExchange (key optional but recommended)
# -----------------------------------------------------------------------------


async def _collect_stackoverflow(
    date: str,
    apis,
    http,
    context,
) -> Tuple[List[Dict], Dict]:
    from_ts, to_ts = iso_date_to_utc_bounds(date)
    req_count = 0

    # Get tags from keywords
    tags = build_stackoverflow_tags()

    # StackExchange max is 100
    stackexchange_key = getattr(apis, "stackexchange_key", None)

    # Calculate and log plan before making requests
    max_pages = 10
    estimated_requests = max_pages
    context.log.info(
        f"StackOverflow collection plan: {max_pages} pages = {estimated_requests} requests max"
    )

    async def fetch_page(page: int):
        nonlocal req_count
        req_count += 1

        params = {
            "fromdate": from_ts,
            "todate": to_ts,
            "site": "stackoverflow",
            "pagesize": 100,
            "page": page,
            # includes body (as HTML)
            "filter": "withbody",
        }
        if tags:
            params["tagged"] = ";".join(tags)
        if stackexchange_key:
            params["key"] = stackexchange_key

        resp = await http.request_with_retry_async(
            "GET",
            "https://api.stackexchange.com/2.3/questions",
            params=params,
        )
        return resp.json().get("items", [])

    pages = await asyncio.gather(*[fetch_page(p) for p in range(1, 11)])
    items: List[Dict] = []
    for page_items in pages:
        for it in page_items:
            title = it.get("title") or ""
            body = it.get("body_markdown") or it.get("body") or ""
            combined_text = f"{title} {body}".lower()

            # Filter by keywords
            if not matches_keywords(combined_text, source="stackoverflow"):
                continue

            created = it.get("creation_date")
            created_iso = (
                datetime.fromtimestamp(int(created), tz=timezone.utc).isoformat()
                if created
                else ""
            )
            items.append(
                {
                    "url": it.get("link") or "",
                    "title": title,
                    # withbody uses "body" (HTML). Some responses include body_markdown depending on filters.
                    "body": body[:BODY_MAX_LENGTH],
                    "created_at": created_iso,
                }
            )

    context.log.info(
        f"StackOverflow collection completed: {req_count} requests made, {len(items)} items collected"
    )
    meta = {"requests": req_count, "used_key": bool(stackexchange_key)}
    return items, meta


# -----------------------------------------------------------------------------
# Reddit (prefer OAuth if credentials provided; fallback to public endpoints)
# -----------------------------------------------------------------------------


async def _collect_reddit(
    date: str,
    apis,
    http,
    context,
) -> Tuple[List[Dict], Dict]:
    from_ts, to_ts = iso_date_to_utc_bounds(date)
    req_count = 0
    subs = REDDIT_SUBREDDITS
    reddit_client_id = getattr(apis, "reddit_client_id", None)
    reddit_client_secret = getattr(apis, "reddit_client_secret", None)
    reddit_user_agent = getattr(apis, "reddit_user_agent", "BurningDemand/0.1")
    limit = 100

    # Calculate and log plan before making requests
    oauth_request = 1 if (reddit_client_id and reddit_client_secret) else 0
    subreddit_requests = len(subs)
    estimated_requests = oauth_request + subreddit_requests
    context.log.info(
        f"Reddit collection plan: {len(subs)} subreddits + {oauth_request} OAuth token = {estimated_requests} requests"
    )

    async def get_oauth_token() -> Optional[str]:
        """
        If client_id/secret are set, use installed-app script flow to fetch an app token.
        """
        nonlocal req_count
        if not reddit_client_id or not reddit_client_secret:
            return None

        req_count += 1
        auth = (reddit_client_id, reddit_client_secret)
        resp = await http.request_with_retry_async(
            "POST",
            "https://www.reddit.com/api/v1/access_token",
            auth=auth,
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": reddit_user_agent},
        )
        data = resp.json()
        return data.get("access_token")

    token = await get_oauth_token()

    # Choose endpoint + headers
    if token:
        base = "https://oauth.reddit.com"
        headers = {"Authorization": f"bearer {token}", "User-Agent": reddit_user_agent}
        auth_mode = "oauth"
    else:
        base = "https://api.reddit.com"
        headers = {"User-Agent": reddit_user_agent}
        auth_mode = "public"

    async def fetch_sub(sub: str):
        nonlocal req_count
        req_count += 1
        resp = await http.request_with_retry_async(
            "GET",
            f"{base}/r/{sub}/new",
            params={"limit": limit},
            headers=headers,
        )
        return resp.json().get("data", {}).get("children", [])

    results = await asyncio.gather(*[fetch_sub(s) for s in subs])

    items: List[Dict] = []
    for children in results:
        for ch in children:
            d = ch.get("data") or {}
            created = int(d.get("created_utc") or 0)
            if from_ts <= created < to_ts:
                title = d.get("title") or ""
                body = d.get("selftext") or ""
                combined_text = f"{title} {body}".lower()

                # Filter by keywords
                if not matches_keywords(combined_text, source="reddit"):
                    continue

                items.append(
                    {
                        "url": f"https://reddit.com{d.get('permalink','')}",
                        "title": title,
                        "body": body[:BODY_MAX_LENGTH],
                        "created_at": datetime.fromtimestamp(
                            created, tz=timezone.utc
                        ).isoformat(),
                    }
                )

    context.log.info(
        f"Reddit collection completed: {req_count} requests made, {len(items)} items collected"
    )
    meta = {
        "requests": req_count,
        "subs": subs,
        "auth_mode": auth_mode,
        "used_oauth": bool(token),
    }
    return items, meta


# -----------------------------------------------------------------------------
# HackerNews (Algolia API; no token)
# -----------------------------------------------------------------------------


async def _collect_hackernews(
    date: str,
    apis,
    http,
    context,
) -> Tuple[List[Dict], Dict]:
    from_ts, to_ts = iso_date_to_utc_bounds(date)
    req_count = 0

    # Calculate and log plan before making requests
    max_pages = 10
    estimated_requests = max_pages
    context.log.info(
        f"HackerNews collection plan: {max_pages} pages = {estimated_requests} requests max"
    )

    async def fetch_page(page: int):
        nonlocal req_count
        req_count += 1
        resp = await http.request_with_retry_async(
            "GET",
            "https://hn.algolia.com/api/v1/search_by_date",
            params={
                "tags": "story",
                "numericFilters": f"created_at_i>{from_ts},created_at_i<{to_ts}",
                "hitsPerPage": 100,
                "page": page,
            },
        )
        return resp.json().get("hits", [])

    pages = await asyncio.gather(*[fetch_page(p) for p in range(0, 10)])

    items: List[Dict] = []
    for hits in pages:
        for it in hits:
            title = it.get("title") or ""
            body = it.get("story_text") or ""
            combined_text = f"{title} {body}".lower()

            # Filter by keywords
            if not matches_keywords(combined_text, source="hackernews"):
                continue

            url = (
                it.get("url")
                or f"https://news.ycombinator.com/item?id={it.get('objectID')}"
            )
            created_i = int(it.get("created_at_i") or 0)
            items.append(
                {
                    "url": url,
                    "title": title,
                    "body": body[:BODY_MAX_LENGTH],
                    "created_at": (
                        datetime.fromtimestamp(created_i, tz=timezone.utc).isoformat()
                        if created_i
                        else ""
                    ),
                }
            )

    context.log.info(
        f"HackerNews collection completed: {req_count} requests made, {len(items)} items collected"
    )
    return items, {"requests": req_count}
