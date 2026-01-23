# burningdemand_dagster/utils/collectors.py
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import httpx

from .retries import request_with_retry_async
from .url import iso_date_to_utc_bounds


async def collect_source_async(
    source: str,
    date: str,
    github_token: str,
    client: httpx.AsyncClient,
) -> Tuple[List[Dict], Dict]:
    if source == "github":
        headers = {"Authorization": f"token {github_token}"}
        req_count = 0

        async def fetch_page(page: int):
            nonlocal req_count
            req_count += 1
            resp = await request_with_retry_async(
                client,
                "GET",
                "https://api.github.com/search/issues",
                params={"q": f"is:issue created:{date}",
                        "per_page": 100, "page": page},
                headers=headers,
            )
            return resp.json().get("items", [])

        pages = await asyncio.gather(*[fetch_page(p) for p in range(1, 11)])
        items: List[Dict] = []
        for page_items in pages:
            for it in page_items:
                items.append(
                    {
                        "url": it["html_url"],
                        "title": it.get("title") or "",
                        "body": (it.get("body") or "")[:500],
                        "created_at": it.get("created_at"),
                    }
                )
        return items, {"requests": req_count}

    if source == "stackoverflow":
        from_ts, to_ts = iso_date_to_utc_bounds(date)
        req_count = 0

        async def fetch_page(page: int):
            nonlocal req_count
            req_count += 1
            resp = await request_with_retry_async(
                client,
                "GET",
                "https://api.stackexchange.com/2.3/questions",
                params={
                    "fromdate": from_ts,
                    "todate": to_ts,
                    "site": "stackoverflow",
                    "pagesize": 100,
                    "page": page,
                    "filter": "withbody",
                },
            )
            return resp.json().get("items", [])

        pages = await asyncio.gather(*[fetch_page(p) for p in range(1, 11)])
        items: List[Dict] = []
        for page_items in pages:
            for it in page_items:
                items.append(
                    {
                        "url": it.get("link") or "",
                        "title": it.get("title") or "",
                        "body": (it.get("body_markdown") or it.get("body") or "")[:500],
                        "created_at": datetime.fromtimestamp(it["creation_date"], tz=timezone.utc).isoformat(),
                    }
                )
        return items, {"requests": req_count}

    if source == "reddit":
        from_ts, to_ts = iso_date_to_utc_bounds(date)
        req_count = 0
        items: List[Dict] = []
        subs = ["entrepreneur", "SaaS", "startups"]

        async def fetch_sub(sub: str):
            nonlocal req_count
            req_count += 1
            resp = await request_with_retry_async(
                client,
                "GET",
                f"https://api.reddit.com/r/{sub}/new",
                params={"limit": 100},
                headers={"User-Agent": "BurningDemand/0.1"},
            )
            return resp.json().get("data", {}).get("children", [])

        results = await asyncio.gather(*[fetch_sub(s) for s in subs])
        for children in results:
            for ch in children:
                d = ch.get("data") or {}
                created = int(d.get("created_utc") or 0)
                if from_ts <= created < to_ts:
                    items.append(
                        {
                            "url": f"https://reddit.com{d.get('permalink','')}",
                            "title": d.get("title") or "",
                            "body": (d.get("selftext") or "")[:500],
                            "created_at": datetime.fromtimestamp(created, tz=timezone.utc).isoformat(),
                        }
                    )
        return items, {"requests": req_count, "subs": subs}

    if source == "hackernews":
        from_ts, to_ts = iso_date_to_utc_bounds(date)
        req_count = 0

        async def fetch_page(page: int):
            nonlocal req_count
            req_count += 1
            resp = await request_with_retry_async(
                client,
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
                url = it.get(
                    "url") or f"https://news.ycombinator.com/item?id={it.get('objectID')}"
                items.append(
                    {
                        "url": url,
                        "title": it.get("title") or "",
                        "body": (it.get("story_text") or "")[:500],
                        "created_at": datetime.fromtimestamp(int(it.get("created_at_i") or 0), tz=timezone.utc).isoformat(),
                    }
                )
        return items, {"requests": req_count}

    return [], {"requests": 0, "note": "unknown source"}
