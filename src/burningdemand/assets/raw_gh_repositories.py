"""Raw GitHub repository signals asset."""

from typing import Any, Dict, List

from dagster import AssetExecutionContext, MaterializeResult, asset

from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.resources.github_resource import GitHubResource
from burningdemand.schema.raw_items import RawItem
from burningdemand.utils.config import config
from burningdemand.utils.raw_utils import materialize_raw


def _repo_to_raw_item(d: Dict[str, Any]) -> RawItem:
    name_with_owner = d.get("nameWithOwner") or ""
    org, product = (name_with_owner.split("/", 1) + [""])[:2]
    description = d.get("description") or ""
    language = ((d.get("primaryLanguage") or {}).get("name") or "").strip()
    stars = d.get("stargazerCount") or 0
    forks = d.get("forkCount") or 0
    open_issues = ((d.get("issues") or {}).get("totalCount") or 0)
    homepage = d.get("homepageUrl") or ""
    license_info = d.get("licenseInfo") or {}
    license_name = (
        license_info.get("spdxId")
        or license_info.get("key")
        or license_info.get("name")
        or ""
    )
    meta = (
        f"\n\n[Repository Signals] stars={stars}, forks={forks}, "
        f"open_issues={open_issues}, language={language}, homepage={homepage}"
    )
    return RawItem(
        url=d.get("url") or "",
        title=name_with_owner,
        body=f"{description}{meta}".strip(),
        created_at=d.get("createdAt") or "",
        source_post_id=str(d.get("id") or ""),
        comments_count=open_issues,
        upvotes_count=stars,
        post_type="repository",
        reactions_count=0,
        org_name=org,
        product_name=product,
        license=license_name,
    )


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw GitHub repositories per day. Stores repo metadata/signals for market-pain and opportunity discovery.",
)
async def raw_gh_repositories(
    context: AssetExecutionContext,
    db: DuckDBResource,
    github: GitHubResource,
) -> MaterializeResult:
    date = context.partition_key
    cfg = config.raw_gh_repositories

    node_fragment = """
        ... on Repository {
            id
            url
            nameWithOwner
            description
            createdAt
            homepageUrl
            licenseInfo { spdxId name }
            stargazerCount
            forkCount
            primaryLanguage { name }
            issues(states: OPEN) { totalCount }
        }
    """
    query_suffix = f"stars:>={cfg.min_stars} sort:stars-desc"
    raw_items, meta = await github.search(
        date,
        node_fragment,
        type="REPOSITORY",
        query_suffix=query_suffix,
        hour_splits=cfg.queries_per_day,
        per_page=cfg.per_page,
    )

    items: List[RawItem] = [_repo_to_raw_item(d) for d in raw_items]
    return await materialize_raw(db, items, meta, "gh_repositories", date)

