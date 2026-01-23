# burningdemand_dagster/utils/url.py
import hashlib
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

_TRACKING_PARAMS_PREFIXES = ("utm_",)
_TRACKING_PARAMS_EXACT = {
    "gclid",
    "fbclid",
    "mc_cid",
    "mc_eid",
    "ref",
    "ref_src",
    "ref_url",
    "source",
    "campaign",
}


def normalize_url(url: str) -> str:
    try:
        p = urlparse(url.strip())
        scheme = (p.scheme or "https").lower()
        netloc = (p.netloc or "").lower()

        if not netloc and p.path and ("." in p.path.split("/")[0]):
            netloc = p.path.split("/")[0].lower()
            path = "/" + "/".join(p.path.split("/")[1:])
        else:
            path = p.path or "/"

        fragment = ""

        q = []
        for k, v in parse_qsl(p.query, keep_blank_values=True):
            kl = k.lower()
            if kl in _TRACKING_PARAMS_EXACT:
                continue
            if any(kl.startswith(pref) for pref in _TRACKING_PARAMS_PREFIXES):
                continue
            q.append((k, v))
        q.sort(key=lambda kv: (kv[0], kv[1]))
        query = urlencode(q, doseq=True)

        return urlunparse((scheme, netloc, path, p.params, query, fragment))
    except Exception:
        return url.strip()


def url_hash(url: str) -> str:
    return hashlib.sha256(normalize_url(url).encode("utf-8")).hexdigest()


def iso_date_to_utc_bounds(date_str: str) -> tuple[int, int]:
    dt = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
    from_ts = int(dt.timestamp())
    to_ts = int((dt + timedelta(days=1)).timestamp())
    return from_ts, to_ts
