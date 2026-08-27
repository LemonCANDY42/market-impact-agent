from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast
from urllib.parse import urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

# GDELT discovery is deliberately non-authoritative; the public DOC endpoint's
# TLS route is intermittently unavailable in China-region networks. Every
# discovered URL is therefore re-fetched from an allow-listed HTTPS publisher
# and independently timestamped before it can enter an evidence manifest.
_GDELT_DOC_ENDPOINT = "http://api.gdeltproject.org/api/v2/doc/doc"
_MAX_RESPONSE_BYTES = 10 * 1024 * 1024
_REGISTERED_PUBLISHER_HOSTS = frozenset(
    {
        "english.news.cn",
        "www.xinhuanet.com",
        "xinhuanet.com",
        "news.xinhuanet.com",
        "www.scmp.com",
        "scmp.com",
    }
)


@dataclass(frozen=True, slots=True)
class GdeltDiscoveredArticle:
    url: str
    title: str
    discovered_at: datetime
    domain: str
    publication_time_authority: bool = False


def discover_gdelt_articles(
    *,
    query: str,
    start_at: datetime,
    end_at: datetime,
    max_records: int = 250,
    timeout_seconds: float = 30.0,
) -> tuple[GdeltDiscoveredArticle, ...]:
    if not query.strip():
        raise ValueError("GDELT discovery query cannot be empty")
    if any(item.tzinfo is None or item.utcoffset() is None for item in (start_at, end_at)):
        raise ValueError("GDELT discovery timestamps must be timezone-aware")
    if start_at >= end_at:
        raise ValueError("GDELT discovery window must be forward")
    if not 1 <= max_records <= 250:
        raise ValueError("GDELT discovery max_records must be within 1..250")
    parameters = {
        "query": query,
        "mode": "artlist",
        "maxrecords": str(max_records),
        "format": "json",
        "startdatetime": _gdelt_timestamp(start_at),
        "enddatetime": _gdelt_timestamp(end_at),
        "sort": "datedesc",
    }
    request = Request(
        f"{_GDELT_DOC_ENDPOINT}?{urlencode(parameters)}",
        headers={
            "Accept": "application/json",
            "User-Agent": "market-impact-agent/0.1 research-discovery",
        },
        method="GET",
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        payload = response.read(_MAX_RESPONSE_BYTES + 1)
    if len(payload) > _MAX_RESPONSE_BYTES:
        raise ValueError("GDELT discovery response exceeds the size limit")
    return parse_gdelt_article_list(payload)


def parse_gdelt_article_list(payload: bytes) -> tuple[GdeltDiscoveredArticle, ...]:
    try:
        raw = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("GDELT discovery response must be valid JSON") from exc
    if not isinstance(raw, dict):
        raise ValueError("GDELT discovery response must be an object")
    body = cast(Mapping[str, object], raw)
    raw_articles = body.get("articles")
    if not isinstance(raw_articles, list):
        raise ValueError("GDELT discovery response requires an articles array")
    articles: list[GdeltDiscoveredArticle] = []
    seen_urls: set[str] = set()
    for raw_article in cast(list[object], raw_articles):
        if not isinstance(raw_article, Mapping):
            continue
        article = cast(Mapping[str, object], raw_article)
        raw_url = article.get("url")
        title = article.get("title")
        seen_date = article.get("seendate")
        domain = article.get("domain")
        if not all(isinstance(item, str) and item for item in (raw_url, title, seen_date)):
            continue
        url = _registered_https_url(cast(str, raw_url))
        if url is None or url in seen_urls:
            continue
        try:
            discovered_at = datetime.strptime(cast(str, seen_date), "%Y%m%dT%H%M%SZ").replace(
                tzinfo=UTC
            )
        except ValueError:
            continue
        seen_urls.add(url)
        articles.append(
            GdeltDiscoveredArticle(
                url=url,
                title=cast(str, title),
                discovered_at=discovered_at,
                domain=domain if isinstance(domain, str) else urlparse(url).hostname or "",
            )
        )
    return tuple(articles)


def _registered_https_url(value: str) -> str | None:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or parsed.hostname not in _REGISTERED_PUBLISHER_HOSTS:
        return None
    return urlunparse(parsed._replace(scheme="https"))


def _gdelt_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y%m%d%H%M%S")
