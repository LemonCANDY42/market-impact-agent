from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from market_impact_agent.news_discovery import parse_gdelt_article_list


def test_gdelt_article_list_is_discovery_only_and_normalizes_registered_hosts() -> None:
    payload = json.dumps(
        {
            "articles": [
                {
                    "url": "http://www.xinhuanet.com/english/2018-01/24/c_136921022.htm",
                    "title": "Chinese shares close higher Wednesday",
                    "seendate": "20180124T091500Z",
                    "domain": "xinhuanet.com",
                },
                {
                    "url": "https://www.scmp.com/business/article/123/example",
                    "title": "China stocks move",
                    "seendate": "20180124T080000Z",
                    "domain": "scmp.com",
                },
                {
                    "url": "https://example.test/unregistered",
                    "title": "Unregistered",
                    "seendate": "20180124T070000Z",
                    "domain": "example.test",
                },
                {
                    "url": "https://www.scmp.com/business/article/123/example",
                    "title": "Duplicate",
                    "seendate": "20180124T080000Z",
                    "domain": "scmp.com",
                },
            ]
        }
    ).encode()

    articles = parse_gdelt_article_list(payload)

    assert tuple(item.url for item in articles) == (
        "https://www.xinhuanet.com/english/2018-01/24/c_136921022.htm",
        "https://www.scmp.com/business/article/123/example",
    )
    assert articles[0].discovered_at == datetime(2018, 1, 24, 9, 15, tzinfo=UTC)
    assert articles[0].publication_time_authority is False


def test_gdelt_article_list_rejects_malformed_json() -> None:
    with pytest.raises(ValueError, match="valid JSON"):
        parse_gdelt_article_list(b"not-json")

    with pytest.raises(ValueError, match="articles array"):
        parse_gdelt_article_list(b"{}")
