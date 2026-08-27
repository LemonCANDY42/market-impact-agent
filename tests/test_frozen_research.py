import asyncio
import json
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.frozen_research import FrozenResearchRepository


def fixture_root(repo_root: Path) -> Path:
    return repo_root / "examples" / "agent" / "energy_supply"


def load_fixture(repo_root: Path) -> FrozenResearchRepository:
    root = fixture_root(repo_root)
    return FrozenResearchRepository.from_files(
        evidence_pack_path=root / "evidence-pack.json",
        evidence_documents_path=root / "evidence-documents.json",
        pattern_pack_paths=(root / "pattern-pack.json",),
    )


def test_synthetic_energy_fixture_is_fully_content_bound(repo_root: Path) -> None:
    repository = load_fixture(repo_root)

    evidence = asyncio.run(repository.read_evidence({"evidence_id": "official-outage"}))
    pattern_id = repository.evidence_pack.pattern_packs[0].pack_id
    pattern = asyncio.run(repository.read_pattern_pack({"pack_id": pattern_id}))
    tools = repository.tool_descriptors()

    assert isinstance(evidence, dict)
    assert evidence["point_in_time_cutoff"] == "2026-01-15T08:30:00Z"
    assert isinstance(pattern, dict)
    assert (
        canonical_hash(cast(dict[str, object], pattern))
        == repository.evidence_pack.pattern_packs[0].content_hash
    )
    assert {tool.name for tool in tools} == {"read_evidence", "read_pattern_pack"}
    by_name = {tool.name: tool for tool in tools}
    assert by_name["read_evidence"].max_result_bytes == 65_536
    assert by_name["read_pattern_pack"].max_result_bytes == 16_384


def test_frozen_energy_fixture_rejects_tampered_content(
    repo_root: Path,
    tmp_path: Path,
) -> None:
    root = fixture_root(repo_root)
    documents = json.loads((root / "evidence-documents.json").read_text(encoding="utf-8"))
    documents["documents"]["official-outage"]["fact"] = "tampered"
    tampered = tmp_path / "tampered.json"
    tampered.write_text(json.dumps(documents), encoding="utf-8")

    with pytest.raises(ValueError, match="content hash mismatch"):
        FrozenResearchRepository.from_files(
            evidence_pack_path=root / "evidence-pack.json",
            evidence_documents_path=tampered,
            pattern_pack_paths=(root / "pattern-pack.json",),
        )


@pytest.fixture
def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]
