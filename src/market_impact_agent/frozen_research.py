from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import (
    EvidencePack,
    PatternPack,
    canonical_hash,
    evidence_pack_from_dict,
    pattern_pack_from_dict,
)
from market_impact_agent.agent_runtime import ToolDescriptor, ToolSideEffect


class FrozenResearchRepository:
    def __init__(
        self,
        *,
        evidence_pack: EvidencePack,
        evidence_documents: Mapping[str, object],
        pattern_packs: Mapping[str, PatternPack],
    ) -> None:
        self.evidence_pack = evidence_pack
        self._evidence_documents = dict(evidence_documents)
        self._pattern_packs = dict(pattern_packs)
        expected_evidence = {item.evidence_id for item in evidence_pack.evidence}
        if set(self._evidence_documents) != expected_evidence:
            raise ValueError("frozen evidence documents must exactly match the Evidence Pack")
        for reference in evidence_pack.evidence:
            document = self._evidence_documents[reference.evidence_id]
            if canonical_hash(document) != reference.content_hash:
                raise ValueError(f"frozen evidence content hash mismatch: {reference.evidence_id}")
        expected_patterns = {item.pack_id for item in evidence_pack.pattern_packs}
        if set(self._pattern_packs) != expected_patterns:
            raise ValueError("frozen Pattern Packs must exactly match the Evidence Pack")
        references = {item.pack_id: item for item in evidence_pack.pattern_packs}
        for pack_id, pattern in self._pattern_packs.items():
            reference = references[pack_id]
            if pattern.pack_id != reference.pack_id or pattern.version != reference.version:
                raise ValueError(f"frozen Pattern Pack identity mismatch: {pack_id}")
            if canonical_hash(pattern.to_dict()) != reference.content_hash:
                raise ValueError(f"frozen Pattern Pack content hash mismatch: {pack_id}")

    @classmethod
    def from_files(
        cls,
        *,
        evidence_pack_path: Path,
        evidence_documents_path: Path,
        pattern_pack_paths: tuple[Path, ...],
    ) -> FrozenResearchRepository:
        evidence_payload = _read_object(evidence_pack_path)
        documents_payload = _read_object(evidence_documents_path)
        documents = documents_payload.get("documents")
        if not isinstance(documents, dict):
            raise TypeError("frozen evidence document file requires a documents object")
        raw_documents = cast(dict[object, object], documents)
        if any(not isinstance(key, str) for key in raw_documents):
            raise TypeError("frozen evidence document ids must be strings")
        patterns: dict[str, PatternPack] = {}
        for path in pattern_pack_paths:
            pattern = pattern_pack_from_dict(_read_object(path))
            if pattern.pack_id in patterns:
                raise ValueError(f"duplicate frozen Pattern Pack: {pattern.pack_id}")
            patterns[pattern.pack_id] = pattern
        return cls(
            evidence_pack=evidence_pack_from_dict(evidence_payload),
            evidence_documents=cast(dict[str, object], documents),
            pattern_packs=patterns,
        )

    async def read_evidence(self, arguments: dict[str, object]) -> object:
        evidence_id = arguments.get("evidence_id")
        if not isinstance(evidence_id, str):
            raise TypeError("evidence_id must be a string")
        reference = next(
            (item for item in self.evidence_pack.evidence if item.evidence_id == evidence_id),
            None,
        )
        if reference is None:
            raise KeyError(f"unknown frozen evidence_id: {evidence_id}")
        return {
            "reference": reference.to_dict(),
            "document": self._evidence_documents[evidence_id],
            "point_in_time_cutoff": self.evidence_pack.to_dict()["as_of"],
        }

    async def read_pattern_pack(self, arguments: dict[str, object]) -> object:
        pack_id = arguments.get("pack_id")
        if not isinstance(pack_id, str):
            raise TypeError("pack_id must be a string")
        pattern = self._pattern_packs.get(pack_id)
        if pattern is None:
            raise KeyError(f"unknown frozen Pattern Pack: {pack_id}")
        return pattern.to_dict()

    def tool_descriptors(self) -> tuple[ToolDescriptor, ...]:
        return (
            ToolDescriptor(
                name="read_evidence",
                version=f"evidence-pack:{self.evidence_pack.pack_id}",
                description="Read one content-verified item from the frozen Evidence Pack.",
                input_schema={
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["evidence_id"],
                    "properties": {
                        "evidence_id": {
                            "type": "string",
                            "enum": [item.evidence_id for item in self.evidence_pack.evidence],
                        }
                    },
                },
                required_capabilities=frozenset({"evidence.read"}),
                side_effect=ToolSideEffect.READ_ONLY,
                timeout_seconds=2,
                max_result_bytes=16_384,
                handler=self.read_evidence,
            ),
            ToolDescriptor(
                name="read_pattern_pack",
                version=f"evidence-pack:{self.evidence_pack.pack_id}",
                description="Read one versioned Pattern Pack frozen before the event cutoff.",
                input_schema={
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["pack_id"],
                    "properties": {
                        "pack_id": {
                            "type": "string",
                            "enum": [item.pack_id for item in self.evidence_pack.pattern_packs],
                        }
                    },
                },
                required_capabilities=frozenset({"pattern.read"}),
                side_effect=ToolSideEffect.READ_ONLY,
                timeout_seconds=2,
                max_result_bytes=16_384,
                handler=self.read_pattern_pack,
            ),
        )


def _read_object(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"frozen research file must contain an object: {path}")
    raw = cast(dict[object, object], payload)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"frozen research object keys must be strings: {path}")
    return cast(dict[str, object], payload)
