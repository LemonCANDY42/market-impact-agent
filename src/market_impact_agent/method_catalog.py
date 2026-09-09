"""Frozen optional methods, read through the existing pi tool journal.

Discovery is Harness-owned. Only metadata is sent initially; the model chooses
which bodies to read. These methods confer no new data or execution authority.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_runtime import (
    LoadedSkill,
    SkillRegistry,
    ToolDescriptor,
    ToolSideEffect,
)
from market_impact_agent.runtime_store import ArtifactStore


@dataclass(frozen=True, slots=True)
class FrozenMethodCatalog:
    skills: tuple[LoadedSkill, ...]

    def __post_init__(self) -> None:
        names = {s.manifest.name for s in self.skills}
        paths = {s.manifest.instructions_path for s in self.skills}
        if len(names) != len(self.skills) or len(paths) != len(self.skills):
            raise ValueError("frozen method names and locations must be unique")
        if any(
            sha256(s.instructions.encode()).hexdigest() != s.manifest.instructions_hash
            for s in self.skills
        ):
            raise ValueError("method body differs from its manifest")

    @classmethod
    def freeze(
        cls, registry: SkillRegistry, *, allowed_capabilities: frozenset[str] = frozenset()
    ) -> FrozenMethodCatalog:
        manifests = registry.discover()
        loaded: dict[str, LoadedSkill] = {}
        for manifest in manifests:
            for skill in registry.load((manifest.name,), allowed_capabilities=allowed_capabilities):
                loaded[skill.manifest.name] = skill
        return cls(tuple(loaded[name] for name in sorted(loaded)))

    def metadata(self) -> list[dict[str, object]]:
        return [
            {
                "name": skill.manifest.name,
                "description": skill.manifest.description,
                "filePath": str(skill.manifest.instructions_path),
            }
            for skill in self.skills
        ]

    def identity(self) -> dict[str, object]:
        return {
            "mode": "on-demand-v1",
            "skills": [
                {
                    **metadata,
                    "manifest_hash": skill.manifest.manifest_hash,
                    "instructions_hash": skill.manifest.instructions_hash,
                }
                for skill, metadata in zip(self.skills, self.metadata(), strict=True)
            ],
        }

    def persist(self, artifacts: ArtifactStore) -> None:
        # Artifacts are evidence, never an additional mutable selection ledger.
        for skill in self.skills:
            artifacts.put_json(
                {"manifest_hash": skill.manifest.manifest_hash, "body": skill.instructions}
            )
        artifacts.put_json(self.identity())

    def read_tool(self) -> ToolDescriptor:
        by_path = {str(s.manifest.instructions_path): s for s in self.skills}

        async def read(arguments: dict[str, object]) -> object:
            path = arguments.get("path")
            # Match exact offered paths before touching the filesystem; no directory
            # traversal, globbing, sibling histories, or implicit global discovery.
            if not isinstance(path, str) or path not in by_path:
                raise PermissionError("read is restricted to frozen method locations")
            skill = by_path[path]
            if skill.manifest.instructions_path.resolve() != skill.manifest.instructions_path:
                raise PermissionError("method location changed after catalog freeze")
            current = skill.manifest.instructions_path.read_bytes()
            if sha256(current).hexdigest() != skill.manifest.instructions_hash:
                raise PermissionError("method body changed after catalog freeze")
            return {
                "name": skill.manifest.name,
                "instructions_hash": skill.manifest.instructions_hash,
                "body": skill.instructions,
            }

        return ToolDescriptor(
            name="read",
            version=canonical_hash(self.identity()),
            description="Read the body of an optional method at its listed location.",
            input_schema={
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
                "additionalProperties": False,
            },
            required_capabilities=frozenset(),
            side_effect=ToolSideEffect.READ_ONLY,
            timeout_seconds=5,
            max_result_bytes=max(
                (len(s.instructions.encode()) + 4096 for s in self.skills), default=4096
            ),
            handler=read,
        )
