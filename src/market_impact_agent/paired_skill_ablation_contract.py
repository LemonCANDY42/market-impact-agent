from __future__ import annotations

from market_impact_agent.agent_contracts import EvidencePack, canonical_hash
from market_impact_agent.method_skills import MethodEvidenceDeclaration

RUNTIME_REF = "market-impact.agent-runtime.local-research.v2"
ALLOWED_CAPABILITIES = frozenset({"evidence.read", "pattern.read"})
ALLOWED_SIDE_EFFECTS = ("read_only",)
ALLOWED_TOOLS = frozenset({"read_evidence", "read_pattern_pack"})


def paired_skill_research_instruction(
    evidence_pack: EvidencePack,
    *,
    eligible_horizon_sessions: int,
) -> str:
    if eligible_horizon_sessions < 1:
        raise ValueError("eligible_horizon_sessions must be positive")
    targets = ", ".join(evidence_pack.allowed_targets)
    horizon_label = (
        "one trading session"
        if eligible_horizon_sessions == 1
        else f"{eligible_horizon_sessions} trading sessions"
    )
    return (
        "Assess this identity-masked, opened development information state without using "
        "information outside the Evidence Pack. Read every Evidence Item and the complete "
        "Pattern Pack before deciding. Use only the registered read-only tools and selected "
        "research methods. Test material counterevidence and abstain when the event-to-target "
        "link or persistence over the registered horizon is unresolved. Do not infer the "
        "historical identity "
        "or use memorized outcomes. The only eligible targets are "
        f"[{targets}], the only direction is up, and the only horizon is {horizon_label}. "
        f"The proposal event_id is [{evidence_pack.event_id}]; copy that exact event_id into "
        "the output and never replace it with an inferred identity or description. "
        "A candidate requires confidence at least 0.5. Return exactly one eligible candidate "
        "or abstain."
    )


def paired_skill_common_input_hash(
    evidence_pack: EvidencePack,
    method_evidence_declaration: MethodEvidenceDeclaration,
    *,
    eligible_horizon_sessions: int,
) -> str:
    evidence_pack_hash = canonical_hash(evidence_pack.to_dict())
    method_evidence_declaration.validate_against(
        evidence_pack_id=evidence_pack.pack_id,
        evidence_pack_hash=evidence_pack_hash,
        evidence_ids=frozenset(item.evidence_id for item in evidence_pack.evidence),
        pattern_pack_ids=frozenset(item.pack_id for item in evidence_pack.pattern_packs),
        outcomes_opened=method_evidence_declaration.outcomes_opened,
    )
    return canonical_hash(
        {
            "runtime_ref": RUNTIME_REF,
            "evidence_pack": evidence_pack.to_dict(),
            "research_instruction": paired_skill_research_instruction(
                evidence_pack,
                eligible_horizon_sessions=eligible_horizon_sessions,
            ),
            "method_evidence_declaration": method_evidence_declaration.to_dict(),
            "allowed_capabilities": sorted(ALLOWED_CAPABILITIES),
            "allowed_side_effects": list(ALLOWED_SIDE_EFFECTS),
            "allowed_tools": sorted(ALLOWED_TOOLS),
        }
    )
