from copy import deepcopy

from market_impact_agent.agent_schema import validate_agent_contract

from .test_agent_contracts import artifact, evidence_pack, pattern_pack, proposal


def test_agent_contract_schemas_accept_canonical_domain_objects() -> None:
    assert validate_agent_contract(pattern_pack().to_dict(), "pattern-pack.schema.json") == ()
    assert validate_agent_contract(evidence_pack().to_dict(), "evidence-pack.schema.json") == ()
    assert validate_agent_contract(proposal().to_dict(), "judgment-proposal.schema.json") == ()
    assert validate_agent_contract(artifact().to_dict(), "judgment-artifact.schema.json") == ()


def test_agent_contract_schemas_reject_extra_fields_and_invalid_abstention() -> None:
    evidence = deepcopy(evidence_pack().to_dict())
    evidence["unexpected"] = True
    errors = validate_agent_contract(evidence, "evidence-pack.schema.json")
    assert any("Additional properties" in error for error in errors)

    invalid = deepcopy(proposal().to_dict())
    invalid["decision"] = "abstain"
    invalid["blockers"] = []
    errors = validate_agent_contract(invalid, "judgment-proposal.schema.json")
    assert any("non-empty" in error or "too short" in error for error in errors)
    assert any("non-empty" in error or "too long" in error for error in errors)
