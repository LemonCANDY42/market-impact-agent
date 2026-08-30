from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from datetime import UTC, datetime

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.csrc_news import (
    CsrcNewsHTTPClient,
    CsrcNewsProvider,
    UrllibCsrcNewsHTTPClient,
    csrc_news_source_from_dict,
)
from market_impact_agent.data_inputs import (
    DataInputHarness,
    DataPITLane,
    DataQuery,
    DataQueryMode,
    DataSnapshot,
    LocalDataSnapshotStore,
)
from market_impact_agent.nbs_macro_release import (
    NbsMacroReleaseHTTPClient,
    NbsMacroReleaseProvider,
    UrllibNbsMacroReleaseHTTPClient,
    nbs_macro_release_source_from_dict,
)
from market_impact_agent.prospective_collection_runtime import (
    ProspectiveCollectionAdapterKind,
    ProspectiveCollectionJob,
)
from market_impact_agent.prospective_data import ProspectiveCollectionPolicy
from market_impact_agent.tushare_observation import (
    TushareObservationProvider,
    TushareObservationTransport,
    tushare_observation_source_from_dict,
)


def collect_prospective_source_snapshot(
    *,
    job: ProspectiveCollectionJob,
    policy: ProspectiveCollectionPolicy,
    source_config: Mapping[str, object],
    store: LocalDataSnapshotStore,
    tushare_token: str | None = None,
    tushare_transport: TushareObservationTransport | None = None,
    csrc_http_client: CsrcNewsHTTPClient | None = None,
    nbs_http_client: NbsMacroReleaseHTTPClient | None = None,
    scheduled_for: datetime | None = None,
    clock: Callable[[], datetime] | None = None,
) -> DataSnapshot:
    """Capture one accepted route and materialize its immutable prospective Snapshot."""

    if job.collection_policy_id != policy.policy_id:
        raise ValueError("prospective collector job and policy do not match")
    if job.source_config_hash != canonical_hash(source_config):
        raise ValueError("prospective collector source config does not match its job")
    if len(policy.sources) != 1:
        raise ValueError("prospective collector requires exactly one source route")
    if policy.rolling_window is not None and scheduled_for is None:
        raise ValueError("rolling prospective collector requires its logical due time")
    request_window_start, request_parameters = policy.resolve_query(
        policy.window_start if scheduled_for is None else scheduled_for
    )

    if job.adapter_kind is ProspectiveCollectionAdapterKind.CSRC_NEWS:
        config = csrc_news_source_from_dict(dict(source_config))
        http_client = (
            UrllibCsrcNewsHTTPClient(timeout_seconds=job.provider_timeout_seconds)
            if csrc_http_client is None
            else csrc_http_client
        )
        provider = CsrcNewsProvider(
            (config,),
            http_client=http_client,
            clock=clock,
        )
        _assert_provider_binding(
            provider_id=provider.manifest.provider_id,
            provider_version=provider.manifest.provider_version,
            manifest_hash=canonical_hash(provider.manifest.to_dict()),
            upstream_source=config.source_id,
            source_config_hash=config.artifact_hash,
            policy=policy,
        )
        captures = asyncio.run(
            provider.collect(
                window_start=policy.window_start,
                parameters=request_parameters,
            )
        )
        capture_cutoff = max(item.retrieved_at for item in captures)
        replay_provider = provider.replay(captures)
    elif job.adapter_kind is ProspectiveCollectionAdapterKind.NBS_MACRO_RELEASE:
        config = nbs_macro_release_source_from_dict(dict(source_config))
        if request_parameters != {"indicators": list(config.indicators)}:
            raise ValueError(
                "NBS macro release collection policy indicators must exactly match "
                "the source config"
            )
        http_client = (
            UrllibNbsMacroReleaseHTTPClient(timeout_seconds=job.provider_timeout_seconds)
            if nbs_http_client is None
            else nbs_http_client
        )
        provider = NbsMacroReleaseProvider(
            (config,),
            http_client=http_client,
            clock=clock,
        )
        _assert_provider_binding(
            provider_id=provider.manifest.provider_id,
            provider_version=provider.manifest.provider_version,
            manifest_hash=canonical_hash(provider.manifest.to_dict()),
            upstream_source=config.source_id,
            source_config_hash=config.artifact_hash,
            policy=policy,
        )
        captures = asyncio.run(
            provider.collect(
                window_start=policy.window_start,
                parameters=request_parameters,
                require_complete_indicator_scope=True,
            )
        )
        capture_cutoff = max(item.retrieved_at for item in captures)
        replay_provider = provider.replay(captures)
    elif job.adapter_kind is ProspectiveCollectionAdapterKind.TUSHARE_OBSERVATION:
        if tushare_token is None or not tushare_token or tushare_token != tushare_token.strip():
            raise ValueError("TUSHARE_TOKEN is not configured")
        config = tushare_observation_source_from_dict(dict(source_config))
        provider = TushareObservationProvider(
            tushare_token,
            (config,),
            timeout_seconds=job.provider_timeout_seconds,
            transport=tushare_transport,
            clock=clock,
        )
        _assert_provider_binding(
            provider_id=provider.manifest.provider_id,
            provider_version=provider.manifest.provider_version,
            manifest_hash=canonical_hash(provider.manifest.to_dict()),
            upstream_source=config.source_id,
            source_config_hash=config.artifact_hash,
            policy=policy,
        )
        capture = asyncio.run(
            provider.collect(
                source_id=config.source_id,
                parameters=request_parameters,
            )
        )
        capture_cutoff = capture.retrieved_at
        replay_provider = provider.replay((capture,))
    else:  # pragma: no cover - closed enum protects this branch
        raise ValueError("unsupported prospective collection adapter")

    query = DataQuery.build(
        capability=policy.capability,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=capture_cutoff.astimezone(UTC),
        window_start=request_window_start,
        source_policy_id=policy.policy_id,
        parameters=request_parameters,
        sources=policy.sources,
        minimum_data_sources=len(policy.sources),
    )
    harness = DataInputHarness(
        store,
        provider_timeout_seconds=job.provider_timeout_seconds,
    )
    harness.register(replay_provider)
    return asyncio.run(harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING))


def _assert_provider_binding(
    *,
    provider_id: str,
    provider_version: str,
    manifest_hash: str,
    upstream_source: str,
    source_config_hash: str,
    policy: ProspectiveCollectionPolicy,
) -> None:
    source = policy.sources[0]
    if (
        source.provider_id != provider_id
        or source.provider_version != provider_version
        or source.manifest_hash != manifest_hash
        or source.upstream_source != upstream_source
        or source.source_config_hash != source_config_hash
    ):
        raise ValueError("prospective collector Provider binding does not match its policy")
