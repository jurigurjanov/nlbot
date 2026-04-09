from __future__ import annotations

import threading

import pytest

from xtb_bot.multi_strategy import (
    AggregatorConfig,
    AggregatorLifecycleState,
    FamilyIntentDecision,
    IntentRejectReason,
    RebalanceMode,
    RollingPercentileNormalizer,
    StrategyIntent,
    VirtualPositionAggregator,
    allocate_cost_pro_rata,
    allocate_partial_fill_pro_rata,
    allocate_partial_fill_seniority_first,
    compute_slippage_cost,
    reconcile_positions,
)


def _intent(
    *,
    strategy_id: str,
    symbol: str = "US100",
    seq: int = 1,
    target: float = 1.0,
    max_abs: float = 1.0,
    confidence: float = 0.8,
    weight: float = 1.0,
    regime_tag: str = "",
    stop_loss_pips: float = 10.0,
    take_profit_pips: float = 20.0,
    ttl_sec: float = 60.0,
    now: float = 1_000.0,
) -> StrategyIntent:
    return StrategyIntent.new(
        strategy_id=strategy_id,
        symbol=symbol,
        seq=seq,
        market_data_seq=1,
        target_qty_lots=target,
        max_abs_qty_lots=max_abs,
        confidence_raw=confidence,
        regime_tag=regime_tag,
        intent_weight=weight,
        stop_loss_pips=stop_loss_pips,
        take_profit_pips=take_profit_pips,
        ttl_sec=ttl_sec,
        created_ts=now,
    )


def test_intent_upsert_rejects_stale_sequence():
    aggregator = VirtualPositionAggregator()
    first = aggregator.upsert_intent(_intent(strategy_id="g1", seq=1), now_ts=1_000.0)
    assert first.accepted is True

    stale = aggregator.upsert_intent(_intent(strategy_id="g1", seq=1, target=0.5), now_ts=1_001.0)
    assert stale.accepted is False
    assert stale.reason == IntentRejectReason.STALE_SEQ

    fresh = aggregator.upsert_intent(_intent(strategy_id="g1", seq=2, target=0.5), now_ts=1_002.0)
    assert fresh.accepted is True
    assert fresh.stored_intent is not None
    assert fresh.stored_intent.target_qty_lots == pytest.approx(0.5)


def test_intent_upsert_clamps_target_to_max_abs():
    aggregator = VirtualPositionAggregator()
    accepted = aggregator.upsert_intent(
        _intent(strategy_id="trend", seq=1, target=2.5, max_abs=1.2),
        now_ts=1_000.0,
    )
    assert accepted.accepted is True
    assert accepted.stored_intent is not None
    assert accepted.stored_intent.target_qty_lots == pytest.approx(1.2)


def test_intent_upsert_rejects_expired_payload():
    aggregator = VirtualPositionAggregator()
    expired = aggregator.upsert_intent(
        _intent(strategy_id="mr", seq=1, ttl_sec=1.0, now=1_000.0),
        now_ts=1_005.0,
    )
    assert expired.accepted is False
    assert expired.reason == IntentRejectReason.EXPIRED


def test_intent_upsert_rejects_zero_ttl_payload_as_invalid():
    aggregator = VirtualPositionAggregator()
    invalid = aggregator.upsert_intent(
        _intent(strategy_id="mr", seq=1, ttl_sec=0.0, now=1_000.0),
        now_ts=1_000.0,
    )

    assert invalid.accepted is False
    assert invalid.reason == IntentRejectReason.INVALID


def test_rolling_percentile_normalizer_uses_history_after_min_samples():
    normalizer = RollingPercentileNormalizer(window=8, min_samples=3, default_value=0.5)
    assert normalizer.normalize("g1", 0.2) == pytest.approx(0.5)
    assert normalizer.normalize("g1", 0.3) == pytest.approx(0.625)
    assert normalizer.normalize("g1", 0.1) == pytest.approx(0.3)
    assert normalizer.normalize("g1", 0.3) == pytest.approx(0.75)


def test_rolling_percentile_normalizer_cleanup_removes_inactive_keys():
    normalizer = RollingPercentileNormalizer(window=8, min_samples=1, default_value=0.5)
    normalizer.normalize("g1", 0.2)
    normalizer.normalize("g2", 0.4)
    removed = normalizer.cleanup({"g1"})
    assert removed == 1
    assert "g1" in normalizer._history
    assert "g2" not in normalizer._history


def test_compute_net_intent_applies_rounding_min_open_and_deadband():
    config = AggregatorConfig(
        lot_step_lots=0.1,
        min_open_lot=0.1,
        deadband_lots=0.1,
        min_conflict_power=0.0,
    )
    normalizer = RollingPercentileNormalizer(window=16, min_samples=1, default_value=1.0)
    aggregator = VirtualPositionAggregator(config=config, normalizer=normalizer)
    aggregator.upsert_intent(_intent(strategy_id="trend", target=0.26), now_ts=1_000.0)

    decision = aggregator.compute_net_intent(symbol="US100", real_position_lots=0.0, now_ts=1_001.0)
    assert decision.mode == RebalanceMode.NORMAL
    assert decision.target_net_qty_lots == pytest.approx(0.3)
    assert decision.execution_delta_lots == pytest.approx(0.3)
    assert decision.state == AggregatorLifecycleState.REBALANCING

    deadband = aggregator.compute_net_intent(symbol="US100", real_position_lots=0.25, now_ts=1_002.0)
    assert deadband.target_net_qty_lots == pytest.approx(0.3)
    assert deadband.execution_delta_lots == pytest.approx(0.0)
    assert deadband.state == AggregatorLifecycleState.LIVE_SYNCED


def test_compute_net_intent_uses_reconciliation_epsilon_for_lot_delta():
    config = AggregatorConfig(
        lot_step_lots=0.00001,
        min_open_lot=0.0,
        deadband_lots=0.0,
        reconciliation_epsilon_lots=0.0001,
    )
    normalizer = RollingPercentileNormalizer(window=16, min_samples=1, default_value=1.0)
    aggregator = VirtualPositionAggregator(config=config, normalizer=normalizer)
    aggregator.upsert_intent(_intent(strategy_id="trend", target=0.10002), now_ts=1_000.0)

    decision = aggregator.compute_net_intent(symbol="US100", real_position_lots=0.1, now_ts=1_001.0)
    assert decision.mode == RebalanceMode.NORMAL
    assert decision.target_net_qty_lots == pytest.approx(0.10002)
    assert decision.execution_delta_lots == pytest.approx(0.0)
    assert decision.state == AggregatorLifecycleState.LIVE_SYNCED


def test_compute_net_intent_derives_weighted_stop_and_take_profit_for_target_side():
    config = AggregatorConfig(
        lot_step_lots=0.1,
        min_open_lot=0.1,
        deadband_lots=0.0,
    )
    normalizer = RollingPercentileNormalizer(window=16, min_samples=1, default_value=1.0)
    aggregator = VirtualPositionAggregator(config=config, normalizer=normalizer)
    aggregator.upsert_intent(
        _intent(strategy_id="a", target=1.0, stop_loss_pips=10.0, take_profit_pips=20.0),
        now_ts=1_000.0,
    )
    aggregator.upsert_intent(
        _intent(strategy_id="b", seq=1, target=0.5, stop_loss_pips=50.0, take_profit_pips=100.0),
        now_ts=1_000.0,
    )
    aggregator.upsert_intent(
        _intent(strategy_id="hedge", seq=1, target=-0.2, stop_loss_pips=30.0, take_profit_pips=60.0),
        now_ts=1_000.0,
    )

    decision = aggregator.compute_net_intent(symbol="US100", real_position_lots=0.0, now_ts=1_001.0)
    assert decision.target_net_qty_lots == pytest.approx(1.3)
    assert decision.weighted_stop_loss_pips == pytest.approx((10.0 + (0.5 * 50.0)) / 1.5)
    assert decision.weighted_take_profit_pips == pytest.approx((20.0 + (0.5 * 100.0)) / 1.5)
    assert decision.weighted_stop_coverage == pytest.approx(1.0)
    assert decision.weighted_take_profit_coverage == pytest.approx(1.0)


def test_compute_net_intent_weighted_stop_tracks_coverage_when_some_intents_have_no_levels():
    config = AggregatorConfig(
        lot_step_lots=0.1,
        min_open_lot=0.1,
        deadband_lots=0.0,
    )
    normalizer = RollingPercentileNormalizer(window=16, min_samples=1, default_value=1.0)
    aggregator = VirtualPositionAggregator(config=config, normalizer=normalizer)
    aggregator.upsert_intent(
        _intent(strategy_id="a", target=1.0, stop_loss_pips=10.0, take_profit_pips=20.0),
        now_ts=1_000.0,
    )
    aggregator.upsert_intent(
        _intent(strategy_id="b", seq=1, target=1.0, stop_loss_pips=0.0, take_profit_pips=0.0),
        now_ts=1_000.0,
    )

    decision = aggregator.compute_net_intent(symbol="US100", real_position_lots=0.0, now_ts=1_001.0)
    assert decision.target_net_qty_lots == pytest.approx(2.0)
    assert decision.weighted_stop_loss_pips == pytest.approx(10.0)
    assert decision.weighted_take_profit_pips == pytest.approx(20.0)
    assert decision.weighted_stop_coverage == pytest.approx(0.5)
    assert decision.weighted_take_profit_coverage == pytest.approx(0.5)


def test_compute_net_intent_conflict_hold_is_reduce_only():
    config = AggregatorConfig(
        lot_step_lots=0.1,
        min_open_lot=0.1,
        deadband_lots=0.0,
        min_conflict_power=0.05,
        conflict_ratio_low=0.8,
        conflict_ratio_high=1.25,
    )
    normalizer = RollingPercentileNormalizer(window=16, min_samples=1, default_value=1.0)
    aggregator = VirtualPositionAggregator(config=config, normalizer=normalizer)
    aggregator.upsert_intent(_intent(strategy_id="trend", target=1.0), now_ts=1_000.0)
    aggregator.upsert_intent(_intent(strategy_id="mr", seq=1, target=-1.0), now_ts=1_000.0)

    decision = aggregator.compute_net_intent(symbol="US100", real_position_lots=0.6, now_ts=1_001.0)
    assert decision.conflict_detected is True
    assert decision.mode == RebalanceMode.CONFLICT_HOLD
    assert decision.reduce_only is True
    assert decision.target_net_qty_lots == pytest.approx(0.0)
    assert decision.execution_delta_lots == pytest.approx(-0.6)


def test_compute_net_intent_conflict_hold_allows_partial_reduce_without_flip():
    config = AggregatorConfig(
        lot_step_lots=0.1,
        min_open_lot=0.1,
        deadband_lots=0.0,
        min_conflict_power=0.05,
        conflict_ratio_low=0.8,
        conflict_ratio_high=1.25,
    )
    normalizer = RollingPercentileNormalizer(window=16, min_samples=1, default_value=1.0)
    aggregator = VirtualPositionAggregator(config=config, normalizer=normalizer)
    aggregator.upsert_intent(_intent(strategy_id="trend", target=1.0), now_ts=1_000.0)
    aggregator.upsert_intent(_intent(strategy_id="mr", seq=1, target=-1.0), now_ts=1_000.0)

    decision = aggregator.compute_net_intent(
        symbol="US100",
        real_position_lots=1.0,
        strategy_weights={"trend": 1.0, "mr": 0.9},
        now_ts=1_001.0,
    )
    assert decision.conflict_detected is True
    assert decision.mode == RebalanceMode.CONFLICT_HOLD
    assert decision.reduce_only is True
    assert decision.requested_net_qty_lots == pytest.approx(0.1)
    assert decision.target_net_qty_lots == pytest.approx(0.1)
    assert decision.execution_delta_lots == pytest.approx(-0.9)


def test_compute_net_intent_conflict_hold_clips_flip_request_to_flat():
    config = AggregatorConfig(
        lot_step_lots=0.1,
        min_open_lot=0.1,
        deadband_lots=0.0,
        min_conflict_power=0.05,
        conflict_ratio_low=0.8,
        conflict_ratio_high=1.25,
    )
    normalizer = RollingPercentileNormalizer(window=16, min_samples=1, default_value=1.0)
    aggregator = VirtualPositionAggregator(config=config, normalizer=normalizer)
    aggregator.upsert_intent(_intent(strategy_id="trend", target=1.0), now_ts=1_000.0)
    aggregator.upsert_intent(_intent(strategy_id="mr", seq=1, target=-1.0), now_ts=1_000.0)

    decision = aggregator.compute_net_intent(
        symbol="US100",
        real_position_lots=1.0,
        strategy_weights={"trend": 0.9, "mr": 1.0},
        now_ts=1_001.0,
    )
    assert decision.conflict_detected is True
    assert decision.mode == RebalanceMode.CONFLICT_HOLD
    assert decision.reduce_only is True
    assert decision.requested_net_qty_lots == pytest.approx(-0.1)
    assert decision.target_net_qty_lots == pytest.approx(0.0)
    assert decision.execution_delta_lots == pytest.approx(-1.0)


def test_compute_net_intent_regime_multipliers_break_conflict_lock_in_hard_trend():
    config = AggregatorConfig(
        lot_step_lots=0.1,
        min_open_lot=0.1,
        deadband_lots=0.0,
        min_conflict_power=0.05,
        conflict_ratio_low=0.8,
        conflict_ratio_high=1.25,
    )
    normalizer = RollingPercentileNormalizer(window=16, min_samples=1, default_value=1.0)
    aggregator = VirtualPositionAggregator(config=config, normalizer=normalizer)
    aggregator.upsert_intent(
        _intent(strategy_id="trend", target=1.0, regime_tag="trend_following"),
        now_ts=1_000.0,
    )
    aggregator.upsert_intent(
        _intent(strategy_id="mr", seq=1, target=-1.0, regime_tag="mean_reversion"),
        now_ts=1_000.0,
    )

    baseline = aggregator.compute_net_intent(symbol="US100", real_position_lots=0.0, now_ts=1_001.0)
    assert baseline.mode == RebalanceMode.CONFLICT_HOLD
    assert baseline.target_net_qty_lots == pytest.approx(0.0)
    assert baseline.conflict_detected is True

    boosted = aggregator.compute_net_intent(
        symbol="US100",
        real_position_lots=0.0,
        regime_multipliers={
            ("trend", "trend_following"): 1.5,
            ("mr", "mean_reversion"): 0.2,
        },
        now_ts=1_002.0,
    )
    assert boosted.mode == RebalanceMode.NORMAL
    assert boosted.conflict_detected is False
    assert boosted.target_net_qty_lots == pytest.approx(1.3)
    assert boosted.execution_delta_lots == pytest.approx(1.3)


def test_compute_net_intent_enters_emergency_mode_on_heartbeat_loss():
    config = AggregatorConfig(lot_step_lots=0.1, min_open_lot=0.1)
    normalizer = RollingPercentileNormalizer(window=16, min_samples=1, default_value=1.0)
    aggregator = VirtualPositionAggregator(config=config, normalizer=normalizer)
    aggregator.upsert_intent(_intent(strategy_id="trend", target=1.0), now_ts=1_000.0)

    decision = aggregator.compute_net_intent(
        symbol="US100",
        real_position_lots=0.4,
        now_ts=1_001.0,
        heartbeat_ok=False,
    )
    assert decision.mode == RebalanceMode.EMERGENCY
    assert decision.state == AggregatorLifecycleState.EMERGENCY
    assert decision.target_net_qty_lots == pytest.approx(0.0)
    assert decision.execution_delta_lots == pytest.approx(-0.4)


def test_resolve_state_updates_symbol_state_under_aggregator_lock():
    aggregator = VirtualPositionAggregator()
    entered = threading.Event()
    completed = threading.Event()

    def _resolve() -> None:
        entered.set()
        aggregator._resolve_state(
            symbol="US100",
            intents_available=True,
            execution_delta_lots=0.0,
            market_data_fresh=True,
            heartbeat_ok=True,
            risk_blocked=False,
            reconcile_error=False,
        )
        completed.set()

    with aggregator._lock:
        thread = threading.Thread(target=_resolve)
        thread.start()
        assert entered.wait(timeout=1.0) is True
        assert completed.wait(timeout=0.05) is False

    thread.join(timeout=1.0)
    assert completed.is_set() is True
    assert aggregator.state_for_symbol("US100") == AggregatorLifecycleState.LIVE_SYNCED


def test_partial_fill_allocation_is_pro_rata_and_deterministic():
    result = allocate_partial_fill_pro_rata(
        fill_qty_lots=0.5,
        requested_deltas_lots={"alpha": 0.7, "beta": 0.3},
        lot_step_lots=0.1,
    )
    assert result.allocations_by_strategy["alpha"] == pytest.approx(0.4)
    assert result.allocations_by_strategy["beta"] == pytest.approx(0.1)
    assert result.allocated_qty_lots == pytest.approx(0.5)
    assert result.dust_qty_lots == pytest.approx(0.0)


def test_partial_fill_allocation_uses_lexicographic_tie_breaker():
    result = allocate_partial_fill_pro_rata(
        fill_qty_lots=0.3,
        requested_deltas_lots={"a": 0.5, "b": 0.5},
        lot_step_lots=0.1,
    )
    assert result.allocations_by_strategy["a"] == pytest.approx(0.2)
    assert result.allocations_by_strategy["b"] == pytest.approx(0.1)


def test_partial_fill_allocation_tracks_dust_when_fill_not_step_aligned():
    result = allocate_partial_fill_pro_rata(
        fill_qty_lots=0.35,
        requested_deltas_lots={"a": 1.0, "b": 1.0},
        lot_step_lots=0.1,
    )
    assert result.allocated_qty_lots == pytest.approx(0.3)
    assert result.dust_qty_lots == pytest.approx(0.05)


def test_partial_fill_allocation_uses_exact_step_counts_for_pro_rata_split():
    result = allocate_partial_fill_pro_rata(
        fill_qty_lots=0.2,
        requested_deltas_lots={"a": 0.1, "b": 0.2, "c": 0.3},
        lot_step_lots=0.1,
    )
    assert result.allocations_by_strategy["a"] == pytest.approx(0.0)
    assert result.allocations_by_strategy["b"] == pytest.approx(0.1)
    assert result.allocations_by_strategy["c"] == pytest.approx(0.1)
    assert result.allocated_qty_lots == pytest.approx(0.2)
    assert result.dust_qty_lots == pytest.approx(0.0)


def test_partial_fill_seniority_first_assigns_fill_to_highest_priority_strategy():
    result = allocate_partial_fill_seniority_first(
        fill_qty_lots=-0.3,
        requested_deltas_lots={"alpha": -0.5, "beta": -0.5},
        strategy_seniority={"alpha": 2.0, "beta": 1.0},
        lot_step_lots=0.1,
    )
    assert result.allocations_by_strategy["alpha"] == pytest.approx(-0.3)
    assert result.allocations_by_strategy["beta"] == pytest.approx(0.0)
    assert result.allocated_qty_lots == pytest.approx(-0.3)
    assert result.dust_qty_lots == pytest.approx(0.0)


def test_partial_fill_seniority_first_spills_to_next_strategy_when_needed():
    result = allocate_partial_fill_seniority_first(
        fill_qty_lots=-0.3,
        requested_deltas_lots={"alpha": -0.1, "beta": -0.3},
        strategy_seniority={"alpha": 2.0, "beta": 1.0},
        lot_step_lots=0.1,
    )
    assert result.allocations_by_strategy["alpha"] == pytest.approx(-0.1)
    assert result.allocations_by_strategy["beta"] == pytest.approx(-0.2)
    assert result.allocated_qty_lots == pytest.approx(-0.3)
    assert result.dust_qty_lots == pytest.approx(0.0)


def test_reconcile_positions_returns_system_error_adjustment():
    reconciliation = reconcile_positions(
        real_position_lots=0.7,
        virtual_positions_lots={"trend": 1.0, "mr": -0.4},
        system_error_lots=0.0,
        epsilon_lots=1e-4,
    )
    assert reconciliation.virtual_sum_lots == pytest.approx(0.6)
    assert reconciliation.diff_lots == pytest.approx(0.1)
    assert reconciliation.requires_adjustment is True
    assert reconciliation.system_error_adjustment_lots == pytest.approx(0.1)


def test_slippage_cost_sign_matches_side_and_price():
    buy_cost = compute_slippage_cost(reference_price=100.0, fill_price=100.2, signed_qty_lots=1.0)
    better_sell_cost = compute_slippage_cost(reference_price=100.0, fill_price=100.2, signed_qty_lots=-1.0)
    assert buy_cost == pytest.approx(-0.2)
    assert better_sell_cost == pytest.approx(0.2)


def test_allocate_cost_pro_rata_preserves_total():
    allocations = allocate_cost_pro_rata({"a": 100.0, "b": 300.0, "c": 0.0}, total_cost=-40.0)
    assert allocations["a"] == pytest.approx(-10.0)
    assert allocations["b"] == pytest.approx(-30.0)
    assert allocations["c"] == pytest.approx(0.0)
    assert sum(allocations.values()) == pytest.approx(-40.0)


# ---------------------------------------------------------------------------
# Family netting tests
# ---------------------------------------------------------------------------

def _family_aggregator(
    *,
    lot_step: float = 0.1,
    min_open: float = 0.1,
    conflict_low: float = 0.85,
    conflict_high: float = 1.12,
) -> VirtualPositionAggregator:
    config = AggregatorConfig(
        lot_step_lots=lot_step,
        min_open_lot=min_open,
        deadband_lots=0.0,
        min_conflict_power=0.05,
        conflict_ratio_low=conflict_low,
        conflict_ratio_high=conflict_high,
    )
    normalizer = RollingPercentileNormalizer(window=16, min_samples=1, default_value=1.0)
    return VirtualPositionAggregator(config=config, normalizer=normalizer)


def test_family_netting_cross_family_no_conflict():
    """Trend BUY + MR SELL should NOT trigger conflict — they are in different families."""
    aggregator = _family_aggregator()
    aggregator.upsert_intent(
        _intent(strategy_id="momentum", target=1.0, confidence=0.8), now_ts=1_000.0,
    )
    aggregator.upsert_intent(
        _intent(strategy_id="mean_reversion_bb", target=-1.0, confidence=0.8, seq=1),
        now_ts=1_000.0,
    )
    family_map = {"momentum": "trend", "mean_reversion_bb": "mean_reversion"}

    decision = aggregator.compute_net_intent(
        symbol="US100",
        real_position_lots=0.0,
        family_map=family_map,
        now_ts=1_001.0,
    )

    # Global conflict should be False (cross-family opposition is by design)
    assert decision.conflict_detected is False
    assert decision.mode == RebalanceMode.NORMAL
    # Both families should have produced decisions
    assert len(decision.family_decisions) == 2
    # Neither family should be internally conflicted
    for fd in decision.family_decisions:
        assert fd.conflict_detected is False

    # Net target: trend +0.8 + MR -0.8 = 0.0 (equal confidence & weight → cancel out)
    # But each family independently contributes its own target
    trend_fd = next(fd for fd in decision.family_decisions if fd.family == "trend")
    mr_fd = next(fd for fd in decision.family_decisions if fd.family == "mean_reversion")
    assert trend_fd.target_net_qty_lots > 0
    assert mr_fd.target_net_qty_lots < 0


def test_family_netting_cross_family_produces_net_position():
    """Trend (strong BUY) + MR (weak SELL) → net BUY position."""
    aggregator = _family_aggregator()
    # 3 trend strategies agreeing on BUY
    aggregator.upsert_intent(
        _intent(strategy_id="momentum", symbol="EURUSD", target=1.0, confidence=0.9, weight=1.3), now_ts=1_000.0,
    )
    aggregator.upsert_intent(
        _intent(strategy_id="trend_following", symbol="EURUSD", target=1.0, confidence=0.8, weight=1.2, seq=1),
        now_ts=1_000.0,
    )
    aggregator.upsert_intent(
        _intent(strategy_id="g1", symbol="EURUSD", target=1.0, confidence=0.7, weight=0.6, seq=1),
        now_ts=1_000.0,
    )
    # 1 MR strategy opposing
    aggregator.upsert_intent(
        _intent(strategy_id="mean_reversion_bb", symbol="EURUSD", target=-1.0, confidence=0.8, weight=0.2, seq=1),
        now_ts=1_000.0,
    )
    family_map = {
        "momentum": "trend",
        "trend_following": "trend",
        "g1": "trend",
        "mean_reversion_bb": "mean_reversion",
    }

    decision = aggregator.compute_net_intent(
        symbol="EURUSD",
        real_position_lots=0.0,
        family_map=family_map,
        now_ts=1_001.0,
    )

    assert decision.conflict_detected is False
    assert decision.target_net_qty_lots > 0  # Net BUY
    # Trend family should have positive target, MR family negative
    trend_fd = next(fd for fd in decision.family_decisions if fd.family == "trend")
    mr_fd = next(fd for fd in decision.family_decisions if fd.family == "mean_reversion")
    assert trend_fd.target_net_qty_lots > 0
    assert mr_fd.target_net_qty_lots < 0
    assert abs(trend_fd.target_net_qty_lots) > abs(mr_fd.target_net_qty_lots)


def test_family_netting_intra_family_conflict_only_blocks_that_family():
    """Two trend strategies conflicting should block trend family, not MR family."""
    aggregator = _family_aggregator()
    # Trend strategy A: BUY
    aggregator.upsert_intent(
        _intent(strategy_id="momentum", symbol="EURUSD", target=1.0, confidence=0.8, weight=1.0), now_ts=1_000.0,
    )
    # Trend strategy B: SELL (conflict within trend family)
    aggregator.upsert_intent(
        _intent(strategy_id="trend_following", symbol="EURUSD", target=-1.0, confidence=0.8, weight=1.0, seq=1),
        now_ts=1_000.0,
    )
    # MR strategy: SELL (no conflict in its own family)
    aggregator.upsert_intent(
        _intent(strategy_id="mean_reversion_bb", symbol="EURUSD", target=-1.0, confidence=0.8, weight=1.0, seq=1),
        now_ts=1_000.0,
    )
    family_map = {
        "momentum": "trend",
        "trend_following": "trend",
        "mean_reversion_bb": "mean_reversion",
    }

    decision = aggregator.compute_net_intent(
        symbol="EURUSD",
        real_position_lots=0.0,
        family_map=family_map,
        now_ts=1_001.0,
    )

    trend_fd = next(fd for fd in decision.family_decisions if fd.family == "trend")
    mr_fd = next(fd for fd in decision.family_decisions if fd.family == "mean_reversion")

    # Trend family should be in conflict (equal BUY vs SELL)
    assert trend_fd.conflict_detected is True
    assert trend_fd.target_net_qty_lots == pytest.approx(0.0)

    # MR family should NOT be in conflict (single strategy, no opposition)
    assert mr_fd.conflict_detected is False
    assert mr_fd.target_net_qty_lots < 0  # SELL target

    # Global: only MR contributes, so net should be SELL
    assert decision.target_net_qty_lots < 0


def test_family_netting_none_family_map_uses_global_netting():
    """When family_map=None, behavior is identical to pre-family-netting."""
    aggregator = _family_aggregator()
    aggregator.upsert_intent(
        _intent(strategy_id="momentum", target=1.0, confidence=0.8), now_ts=1_000.0,
    )
    aggregator.upsert_intent(
        _intent(strategy_id="mean_reversion_bb", target=-1.0, confidence=0.8, seq=1),
        now_ts=1_000.0,
    )

    # Without family_map: global netting → conflict
    decision_global = aggregator.compute_net_intent(
        symbol="US100",
        real_position_lots=0.0,
        family_map=None,
        now_ts=1_001.0,
    )
    assert decision_global.conflict_detected is True
    assert len(decision_global.family_decisions) == 0

    # With family_map: no global conflict
    family_map = {"momentum": "trend", "mean_reversion_bb": "mean_reversion"}
    decision_family = aggregator.compute_net_intent(
        symbol="US100",
        real_position_lots=0.0,
        family_map=family_map,
        now_ts=1_002.0,
    )
    assert decision_family.conflict_detected is False
    assert len(decision_family.family_decisions) == 2


def test_family_netting_sl_tp_from_dominant_family():
    """SL/TP should come from the family with the larger absolute target."""
    aggregator = _family_aggregator()
    # Trend: BUY with SL=15, TP=30
    aggregator.upsert_intent(
        _intent(
            strategy_id="momentum", symbol="EURUSD", target=1.0, confidence=0.9, weight=1.5,
            stop_loss_pips=15.0, take_profit_pips=30.0,
        ),
        now_ts=1_000.0,
    )
    # MR: SELL with SL=5, TP=10
    aggregator.upsert_intent(
        _intent(
            strategy_id="mean_reversion_bb", symbol="EURUSD", target=-1.0, confidence=0.5, weight=0.3,
            stop_loss_pips=5.0, take_profit_pips=10.0, seq=1,
        ),
        now_ts=1_000.0,
    )
    family_map = {"momentum": "trend", "mean_reversion_bb": "mean_reversion"}

    decision = aggregator.compute_net_intent(
        symbol="EURUSD",
        real_position_lots=0.0,
        family_map=family_map,
        now_ts=1_001.0,
    )

    # Trend family is dominant (higher weight * confidence)
    assert decision.target_net_qty_lots > 0
    # SL/TP should come from trend family
    assert decision.weighted_stop_loss_pips == pytest.approx(15.0)
    assert decision.weighted_take_profit_pips == pytest.approx(30.0)


def test_family_netting_global_reduce_only_overrides_all_families():
    """Emergency/degraded mode should override all family targets to reduce-only."""
    aggregator = _family_aggregator()
    aggregator.upsert_intent(
        _intent(strategy_id="momentum", target=1.0, confidence=0.9), now_ts=1_000.0,
    )
    aggregator.upsert_intent(
        _intent(strategy_id="mean_reversion_bb", target=-1.0, confidence=0.8, seq=1),
        now_ts=1_000.0,
    )
    family_map = {"momentum": "trend", "mean_reversion_bb": "mean_reversion"}

    decision = aggregator.compute_net_intent(
        symbol="US100",
        real_position_lots=0.0,
        family_map=family_map,
        heartbeat_ok=False,  # Emergency!
        now_ts=1_001.0,
    )

    assert decision.mode == RebalanceMode.EMERGENCY
    assert decision.reduce_only is True
    assert decision.target_net_qty_lots == pytest.approx(0.0)


def test_family_netting_single_family_matches_global():
    """When all strategies are in one family, result should match global netting."""
    aggregator = _family_aggregator()
    aggregator.upsert_intent(
        _intent(strategy_id="momentum", target=1.0, confidence=0.9, weight=1.0), now_ts=1_000.0,
    )
    aggregator.upsert_intent(
        _intent(strategy_id="trend_following", target=1.0, confidence=0.8, weight=1.0, seq=1),
        now_ts=1_000.0,
    )

    # Global netting
    decision_global = aggregator.compute_net_intent(
        symbol="US100", real_position_lots=0.0, family_map=None, now_ts=1_001.0,
    )

    # Family netting with all in same family
    family_map = {"momentum": "trend", "trend_following": "trend"}
    decision_family = aggregator.compute_net_intent(
        symbol="US100", real_position_lots=0.0, family_map=family_map, now_ts=1_002.0,
    )

    assert decision_global.target_net_qty_lots == pytest.approx(decision_family.target_net_qty_lots)
    assert decision_global.conflict_detected == decision_family.conflict_detected
    assert len(decision_family.family_decisions) == 1
    assert decision_family.family_decisions[0].family == "trend"
