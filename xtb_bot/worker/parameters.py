from __future__ import annotations

from collections import deque
import math
import random

from xtb_bot.multi_strategy import clip01


class WorkerParameterRuntime:
    def __init__(self, worker: object) -> None:
        self.worker = worker

    def initialize_parameter_config(self, *, strategy_params: dict[str, object]) -> None:
        self._initialize_trade_and_entry_parameters(strategy_params=strategy_params)
        self._initialize_diagnostics_and_protective_parameters(strategy_params=strategy_params)
        self._initialize_runtime_control_parameters(strategy_params=strategy_params)

    def _initialize_trade_and_entry_parameters(self, *, strategy_params: dict[str, object]) -> None:
        worker = self.worker
        worker.db_first_active_position_direct_tick_fallback_enabled = worker._strategy_bool_param(
            strategy_params,
            "worker_db_first_active_position_direct_tick_fallback_enabled",
            True,
        )
        seed = f"{worker.bot_magic_instance}:{worker.symbol}"
        worker._rng = random.Random(seed)
        worker.iteration = 0
        worker.min_stop_loss_pips = max(1.0, float(worker.risk.cfg.min_stop_loss_pips))
        worker.min_tp_sl_ratio = max(1.0, float(worker.risk.cfg.min_tp_sl_ratio))
        worker.trailing_activation_ratio = max(
            0.0,
            min(1.0, float(worker.risk.cfg.trailing_activation_ratio)),
        )
        worker.trailing_distance_pips = max(0.1, float(worker.risk.cfg.trailing_distance_pips))
        worker.trailing_breakeven_offset_pips = max(
            0.0,
            float(worker.risk.cfg.trailing_breakeven_offset_pips),
        )
        worker.trailing_breakeven_offset_pips_fx = max(
            0.0,
            float(worker.risk.cfg.trailing_breakeven_offset_pips_fx),
        )
        worker.trailing_breakeven_offset_pips_index = max(
            0.0,
            float(worker.risk.cfg.trailing_breakeven_offset_pips_index),
        )
        worker.trailing_breakeven_offset_pips_commodity = max(
            0.0,
            float(worker.risk.cfg.trailing_breakeven_offset_pips_commodity),
        )
        worker.adaptive_trailing_enabled = bool(worker.risk.cfg.adaptive_trailing_enabled)
        worker.adaptive_trailing_atr_base_multiplier = max(
            0.1,
            float(worker.risk.cfg.adaptive_trailing_atr_base_multiplier),
        )
        worker.adaptive_trailing_heat_trigger = clip01(
            float(worker.risk.cfg.adaptive_trailing_heat_trigger)
        )
        worker.adaptive_trailing_heat_full = clip01(
            float(worker.risk.cfg.adaptive_trailing_heat_full)
        )
        worker.adaptive_trailing_distance_factor_at_full = clip01(
            float(worker.risk.cfg.adaptive_trailing_distance_factor_at_full)
        )
        worker.adaptive_trailing_profit_lock_r_at_full = max(
            0.0,
            float(worker.risk.cfg.adaptive_trailing_profit_lock_r_at_full),
        )
        worker.adaptive_trailing_profit_lock_min_progress = clip01(
            float(worker.risk.cfg.adaptive_trailing_profit_lock_min_progress)
        )
        worker.adaptive_trailing_min_distance_stop_ratio = max(
            0.0,
            float(worker.risk.cfg.adaptive_trailing_min_distance_stop_ratio),
        )
        worker.adaptive_trailing_min_distance_spread_multiplier = max(
            0.0,
            float(worker.risk.cfg.adaptive_trailing_min_distance_spread_multiplier),
        )
        worker.trade_cooldown_sec = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "trade_cooldown_sec",
                0.0,
            ),
        )
        worker.trade_cooldown_win_sec = worker._strategy_float_param(
            strategy_params,
            "trade_cooldown_win_sec",
            worker.trade_cooldown_sec,
        )
        worker.trade_cooldown_loss_sec = worker._strategy_float_param(
            strategy_params,
            "trade_cooldown_loss_sec",
            worker.trade_cooldown_sec,
        )
        worker.trade_cooldown_flat_sec = worker._strategy_float_param(
            strategy_params,
            "trade_cooldown_flat_sec",
            worker.trade_cooldown_sec,
        )
        worker.trade_cooldown_win_sec = max(0.0, worker.trade_cooldown_win_sec)
        worker.trade_cooldown_loss_sec = max(0.0, worker.trade_cooldown_loss_sec)
        worker.trade_cooldown_flat_sec = max(0.0, worker.trade_cooldown_flat_sec)
        base_reject_cooldown_default = (
            worker.trade_cooldown_loss_sec
            if worker.trade_cooldown_loss_sec > 0
            else (
                worker.trade_cooldown_sec
                if worker.trade_cooldown_sec > 0
                else 60.0
            )
        )
        worker.open_reject_cooldown_sec = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "open_reject_cooldown_sec",
                max(20.0, min(180.0, base_reject_cooldown_default)),
            ),
        )
        worker.same_side_reentry_win_cooldown_sec = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "same_side_reentry_win_cooldown_sec",
                0.0,
            ),
        )
        worker.same_side_reentry_reset_on_opposite_signal = worker._strategy_bool_param(
            strategy_params,
            "same_side_reentry_reset_on_opposite_signal",
            True,
        )
        worker.entry_signal_persistence_enabled = worker._strategy_bool_param(
            strategy_params,
            "entry_signal_persistence_enabled",
            True,
        )
        worker.entry_signal_persistence_trend_only = worker._strategy_bool_param(
            strategy_params,
            "entry_signal_persistence_trend_only",
            True,
        )
        worker.entry_signal_min_persistence_sec = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "entry_signal_min_persistence_sec",
                2.0,
            ),
        )
        worker.entry_signal_min_consecutive_evals = max(
            1,
            int(
                worker._strategy_float_param(
                    strategy_params,
                    "entry_signal_min_consecutive_evals",
                    2.0,
                )
            ),
        )
        worker.entry_signal_index_trend_min_persistence_sec = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "entry_signal_index_trend_min_persistence_sec",
                8.0,
            ),
        )
        worker.entry_signal_index_trend_min_consecutive_evals = max(
            1,
            int(
                worker._strategy_float_param(
                    strategy_params,
                    "entry_signal_index_trend_min_consecutive_evals",
                    3.0,
                )
            ),
        )
        worker.entry_pending_open_block_window_sec = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "entry_pending_open_block_window_sec",
                45.0,
            ),
        )
        worker.entry_index_trend_min_breakout_to_spread_ratio = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "entry_index_trend_min_breakout_to_spread_ratio",
                2.0,
            ),
        )
        worker.entry_index_trend_max_entry_quality_penalty = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "entry_index_trend_max_entry_quality_penalty",
                0.12,
            ),
        )
        worker.entry_spike_guard_enabled = worker._strategy_bool_param(
            strategy_params,
            "entry_spike_guard_enabled",
            True,
        )
        worker.entry_spike_guard_trend_only = worker._strategy_bool_param(
            strategy_params,
            "entry_spike_guard_trend_only",
            True,
        )
        worker.entry_spike_guard_lookback_samples = max(
            1,
            int(
                worker._strategy_float_param(
                    strategy_params,
                    "entry_spike_guard_lookback_samples",
                    2.0,
                )
            ),
        )
        worker.entry_spike_guard_max_window_sec = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "entry_spike_guard_max_window_sec",
                6.0,
            ),
        )
        worker.entry_spike_guard_atr_multiplier = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "entry_spike_guard_atr_multiplier",
                0.5,
            ),
        )
        worker.entry_spike_guard_spread_multiplier = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "entry_spike_guard_spread_multiplier",
                2.5,
            ),
        )
        worker.micro_chop_cooldown_sec = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "micro_chop_cooldown_sec",
                480.0,
            ),
        )
        worker.micro_chop_trade_max_age_sec = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "micro_chop_trade_max_age_sec",
                90.0,
            ),
        )
        worker.micro_chop_max_favorable_pips = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "micro_chop_max_favorable_pips",
                1.0,
            ),
        )
        worker.micro_chop_max_tp_progress = clip01(
            worker._strategy_float_param(
                strategy_params,
                "micro_chop_max_tp_progress",
                0.08,
            )
        )
        worker.protective_index_trend_reversal_grace_sec = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "protective_index_trend_reversal_grace_sec",
                12.0,
            ),
        )
        worker.protective_index_trend_reversal_grace_max_adverse_spread_ratio = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "protective_index_trend_reversal_grace_max_adverse_spread_ratio",
                2.0,
            ),
        )
        worker.protective_fresh_reversal_grace_sec = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "protective_fresh_reversal_grace_sec",
                12.0,
            ),
        )
        worker.protective_fresh_reversal_grace_max_adverse_spread_ratio = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "protective_fresh_reversal_grace_max_adverse_spread_ratio",
                2.5,
            ),
        )
        worker.protective_fresh_reversal_grace_max_adverse_stop_ratio = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "protective_fresh_reversal_grace_max_adverse_stop_ratio",
                0.12,
            ),
        )
        worker.continuation_reentry_guard_enabled = worker._strategy_bool_param(
            strategy_params,
            "continuation_reentry_guard_enabled",
            True,
        )
        worker.continuation_reentry_reset_on_opposite_signal = worker._strategy_bool_param(
            strategy_params,
            "continuation_reentry_reset_on_opposite_signal",
            True,
        )
        worker.continuation_reentry_post_win_reset_guard_enabled = worker._strategy_bool_param(
            strategy_params,
            "continuation_reentry_post_win_reset_guard_enabled",
            True,
        )
        worker.continuation_reentry_post_win_min_reset_stop_ratio = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "continuation_reentry_post_win_min_reset_stop_ratio",
                0.25,
            ),
        )
        worker.continuation_reentry_post_win_max_age_sec = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "continuation_reentry_post_win_max_age_sec",
                1200.0,
            ),
        )
        worker.session_close_buffer_sec = max(0, int(worker.risk.cfg.session_close_buffer_min)) * 60
        worker.news_event_buffer_sec = max(0, int(worker.risk.cfg.news_event_buffer_min)) * 60
        worker.news_filter_enabled = bool(worker.risk.cfg.news_filter_enabled)
        worker.news_event_action = str(worker.risk.cfg.news_event_action).lower()
        worker.spread_filter_enabled = bool(worker.risk.cfg.spread_filter_enabled)
        worker.spread_anomaly_multiplier = max(1.0, float(worker.risk.cfg.spread_anomaly_multiplier))
        worker.spread_min_samples = max(1, int(worker.risk.cfg.spread_min_samples))
        worker.spread_pct_filter_enabled = bool(worker.risk.cfg.spread_pct_filter_enabled)
        worker.spread_max_pct = max(0.0, float(worker.risk.cfg.spread_max_pct))
        worker.spread_max_pct_cfd = max(0.0, float(worker.risk.cfg.spread_max_pct_cfd))
        worker.spread_max_pct_crypto = max(0.0, float(worker.risk.cfg.spread_max_pct_crypto))
        spread_window = max(2, int(worker.risk.cfg.spread_avg_window))
        worker.spreads_pips = deque(maxlen=spread_window)
        worker.connectivity_check_enabled = bool(worker.risk.cfg.connectivity_check_enabled)
        worker.connectivity_max_latency_ms = max(1.0, float(worker.risk.cfg.connectivity_max_latency_ms))
        worker.connectivity_pong_timeout_sec = max(
            0.1,
            float(worker.risk.cfg.connectivity_pong_timeout_sec),
        )
        worker.connectivity_check_interval_sec = max(
            1.0,
            worker._strategy_float_param(
                strategy_params,
                "worker_connectivity_check_interval_sec",
                max(5.0, worker.poll_interval_sec),
            ),
        )
        worker.stream_health_check_enabled = bool(worker.risk.cfg.stream_health_check_enabled)
        worker.stream_max_tick_age_sec = max(0.5, float(worker.risk.cfg.stream_max_tick_age_sec))
        configured_global_entry_tick_max_age_sec = max(
            0.0,
            float(worker.risk.cfg.entry_tick_max_age_sec),
        )
        default_entry_tick_max_age_sec = min(
            worker.db_first_tick_max_age_sec,
            max(30.0, worker.db_first_tick_max_age_sec * 0.5),
        )
        if configured_global_entry_tick_max_age_sec > 0:
            default_entry_tick_max_age_sec = min(
                configured_global_entry_tick_max_age_sec,
                worker.db_first_tick_max_age_sec,
            )
        worker.entry_tick_max_age_sec = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "entry_tick_max_age_sec",
                default_entry_tick_max_age_sec,
            ),
        )
        worker.entry_tick_max_age_sec = min(
            worker.entry_tick_max_age_sec,
            worker.db_first_tick_max_age_sec,
        )
        worker.stream_event_cooldown_sec = max(5.0, float(worker.risk.cfg.stream_event_cooldown_sec))
        worker.hold_reason_log_interval_sec = max(
            1.0,
            float(worker.risk.cfg.hold_reason_log_interval_sec),
        )
        worker.hold_reason_metadata_verbosity = worker._strategy_choice_param(
            strategy_params,
            "hold_reason_metadata_verbosity",
            ("minimal", "basic", "full"),
            "basic",
        )
        worker.worker_state_flush_interval_sec = max(
            0.5,
            float(worker.risk.cfg.worker_state_flush_interval_sec),
        )
        worker.account_snapshot_persist_interval_sec = max(
            1.0,
            float(worker.risk.cfg.account_snapshot_persist_interval_sec),
        )
        worker.symbol_auto_disable_on_epic_unavailable = bool(
            worker.risk.cfg.symbol_auto_disable_on_epic_unavailable
        )
        worker.symbol_auto_disable_epic_unavailable_threshold = max(
            1,
            int(worker.risk.cfg.symbol_auto_disable_epic_unavailable_threshold),
        )

    def _initialize_diagnostics_and_protective_parameters(
        self,
        *,
        strategy_params: dict[str, object],
    ) -> None:
        worker = self.worker
        strategy_name_lower = str(worker.strategy_name or "").strip().lower()
        worker._trend_hold_summary_enabled = (
            strategy_name_lower in {"trend_following", "crypto_trend_following"}
            and worker._strategy_bool_param(strategy_params, "hold_summary_enabled", True)
        )
        worker._trend_hold_summary_interval_sec = max(
            10.0,
            worker._strategy_float_param(
                strategy_params,
                "hold_summary_interval_sec",
                180.0,
            ),
        )
        trend_hold_summary_window_raw = worker._strategy_float_param(
            strategy_params,
            "hold_summary_window",
            240.0,
        )
        if not math.isfinite(trend_hold_summary_window_raw):
            trend_hold_summary_window_raw = 240.0
        worker._trend_hold_summary_window = max(20, int(trend_hold_summary_window_raw))
        trend_hold_summary_min_samples_raw = worker._strategy_float_param(
            strategy_params,
            "hold_summary_min_samples",
            30.0,
        )
        if not math.isfinite(trend_hold_summary_min_samples_raw):
            trend_hold_summary_min_samples_raw = 30.0
        worker._trend_hold_summary_min_samples = min(
            worker._trend_hold_summary_window,
            max(5, int(trend_hold_summary_min_samples_raw)),
        )
        worker.debug_indicators_enabled = worker._strategy_bool_param(
            strategy_params,
            "debug_indicators",
            False,
        )
        worker.min_confidence_for_entry = max(
            0.0,
            min(
                1.0,
                worker._strategy_float_param(
                    strategy_params,
                    "min_confidence_for_entry",
                    0.0,
                ),
            ),
        )
        worker.protective_exit_enabled = worker._strategy_bool_param(
            strategy_params,
            "protective_exit_enabled",
            True,
        )
        worker.protective_exit_loss_ratio = max(
            0.0,
            min(
                1.0,
                worker._strategy_float_param(
                    strategy_params,
                    "protective_exit_loss_ratio",
                    0.82,
                ),
            ),
        )
        worker.protective_exit_allow_adx_regime_loss = worker._strategy_bool_param(
            strategy_params,
            "protective_exit_allow_adx_regime_loss",
            True,
        )
        worker.protective_exit_on_signal_invalidation = worker._strategy_bool_param(
            strategy_params,
            "protective_exit_on_signal_invalidation",
            True,
        )
        worker.protective_exit_require_armed_profit = worker._strategy_bool_param(
            strategy_params,
            "protective_exit_require_armed_profit",
            True,
        )
        worker.protective_exit_arm_tp_progress = max(
            0.0,
            min(
                1.0,
                worker._strategy_float_param(
                    strategy_params,
                    "protective_exit_arm_tp_progress",
                    0.25,
                ),
            ),
        )
        worker.protective_exit_arm_min_profit_pips = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "protective_exit_arm_min_profit_pips",
                0.0,
            ),
        )
        worker.protective_profit_lock_on_reversal = worker._strategy_bool_param(
            strategy_params,
            "protective_profit_lock_on_reversal",
            False,
        )
        worker.protective_profit_lock_on_hold_invalidation = worker._strategy_bool_param(
            strategy_params,
            "protective_profit_lock_on_hold_invalidation",
            False,
        )
        worker.protective_profit_lock_min_profit_pips = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "protective_profit_lock_min_profit_pips",
                0.0,
            ),
        )
        worker.protective_peak_drawdown_exit_enabled = worker._strategy_bool_param(
            strategy_params,
            "protective_peak_drawdown_exit_enabled",
            True,
        )
        worker.protective_peak_drawdown_ratio = max(
            0.0,
            min(
                1.0,
                worker._strategy_float_param(
                    strategy_params,
                    "protective_peak_drawdown_ratio",
                    0.25,
                ),
            ),
        )
        worker.protective_peak_drawdown_min_peak_pips = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "protective_peak_drawdown_min_peak_pips",
                4.0,
            ),
        )
        worker.protective_peak_drawdown_hard_exit_enabled = worker._strategy_bool_param(
            strategy_params,
            "protective_peak_drawdown_hard_exit_enabled",
            True,
        )
        worker.protective_breakeven_lock_enabled = worker._strategy_bool_param(
            strategy_params,
            "protective_breakeven_lock_enabled",
            True,
        )
        worker.protective_breakeven_min_peak_pips = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "protective_breakeven_min_peak_pips",
                4.0,
            ),
        )
        worker.protective_breakeven_min_tp_progress = max(
            0.0,
            min(
                1.0,
                worker._strategy_float_param(
                    strategy_params,
                    "protective_breakeven_min_tp_progress",
                    0.55,
                ),
            ),
        )
        worker.protective_breakeven_offset_pips = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "protective_breakeven_offset_pips",
                0.0,
            ),
        )
        worker.protective_peak_stagnation_exit_enabled = worker._strategy_bool_param(
            strategy_params,
            "protective_peak_stagnation_exit_enabled",
            True,
        )
        worker.protective_peak_stagnation_timeout_sec = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "protective_peak_stagnation_timeout_sec",
                420.0,
            ),
        )
        worker.protective_peak_stagnation_min_peak_pips = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "protective_peak_stagnation_min_peak_pips",
                8.0,
            ),
        )
        worker.protective_peak_stagnation_min_retain_ratio = max(
            0.0,
            min(
                1.0,
                worker._strategy_float_param(
                    strategy_params,
                    "protective_peak_stagnation_min_retain_ratio",
                    0.55,
                ),
            ),
        )
        worker.protective_stale_loser_exit_enabled = worker._strategy_bool_param(
            strategy_params,
            "protective_stale_loser_exit_enabled",
            True,
        )
        worker.protective_stale_loser_timeout_sec = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "protective_stale_loser_timeout_sec",
                1800.0,
            ),
        )
        worker.protective_stale_loser_loss_ratio = max(
            0.0,
            min(
                1.0,
                worker._strategy_float_param(
                    strategy_params,
                    "protective_stale_loser_loss_ratio",
                    0.55,
                ),
            ),
        )
        worker.protective_stale_loser_profit_tolerance_pips = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "protective_stale_loser_profit_tolerance_pips",
                0.0,
            ),
        )
        worker.protective_fast_fail_exit_enabled = worker._strategy_bool_param(
            strategy_params,
            "protective_fast_fail_exit_enabled",
            True,
        )
        worker.protective_fast_fail_timeout_sec = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "protective_fast_fail_timeout_sec",
                300.0,
            ),
        )
        worker.protective_fast_fail_loss_ratio = max(
            0.0,
            min(
                1.0,
                worker._strategy_float_param(
                    strategy_params,
                    "protective_fast_fail_loss_ratio",
                    0.4,
                ),
            ),
        )
        worker.protective_fast_fail_profit_tolerance_pips = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "protective_fast_fail_profit_tolerance_pips",
                6.0,
            ),
        )
        worker.protective_fast_fail_peak_tp_progress_tolerance = max(
            0.0,
            min(
                1.0,
                worker._strategy_float_param(
                    strategy_params,
                    "protective_fast_fail_peak_tp_progress_tolerance",
                    0.18,
                ),
            ),
        )
        worker.protective_fast_fail_zero_followthrough_timeout_sec = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "protective_fast_fail_zero_followthrough_timeout_sec",
                240.0,
            ),
        )
        worker.protective_fast_fail_zero_followthrough_loss_ratio = max(
            0.0,
            min(
                1.0,
                worker._strategy_float_param(
                    strategy_params,
                    "protective_fast_fail_zero_followthrough_loss_ratio",
                    0.2,
                ),
            ),
        )
        worker._protective_fast_fail_zero_followthrough_timeout_sec_by_symbol = (
            worker._strategy_symbol_float_map_param(
                strategy_params,
                "protective_fast_fail_zero_followthrough_timeout_sec_by_symbol",
            )
        )
        worker._protective_fast_fail_zero_followthrough_loss_ratio_by_symbol = (
            worker._strategy_symbol_float_map_param(
                strategy_params,
                "protective_fast_fail_zero_followthrough_loss_ratio_by_symbol",
            )
        )
        worker.protective_runner_preservation_enabled = worker._strategy_bool_param(
            strategy_params,
            "protective_runner_preservation_enabled",
            True,
        )
        worker.protective_runner_min_tp_progress = max(
            0.0,
            min(
                1.0,
                worker._strategy_float_param(
                    strategy_params,
                    "protective_runner_min_tp_progress",
                    0.35,
                ),
            ),
        )
        worker.protective_runner_distance_min_tp_progress = max(
            worker.protective_runner_min_tp_progress,
            min(
                1.0,
                worker._strategy_float_param(
                    strategy_params,
                    "protective_runner_distance_min_tp_progress",
                    0.55,
                ),
            ),
        )
        worker.protective_runner_min_peak_pips = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "protective_runner_min_peak_pips",
                8.0,
            ),
        )
        worker.protective_runner_min_retain_ratio = max(
            0.0,
            min(
                1.0,
                worker._strategy_float_param(
                    strategy_params,
                    "protective_runner_min_retain_ratio",
                    0.45,
                ),
            ),
        )
        worker.protective_runner_peak_drawdown_ratio_multiplier = max(
            1.0,
            worker._strategy_float_param(
                strategy_params,
                "protective_runner_peak_drawdown_ratio_multiplier",
                1.5,
            ),
        )
        worker.protective_runner_peak_stagnation_timeout_multiplier = max(
            1.0,
            worker._strategy_float_param(
                strategy_params,
                "protective_runner_peak_stagnation_timeout_multiplier",
                2.0,
            ),
        )
        worker._protective_peak_drawdown_ratio_by_symbol = worker._strategy_symbol_float_map_param(
            strategy_params,
            "protective_peak_drawdown_ratio_by_symbol",
        )
        worker._protective_peak_drawdown_min_peak_pips_by_symbol = (
            worker._strategy_symbol_float_map_param(
                strategy_params,
                "protective_peak_drawdown_min_peak_pips_by_symbol",
            )
        )
        worker._protective_breakeven_min_peak_pips_by_symbol = worker._strategy_symbol_float_map_param(
            strategy_params,
            "protective_breakeven_min_peak_pips_by_symbol",
        )
        worker._protective_peak_stagnation_timeout_sec_by_symbol = (
            worker._strategy_symbol_float_map_param(
                strategy_params,
                "protective_peak_stagnation_timeout_sec_by_symbol",
            )
        )
        worker._protective_peak_stagnation_min_retain_ratio_by_symbol = (
            worker._strategy_symbol_float_map_param(
                strategy_params,
                "protective_peak_stagnation_min_retain_ratio_by_symbol",
            )
        )
        worker._protective_stale_loser_timeout_sec_by_symbol = worker._strategy_symbol_float_map_param(
            strategy_params,
            "protective_stale_loser_timeout_sec_by_symbol",
        )
        worker._protective_stale_loser_loss_ratio_by_symbol = worker._strategy_symbol_float_map_param(
            strategy_params,
            "protective_stale_loser_loss_ratio_by_symbol",
        )
        worker._protective_fast_fail_timeout_sec_by_symbol = worker._strategy_symbol_float_map_param(
            strategy_params,
            "protective_fast_fail_timeout_sec_by_symbol",
        )
        worker._protective_fast_fail_loss_ratio_by_symbol = worker._strategy_symbol_float_map_param(
            strategy_params,
            "protective_fast_fail_loss_ratio_by_symbol",
        )
        worker._protective_fast_fail_peak_tp_progress_tolerance_by_symbol = (
            worker._strategy_symbol_float_map_param(
                strategy_params,
                "protective_fast_fail_peak_tp_progress_tolerance_by_symbol",
            )
        )
        worker.protective_peak_drawdown_ratio = worker._symbol_override_float(
            worker.symbol,
            default=worker.protective_peak_drawdown_ratio,
            mapping=worker._protective_peak_drawdown_ratio_by_symbol,
            floor=0.0,
            ceil=1.0,
        )
        worker.protective_peak_drawdown_min_peak_pips = worker._symbol_override_float(
            worker.symbol,
            default=worker.protective_peak_drawdown_min_peak_pips,
            mapping=worker._protective_peak_drawdown_min_peak_pips_by_symbol,
            floor=0.0,
        )
        worker.protective_breakeven_min_peak_pips = worker._symbol_override_float(
            worker.symbol,
            default=worker.protective_breakeven_min_peak_pips,
            mapping=worker._protective_breakeven_min_peak_pips_by_symbol,
            floor=0.0,
        )
        worker.protective_peak_stagnation_timeout_sec = worker._symbol_override_float(
            worker.symbol,
            default=worker.protective_peak_stagnation_timeout_sec,
            mapping=worker._protective_peak_stagnation_timeout_sec_by_symbol,
            floor=0.0,
        )
        worker.protective_peak_stagnation_min_retain_ratio = worker._symbol_override_float(
            worker.symbol,
            default=worker.protective_peak_stagnation_min_retain_ratio,
            mapping=worker._protective_peak_stagnation_min_retain_ratio_by_symbol,
            floor=0.0,
            ceil=1.0,
        )
        worker.protective_stale_loser_timeout_sec = worker._symbol_override_float(
            worker.symbol,
            default=worker.protective_stale_loser_timeout_sec,
            mapping=worker._protective_stale_loser_timeout_sec_by_symbol,
            floor=0.0,
        )
        worker.protective_stale_loser_loss_ratio = worker._symbol_override_float(
            worker.symbol,
            default=worker.protective_stale_loser_loss_ratio,
            mapping=worker._protective_stale_loser_loss_ratio_by_symbol,
            floor=0.0,
            ceil=1.0,
        )
        worker.protective_fast_fail_timeout_sec = worker._symbol_override_float(
            worker.symbol,
            default=worker.protective_fast_fail_timeout_sec,
            mapping=worker._protective_fast_fail_timeout_sec_by_symbol,
            floor=0.0,
        )
        worker.protective_fast_fail_loss_ratio = worker._symbol_override_float(
            worker.symbol,
            default=worker.protective_fast_fail_loss_ratio,
            mapping=worker._protective_fast_fail_loss_ratio_by_symbol,
            floor=0.0,
            ceil=1.0,
        )
        worker.protective_fast_fail_peak_tp_progress_tolerance = worker._symbol_override_float(
            worker.symbol,
            default=worker.protective_fast_fail_peak_tp_progress_tolerance,
            mapping=worker._protective_fast_fail_peak_tp_progress_tolerance_by_symbol,
            floor=0.0,
            ceil=1.0,
        )
        worker.protective_fast_fail_zero_followthrough_timeout_sec = worker._symbol_override_float(
            worker.symbol,
            default=worker.protective_fast_fail_zero_followthrough_timeout_sec,
            mapping=worker._protective_fast_fail_zero_followthrough_timeout_sec_by_symbol,
            floor=0.0,
        )
        worker.protective_fast_fail_zero_followthrough_loss_ratio = worker._symbol_override_float(
            worker.symbol,
            default=worker.protective_fast_fail_zero_followthrough_loss_ratio,
            mapping=worker._protective_fast_fail_zero_followthrough_loss_ratio_by_symbol,
            floor=0.0,
            ceil=1.0,
        )
        worker.partial_take_profit_enabled = worker._strategy_bool_param(
            strategy_params,
            "partial_take_profit_enabled",
            True,
        )
        worker.partial_take_profit_r_multiple = max(
            0.1,
            worker._strategy_float_param(
                strategy_params,
                "partial_take_profit_r_multiple",
                1.4,
            ),
        )
        worker.partial_take_profit_fraction = max(
            0.0,
            min(
                0.95,
                worker._strategy_float_param(
                    strategy_params,
                    "partial_take_profit_fraction",
                    0.5,
                ),
            ),
        )
        worker.partial_take_profit_min_remaining_lots = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "partial_take_profit_min_remaining_lots",
                0.1,
            ),
        )
        worker.partial_take_profit_skip_trend_positions = worker._strategy_bool_param(
            strategy_params,
            "partial_take_profit_skip_trend_positions",
            True,
        )
        worker.partial_take_profit_skip_runner_positions = worker._strategy_bool_param(
            strategy_params,
            "partial_take_profit_skip_runner_positions",
            True,
        )
        worker.protective_early_loss_cut_enabled = worker._strategy_bool_param(
            strategy_params,
            "protective_early_loss_cut_enabled",
            True,
        )
        worker.protective_early_loss_cut_loss_ratio = max(
            0.0,
            min(
                1.0,
                worker._strategy_float_param(
                    strategy_params,
                    "protective_early_loss_cut_loss_ratio",
                    0.4,
                ),
            ),
        )
        worker.protective_early_loss_cut_never_profitable_only = worker._strategy_bool_param(
            strategy_params,
            "protective_early_loss_cut_never_profitable_only",
            True,
        )
        worker.protective_early_loss_cut_profit_tolerance_pips = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "protective_early_loss_cut_profit_tolerance_pips",
                0.0,
            ),
        )
        worker.protective_early_loss_cut_on_hold_invalidation = worker._strategy_bool_param(
            strategy_params,
            "protective_early_loss_cut_on_hold_invalidation",
            True,
        )
        worker.stop_slippage_auto_guaranteed_stop_enabled = worker._strategy_bool_param(
            strategy_params,
            "stop_slippage_auto_guaranteed_stop_enabled",
            True,
        )
        worker.stop_slippage_auto_guaranteed_stop_ratio = max(
            1.0,
            worker._strategy_float_param(
                strategy_params,
                "stop_slippage_auto_guaranteed_stop_ratio",
                1.35,
            ),
        )
        worker.protective_exit_on_trend_reversal = worker._strategy_bool_param(
            strategy_params,
            "protective_exit_on_trend_reversal",
            True,
        )
        worker.g1_protective_exit_enabled = worker.protective_exit_enabled
        worker.g1_protective_exit_loss_ratio = worker.protective_exit_loss_ratio
        worker.g1_protective_exit_allow_adx_regime_loss = (
            worker.protective_exit_allow_adx_regime_loss
        )
        worker.g1_protective_exit_on_trend_reversal = worker.protective_exit_on_trend_reversal
        worker.debug_indicators_interval_sec = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "debug_indicators_interval_sec",
                0.0,
            ),
        )

    def _initialize_runtime_control_parameters(self, *, strategy_params: dict[str, object]) -> None:
        worker = self.worker
        worker.operational_guard_enabled = worker._strategy_bool_param(
            strategy_params,
            "operational_guard_enabled",
            True,
        )
        worker.operational_guard_stale_tick_streak_threshold = max(
            1,
            int(
                worker._strategy_float_param(
                    strategy_params,
                    "operational_guard_stale_tick_streak_threshold",
                    4.0,
                )
            ),
        )
        worker.operational_guard_allowance_backoff_streak_threshold = max(
            1,
            int(
                worker._strategy_float_param(
                    strategy_params,
                    "operational_guard_allowance_backoff_streak_threshold",
                    4.0,
                )
            ),
        )
        worker.operational_guard_cooldown_sec = max(
            5.0,
            worker._strategy_float_param(
                strategy_params,
                "operational_guard_cooldown_sec",
                180.0,
            ),
        )
        worker.operational_guard_reduce_risk_on_profit = worker._strategy_bool_param(
            strategy_params,
            "operational_guard_reduce_risk_on_profit",
            True,
        )
        worker.operational_guard_reduce_risk_min_pnl = worker._strategy_float_param(
            strategy_params,
            "operational_guard_reduce_risk_min_pnl",
            0.0,
        )
        worker._last_price_freshness_ts = 0.0
        worker._last_price_freshness_monotonic_ts = 0.0
        worker._last_stale_entry_history_block_log_ts = 0.0
        worker.symbol_auto_disable_min_error_window_sec = max(
            0.0,
            worker._strategy_float_param(
                strategy_params,
                "symbol_auto_disable_min_error_window_sec",
                180.0,
            ),
        )
        worker.manual_close_sync_interval_sec = max(
            5.0,
            worker._strategy_float_param(
                strategy_params,
                "worker_manual_close_sync_interval_sec",
                15.0,
            ),
        )
        worker.missing_position_reconcile_timeout_sec = max(
            10.0,
            worker._strategy_float_param(
                strategy_params,
                "worker_missing_position_reconcile_timeout_sec",
                max(30.0, worker.stream_event_cooldown_sec * 3.0),
            ),
        )
        worker._multi_strategy_scale_out_offset_by_name = {}
        worker._strategy_trailing_store_key = f"worker.trailing_override.{worker.symbol}"
        worker.active_position_poll_interval_sec = max(
            0.1,
            worker._strategy_float_param(
                strategy_params,
                "active_position_poll_interval_sec",
                min(1.0, max(worker.poll_interval_sec, 0.1)),
            ),
        )
        worker.account_snapshot_cache_ttl_sec = max(
            0.2,
            worker._strategy_float_param(
                strategy_params,
                "worker_account_snapshot_cache_ttl_sec",
                min(5.0, max(worker.poll_interval_sec, 0.2)),
            ),
        )
