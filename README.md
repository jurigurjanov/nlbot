# IG Trading Bot (Python)

Многопоточный бот для IG.com (с совместимостью XTB) с изоляцией по инструментам, риск-контролем и персистентным состоянием.

## Что реализовано

- Работа с основными инструментами:
  - По умолчанию: `EURUSD, GBPUSD, USDJPY, US100, US2000, XAUUSD, WTI`
  - Глобальный список задается в `IG_SYMBOLS`/`XTB_SYMBOLS` в `.env`
  - Поддерживаются отдельные наборы под стратегию: `IG_SYMBOLS_<STRATEGY>` / `XTB_SYMBOLS_<STRATEGY>`
- Защита капитала:
  - `max_risk_per_trade_pct`
  - `max_daily_drawdown_pct`
  - `max_total_drawdown_pct`
  - `max_open_positions`
  - дневной lock: при достижении дневной просадки торговля блокируется до следующего дня
  - дневная просадка считается от intraday `equity` high-water mark (а не от утреннего `balance`)
  - смена торгового дня для risk lock выполняется в `UTC`
  - динамический размер позиции перед каждой сделкой: от текущего баланса и фактической дистанции SL
  - sizing учитывает текущий спред инструмента (увеличивает effective stop distance и снижает объем в шумные периоды)
  - pre-check по марже перед открытием сделки (`margin_check_enabled`, `margin_safety_buffer`, `margin_fallback_leverage`)
  - для открытия сделки обязателен `symbol_spec` от брокера (без fallback-хардкодов по pip/tick value)
  - атомарный `open-slot` гейт в `RiskManager` через SQLite reservations: защита от race-condition между потоками и процессами при проверке `max_open_positions`
  - reservations имеют lease TTL, чтобы после падения процесса слот не блокировался навсегда
  - lease TTL настраивается через `XTB_OPEN_SLOT_LEASE_SEC` (default `30`)
  - `force_flatten` не запускается при `open_positions_count=0` (защита от бессмысленного повторного закрытия)
  - ограничение на риск в сделке: `max_risk_per_trade_pct` в диапазоне `(0, 2]`
  - ограничение выхода по цели: `min_tp_sl_ratio` в диапазоне `2.0..3.0` (TP не меньше SL * ratio)
  - time-based выходы: `session_close_buffer_min` для индексов и `news_event_buffer_min` для high-impact событий
  - фильтр спреда перед входом: блокировка входа при аномальном расширении (`spread_anomaly_multiplier`, default `3.0`)
  - проверка связи перед входом: при latency > `connectivity_max_latency_ms` или отсутствии `PONG` вход блокируется
  - проверка здоровья потока/котировок перед входом: при деградации вход блокируется до восстановления
- Многопоточность:
  - Отдельный поток на каждый инструмент (`worker-<SYMBOL>`)
  - Автоматический рестарт упавшего воркера
  - Для открытых позиций воркер автоматически ускоряет цикл опроса (частые проверки SL/TP/трейлинга)
- Независимое состояние по потоку и сделке:
  - SQLite (`worker_states`, `trades`, `events`, `account_snapshots`)
  - Восстановление открытых позиций после перезапуска
  - Персистентная история цен в SQLite (`price_history`) и автовосстановление буфера цен при рестарте воркера
- Переключение стратегий:
  - `g1` (EMA cross + ADX/ATR + динамический SL/TP, с отдельными авто-профилями для FX и индексов)
  - `g2` (higher-timeframe index pullback: EMA 10/50/200 + RSI/ATR + вход только по закрытым свечам после отката в зрелом тренде)
  - `donchian_breakout` (breakout-only по каналу Дончиана)
  - `index_hybrid` (гибрид для индексов: trend-following + mean-reversion + dynamic exit по каналу; неиндексные символы автоматически пропускаются)
  - `mean_breakout_v2` (EMA-based z-score + Donchian breakout + slope filter + exit hints)
  - `mean_reversion_bb` (Bollinger Bands mean-reversion, опционально с RSI-фильтром)
  - `momentum`
  - `momentum_index` (alias `momentum` для отдельного пресета индексов в одном процессе)
  - `momentum_fx` (alias `momentum` для отдельного пресета FX в одном процессе)
  - `trend_following` (EMA 20/80 + trend-filter по Donchian breakout и вход по pullback к EMA/середине канала)
  - `crypto_trend_following` (наследник `trend_following` с crypto-ориентированными дефолтами; поддерживает только crypto-symbols)
- Рекомендуемое соответствие стратегий и классов инструментов:
  - `index_hybrid` -> только индексы (`US100`, `US500`, `DE40`, `UK100`, `US30`, `AUS200`)
  - `g2` -> только индексы, лучше всего M15/H1 pullback-входы (`US100`, `US500`, `US2000`, `DE40`, `UK100`, `TOPIX`, `JPN225`, `AUS200`)
  - `crypto_trend_following` -> только crypto (`BTC`, `ETH`, `LTC`, `SOL`, `XRP`, `DOGE`)
  - `momentum` -> индексы/коммодити (`US*`, `DE40`, `WTI`, `BRENT`, `GOLD`)
  - `g1` -> FX + индексы + при необходимости коммодити
- Переключение режимов:
  - `signal_only` (только сигналы)
  - `paper` (бумажная торговля)
  - `execution` (реальные заявки через API брокера)
- Поддержка demo/live:
  - `account_type=demo|live`
  - endpoint выбирается автоматически, можно переопределить `IG_ENDPOINT`/`XTB_ENDPOINT`
- Magic Number / customComment:
  - каждая сделка получает уникальный `trade_uid` и тег в формате `MAGIC_PREFIX:INSTANCE:TRADE_UID` (короткий формат, чтобы не упираться в лимит длины комментария у брокера)
  - тег передается в поле комментария/ссылки сделки и позволяет отличать сделки бота
- Execution-подтверждение:
  - после отправки сделки выполняется опрос статуса подтверждения брокера
  - сделка считается открытой/закрытой только после статуса accepted
- Streaming котировки:
  - для XTB и IG используется stream-кэш котировок (для IG через Lightstreamer)
  - для IG включен auto-reconnect + автопереподписка и fallback на REST при деградации стрима
  - стрим IG можно включать/выключать через `IG_STREAM_ENABLED=true|false`
  - при входе/модификации всегда используется актуальная `bid/ask` из брокерского клиента

## Структура

- `run_bot.py` — CLI entrypoint
- `xtb_bot/bot.py` — оркестратор
- `xtb_bot/worker.py` — поток на инструмент
- `xtb_bot/client.py` — базовый брокерский интерфейс, XTB и mock клиент
- `xtb_bot/ig_client.py` — IG API клиент
- `xtb_bot/risk_manager.py` — риск-менеджмент и ограничения
- `xtb_bot/state_store.py` — SQLite persistence
- `xtb_bot/strategies/` — стратегии и фабрика
- `xtb_bot/config.py` — конфиг из `.env`/env

## Быстрый старт

1. Установить зависимости:

```bash
python -m pip install -e .
```

`numpy` включен в базовые зависимости и используется для ускорения тяжелых расчетов индикаторов (EMA/ATR/ADX/RSI/Z-score). При отсутствии `numpy` бот продолжает работать на Python fallback-реализациях.

2. Создать `.env` из примера:

```bash
cp .env.example .env
```

3. Заполнить `.env` и запуск:

```bash
xtb-bot --mode paper --strategy momentum
```

## Полезные команды

Список стратегий:

```bash
xtb-bot --list-strategies
```

Запуск в сигнальном режиме:

```bash
xtb-bot --mode signal_only
```

Запуск в live execution:

```bash
xtb-bot --mode execution --strategy mean_breakout_v2
```

Запуск `g1`:

```bash
xtb-bot --strategy g1
```

Запуск `g2`:

```bash
xtb-bot --strategy g2
```

`index_hybrid`: где торговать и какие символы ставить

- Стратегия рассчитана только на индексы.
- Рекомендуемые базовые символы:
  - `US500` (S&P 500)
  - `US100` (US Tech 100 / Nasdaq 100)
  - `US2000` (US Russell 2000)
  - `TOPIX` (Tokyo First Section / Tokyo Stock Price Index)
  - `DE40`
  - `UK100`
- Расширенная европейская корзина для текущего режима:
  - `FR40`
  - `EU50`
  - `IT40`
  - `ES35`
  - `SK20`
  - `DEMID50`
- Неиндексные символы (`EURUSD`, `GBPUSD`, `USDJPY`, `WTI`, `XAUUSD`) будут автоматически пропущены для этой стратегии.
- Дополнительные commodity-symbols, добавленные в global/scoped lists:
  - `CARBON`
  - `UKNATGAS`
  - `GASOLINE`
  - `WHEAT`
  - `COCOA`
  - `CORN`
  - `SOYBEANOIL`
  - `OATS`

Пример `.env` для `index_hybrid`:

```bash
# Новый strategy-scoped формат (предпочтительно)
IG_SYMBOLS_INDEX_HYBRID=US500,US100,US2000,JPN225,TOPIX,DE40,FR40,EU50,IT40,ES35,SK20,NK20,DEMID50,UK100
XTB_SYMBOLS_INDEX_HYBRID=US500,US100,US2000,JPN225,TOPIX,DE40,FR40,EU50,IT40,ES35,SK20,NK20,DEMID50,UK100

# Либо общий список символов (если не используете strategy-specific ключи)
IG_SYMBOLS=US500,US100,US2000,JPN225,TOPIX,DE40,FR40,EU50,IT40,ES35,SK20,NK20,DEMID50,UK100
XTB_SYMBOLS=US500,US100,US2000,JPN225,TOPIX,DE40,FR40,EU50,IT40,ES35,SK20,NK20,DEMID50,UK100

# Legacy-ключи (оставлены для совместимости):
IG_INDEX_HYBRID_SYMBOLS=US500,US100,US2000,JPN225,TOPIX,DE40,FR40,EU50,IT40,ES35,SK20,NK20,DEMID50,UK100
XTB_INDEX_HYBRID_SYMBOLS=US500,US100,US2000,JPN225,TOPIX,DE40,FR40,EU50,IT40,ES35,SK20,NK20,DEMID50,UK100
```

Общий формат strategy-scoped символов:

- `IG_SYMBOLS_<STRATEGY>` / `XTB_SYMBOLS_<STRATEGY>`, где `<STRATEGY>` — верхний регистр с `_`, например:
  - `DONCHIAN_BREAKOUT`
  - `G1`
  - `G2`
  - `INDEX_HYBRID`
  - `MEAN_BREAKOUT_V2`
  - `MEAN_REVERSION_BB`
  - `MOMENTUM`
  - `TREND_FOLLOWING`
- Приоритет выбора symbols: `*_SYMBOLS_<ACTIVE_STRATEGY>` -> `*_INDEX_HYBRID_SYMBOLS` (legacy только для index_hybrid) -> `*_SYMBOLS` -> defaults.

Пример наборов тикеров по стратегиям:

```bash
IG_SYMBOLS_MOMENTUM=US100,US2000,US500,JPN225,TOPIX,DE40,FR40,EU50,IT40,ES35,SK20,NK20,DEMID50,US30,GOLD,WTI,BRENT,CARBON,UKNATGAS,GASOLINE,WHEAT,COCOA,CORN,SOYBEANOIL,OATS
IG_SYMBOLS_MOMENTUM_INDEX=US100,US2000,US500,JPN225,TOPIX,DE40,FR40,EU50,IT40,ES35,SK20,NK20,DEMID50,US30
IG_SYMBOLS_MOMENTUM_FX=EURUSD,GBPUSD,USDJPY
IG_SYMBOLS_G2=US500,US100,US2000,JPN225,TOPIX,DE40,UK100,NK20,AUS200
IG_SYMBOLS_MEAN_BREAKOUT_V2=US100,US2000,JPN225,TOPIX,WTI,USDJPY,DE40,FR40,EU50,IT40,ES35,SK20,NK20,DEMID50,GOLD,CARBON,UKNATGAS,GASOLINE,WHEAT,COCOA,CORN,SOYBEANOIL,OATS
IG_SYMBOLS_DONCHIAN_BREAKOUT=US100,US2000,US500,JPN225,TOPIX,UK100,AUS200,DE40,FR40,EU50,IT40,ES35,SK20,NK20,DEMID50,US30,USDJPY,USDCHF,EURUSD,EURGBP,EURCHF,AUDUSD,GBPUSD,GBPJPY,USDCAD,GOLD,WTI,BRENT,CARBON,UKNATGAS,GASOLINE,WHEAT,COCOA,CORN,SOYBEANOIL,OATS,XRP,DOGE,BTC,ETH,SOL,LTC
IG_SYMBOLS_TREND_FOLLOWING=US500,US2000,JPN225,TOPIX,US30,DE40,FR40,EU50,IT40,ES35,SK20,NK20,DEMID50,UK100,GOLD,WTI,BRENT,CARBON,UKNATGAS,GASOLINE,WHEAT,COCOA,CORN,SOYBEANOIL,OATS,AAPL,MSFT
IG_SYMBOLS_MEAN_REVERSION_BB=US100,US2000,US500,JPN225,TOPIX,UK100,AUS200,DE40,FR40,EU50,IT40,ES35,SK20,NK20,DEMID50,US30,USDJPY,USDCHF,EURUSD,EURGBP,EURCHF,AUDUSD,GBPUSD,GBPJPY,USDCAD
```

Запуск:

```bash
xtb-bot --mode paper --strategy index_hybrid
```

Быстрое переключение профиля без правки `.env`:

```bash
xtb-bot --mode paper --strategy index_hybrid --strategy-profile safe
xtb-bot --mode paper --strategy index_hybrid --strategy-profile aggressive
xtb-bot --mode paper --strategy mean_breakout_v2 --strategy-profile conservative
xtb-bot --mode paper --strategy mean_breakout_v2 --strategy-profile aggressive
xtb-bot --mode paper --strategy mean_reversion_bb --strategy-profile conservative
xtb-bot --mode paper --strategy mean_reversion_bb --strategy-profile aggressive
```

Доступные built-in профили:

- `index_hybrid`: `safe`, `conservative`, `aggressive`
- `mean_breakout_v2`: `conservative` (`safe` alias), `aggressive`
- `mean_reversion_bb`: `conservative` (`safe` alias), `aggressive`

Для `index_hybrid` по умолчанию включён `index_parameter_surface_mode="tiered"`:
- профиль задаёт фиксированный execution/session/volume/risk shape;
- поверх профиля можно тюнить только core knobs: `EMA`, `Donchian`, `zscore` окна и regime thresholds;
- если нужен research-режим без ограничений, включайте `index_parameter_surface_mode="open"` осознанно, а не как production default.

Можно задать профиль и через env (если не передан `--strategy-profile`):

```bash
XTB_STRATEGY_PROFILE=conservative
# или
IG_STRATEGY_PROFILE=aggressive
```

Раздельные пресеты стратегий:

- `XTB_STRATEGY_PARAMS` — общий слой для всех стратегий.
- `XTB_STRATEGY_PRESETS` — JSON-объект вида `{ "strategy_name": { ... } }`.
- `XTB_STRATEGY_PARAMS_<STRATEGY>` — точечный слой для активной стратегии (максимальный приоритет).
- Для `BROKER=ig` доступны алиасы `IG_*` с тем же поведением.

Приоритет применения: `DEFAULTS -> *_STRATEGY_PARAMS -> *_STRATEGY_PRESETS[active] -> *_STRATEGY_PARAMS_<ACTIVE_STRATEGY>`.

Параметры multi-strategy netting (задаются в `XTB_STRATEGY_PARAMS` или `XTB_STRATEGY_PARAMS_<STRATEGY>`):

```bash
XTB_STRATEGY_PARAMS='{
  "entry_tick_max_age_sec": 1.5,
  "worker_connectivity_check_interval_sec": 5.0,
  "continuation_reentry_post_win_reset_guard_enabled": true,
  "continuation_reentry_post_win_min_reset_stop_ratio": 0.25,
  "continuation_reentry_post_win_max_age_sec": 1200.0,
  "hold_reason_metadata_verbosity": "basic",
  "protective_peak_drawdown_exit_enabled": true,
  "protective_peak_drawdown_ratio": 0.25,
  "protective_peak_drawdown_min_peak_pips": 4.0,
  "protective_peak_drawdown_hard_exit_enabled": true,
  "protective_peak_drawdown_ratio_by_symbol": {},
  "protective_peak_drawdown_min_peak_pips_by_symbol": {},
  "protective_breakeven_lock_enabled": true,
  "protective_breakeven_min_peak_pips": 4.0,
  "protective_breakeven_min_tp_progress": 0.55,
  "protective_breakeven_offset_pips": 0.0,
  "trailing_breakeven_offset_pips": 2.5,
  "protective_breakeven_min_peak_pips_by_symbol": {},
  "protective_peak_stagnation_exit_enabled": true,
  "protective_peak_stagnation_timeout_sec": 420.0,
  "protective_peak_stagnation_min_peak_pips": 8.0,
  "protective_peak_stagnation_min_retain_ratio": 0.55,
  "protective_peak_stagnation_timeout_sec_by_symbol": {},
  "protective_peak_stagnation_min_retain_ratio_by_symbol": {},
  "protective_stale_loser_exit_enabled": true,
  "protective_stale_loser_timeout_sec": 1800.0,
  "protective_stale_loser_timeout_sec_by_symbol": {"AUS200": 1200.0},
  "protective_stale_loser_loss_ratio": 0.55,
  "protective_stale_loser_loss_ratio_by_symbol": {"AUS200": 0.35},
  "protective_stale_loser_profit_tolerance_pips": 0.0,
  "protective_fast_fail_exit_enabled": true,
  "protective_fast_fail_timeout_sec": 300.0,
  "protective_fast_fail_timeout_sec_by_symbol": {"WTI": 300.0, "AUS200": 240.0, "GOLD": 300.0, "US30": 180.0, "US100": 180.0, "US500": 180.0, "DE40": 180.0, "FR40": 180.0, "EU50": 180.0, "UK100": 180.0, "IT40": 180.0, "ES35": 180.0, "SK20": 180.0, "NK20": 180.0, "DEMID50": 180.0, "JPN225": 180.0},
  "protective_fast_fail_loss_ratio": 0.45,
  "protective_fast_fail_loss_ratio_by_symbol": {"WTI": 0.45, "AUS200": 0.45, "GOLD": 0.45, "US30": 0.45, "US100": 0.45, "US500": 0.45, "DE40": 0.45, "FR40": 0.45, "EU50": 0.45, "UK100": 0.45, "IT40": 0.45, "ES35": 0.45, "SK20": 0.45, "NK20": 0.45, "DEMID50": 0.45, "JPN225": 0.45},
  "protective_fast_fail_profit_tolerance_pips": 6.0,
  "protective_fast_fail_peak_tp_progress_tolerance": 0.18,
  "protective_fast_fail_zero_followthrough_timeout_sec": 180.0,
  "protective_fast_fail_zero_followthrough_loss_ratio": 0.2,
  "protective_fast_fail_zero_followthrough_timeout_sec_by_symbol": {"WTI": 90.0, "BRENT": 90.0, "US30": 90.0, "US100": 90.0, "US500": 90.0, "DE40": 90.0, "FR40": 90.0, "EU50": 90.0, "UK100": 90.0, "IT40": 90.0, "ES35": 90.0, "SK20": 60.0, "NK20": 60.0, "DEMID50": 90.0, "JPN225": 60.0},
  "protective_fast_fail_zero_followthrough_loss_ratio_by_symbol": {"WTI": 0.12, "BRENT": 0.12, "US30": 0.14, "US100": 0.14, "US500": 0.14, "DE40": 0.14, "FR40": 0.14, "EU50": 0.14, "UK100": 0.14, "IT40": 0.14, "ES35": 0.14, "SK20": 0.10, "NK20": 0.10, "DEMID50": 0.14, "JPN225": 0.10},
  "protective_fast_fail_peak_tp_progress_tolerance_by_symbol": {"WTI": 0.12, "AUS200": 0.12, "GOLD": 0.15, "US30": 0.1, "US100": 0.1, "US500": 0.1, "DE40": 0.1, "FR40": 0.1, "EU50": 0.1, "UK100": 0.1, "IT40": 0.1, "ES35": 0.1, "SK20": 0.1, "NK20": 0.1, "DEMID50": 0.1, "JPN225": 0.1},
  "protective_profit_lock_on_reversal": false,
  "protective_profit_lock_on_hold_invalidation": false,
  "protective_runner_preservation_enabled": true,
  "protective_runner_min_tp_progress": 0.35,
  "protective_runner_distance_min_tp_progress": 0.55,
  "protective_runner_min_peak_pips": 8.0,
  "protective_runner_min_retain_ratio": 0.45,
  "protective_runner_peak_drawdown_ratio_multiplier": 1.5,
  "protective_runner_peak_stagnation_timeout_multiplier": 2.0,
  "partial_take_profit_enabled": true,
  "partial_take_profit_r_multiple": 1.4,
  "partial_take_profit_fraction": 0.5,
  "partial_take_profit_min_remaining_lots": 0.1,
  "partial_take_profit_skip_trend_positions": true,
  "partial_take_profit_skip_runner_positions": true,
  "operational_guard_enabled": true,
  "operational_guard_stale_tick_streak_threshold": 4,
  "operational_guard_allowance_backoff_streak_threshold": 4,
  "operational_guard_cooldown_sec": 180.0,
  "operational_guard_reduce_risk_on_profit": true,
  "operational_guard_reduce_risk_min_pnl": 0.0,
  "close_reconcile_enabled": true,
  "close_reconcile_pnl_alert_threshold": 0.5,
  "close_reconcile_recent_window_sec": 14400.0,
  "close_reconcile_max_passes": 3,
  "entry_signal_persistence_enabled": false,
  "entry_signal_persistence_trend_only": true,
  "entry_signal_min_persistence_sec": 0.0,
  "entry_signal_min_consecutive_evals": 1,
  "entry_signal_index_trend_min_persistence_sec": 0.0,
  "entry_signal_index_trend_min_consecutive_evals": 1,
  "entry_pending_open_block_window_sec": 45.0,
  "entry_index_trend_min_breakout_to_spread_ratio": 2.0,
  "entry_index_trend_max_entry_quality_penalty": 0.12,
  "entry_spike_guard_enabled": true,
  "entry_spike_guard_trend_only": true,
  "entry_spike_guard_lookback_samples": 2,
  "entry_spike_guard_max_window_sec": 6.0,
  "entry_spike_guard_atr_multiplier": 0.5,
  "entry_spike_guard_spread_multiplier": 2.5,
  "micro_chop_cooldown_sec": 480.0,
  "micro_chop_trade_max_age_sec": 90.0,
  "micro_chop_max_favorable_pips": 1.0,
  "micro_chop_max_tp_progress": 0.08,
  "protective_index_trend_reversal_grace_sec": 12.0,
  "protective_index_trend_reversal_grace_max_adverse_spread_ratio": 2.0,
  "protective_fresh_reversal_grace_sec": 12.0,
  "protective_fresh_reversal_grace_max_adverse_spread_ratio": 2.5,
  "protective_fresh_reversal_grace_max_adverse_stop_ratio": 0.12,
  "multi_strategy_enabled": true,
  "multi_strategy_rollout_stage": "full",
  "multi_strategy_rollout_canary_ratio": 0.30,
  "multi_strategy_rollout_canary_symbols": "",
  "multi_strategy_rollout_seed": "",
  "multi_strategy_names": "momentum,g1,donchian_breakout,trend_following,g2,mean_breakout_v2,mean_reversion_bb,index_hybrid",
  "multi_strategy_default_names": "momentum,g1,donchian_breakout,trend_following,g2,mean_breakout_v2,mean_reversion_bb,index_hybrid",
  "multi_strategy_weights": {"g1":0.55,"trend_following":1.15,"g2":1.10,"index_hybrid":1.20},
  "multi_strategy_secondary_weights": {"donchian_breakout":0.35,"g1":0.55,"trend_following":1.15,"g2":1.10,"mean_breakout_v2":1.05,"mean_reversion_bb":1.05,"index_hybrid":1.15},
  "multi_strategy_index_weights": {"g1":0.40,"momentum":0.85,"trend_following":1.20,"g2":1.35,"mean_breakout_v2":1.05,"mean_reversion_bb":1.05,"index_hybrid":1.45},
  "multi_strategy_fx_weights": {"g1":0.95,"momentum":1.10,"trend_following":0.95,"g2":0.0,"mean_breakout_v2":1.00,"mean_reversion_bb":1.10,"index_hybrid":0.10},
  "multi_strategy_commodity_weights": {"g1":0.40,"momentum":0.90,"trend_following":1.05,"g2":0.0,"mean_breakout_v2":1.00,"mean_reversion_bb":0.80,"index_hybrid":0.0},
  "multi_strategy_crypto_weights": {"g1":0.50,"momentum":0.90,"trend_following":1.00,"g2":0.0,"crypto_trend_following":1.25,"index_hybrid":0.0},
  "multi_strategy_seniority": {},
  "multi_strategy_intent_ttl_sec": 5.0,
  "multi_strategy_intent_ttl_grace_sec": 0.0,
  "multi_strategy_lot_step_lots": 0.1,
  "multi_strategy_min_open_lot": 0.1,
  "multi_strategy_deadband_lots": 0.0,
  "multi_strategy_min_conflict_power": 0.05,
  "multi_strategy_conflict_ratio_low": 0.85,
  "multi_strategy_conflict_ratio_high": 1.12,
  "multi_strategy_normalizer_window": 64,
  "multi_strategy_normalizer_min_samples": 12,
  "multi_strategy_normalizer_default": 0.65,
  "multi_strategy_reconciliation_epsilon_lots": 0.000001,
  "multi_strategy_emergency_event_cooldown_sec": 30.0
}'
```

- `multi_strategy_names` задаёт фактический состав компонентов.
- `multi_strategy_default_names` используется только как fallback, если `multi_strategy_names` не задан.
- `multi_strategy_weights` и `multi_strategy_seniority` — JSON-объекты вида `{"momentum":1.0,"g1":0.8}`.
- `multi_strategy_secondary_weights` задаёт отдельные multiplier'ы только для non-base компонентов; по умолчанию `donchian_breakout` уже входит в дефолтный basket, но остаётся облегчённым secondary breakout signal.
- `multi_strategy_index_weights`, `multi_strategy_fx_weights`, `multi_strategy_commodity_weights`, `multi_strategy_crypto_weights` задают family-aware ownership multipliers поверх базовых весов. Это главный способ развести роли стратегий: на индексах `index_hybrid`/`g2`/`trend_following` должны чаще выигрывать у `g1`, а на FX и commodities ownership может быть другим.
- `multi_strategy_reconciliation_epsilon_lots` используется как epsilon при reconciliation/net-delta сравнениях.
- `multi_strategy_rollout_stage` поддерживает `full`, `canary`, `shadow`:
  - `full` — все символы работают в текущем `XTB_MODE`.
  - `canary` — часть символов остаётся в `execution`, остальные переводятся в `signal_only`.
  - `shadow` — все символы переводятся в `signal_only` без отключения расчётов.
- `multi_strategy_rollout_canary_symbols` задаёт явный список canary-символов, `multi_strategy_rollout_canary_ratio` — fallback-долю, если список пуст.
- `multi_strategy_rollout_seed` задаёт стабильный seed для детерминированного отбора canary-символов по ratio.
- `protective_peak_drawdown_*` включает глобальный profit-lock по откату от локального MFE:
  - `protective_peak_drawdown_hard_exit_enabled=true` закрывает позицию на резком retrace даже без reverse-сигнала.
- `protective_*_by_symbol` (`*_ratio_by_symbol`, `*_min_peak_pips_by_symbol`, `*_timeout_sec_by_symbol`) позволяет задавать symbol-specific защитные пороги.
- `protective_breakeven_*` поднимает SL к breakeven после минимального MFE-порога; для trend-family baseline дополнительно требует более поздний `TP-progress`, чтобы не превращать быстрый импульс в серию микро-выходов.
- `protective_peak_stagnation_*` закрывает застрявшую прибыльную позицию, если после пика нет прогресса и доля удержанной прибыли упала ниже порога.
- `protective_stale_loser_*` закрывает затянувшийся убыточный вход до полного `SL`, если позиция уже слишком долго не раскрывается, съела заметную долю риска и так и не показала meaningful `MFE`; для `AUS200` по умолчанию стоит более жёсткий timeout/loss-ratio.
- `protective_fast_fail_*` режет ранний плохой trend-entry до полного `SL`, если в первые минуты после входа позиция быстро забирает значимую долю стопа и так и не показывает meaningful follow-through; baseline теперь дополнительно смотрит не только на raw `MFE` в пипсах, но и на долю уже пройденного пути к `TP`, чтобы не держать “псевдо-отскоки” на `WTI/GOLD`.
- `protective_runner_*` защищает trend-runner'ы от преждевременного drawdown/stagnation exit: зрелая трендовая позиция получает больше места, если уже прошла meaningful путь к `TP` и всё ещё удерживает существенную долю `MFE`; отдельный `protective_runner_distance_min_tp_progress` задерживает именно `distance`-trailing, не трогая `fast_ma` path.
- `partial_take_profit_*` включает частичную фиксацию прибыли; текущий baseline оставляет scale-out для non-trend кейсов, а `partial_take_profit_skip_trend_positions=true` по умолчанию вообще не режет trend-family на частичных выходах.
- `partial_take_profit_skip_runner_positions=true` запрещает частично обрезать уже созревшие trend-runner'ы, если позиция выглядит как настоящий carry/trend continuation, а не как быстрый scalp.
- `operational_guard_*` включает runtime guardrail: при серии stale tick / allowance backoff блокируются новые входы, а прибыльные позиции переводятся в breakeven-защиту.
- `close_reconcile_*` включает многошаговый post-close reconcile с брокером и алерт при рассинхроне `local_pnl` vs `broker_pnl`.
- `entry_spike_guard_*` добавляет общий execution-level anti-spike guard: если directional entry пытается запрыгнуть в слишком резкий микро-выброс за последние несколько сэмплов, бот переводит вход в `HOLD` с `reason=entry_spike_detected` вместо того, чтобы открыть и сразу отдать спред брокеру; порог считается от `ATR` и текущего спреда.
- `hold_reason_metadata_verbosity` управляет размером `Signal hold reason` payload для всех стратегий:
  - `minimal` — только базовые поля (`reason`, `indicator`)
  - `basic` — унифицированный короткий набор диагностических ключей (рекомендуется для live); для `multi_strategy` сюда теперь входят и summary-поля вроде `buy_power/sell_power`, component counts и `multi_*_flat` reason taxonomy
  - `full` — полный `signal.metadata` стратегии (максимальная детализация и объём)

Расписание стратегий без перезапуска процесса:

- `XTB_STRATEGY_SCHEDULE` / `IG_STRATEGY_SCHEDULE` — JSON-массив слотов или JSON-объект вида:
  - `{"timezone":"UTC","slots":[ ... ]}`
- `XTB_STRATEGY_SCHEDULE_TIMEZONE` / `IG_STRATEGY_SCHEDULE_TIMEZONE` — таймзона для расписания, если она не задана внутри JSON. По умолчанию `UTC`.
- Каждый слот поддерживает:
  - `strategy`
  - `symbols` — если не указать, бот возьмёт дефолтный набор символов для этой стратегии
  - `start`, `end` — время `HH:MM`
  - `weekdays` или `days` — например `"mon-fri"` или `["mon","tue","wed","thu","fri"]`
  - `priority` — если окна пересекаются по одному и тому же символу, выигрывает больший `priority`
  - `label` — удобное имя окна
  - `params` / `strategy_params` — локальные override-параметры только для этого окна

Практические правила для live-расписания:

- Если вы хотите разные пресеты для FX и индексов, в поле `strategy` указывайте именно `momentum_fx` и `momentum_index`, а не общий `momentum`.
- Не делайте overlapping-окна с одинаковым `priority` на один и тот же символ: так сложнее понять, кто реально управляет входом, и символ может фактически достаться последнему подходящему слоту.
- `index_hybrid` используйте только для индексов. Для `GOLD`, `WTI`, `BRENT` выбирайте `momentum_index` или другую совместимую стратегию.

Поведение:

- При активном расписании новые входы открываются только по активным окнам.
- Переключение происходит без перезапуска бота.
- Если по символу уже есть открытая позиция, бот не перескакивает на новую стратегию мгновенно:
  старая стратегия продолжает сопровождать позицию до закрытия, и только потом символ переключается на новое окно.
- Для ручной синхронизации открытых broker-side позиций без запуска воркеров можно использовать:
  - `xtb-bot --sync-open-positions`

Пример `UTC`-расписания:

```bash
XTB_STRATEGY_SCHEDULE='{"timezone":"UTC","slots":[
  {"label":"night_index_hybrid","strategy":"index_hybrid","symbols":["US100","US500","JPN225","UK100","AUS200"],"start":"02:30","end":"06:30","weekdays":"mon-fri","priority":20},
  {"label":"eu_open_index_breakout","strategy":"momentum_index","symbols":["DE40","UK100"],"start":"06:30","end":"08:30","weekdays":"mon-fri","priority":20},
  {"label":"eu_open_fx_breakout","strategy":"momentum_fx","symbols":["EURUSD","USDCHF","GBPUSD"],"start":"06:30","end":"08:30","weekdays":"mon-fri","priority":20},
  {"label":"eu_day_trend_g1","strategy":"g1","symbols":["DE40","UK100","EURUSD","GBPUSD"],"start":"08:30","end":"11:30","weekdays":"mon-fri","priority":20},
  {"label":"midday_mean_reversion_bb","strategy":"mean_reversion_bb","symbols":["EURGBP","EURCHF","AUDUSD","GBPUSD","AUS200"],"start":"11:30","end":"13:25","weekdays":"mon-fri","priority":20},
  {"label":"ny_open_index_hybrid","strategy":"index_hybrid","symbols":["US100","US500","US30"],"start":"13:30","end":"15:30","weekdays":"mon-fri","priority":20},
  {"label":"ny_open_commodities_breakout","strategy":"momentum_index","symbols":["DE40","GOLD"],"start":"13:30","end":"15:30","weekdays":"mon-fri","priority":20},
  {"label":"us_evening_trend_following","strategy":"trend_following","symbols":["US100","US500","GOLD"],"start":"15:30","end":"20:00","weekdays":"mon-fri","priority":20},
  {"label":"overnight_mean_reversion_bb","strategy":"mean_reversion_bb","symbols":["EURUSD","UK100","AUDUSD","USDCHF"],"start":"20:00","end":"23:00","weekdays":"mon-fri","priority":20},
  {"label":"japan_open_index","strategy":"momentum_index","symbols":["JPN225","AUS200"],"start":"23:00","end":"02:30","weekdays":"mon-fri","priority":20},
  {"label":"japan_open_fx","strategy":"momentum_fx","symbols":["USDJPY","AUDUSD","USDCHF"],"start":"23:00","end":"02:30","weekdays":"mon-fri","priority":20},
  {"label":"weekend_crypto","strategy":"crypto_trend_following","symbols":["BTC","ETH","LTC","SOL","XRP","DOGE"],"start":"00:00","end":"23:59","weekdays":"sat-sun","priority":30}
]}'
```

Примечание: `momentum_fx` и `momentum_index` в расписании нужны не для красоты, а чтобы реально активировались отдельные пресеты. Если оставить общий `momentum`, бот возьмёт только общий набор параметров.
`index_hybrid` используйте только для индексов, а `crypto_trend_following` только для crypto-symbols.
Для 24/7-инструментов оставляйте отдельный `sat-sun` слот (`BTC`, `ETH`, `LTC`, `SOL`, `XRP`, `DOGE`).
Если IG allowance ограничивает REST, лучше сократить число одновременно активных crypto-символов в вечернем окне.

Тюнинг `momentum` (рекомендуемый отдельный пресет):

```bash
# подтверждение кросса, ATR-стопы, slope-фильтр, лимит спреда и outcome-cooldown
XTB_STRATEGY_PARAMS_MOMENTUM={
  "fast_window":8,
  "slow_window":21,
  "momentum_ma_type":"ema",
  "momentum_entry_mode":"cross_or_trend",
  "momentum_confirm_bars":1,
  "momentum_low_tf_min_confirm_bars":1,
  "momentum_low_tf_max_confirm_bars":1,
  "momentum_high_tf_max_confirm_bars":1,
  "momentum_auto_confirm_by_timeframe":false,
  "momentum_timeframe_sec":60,
  "momentum_session_filter_enabled":false,
  "momentum_max_spread_pips":2.4,
  "momentum_max_spread_to_stop_ratio":0.20,
  "momentum_max_spread_to_atr_ratio":0.40,
  "momentum_require_context_tick_size":false,
  "momentum_trade_cooldown_sec":360,
  "momentum_trade_cooldown_win_sec":90,
  "momentum_trade_cooldown_loss_sec":420,
  "momentum_trade_cooldown_flat_sec":180,
  "momentum_atr_window":14,
  "momentum_atr_multiplier":1.7,
  "momentum_risk_reward_ratio":2.0,
  "momentum_min_stop_loss_pips":14.0,
  "momentum_min_take_profit_pips":24.0,
  "momentum_low_tf_risk_profile_enabled":true,
  "momentum_low_tf_max_timeframe_sec":300,
  "momentum_low_tf_atr_multiplier_cap":1.7,
  "momentum_low_tf_risk_reward_ratio_cap":2.0,
  "momentum_low_tf_min_stop_loss_pips":10.0,
  "momentum_low_tf_min_take_profit_pips":18.0,
  "momentum_low_tf_max_stop_loss_atr":1.7,
  "momentum_low_tf_max_take_profit_atr":3.4,
  "momentum_min_relative_stop_pct":0.0010,
  "momentum_max_price_slow_gap_atr":1.65,
  "momentum_pullback_entry_max_gap_atr":1.05,
  "momentum_continuation_confidence_cap":0.80,
  "momentum_continuation_price_alignment_bonus_multiplier":0.40,
  "momentum_continuation_late_penalty_multiplier":0.90,
  "momentum_kama_gate_enabled":true,
  "momentum_kama_er_window":10,
  "momentum_kama_fast_window":2,
  "momentum_kama_slow_window":30,
  "momentum_kama_min_efficiency_ratio":0.12,
  "momentum_kama_min_slope_atr_ratio":0.05,
  "momentum_vwap_filter_enabled":true,
  "momentum_vwap_reclaim_required":true,
  "momentum_vwap_min_session_bars":8,
  "momentum_vwap_min_volume_samples":8,
  "momentum_vwap_min_volume_quality":0.5,
  "momentum_vwap_overstretch_sigma":2.0,
  "momentum_confirm_gap_relief_per_bar":0.25,
  "momentum_price_gap_mode":"wait_pullback",
  "momentum_min_slope_atr_ratio":0.07,
  "momentum_min_trend_gap_atr":0.12,
  "momentum_fresh_cross_filter_relief_enabled":false,
  "momentum_volume_confirmation":true,
  "momentum_volume_window":20,
  "momentum_min_volume_ratio":1.3,
  "momentum_volume_min_samples":8,
  "momentum_volume_allow_missing":false,
  "momentum_higher_tf_bias_enabled":true,
  "momentum_higher_tf_bias_timeframe_sec":300,
  "momentum_higher_tf_bias_fast_window":8,
  "momentum_higher_tf_bias_slow_window":21,
  "momentum_higher_tf_bias_allow_missing":false,
  "momentum_regime_adx_filter_enabled":true,
  "momentum_regime_adx_window":8,
  "momentum_regime_min_adx":21.0,
  "momentum_regime_adx_hysteresis":1.0,
  "momentum_regime_use_hysteresis_state":true,
  "momentum_signal_only_min_confidence_for_entry":0.58,
  "momentum_paper_min_confidence_for_entry":0.62,
  "momentum_execution_min_confidence_for_entry":0.66,
  "momentum_fast_ma_trailing_enabled":true,
  "momentum_fast_ma_trailing_use_closed_candle":true,
  "momentum_fast_ma_trailing_timeframe_sec":60,
  "momentum_fast_ma_trailing_activation_r_multiple":0.8,
  "momentum_fast_ma_trailing_activation_min_profit_pips":0.0,
  "momentum_fast_ma_trailing_buffer_atr":0.15,
  "momentum_fast_ma_trailing_buffer_pips":0.0,
  "momentum_fast_ma_trailing_min_step_pips":0.5,
  "momentum_fast_ma_trailing_update_cooldown_sec":5.0
}
```

Важный override для сырья:
- `momentum_entry_filters_by_symbol.<SYMBOL>.momentum_entry_mode` поддерживает `cross_only` и `cross_or_trend`.
- Боевой baseline держит `WTI` и `BRENT` в `cross_only` и одновременно ужимает `momentum_max_price_slow_gap_atr` / `momentum_pullback_entry_max_gap_atr`, чтобы не покупать поздний `ema_trend_up` на излёте импульса.

Раздельные боевые пресеты для IG M1 (чтобы не смешивать индексы и FX в одном наборе):
- `momentum_index` можно держать с `momentum_session_filter_enabled=true`: для индексов стратегия сама переключается на локальные market hours по символу.
- `momentum_fx` лучше держать с `momentum_session_filter_enabled=false` и управлять окнами через `XTB_STRATEGY_SCHEDULE`, иначе внутреннее UTC-окно режет APAC-слоты.

```bash
XTB_STRATEGY_PARAMS_MOMENTUM_INDEX={
  "fast_window":8,
  "slow_window":21,
  "momentum_ma_type":"ema",
  "momentum_entry_mode":"cross_or_trend",
  "momentum_confirm_bars":1,
  "momentum_low_tf_min_confirm_bars":1,
  "momentum_low_tf_max_confirm_bars":1,
  "momentum_auto_confirm_by_timeframe":false,
  "momentum_timeframe_sec":60,
  "momentum_session_filter_enabled":true,
  "momentum_max_spread_pips":2.6,
  "momentum_max_spread_to_stop_ratio":0.20,
  "momentum_max_spread_to_atr_ratio":0.40,
  "momentum_require_context_tick_size":false,
  "momentum_trade_cooldown_sec":360,
  "momentum_trade_cooldown_win_sec":60,
  "momentum_trade_cooldown_loss_sec":420,
  "momentum_trade_cooldown_flat_sec":180,
  "momentum_atr_window":14,
  "momentum_atr_multiplier":1.7,
  "momentum_risk_reward_ratio":2.0,
  "momentum_min_stop_loss_pips":18.0,
  "momentum_min_take_profit_pips":36.0,
  "momentum_low_tf_risk_profile_enabled":true,
  "momentum_low_tf_max_timeframe_sec":300,
  "momentum_low_tf_atr_multiplier_cap":1.7,
  "momentum_low_tf_risk_reward_ratio_cap":2.0,
  "momentum_low_tf_min_stop_loss_pips":12.0,
  "momentum_low_tf_min_take_profit_pips":24.0,
  "momentum_low_tf_max_stop_loss_atr":1.7,
  "momentum_low_tf_max_take_profit_atr":3.4,
  "momentum_min_relative_stop_pct":0.0010,
  "momentum_max_price_slow_gap_atr":1.65,
  "momentum_pullback_entry_max_gap_atr":1.05,
  "momentum_continuation_confidence_cap":0.80,
  "momentum_continuation_price_alignment_bonus_multiplier":0.40,
  "momentum_continuation_late_penalty_multiplier":0.90,
  "momentum_confirm_gap_relief_per_bar":0.25,
  "momentum_price_gap_mode":"wait_pullback",
  "momentum_min_slope_atr_ratio":0.07,
  "momentum_min_trend_gap_atr":0.14,
  "momentum_fresh_cross_filter_relief_enabled":false,
  "momentum_volume_confirmation":true,
  "momentum_volume_window":20,
  "momentum_min_volume_ratio":1.3,
  "momentum_volume_min_samples":8,
  "momentum_volume_allow_missing":false,
  "momentum_higher_tf_bias_enabled":true,
  "momentum_higher_tf_bias_timeframe_sec":300,
  "momentum_higher_tf_bias_fast_window":8,
  "momentum_higher_tf_bias_slow_window":21,
  "momentum_higher_tf_bias_allow_missing":false,
  "momentum_regime_adx_filter_enabled":true,
  "momentum_regime_adx_window":8,
  "momentum_regime_min_adx":21.0,
  "momentum_regime_adx_hysteresis":1.0,
  "momentum_regime_use_hysteresis_state":true,
  "momentum_signal_only_min_confidence_for_entry":0.58,
  "momentum_paper_min_confidence_for_entry":0.62,
  "momentum_execution_min_confidence_for_entry":0.68,
  "momentum_fast_ma_trailing_enabled":true,
  "momentum_fast_ma_trailing_use_closed_candle":true,
  "momentum_fast_ma_trailing_timeframe_sec":60,
  "momentum_fast_ma_trailing_activation_r_multiple":0.8,
  "momentum_fast_ma_trailing_activation_min_profit_pips":0.0,
  "momentum_fast_ma_trailing_buffer_atr":0.15,
  "momentum_fast_ma_trailing_buffer_pips":0.0,
  "momentum_fast_ma_trailing_min_step_pips":0.5,
  "momentum_fast_ma_trailing_update_cooldown_sec":5.0
}

XTB_STRATEGY_PARAMS_MOMENTUM_FX={
  "fast_window":8,
  "slow_window":21,
  "momentum_ma_type":"ema",
  "momentum_entry_mode":"cross_or_trend",
  "momentum_confirm_bars":1,
  "momentum_low_tf_min_confirm_bars":1,
  "momentum_low_tf_max_confirm_bars":1,
  "momentum_auto_confirm_by_timeframe":false,
  "momentum_timeframe_sec":60,
  "momentum_session_filter_enabled":false,
  "momentum_max_spread_pips":1.0,
  "momentum_max_spread_to_stop_ratio":0.20,
  "momentum_max_spread_to_atr_ratio":0.40,
  "momentum_require_context_tick_size":false,
  "momentum_trade_cooldown_sec":240,
  "momentum_trade_cooldown_win_sec":45,
  "momentum_trade_cooldown_loss_sec":360,
  "momentum_trade_cooldown_flat_sec":120,
  "momentum_atr_window":14,
  "momentum_atr_multiplier":1.8,
  "momentum_risk_reward_ratio":2.0,
  "momentum_min_stop_loss_pips":9.0,
  "momentum_min_take_profit_pips":18.0,
  "momentum_low_tf_risk_profile_enabled":true,
  "momentum_low_tf_max_timeframe_sec":300,
  "momentum_low_tf_atr_multiplier_cap":1.8,
  "momentum_low_tf_risk_reward_ratio_cap":2.0,
  "momentum_low_tf_min_stop_loss_pips":7.0,
  "momentum_low_tf_min_take_profit_pips":14.0,
  "momentum_low_tf_max_stop_loss_atr":1.8,
  "momentum_low_tf_max_take_profit_atr":3.0,
  "momentum_min_relative_stop_pct":0.0006,
  "momentum_max_price_slow_gap_atr":1.5,
  "momentum_pullback_entry_max_gap_atr":1.0,
  "momentum_continuation_confidence_cap":0.80,
  "momentum_continuation_price_alignment_bonus_multiplier":0.40,
  "momentum_continuation_late_penalty_multiplier":0.90,
  "momentum_confirm_gap_relief_per_bar":0.25,
  "momentum_price_gap_mode":"wait_pullback",
  "momentum_min_slope_atr_ratio":0.07,
  "momentum_min_trend_gap_atr":0.12,
  "momentum_fresh_cross_filter_relief_enabled":false,
  "momentum_volume_confirmation":true,
  "momentum_volume_window":20,
  "momentum_min_volume_ratio":1.3,
  "momentum_volume_min_samples":8,
  "momentum_volume_allow_missing":false,
  "momentum_higher_tf_bias_enabled":true,
  "momentum_higher_tf_bias_timeframe_sec":300,
  "momentum_higher_tf_bias_fast_window":8,
  "momentum_higher_tf_bias_slow_window":21,
  "momentum_higher_tf_bias_allow_missing":false,
  "momentum_regime_adx_filter_enabled":true,
  "momentum_regime_adx_window":8,
  "momentum_regime_min_adx":21.0,
  "momentum_regime_adx_hysteresis":1.0,
  "momentum_regime_use_hysteresis_state":true,
  "momentum_signal_only_min_confidence_for_entry":0.58,
  "momentum_paper_min_confidence_for_entry":0.62,
  "momentum_execution_min_confidence_for_entry":0.68,
  "momentum_fast_ma_trailing_enabled":true,
  "momentum_fast_ma_trailing_use_closed_candle":true,
  "momentum_fast_ma_trailing_timeframe_sec":60,
  "momentum_fast_ma_trailing_activation_r_multiple":0.8,
  "momentum_fast_ma_trailing_activation_min_profit_pips":0.0,
  "momentum_fast_ma_trailing_buffer_atr":0.15,
  "momentum_fast_ma_trailing_buffer_pips":0.0,
  "momentum_fast_ma_trailing_min_step_pips":0.5,
  "momentum_fast_ma_trailing_update_cooldown_sec":5.0
}
```

Параметры cooldown по исходу сделки:
- `*_trade_cooldown_win_sec` — задержка перед новым входом после прибыльного закрытия.
- `*_trade_cooldown_loss_sec` — задержка после убыточного закрытия.
- `*_trade_cooldown_flat_sec` — задержка после около-нулевого результата.
- Если outcome-поля не заданы, используется `*_trade_cooldown_sec` (обратная совместимость).
- `*_continuation_reentry_guard_enabled` — глобальный guard для `*_trend_up/down`: после входа по continuation блокирует повторный continuation-вход в ту же сторону, пока не появится новый `*_cross_up/down`.
- `*_continuation_reentry_reset_on_opposite_signal` — сбрасывает continuation-guard при сигнале противоположного направления.
- После прибыльного выхода trend-позиции worker тоже arm’ит continuation-guard: повторный same-side continuation запрещён до нового `*_cross_up/down`, а fresh cross в ту же сторону теперь тоже требует реального reset рынка через `continuation_reentry_post_win_*` вместо мгновенного recross на излёте импульса.
- `entry_signal_persistence_*` — execution-layer debounce для directional entry. Боевой baseline теперь держит его выключенным по умолчанию (`enabled=false`), чтобы не добавлять 1-2 extra eval-cycle после уже готового сигнала; включайте только как явный anti-flicker override.
- `entry_signal_index_trend_*` — отдельный debounce для trend-entry от `index_hybrid` на индексах/commodities. В baseline тоже выключен по умолчанию; если включаете обратно, это уже осознанный компромисс между latency и anti-chop.
- `entry_pending_open_block_window_sec` — не даёт открыть новый вход по символу, пока в store висит свежий unresolved `pending_open`. Это страховка от duplicate-open churn, если broker/open sync ещё не успел добежать до локального `position_book`.
- `entry_index_trend_*` — execution-layer anti-chop guard для index trend breakout’ов: вход блокируется, если breakout слишком мал относительно текущего спреда или сам сетап уже выглядит слишком “рваным” по `entry_quality_penalty`.
- `micro_chop_*` — отдельный cooldown после короткого убыточного trend-trade без реального follow-through. Если сделка прожила секунды/десятки секунд, почти не дала `MFE` и закрылась в ноль/минус, worker не лезет обратно в ту же флэтовую кашу сразу следующей итерацией.
- `protective_fresh_reversal_grace_*` — глобальный execution-layer grace для новой позиции: если в первые секунды после входа приходит `reverse_signal`, `signal_invalidation` или `trend_reversal`, но adverse move всё ещё укладывается в шумовой диапазон относительно спреда/стопа, worker не закрывает сделку мгновенно по рыночной цене.
- `protective_index_trend_reversal_grace_*` — короткий grace-period для свежих index trend-входов: мгновенный reversal по шуму внутри нескольких spread больше не должен сразу превращаться в market-close через секунду после входа.
- `protective_fast_fail_zero_followthrough_*` — более жёсткий early-cut для trend-входов, которые почти не показали `MFE` и быстро начинают сползать против позиции; baseline теперь смотрит не только на raw `MFE` в пипсах, но и на фактический прогресс к `TP`, поэтому дорогой index/commodity вход с крошечным плюсом больше не считается valid follow-through.
- `protective_fast_fail_zero_followthrough_*_by_symbol` позволяет ужать этот early-cut отдельно для нефти и дорогих index CFD, где даже короткий no-followthrough drift быстро превращается в трёхзначный EUR-лосс.

`momentum_entry_mode`:
- `cross_only` — вход только на свежем пересечении MA.
- `cross_or_trend` — разрешает вход по продолжению тренда (если MA уже разошлись и подтверждены).

`momentum_fresh_cross_filter_relief_enabled`:
- если `true`, свежий `ma_cross_up/down` может смягчить только `volume_confirmation`, `regime_adx` и soft-zone `wait_pullback` через penalty к confidence.
- `higher_tf_bias` больше не смягчается: если M5 bias против сигнала, стратегия держит `HOLD`.
- hard cap по spread, session, ATR availability и `momentum_max_price_slow_gap_atr` остаётся жёстким.
- рекомендуемый боевой baseline для M1 держит `momentum_fresh_cross_filter_relief_enabled=false`, чтобы momentum не покупал “почти хороший” вход.
- `momentum_continuation_fast_ma_retest_atr_tolerance` делает continuation-входы reset-aware: straight-line `ma_trend_up/down` больше не считается достаточным, цена должна реально остыть обратно к fast MA.
- Для дорогих/нервных индексов используйте symbol-specific override через `momentum_entry_filters_by_symbol.<SYMBOL>.momentum_continuation_fast_ma_retest_atr_tolerance`; production baseline уже ужимает `US100`, `US500`, `DE40`, `JPN225`, `SK20`, `NK20`.
- `momentum_continuation_confidence_cap`, `momentum_continuation_price_alignment_bonus_multiplier` и `momentum_continuation_late_penalty_multiplier` теперь отделяют continuation от fresh cross на уровне confidence: зрелый pullback ещё можно взять, но straight-line continuation больше не получает тот же bonus/ceiling, что свежий импульс.
- Базовая confidence-модель больше не умирает от пяти мелких штрафов подряд: baseline выше, а свежие auxiliary penalties мягче, поэтому хороший trend-entry не схлопывается в ноль только из-за набора вторичных фильтров.
- `momentum_kama_gate_*` добавляет adaptive chop-filter: если `Efficiency Ratio` низкий и наклон `KAMA` почти нулевой относительно ATR, momentum блокирует вход с причиной `kama_chop_regime` вместо серии пустых continuation/cross сделок во флэте.
- `momentum_vwap_*` включает session-aware `VWAP` как intraday fair-value filter для индексов и commodities: continuation-входы не берутся, если цена уже ушла в `VWAP + overstretch_sigma`, а long/short против `VWAP` требуют reclaim обратно через fair value.
- Для fresh cross `VWAP` не является тупым hard block: если цена уже растянута за fair-value bands, стратегия даёт penalty к confidence вместо немедленного запрета. Это помогает не убивать хорошие trend day cross-сетапы тем же правилом, которое нужно для late continuation.

`momentum_volume_confirmation`:
- если `true`, бот проверяет, что `current_volume >= avg(volume_window) * min_volume_ratio`;
- если объём недоступен у брокера, поведение управляется `momentum_volume_allow_missing` (`true` — не блокировать вход, `false` — блокировать).

`momentum_max_spread_to_stop_ratio` / `momentum_max_spread_to_atr_ratio`:
- дополнительные фильтры издержек поверх абсолютного `momentum_max_spread_pips`: блокируют вход, если spread слишком большой относительно рассчитанного стопа или ATR.
- для M1 разумная отправная точка: `spread <= 20% stop` и `spread <= 40% ATR`.

`momentum_fast_ma_trailing_*`:
- `momentum_fast_ma_trailing_enabled=true` включает трейлинг от быстрой MA.
- трейлинг вооружается только после `activation_r_multiple` или `activation_min_profit_pips`.
- новый SL ставится по `fast_ma ± buffer` (буфер в ATR/пипсах), только в сторону уменьшения риска.
- обновление ограничивается `momentum_fast_ma_trailing_min_step_pips` и `momentum_fast_ma_trailing_update_cooldown_sec`, чтобы не дергать модификации на каждом тике.

Авто-правило `momentum_confirm_bars`:
- `<= M5` (`momentum_timeframe_sec <= 300`): подтверждение автоматически держится на одном баре (`momentum_low_tf_min_confirm_bars=1`, `momentum_low_tf_max_confirm_bars=1`), чтобы не отдавать импульс рынку
- `>= H1` (`momentum_timeframe_sec >= 3600`): подтверждение ограничивается сверху `momentum_high_tf_max_confirm_bars` (по умолчанию `1`)
- отключить адаптацию можно через `momentum_auto_confirm_by_timeframe=false` (рекомендуется для индексного M1-профиля с `momentum_confirm_bars=1`)
- подтверждение ищет кросс **внутри окна**, а не только в одной фиксированной точке, поэтому бот не пропускает вход, если пересечение случилось на 1-2 свечи раньше.

`momentum_session_filter_enabled`:
- для индексов фильтр не живёт в тупом `UTC 6..22`: стратегия маппит символ на локальные market hours биржи.
- для FX/CFD без такого профиля внутренний UTC-фильтр легко режет APAC-окна, поэтому `momentum_fx` лучше держать с `false` и ограничивать торговлю расписанием.

`momentum_price_gap_mode`:
- `wait_pullback` — если цена слишком далеко от slow MA (по ATR), бот не входит и ждёт pullback; при hard-cap возвращает `waiting_for_limit_pullback` и пишет `suggested_entry_price` на fast MA.
- при `momentum_fresh_cross_filter_relief_enabled=true` свежий cross может пройти soft-zone `wait_pullback` с penalty к confidence, пока цена не вышла за hard cap `momentum_max_price_slow_gap_atr`.
- `block` — жестко блокирует вход при перерастяжении цены.

`momentum_low_tf_*` (short-horizon risk profile):
- при `momentum_low_tf_risk_profile_enabled=true` и `timeframe_sec <= momentum_low_tf_max_timeframe_sec` стратегия сжимает риск-параметры под M1/M5: ограничивает `atr_multiplier`/`risk_reward_ratio`, а также применяет отдельные low-TF floor/cap для SL/TP.
- цель: не требовать M5/M15-амплитуду у сигнала с минутным half-life и не оставлять только поздние растянутые входы.

`momentum_pullback_entry_max_gap_atr`:
- мягкий лимит для входа в `wait_pullback`-режиме: даже если жесткий лимит не нарушен, вход выполняется только когда цена вернулась достаточно близко к slow MA.
- если не задан, используется `momentum_max_price_slow_gap_atr` (поведение совместимо с предыдущей версией).
- для M1-профилей лучше держать в зоне `~1.2..2.2 ATR`; значения около `5 ATR` практически выключают логику ожидания pullback.

`momentum_confirm_gap_relief_per_bar`:
- добавляет адаптивный запас к `momentum_max_price_slow_gap_atr` и `momentum_pullback_entry_max_gap_atr` для больших `confirm_bars`, чтобы подтверждение кросса и gap-фильтр не конфликтовали между собой.

`momentum_require_context_tick_size`:
- если `true`, стратегия не откроет вход без `ctx.tick_size` и вернет `HOLD` с причиной `tick_size_unavailable`.

Тюнинг `trend_following` (pullback entry + стоп по противоположной границе канала):

```bash
XTB_STRATEGY_PARAMS_TREND_FOLLOWING={
  "fast_ema_window":13,
  "slow_ema_window":55,
  "donchian_window":20,
  "use_donchian_filter":true,
  "trend_require_context_tick_size":false,
  "trend_breakout_lookback_bars":20,
  "trend_atr_window":14,
  "trend_pullback_max_distance_ratio":0.0045,
  "trend_pullback_max_distance_atr":1.5,
  "trend_crypto_max_pullback_distance_atr":1.8,
  "trend_pullback_ema_tolerance_ratio":0.004,
  "trend_pullback_ema_tolerance_atr_multiplier":1.0,
  "trend_slope_window":4,
  "trend_live_candle_entry_enabled":true,
  "trend_pullback_bounce_required":true,
  "trend_bounce_rejection_enabled":true,
  "trend_pullback_bounce_min_retrace_atr_ratio":0.08,
  "trend_bounce_rejection_min_wick_to_range_ratio":0.30,
  "trend_bounce_rejection_min_wick_to_body_ratio":0.65,
  "trend_bounce_rejection_buy_min_close_location":0.50,
  "trend_bounce_rejection_sell_max_close_location":0.50,
  "trend_runaway_entry_enabled":true,
  "trend_runaway_max_distance_atr":1.1,
  "trend_runaway_adx_window":14,
  "trend_runaway_strong_trend_min_adx":30.0,
  "trend_runaway_strong_trend_distance_atr":1.2,
  "trend_runaway_confidence_penalty":0.08,
  "trend_runaway_strong_trend_confidence_penalty":0.0,
  "trend_index_mature_trend_confidence_bonus":0.04,
  "trend_slope_mode":"fast_with_slow_tolerance",
  "trend_slow_slope_tolerance_ratio":0.0008,
  "trend_kama_gate_enabled":true,
  "trend_kama_er_window":10,
  "trend_kama_fast_window":2,
  "trend_kama_slow_window":30,
  "trend_kama_min_efficiency_ratio":0.14,
  "trend_kama_min_slope_atr_ratio":0.04,
  "trend_confidence_velocity_norm_ratio":0.0005,
  "trend_confidence_gap_velocity_positive_threshold":0.20,
  "trend_confidence_gap_velocity_negative_threshold":-0.10,
  "trend_confidence_gap_velocity_positive_bonus":0.12,
  "trend_confidence_gap_velocity_negative_penalty":0.15,
  "trend_crypto_min_ema_gap_ratio":0.0012,
  "trend_crypto_min_fast_slope_ratio":0.0002,
  "trend_crypto_min_slow_slope_ratio":0.00005,
  "trend_crypto_min_atr_pct":0.18,
  "trend_volume_confirmation":true,
  "trend_volume_window":20,
  "trend_min_volume_ratio":1.3,
  "trend_volume_min_samples":8,
  "trend_volume_allow_missing":false,
  "trend_volume_require_spike":false,
  "trend_volume_confidence_boost":0.1,
  "trend_max_spread_to_stop_ratio":0.25,
  "trend_max_spread_pips_by_symbol":{"NK20":3.0},
  "trend_following_worker_manual_close_sync_interval_sec":60,
  "trend_following_worker_connectivity_check_interval_sec":5.0,
  "trend_following_hold_summary_enabled":true,
  "trend_following_hold_summary_interval_sec":60,
  "trend_following_hold_summary_window":120,
  "trend_following_hold_summary_min_samples":20,
  "trend_following_signal_only_min_confidence_for_entry":0.58,
  "trend_following_paper_min_confidence_for_entry":0.62,
  "trend_following_execution_min_confidence_for_entry":0.68,
  "trend_risk_reward_ratio":2.5,
  "trend_min_stop_loss_pips":20.0,
  "trend_min_take_profit_pips":50.0,
  "trend_crypto_entry_stop_invalidation_enabled":false,
  "trend_index_min_stop_pct":0.05,
  "trend_strength_norm_ratio":0.0025
}
```

`trend_require_context_tick_size`:
- если `true`, стратегия не откроет вход без `ctx.tick_size` и вернет `HOLD` с причиной `tick_size_unavailable`.

`trend_following_worker_manual_close_sync_interval_sec`:
- периодическая сверка открытой execution-позиции с историей IG (чтобы бот корректно зафиксировал ручное закрытие на стороне брокера).

`trend_following_hold_summary_*`:
- runtime-агрегация top-3 причин `HOLD` в event `Trend hold summary` (видно, что именно блокирует входы).

`trend_crypto_*`:
- доп. фильтры только для crypto-symbols (`BTC/ETH/LTC/BCH/DOGE/XRP/SOL`): минимальная сила тренда (EMA gap/slope), минимальная ATR%-волатильность и более строгий лимит pullback по ATR.

`trend_slope_window`:
- сглаживает slope-фильтр через median drift последних EMA-шагов, чтобы одиночный аномальный бар не ломал весь trend regime.

`trend_pullback_ema_tolerance_ratio` и `trend_pullback_ema_tolerance_atr_multiplier`:
- базовая зона pullback вокруг fast EMA теперь может расширяться через ATR. В спокойном рынке стратегия остаётся точной, а в волатильном больше не ждёт нереалистичный “попадание в пункт”.

`trend_pullback_bounce_*`:
- bounce больше не считается по одному `last >= prev`; теперь нужен локальный экстремум на предыдущем баре и минимальный retrace против ATR, что режет micro-bounce noise в середине реального падения.

`trend_bounce_rejection_*`:
- дополнительный candle-structure gate для bounce: стратегия смотрит на wick/body, wick/range и location закрытия внутри свечи. Для FX и индексов baseline теперь мягче (`wick/body ~= 0.65`), чтобы M1-откаты не требовали идеального pinbar, а crypto может оставаться строже.

`trend_runaway_entry_*`:
- controlled continuation path для супер-трендов, которые не возвращаются к fast EMA: стратегия может взять `runaway_continuation`, только если цена всё ещё держится по одну сторону fast EMA, структура последних баров направлена по тренду, а дистанция до EMA остаётся в разумном ATR-коридоре.
- если ADX уже показывает сильный тренд и `ema_gap` ускоряется, strategy расширяет runaway-коридор до `trend_runaway_strong_trend_distance_atr` и снимает обычный confidence penalty за continuation-entry.

`trend_confidence_gap_velocity_*`:
- ранний фильтр силы тренда по ускорению расхождения EMA: ускоряющийся gap добавляет confidence на старте тренда, а сужающийся gap режет уверенность в late-entry перед выдохом движения.
- `trend_index_mature_trend_confidence_bonus` даёт индексам небольшой boost только в уже зрелом тренде: когда gap EMA ускоряется, скорость не дохлая и вход не выглядит экстремально перерастянутым. Это не “раздушивает” стратегию глобально, а помогает хорошим index-trend setups чаще выиграть ownership у более шумных M1 engines.

`trend_kama_gate_*`:
- adaptive regime-gate поверх EMA-логики: если `ER` низкий и `KAMA` почти плоская относительно ATR, стратегия считает режим `CHOP` и не открывает trend-entry.
- Это полезно именно против серий стопов в боковике: EMA могут ещё смотреть “вверх”, а `KAMA` уже показывает, что движения неэффективны и шумовые.

`trend_live_candle_entry_enabled`:
- при ресемплинге стратегия может добавить preview текущей формирующейся свечи поверх закрытых баров, чтобы не отдавать целую свечу входа на сильном развороте pullback.

`trend_crypto_entry_stop_invalidation_enabled`:
- позволяет отключить быстрый invalidation-стоп по EMA для crypto-entry. Это снижает число выходов на обычных squeeze/wick-сбоях, где структура канала ещё жива, а EMA только “задели”.

`trend_spread_buffer_factor` и `trend_max_spread_to_stop_ratio`:
- защита от широкого спреда: стоп расширяется на долю спреда и блокируется вход, если спред становится слишком большим относительно стопа.

`trend_volume_*`:
- опциональное подтверждение входа по объему: при наличии `volume_spike` стратегия поднимает confidence на `trend_volume_confidence_boost`; при `trend_volume_require_spike=true` без spike вход блокируется (`reason=volume_not_confirmed`).

`trend_max_stop_loss_atr` / `trend_crypto_max_stop_loss_atr`:
- верхняя граница для стопа в ATR; если структура требует слишком широкий стоп, стратегия ограничивает его cap-значением.

`trend_max_timestamp_gap_sec`:
- защита от больших разрывов в потоке свечей: при превышении порога стратегия вернет `HOLD` с причиной `timestamp_gap_too_wide`.

Тюнинг `crypto_trend_following` (ночной IG-режим с мягче spread/pullback фильтрами):

```bash
XTB_STRATEGY_PARAMS_CRYPTO_TREND_FOLLOWING={
  "fast_ema_window":21,
  "slow_ema_window":89,
  "donchian_window":20,
  "use_donchian_filter":false,
  "trend_breakout_lookback_bars":15,
  "trend_pullback_max_distance_ratio":0.0025,
  "trend_pullback_max_distance_atr":1.5,
  "trend_crypto_max_pullback_distance_atr":1.5,
  "trend_pullback_ema_tolerance_ratio":0.002,
  "trend_pullback_ema_tolerance_atr_multiplier":1.0,
  "trend_slope_window":5,
  "trend_live_candle_entry_enabled":true,
  "trend_crypto_pullback_ema_tolerance_ratio":0.02,
  "trend_crypto_min_ema_gap_ratio":0.0015,
  "trend_crypto_min_fast_slope_ratio":0.00025,
  "trend_crypto_min_slow_slope_ratio":0.00008,
  "trend_crypto_min_atr_pct":0.20,
  "trend_pullback_bounce_required":false,
  "trend_bounce_rejection_enabled":true,
  "trend_pullback_bounce_min_retrace_atr_ratio":0.08,
  "trend_bounce_rejection_min_wick_to_range_ratio":0.30,
  "trend_bounce_rejection_min_wick_to_body_ratio":1.0,
  "trend_bounce_rejection_buy_min_close_location":0.50,
  "trend_bounce_rejection_sell_max_close_location":0.50,
  "trend_runaway_entry_enabled":false,
  "trend_runaway_max_distance_atr":0.75,
  "trend_runaway_adx_window":14,
  "trend_runaway_strong_trend_min_adx":30.0,
  "trend_runaway_strong_trend_distance_atr":1.2,
  "trend_runaway_confidence_penalty":0.08,
  "trend_runaway_strong_trend_confidence_penalty":0.0,
  "trend_confidence_velocity_norm_ratio":0.0005,
  "trend_confidence_gap_velocity_positive_threshold":0.20,
  "trend_confidence_gap_velocity_negative_threshold":-0.10,
  "trend_confidence_gap_velocity_positive_bonus":0.12,
  "trend_confidence_gap_velocity_negative_penalty":0.15,
  "trend_spread_buffer_factor":1.0,
  "trend_max_spread_to_stop_ratio":0.60,
  "trend_crypto_min_stop_pct":1.0,
  "trend_crypto_max_stop_loss_atr":3.0,
  "trend_crypto_entry_stop_invalidation_enabled":false,
  "trend_max_timestamp_gap_sec":3600.0,
  "trend_risk_reward_ratio":2.0,
  "trend_min_stop_loss_pips":150.0,
  "trend_min_take_profit_pips":120.0,
  "trend_strength_norm_ratio":0.0025
}
```

Для `crypto_trend_following` держите stop в процентах/ATR, иначе ночной spread и шум IG будут часто выбивать по короткому стопу.
Перевод в безубыток для trend-стратегий живёт не внутри самой стратегии, а в общем execution-layer через `protective_breakeven_lock_*`, чтобы одна и та же логика работала одинаково для всех trend-entry без локальных костылей.

Тюнинг `donchian_breakout` (compact live surface для breakout core + ATR/volume + Donchian exit channel):

```bash
XTB_STRATEGY_PARAMS_DONCHIAN_BREAKOUT={
  "donchian_breakout_window":24,
  "donchian_breakout_exit_window":8,
  "donchian_breakout_atr_window":14,
  "donchian_breakout_atr_multiplier":1.5,
  "donchian_breakout_risk_reward_ratio":3.0,
  "donchian_breakout_min_stop_loss_pips":30.0,
  "donchian_breakout_min_take_profit_pips":90.0,
  "donchian_breakout_min_breakout_atr_ratio":0.15,
  "donchian_breakout_max_breakout_atr_ratio":1.8,
  "donchian_breakout_min_channel_width_atr":1.0,
  "donchian_breakout_candle_timeframe_sec":60.0,
  "donchian_breakout_resample_mode":"auto",
  "donchian_breakout_volume_confirmation":true
}
```

Что это даёт:
- live `.env` держит только core breakout/risk knobs, а не вторичный research-surface;
- secondary `session` / `RSI` / `regime` фильтры больше не считаются обязательной частью боевого конфига;
- объём на пробое остаётся основным live-confirmation, а trailing по-прежнему якорится на exit-канале Donchian.

Для IG:
- если брокер передаёт `tick_size`, стратегия использует его как источник `pip_size`;
- если `tick_size` недоступен для CFD/индексов/коммодити и включён `mb_require_context_tick_size_for_cfd=true`, вход блокируется (`reason=tick_size_unavailable`);
- symbol fallback используется только как резерв и в первую очередь для FX.
- slope для `mean_breakout_v2` теперь считается не по “одной дельте между двумя EMA-точками”, а по сглаженному EMA-regression drift на более широком окне, чтобы один микро-откат не выбивал breakout-сигнал;
- стратегия жёстко блокирует `breakout_exhausted`, если вход уже слишком перегрет по `zscore`, размеру пробоя к риску, доминированию последнего импульса и `sprint`-скорости прогона через канал;
- `confidence` сохраняет компонентную диагностику, но cost-score теперь мягче к небольшим расширениям spread, а итоговое значение квантуется крупнее, чтобы сайзинг не дёргался от микрошума.

Тюнинг `mean_breakout_v2` (Z-Score + breakout + slope):

```bash
XTB_STRATEGY_PARAMS_MEAN_BREAKOUT_V2={
  "mb_zscore_window":50,
  "mb_breakout_window":20,
  "mb_slope_window":7,
  "mb_zscore_threshold":1.85,
  "mb_zscore_entry_mode":"directional_extreme",
  "mb_min_slope_ratio":0.0006,
  "mb_exit_z_level":0.5,
  "mb_stop_loss_pips":50.0,
  "mb_take_profit_pips":120.0,
  "mb_risk_reward_ratio":2.2,
  "mb_atr_window":14,
  "mb_atr_multiplier":1.5,
  "mb_min_stop_loss_pips":60.0,
  "mb_min_take_profit_pips":120.0,
  "mb_min_relative_stop_pct":0.0012,
  "mb_dynamic_tp_only":true,
  "mb_adaptive_sl_max_atr_ratio":4.5,
  "mb_require_context_tick_size_for_cfd":true,
  "mb_breakout_min_buffer_atr_ratio":0.1,
  "mb_breakout_min_buffer_spread_multiplier":2.0,
  "mb_exhaustion_sprint_lookback_bars":3,
  "mb_exhaustion_sprint_move_channel_ratio":0.8,
  "mb_volume_confirmation":true,
  "mb_volume_window":20,
  "mb_min_volume_ratio":1.4,
  "mb_volume_min_samples":10,
  "mb_volume_allow_missing":false,
  "mb_volume_require_spike":true,
  "mb_volume_confidence_boost":0.1,
  "mb_timeframe_sec":60,
  "mb_candle_timeframe_sec":60,
  "mb_resample_mode":"auto",
  "mb_m5_max_sec":300,
  "mb_m15_max_sec":900,
  "mb_sl_mult_m5":1.0,
  "mb_tp_mult_m5":1.0,
  "mb_sl_mult_m15":1.3,
  "mb_tp_mult_m15":1.6,
  "mb_sl_mult_h1":1.8,
  "mb_tp_mult_h1":2.4,
  "mb_trailing_enabled":true,
  "mb_trailing_atr_multiplier":1.7,
  "mb_trailing_activation_stop_ratio":0.75,
  "mb_trailing_breakeven_offset_pips":2.0,
  "mb_trailing_min_activation_pips":0.0,
}
```

`mean_breakout_v2` сначала пытается определить ТФ по `timestamps` (медиана дельт), иначе использует `mb_timeframe_sec`.
Для импульсного режима используйте `mb_zscore_entry_mode=directional_extreme`: входы проходят только когда пробой подтвержден направленным экстремальным z-score.
`mb_exhaustion_sprint_*` добавляет анти-FOMO слой: если цена пробегает слишком большую долю текущего канала за последние 3 свечи и уже перегрета по `zscore`/`breakout_to_sl`, стратегия держит `HOLD` вместо погони за выносом.
Итоговый SL/TP:
- `SL = max(min_stop, base_sl * tf_multiplier, ATR_pips * mb_atr_multiplier, price * mb_min_relative_stop_pct / pip_size)`
- если включён `mb_adaptive_sl_max_atr_ratio`, статический `base_sl * tf_multiplier` не может разрастись выше `ATR_pips * mb_adaptive_sl_max_atr_ratio` до финального `max(...)`;
- если `mb_dynamic_tp_only=true`, `TP = max(min_tp, SL * rr_ratio)` (без жёсткой привязки к `base_tp`);
- иначе `TP = max(min_tp, base_tp * tf_multiplier, SL * rr_ratio)`.

Если `mb_trailing_enabled=true`, стратегия добавляет в `Signal.metadata.trailing_stop` рекомендации для воркера:
- `trailing_distance_pips = ATR_pips * mb_trailing_atr_multiplier`
- `trailing_activation_pips = max(mb_trailing_min_activation_pips, SL * mb_trailing_activation_stop_ratio)`
- если задан `mb_trailing_breakeven_offset_pips`, trailing подтягивает стоп хотя бы к `breakeven + offset`, а не просто к голому `entry`.

`mb_volume_*`:
- подтверждение breakout по объему. В боевом профиле теперь требуется реальный spike; при spike confidence повышается на `mb_volume_confidence_boost`, а без spike вход блокируется (`reason=volume_not_confirmed`).

Тюнинг `mean_reversion_bb` (Bollinger re-entry + weighted Connors RSI + ADX range-band + volume spike confirmation):

```bash
XTB_STRATEGY_PARAMS_MEAN_REVERSION_BB={
  "mean_reversion_bb_window":20,
  "mean_reversion_bb_std_dev":2.2,
  "mean_reversion_bb_entry_mode":"reentry",
  "mean_reversion_bb_reentry_tolerance_sigma":0.18,
  "mean_reversion_bb_reentry_min_reversal_sigma":0.22,
  "mean_reversion_bb_use_rsi_filter":true,
  "mean_reversion_bb_rsi_period":14,
  "mean_reversion_bb_rsi_history_multiplier":3.0,
  "mean_reversion_bb_rsi_method":"wilder",
  "mean_reversion_bb_rsi_overbought":85.0,
  "mean_reversion_bb_rsi_oversold":15.0,
  "mean_reversion_bb_oscillator_mode":"connors",
  "mean_reversion_bb_oscillator_gate_mode":"soft",
  "mean_reversion_bb_connors_price_rsi_period":3,
  "mean_reversion_bb_connors_streak_rsi_period":2,
  "mean_reversion_bb_connors_rank_period":20,
  "mean_reversion_bb_connors_price_rsi_weight":0.5,
  "mean_reversion_bb_connors_streak_rsi_weight":0.4,
  "mean_reversion_bb_connors_rank_weight":0.1,
  "mean_reversion_bb_oscillator_soft_zone_width":10.0,
  "mean_reversion_bb_oscillator_soft_confidence_penalty":0.15,
  "mean_reversion_bb_candle_timeframe_sec":60,
  "mean_reversion_bb_resample_mode":"auto",
  "mean_reversion_bb_ignore_sunday_candles":true,
  "mean_reversion_bb_allowed_asset_classes":"fx,index",
  "mean_reversion_bb_symbol_whitelist":"",
  "mean_reversion_bb_symbol_blacklist":"",
  "mean_reversion_bb_params_by_asset_class":{
    "fx":{
      "mean_reversion_bb_std_dev":2.2,
      "mean_reversion_bb_min_band_extension_ratio":0.06,
      "mean_reversion_bb_trend_slope_strict_threshold":0.00012,
      "mean_reversion_bb_min_confidence_for_entry":0.62,
      "mean_reversion_bb_volume_require_spike":false
    },
    "index":{
      "mean_reversion_bb_std_dev":2.2,
      "mean_reversion_bb_min_band_extension_ratio":0.10,
      "mean_reversion_bb_trend_slope_strict_threshold":0.00018,
      "mean_reversion_bb_min_confidence_for_entry":0.68,
      "mean_reversion_bb_volume_require_spike":true
    }
  },
  "mean_reversion_bb_min_std_ratio":0.00005,
  "mean_reversion_bb_min_band_extension_ratio":0.08,
  "mean_reversion_bb_max_band_extension_ratio":1.5,
  "mean_reversion_bb_trend_filter_enabled":true,
  "mean_reversion_bb_trend_ma_window":100,
  "mean_reversion_bb_trend_filter_mode":"strict",
  "mean_reversion_bb_trend_filter_extreme_sigma":2.8,
  "mean_reversion_bb_trend_slope_lookback_bars":5,
  "mean_reversion_bb_trend_slope_strict_threshold":0.00015,
  "mean_reversion_bb_trend_countertrend_min_distance_sigma":2.0,
  "mean_reversion_bb_trend_slope_block_max_distance_sigma":1.8,
  "mean_reversion_bb_regime_adx_filter_enabled":true,
  "mean_reversion_bb_regime_adx_window":8,
  "mean_reversion_bb_regime_min_adx":16.0,
  "mean_reversion_bb_regime_max_adx":20.0,
  "mean_reversion_bb_regime_adx_hysteresis":1.0,
  "mean_reversion_bb_regime_use_hysteresis_state":true,
  "mean_reversion_bb_regime_volatility_expansion_filter_enabled":true,
  "mean_reversion_bb_regime_volatility_short_window":14,
  "mean_reversion_bb_regime_volatility_long_window":56,
  "mean_reversion_bb_regime_volatility_expansion_max_ratio":1.35,
  "mean_reversion_bb_session_filter_enabled":true,
  "mean_reversion_bb_session_start_hour_utc":6,
  "mean_reversion_bb_session_end_hour_utc":22,
  "mean_reversion_bb_volume_confirmation":true,
  "mean_reversion_bb_volume_window":20,
  "mean_reversion_bb_min_volume_ratio":1.6,
  "mean_reversion_bb_volume_min_samples":10,
  "mean_reversion_bb_volume_allow_missing":false,
  "mean_reversion_bb_volume_require_spike":true,
  "mean_reversion_bb_volume_spike_mode":"reversal_gate",
  "mean_reversion_bb_volume_confidence_boost":0.15,
  "mean_reversion_bb_vwap_filter_enabled":true,
  "mean_reversion_bb_vwap_target_enabled":true,
  "mean_reversion_bb_vwap_reclaim_required":true,
  "mean_reversion_bb_vwap_entry_band_sigma":2.0,
  "mean_reversion_bb_vwap_min_session_bars":8,
  "mean_reversion_bb_vwap_min_volume_samples":8,
  "mean_reversion_bb_reentry_base_confidence":0.62,
  "mean_reversion_bb_reentry_rsi_bonus":0.1,
  "mean_reversion_bb_reentry_extension_confidence_weight":0.15,
  "mean_reversion_bb_exit_on_midline":false,
  "mean_reversion_bb_exit_midline_tolerance_sigma":0.15,
  "mean_reversion_bb_rejection_gate_enabled":true,
  "mean_reversion_bb_rejection_min_wick_to_body_ratio":0.5,
  "mean_reversion_bb_rejection_min_wick_to_range_ratio":0.18,
  "mean_reversion_bb_rejection_sell_max_close_location":0.45,
  "mean_reversion_bb_rejection_buy_min_close_location":0.55,
  "mean_reversion_bb_use_atr_sl_tp":true,
  "mean_reversion_bb_atr_window":14,
  "mean_reversion_bb_atr_multiplier":1.5,
  "mean_reversion_bb_take_profit_mode":"rr",
  "mean_reversion_bb_signal_only_min_confidence_for_entry":0.58,
  "mean_reversion_bb_min_confidence_for_entry":0.62,
  "mean_reversion_bb_paper_min_confidence_for_entry":0.68,
  "mean_reversion_bb_execution_min_confidence_for_entry":0.75,
  "mean_reversion_bb_trade_cooldown_sec":360,
  "mean_reversion_bb_trade_cooldown_win_sec":480,
  "mean_reversion_bb_trade_cooldown_loss_sec":600,
  "mean_reversion_bb_trade_cooldown_flat_sec":360,
  "mean_reversion_bb_risk_reward_ratio":2.2,
  "mean_reversion_bb_min_stop_loss_pips":25.0,
  "mean_reversion_bb_min_take_profit_pips":40.0
}
```

Логика:
- вход по re-entry: сначала выход за полосу, затем возврат внутрь канала с минимальной глубиной возврата (`mean_reversion_bb_reentry_tolerance_sigma`) и минимальным разворотным импульсом (`mean_reversion_bb_reentry_min_reversal_sigma`), чтобы не входить на шумовом касании;
- `mean_reversion_bb_allowed_asset_classes` ограничивает рынки (`fx,index,commodity,crypto,other`, либо `all`);
- `mean_reversion_bb_symbol_whitelist`/`mean_reversion_bb_symbol_blacklist` позволяют жёстко включать/исключать тикеры;
- `mean_reversion_bb_params_by_asset_class` даёт разный набор параметров для FX и индексов внутри одной стратегии;
- RSI по умолчанию считается по Уайлдеру (`mean_reversion_bb_rsi_method="wilder"`), при этом используется расширенная история (`mean_reversion_bb_rsi_history_multiplier`) для стабильного сглаживания;
- осциллятор по умолчанию переведён в `Connors RSI` (`mean_reversion_bb_oscillator_mode="connors"`) и `soft` режим; его компоненты теперь можно взвешивать отдельно через `mean_reversion_bb_connors_*_weight`, чтобы на M1 сильнее учитывать streak-exhaustion, а не только rank;
- при `mean_reversion_bb_resample_mode=auto` стратегия использует closed-candle path только на candle-like входе; на snapshot/tick-потоке она считает сигнал по raw ценам без скрытого ожидания закрытия свечи, чтобы не отдавать reversal рынку;
- при слишком низкой волатильности (`std/mean < mean_reversion_bb_min_std_ratio`) стратегия не входит;
- есть защита от экстремальных «ножей» через `mean_reversion_bb_max_band_extension_ratio`;
- добавлены regime-гейты: ADX-range band (`mean_reversion_bb_regime_min_adx` ... `mean_reversion_bb_regime_max_adx`) и фильтр расширения волатильности (`mean_reversion_bb_regime_volatility_*`) для отсечения как trend/breakout режимов, так и совсем «мертвого» дрейфа;
- добавлен session-фильтр (`mean_reversion_bb_session_filter_enabled`, `mean_reversion_bb_session_start_hour_utc`, `mean_reversion_bb_session_end_hour_utc`) для ограничения входов активными часами;
- для подтверждения exhaustion/rejection добавлен `candle rejection gate` (`mean_reversion_bb_rejection_gate_*`): проверяются wick/body asymmetry и позиция close внутри диапазона свечи;
- в режиме `strict` trend-filter больше не разрешает контртренд только из-за overextension: если цена стоит против `trend_ma` и adverse slope ещё силён, сигнал блокируется; `extreme_override` имеет смысл только для `reentry` после экстремального выноса и остывающего slope;
- `mean_reversion_bb_volume_require_spike=true` по умолчанию: volume теперь реально подтверждает вход, а не только поднимает confidence;
- `mean_reversion_bb_volume_spike_mode` управляет трактовкой объёма: `agnostic` (старое поведение), `reversal_boost_only` (boost только при признаке разворота), `reversal_gate` (по умолчанию: spike без разворота блокирует вход как continuation);
- по умолчанию стратегия больше не режет сделку ранним `midline`-exit, а тянет целевой `RR`/`TP`, чтобы не жить в зоне слабого payoff;
- для FX raw tick-volume spike теперь дополнительно проходит `liquidity/session` sanity-check: вне окна `session_start/end_hour_utc` такой spike не считается валидным подтверждением, а при обязательном spike (`volume_require_spike*`) вход блокируется;
- если хотите режим «только boost без блокировки», явно задайте `mean_reversion_bb_volume_require_spike=false`; для FX это разумнее делать через `mean_reversion_bb_params_by_asset_class.fx`;
- `mean_reversion_bb_vwap_filter_enabled=true` включает intraday fair-value filter: стратегия предпочитает reversion только тогда, когда экстремум действительно удалён от session VWAP, а не просто у края полос.
- `mean_reversion_bb_vwap_reclaim_required=true` требует reclaim обратно в сторону VWAP перед входом, чтобы не ловить продолжающийся one-way auction.
- `mean_reversion_bb_vwap_target_enabled=true` использует `VWAP touch` как exit-anchor и пишет это в metadata как `fair_value_target_kind="vwap"` и `exit_hint="close_on_vwap_touch"`.
- `mean_reversion_bb_vwap_entry_band_sigma` задаёт, насколько далеко цена должна уйти от VWAP в сигмах, прежде чем стратегия вообще начнёт считать движение mean-reversion candidate.
- Если в текущей сессии мало валидных баров или объёмов, стратегия автоматически откатывается к старому поведению через BB midline и не притворяется, что знает VWAP.
- для открытой позиции бот закрывает сделку только по `HOLD`-сигналу с `reason=mean_reversion_target_reached` и `exit_hint=close_on_bb_midline` (`exit_action=close_position`), но только если execution-layer не был вынужден поднять TP floor;
- `take_profit_mode=midline` теперь использует «чистую» цель до midline без принудительного floor; в metadata отдельно публикуются `strategy_take_profit_pips` и execution-layer `execution_take_profit_pips`/`execution_tp_floor_applied`;
- `midline`-exit теперь вообще не вооружается, если до средней линии слишком мало хода и цель не перекрывает минимальный TP-floor стратегии;
- `SL/TP` можно сделать ATR-адаптивными через `mean_reversion_bb_use_atr_sl_tp=true`, TP по умолчанию в режиме `rr`.

Тюнинг `index_hybrid` для IG/XTB (консервативный intraday-профиль):

```bash
XTB_STRATEGY_PARAMS_INDEX_HYBRID={
  "index_parameter_surface_mode":"tiered",
  "index_parameter_surface_tier":"conservative",
  "index_fast_ema_window":21,
  "index_slow_ema_window":89,
  "index_donchian_window":14,
  "index_zscore_window":48,
  "index_zscore_mode":"detrended",
  "index_trend_gap_threshold":0.00040,
  "index_mean_reversion_gap_threshold":0.00016,
  "index_trend_volatility_explosion_min_ratio":1.20,
  "index_mean_reversion_confidence_base":0.10,
  "index_hybrid_execution_min_confidence_for_entry":0.70,
  "index_hybrid_mean_reversion_execution_confidence_hard_floor":0.55
}
```

Почему этот профиль полезен:
- live `.env` теперь держит только real execution core, а tiered profile закрывает остальной shape в коде;
- `zscore_mode` остаётся явным и больше не скрывается за runtime auto-fix;
- regime gap thresholds остаются основными live knobs, а secondary session/volume/ATR shape больше не нужно тащить в каждый env;
- отдельный `index_hybrid_mean_reversion_execution_confidence_hard_floor` сохраняет жёсткий execution floor для mean-reversion даже при более мягком signal layer.

Важно:
- `index_session_profile_mode="market_presets"` использует локальные часы рынков, а `index_trend_session_start_hour_utc` / `end_hour_utc` остаются только legacy fallback для режима `legacy_utc`;
- это уже более защитный intraday пресет, но он всё равно чувствителен к коррелированным движениям индексов;
- для live имеет смысл начинать с `XTB_MAX_RISK_PER_TRADE_PCT <= 0.75` и `XTB_MAX_OPEN_POSITIONS <= 4`.
- для оптимизации не гоняйте весь старый набор `index_*` параметров сразу: live surface теперь намеренно compact, а research knobs не должны жить в production `.env`.
- если нужен полный research surface, явно ставьте `index_parameter_surface_mode="open"`; production default теперь специально этому мешает.
- если всё же хотите runtime auto-fix misconfigured thresholds, включайте `index_auto_correct_regime_thresholds` и `index_enforce_gap_hysteresis` явно; production default больше не делает это тихо.

`index_window_sync_mode`:
- `auto` — автоматически синхронизирует `index_zscore_window` с целевым отношением к `index_donchian_window`.
- `warn` — не меняет окно, только ставит статус рассинхронизации в metadata.
- `off` — выключает синхронизацию окон; для intraday index mean-reversion это production default, чтобы держать длинный `zscore_window`, не привязанный к короткому Donchian breakout horizon.

Тюнинг `g2` (intraday index pullback с low-latency `auto` baseline):

```bash
XTB_STRATEGY_PARAMS_G2={
  "g2_candle_timeframe_sec":60,
  "g2_resample_mode":"auto",
  "g2_ignore_sunday_candles":true,
  "g2_ema_fast":10,
  "g2_ema_trend":50,
  "g2_ema_macro":200,
  "g2_rsi_window":14,
  "g2_rsi_oversold":35.0,
  "g2_rsi_overbought":65.0,
  "g2_atr_window":14,
  "g2_atr_sl_multiplier":2.4,
  "g2_risk_reward_ratio":2.2,
  "g2_min_stop_loss_pips":35.0,
  "g2_min_take_profit_pips":75.0,
  "g2_allow_shorts":false,
  "g2_min_trend_gap_ratio":0.00045,
  "g2_min_trend_slope_ratio":0.00008,
  "g2_min_macro_slope_ratio":0.00005,
  "g2_min_atr_pct":0.00045,
  "g2_min_pullback_depth_atr":0.12,
  "g2_max_pullback_depth_atr":0.65,
  "g2_max_trend_ema_breach_atr":0.35,
  "g2_max_close_to_fast_ema_distance_atr":0.25,
  "g2_reclaim_buffer_atr":0.18,
  "g2_recovery_min_close_location":0.55,
  "g2_max_spread_pips":0.0,
  "g2_max_spread_pips_by_symbol":{"NK20":3.0},
  "g2_max_spread_to_stop_ratio":0.25,
  "g2_vwap_filter_enabled":true,
  "g2_vwap_reclaim_required":false,
  "g2_vwap_min_session_bars":8,
  "g2_vwap_min_volume_samples":8,
  "g2_session_filter_enabled":false,
  "g2_session_open_delay_minutes":0,
  "g2_america_session_timezone":"America/New_York",
  "g2_america_session_start_local":"09:30",
  "g2_america_session_end_local":"16:00",
  "g2_europe_session_timezone":"Europe/London",
  "g2_europe_session_start_local":"08:00",
  "g2_europe_session_end_local":"17:00",
  "g2_japan_session_timezone":"Asia/Tokyo",
  "g2_japan_session_start_local":"09:00",
  "g2_japan_session_end_local":"15:00",
  "g2_australia_session_timezone":"Australia/Sydney",
  "g2_australia_session_start_local":"10:00",
  "g2_australia_session_end_local":"16:00",
  "g2_confidence_base":0.50,
  "g2_confidence_trend_gap_weight":0.14,
  "g2_confidence_slope_weight":0.10,
  "g2_confidence_pullback_depth_weight":0.08,
  "g2_confidence_recovery_weight":0.10,
  "g2_confidence_rsi_recovery_weight":0.08,
  "g2_confidence_vwap_reclaim_weight":0.06,
  "g2_confidence_max":0.90,
  "g2_confidence_threshold_cap":0.82,
  "g2_signal_only_min_confidence_for_entry":0.58,
  "g2_paper_min_confidence_for_entry":0.62,
  "g2_execution_min_confidence_for_entry":0.68
}
```

Практический смысл baseline:
- `g2` больше не зажат в `>=5m` candles: стратегия принимает и по умолчанию использует `60s` closed candles, чтобы не опаздывать на 2-3 минуты к уже состоявшемуся reversal.
- `g2_max_pullback_depth_atr=0.65` режет поздние “почти полный откат” входы, где pullback уже слишком похож на настоящий разворот тренда.
- `g2_reclaim_buffer_atr=0.18` даёт recovery закрыться обратно над/под `EMA fast` с нормальным запасом, а не требовать почти идеальное касание.
- `g2_session_filter_enabled=false` оставляет pre/post-market reversal окна доступными; если нужен только cash-session режим, включайте фильтр явно через пресет.

`g2` проектировалась как отдельный industrial pullback-engine, а не как ещё одна M1 momentum-стратегия:
- торгует только индексы; на candle-like входе использует закрытые бары, а на snapshot/tick-потоке `auto` больше не добавляет искусственную задержку через скрытый closed-candle resample;
- требует зрелый тренд (`EMA 50 > EMA 200` или обратный вариант для short), нормальный gap между средними и положительный slope, а не просто один импульсный бар;
- ищет именно завершение отката: вход появляется только когда цена вернулась к fast EMA/зоне тренда и закрылась с подтверждением recovery;
- использует ATR-структурный стоп и spread-to-stop gate, чтобы не заходить в ситуации, где спред съедает геометрию сделки;
- session-filter привязан к локальным часам биржи конкретного индексного family, поэтому `US100`, `DE40`, `TOPIX` и `AUS200` живут по своим окнам, а не по единому UTC-костылю.
- session `VWAP` используется как bias/filter, а не как основной trigger: хороший long pullback не должен оставаться structurally ниже intraday fair value без reclaim.
- если `g2_vwap_reclaim_required=true`, стратегия дополнительно ждёт возврат цены обратно над VWAP для long и под VWAP для short; при подтверждённом reclaim можно дать небольшой confidence boost через `g2_confidence_vwap_reclaim_weight`.

Практический смысл `g2`:
- она не должна конкурировать с `g1` за быстрый cross и не должна конкурировать с `index_hybrid` за внутриминутный regime-switch;
- лучший use-case — зрелый тренд на `M15/H1`, где нужен осмысленный buy-the-dip вместо chase входа;
- если включаете `g2` в multi-strategy orchestration, держите её как отдельный index pullback owner, а не как broad-market default.

Тюнинг `g1` (anti-whipsaw + candle confirmation + cooldown):

```bash
XTB_STRATEGY_PARAMS_G1={
  "g1_trade_cooldown_sec":1800,
  "g1_candle_timeframe_sec":60,
  "g1_candle_confirm_bars":1,
  "g1_use_incomplete_candle_for_entry":true,
  "g1_ignore_sunday_candles":true,
  "g1_entry_mode":"cross_only",
  "g1_entry_mode_by_symbol":{},
  "g1_min_trend_gap_ratio":0.00015,
  "g1_min_cross_gap_ratio":0.00010,
  "g1_continuation_fast_ema_retest_atr_tolerance":0.20,
  "g1_fx_continuation_fast_ema_retest_atr_tolerance":0.18,
  "g1_index_continuation_fast_ema_retest_atr_tolerance":0.16,
  "g1_commodity_continuation_fast_ema_retest_atr_tolerance":0.14,
  "g1_continuation_adx_multiplier":0.90,
  "g1_continuation_min_adx":22.0,
  "g1_continuation_min_entry_threshold_ratio":0.90,
  "g1_cross_adx_multiplier":0.85,
  "g1_cross_min_adx":22.0,
  "g1_cross_min_entry_threshold_ratio":0.82,
  "g1_adx_warmup_multiplier":2.0,
  "g1_adx_warmup_extra_bars":2,
  "g1_adx_warmup_cap_bars":48,
  "g1_use_adx_hysteresis_state":true,
  "g1_index_require_context_tick_size":false,
  "g1_resample_mode":"auto",
  "g1_debug_indicators":false,
  "g1_debug_indicators_interval_sec":0.0,
  "g1_adx_hysteresis":1.0,
  "g1_min_slow_slope_ratio":0.00003,
  "g1_max_price_ema_gap_ratio":0.006,
  "g1_max_price_ema_gap_atr_multiple":0.8,
  "g1_fx_max_price_ema_gap_ratio":0.005,
  "g1_fx_max_price_ema_gap_atr_multiple":0.8,
  "g1_index_max_price_ema_gap_ratio":0.012,
  "g1_index_max_price_ema_gap_atr_multiple":0.9,
  "g1_commodity_max_price_ema_gap_ratio":0.010,
  "g1_commodity_max_price_ema_gap_atr_multiple":0.9,
  "g1_index_low_vol_atr_pct_threshold":0.1,
  "g1_index_low_vol_multiplier":1.2,
  "g1_min_relative_stop_pct":0.0008,
  "g1_volume_confirmation":true,
  "g1_volume_window":20,
  "g1_min_volume_ratio":1.5,
  "g1_volume_min_ratio_for_entry":1.0,
  "g1_volume_min_samples":8,
  "g1_volume_allow_missing":false,
  "g1_volume_require_spike":true,
  "g1_volume_confidence_boost":0.08,
  "g1_confidence_velocity_norm_ratio":0.0005,
  "g1_confidence_base":0.10,
  "g1_confidence_adx_weight":0.35,
  "g1_confidence_gap_weight":0.15,
  "g1_confidence_velocity_weight":0.45,
  "g1_cross_confidence_cap":1.0,
  "g1_continuation_confidence_cap":0.72,
  "g1_continuation_velocity_weight_multiplier":0.65,
  "g1_continuation_price_alignment_bonus_multiplier":0.0,
  "g1_kama_gate_enabled":true,
  "g1_kama_er_window":10,
  "g1_kama_fast_window":2,
  "g1_kama_slow_window":30,
  "g1_kama_min_efficiency_ratio":0.12,
  "g1_kama_min_slope_atr_ratio":0.06,
  "g1_fx_atr_multiplier":1.8,
  "g1_fx_min_stop_loss_pips":10.0,
  "g1_index_atr_multiplier":2.2,
  "g1_index_min_stop_loss_pips":30.0,
  "g1_commodity_atr_multiplier":2.6,
  "g1_commodity_min_stop_loss_pips":30.0,
  "g1_commodity_max_spread_pips":5.0,
  "g1_index_max_spread_pips_by_symbol":{"NK20":3.0},
  "g1_commodity_adx_threshold":24.0,
  "g1_commodity_volume_confirmation":false,
  "g1_signal_only_min_confidence_for_entry":0.60,
  "g1_paper_min_confidence_for_entry":0.64,
  "g1_execution_min_confidence_for_entry":0.68
}
```

`g1` теперь cross-first и continuation opt-in:
- `g1_entry_mode="cross_only"` в live baseline делает `g1` стратегией свежего пересечения, а не универсальным M1 continuation-engine.
- continuation возвращайте только явным whitelist через `g1_entry_mode_by_symbol`, если по конкретному символу уже доказан edge.
- для broad-market оркестрации не пытайтесь лечить `g1` только фильтрами: ownership на индексах должен уходить к `index_hybrid` и `trend_following`, а `g1` должна оставаться cross-first компонентом.
- Практический смысл: по умолчанию `g1` перестаёт догонять late `ema_trend_up/down`, а оставляет continuation-путь более контекстным стратегиям.

`g1` continuation, если он явно разрешён, всё ещё reset-aware:
- `g1_continuation_fast_ema_retest_atr_tolerance` задаёт, насколько близко continuation-свеча должна вернуться к fast EMA в долях ATR.
- profile-specific ключи `g1_fx_*`, `g1_index_*`, `g1_commodity_*` позволяют держать более жёсткий reset на индексах и сырье, не ломая FX.
- Практический смысл: `ema_trend_up/down` больше не покупает/шортит straight-line extension, а ждёт локального охлаждения к fast EMA при сохранённом slow-trend.
- `g1_continuation_confidence_cap`, `g1_continuation_velocity_weight_multiplier` и `g1_continuation_price_alignment_bonus_multiplier` дополнительно режут переоценку continuation: velocity и alignment остаются важны для fresh cross, но больше не раздувают continuation до уровня “нового импульса”.
- `g1_kama_gate_*` добавляет общий anti-chop gate: если рынок статистически шумовой, а `KAMA` почти не имеет наклона относительно ATR, `g1` не открывает ни cross, ни continuation и возвращает `HOLD` с причиной `kama_chop_regime`.

Для `g1` `g1_entry_mode_by_symbol` задаёт symbol-specific режим входа поверх глобального `g1_entry_mode`. В текущем live baseline continuation для `g1` выключен глобально; этот override теперь нужен именно как whitelist для точечного возврата `cross_or_trend`, а не как список ad-hoc исключений.

Профили `g1`:
- `fx` — валюты.
- `index` — индексы.
- `commodity` — сырьё/металлы/энергоносители/агро (`GOLD`, `WTI`, `BRENT`, `CARBON`, `UKNATGAS`, `GASOLINE`, `WHEAT`, `COCOA`, `CORN`, `SOYBEANOIL`, `OATS`, `XAU*`, `XAG*` и т.п.).

`g1_profile_override` поддерживает: `auto`, `fx`, `index`, `commodity`.

Практичный стартовый набор `min_confidence_for_entry` для более защитного live-профиля:
- `momentum`: `signal_only=0.58`, `paper=0.62`, `execution=0.66`
- `g1`: `signal_only=0.58`, `paper=0.62`, `execution=0.66`
- `g2`: `signal_only=0.58`, `paper=0.62`, `execution=0.68`
- `index_hybrid`: `signal_only=0.60`, `paper=0.64`, `execution=0.70`
- `crypto_trend_following`: `signal_only=0.60`, `paper=0.64`, `execution=0.68`
- для остальных стратегий фильтр можно поднимать постепенно, если увидите избыточный шум во входах.

Рекомендуемый live-базис для глобального risk/execution слоя:

```bash
XTB_MAX_RISK_PER_TRADE_PCT=0.75
XTB_DRAWDOWN_RISK_THROTTLE_ENABLED=true
XTB_DRAWDOWN_RISK_THROTTLE_DAILY_START_RATIO=0.50
XTB_DRAWDOWN_RISK_THROTTLE_TOTAL_START_RATIO=0.50
XTB_DRAWDOWN_RISK_THROTTLE_MIN_MULTIPLIER=0.50
XTB_MAX_OPEN_POSITIONS=4
XTB_NEWS_EVENT_BUFFER_MIN=10
XTB_NEWS_EVENT_ACTION=close
XTB_SPREAD_MAX_PCT=0.02
XTB_SPREAD_RISK_WEIGHT=0.75
XTB_MARGIN_MIN_LEVEL_PCT=200
XTB_ENTRY_TICK_MAX_AGE_SEC=10
```

`XTB_DRAWDOWN_RISK_THROTTLE_*`:
- soft-lock для risk budget: бот не ждёт полного daily lock, а начинает линейно сжимать `max_risk_per_trade_pct` после достижения заданной доли daily/total drawdown.
- practical baseline: старт throttle с `50%` лимита и floor `0.50`, то есть перед hard-lock риск на сделку уже вдвое меньше.

`XTB_SPREAD_RISK_WEIGHT`:
- вес спреда внутри volume sizing. `1.0` означает полностью консервативный расчёт `SL + spread`, `0.0` полностью игнорирует спред в sizing.
- для CFD/crypto обычно разумнее держать `0.5 .. 0.9`, чем жёстко прибивать всё к `1.0`.

`XTB_MARGIN_MIN_LEVEL_PCT`:
- отдельный hard gate по margin level (`equity / used_margin * 100`), который проверяется и до открытия сделки, и для projected state после открытия.
- если хотите полностью отключить эту защиту, ставьте `0`.

Приоритет ключей для confidence-порога:
- `<strategy>_<mode>_min_confidence_for_entry`
- `<mode>_min_confidence_for_entry`
- `<strategy>_min_confidence_for_entry`
- `min_confidence_for_entry`

Принудительно включить/выключить IG stream из CLI:

```bash
xtb-bot --ig-stream-enabled
xtb-bot --no-ig-stream-enabled
```

Анти-лимит настройки для IG (уменьшить burst REST-запросов):

```bash
# использовать официальный Lightstreamer Python SDK для IG stream
IG_STREAM_SDK_ENABLED=true

# держать stream-тик дольше перед fallback на REST
IG_STREAM_TICK_MAX_AGE_SEC=15

# глобально разнести /markets-запросы
IG_REST_MARKET_MIN_INTERVAL_SEC=1.0

# десинхронизировать циклы воркеров по символам
XTB_WORKER_POLL_JITTER_SEC=1.0

# для 30+ символов держать SQLite busy-timeout выше, чтобы сгладить write-lock contention
XTB_DB_SQLITE_TIMEOUT_SEC=60

# шарить connectivity probe (/session) между воркерами
IG_CONNECTIVITY_STATUS_CACHE_TTL_SEC=10
IG_CONNECTIVITY_STATUS_UNHEALTHY_CACHE_TTL_SEC=2

# локальный бюджет bot-level для non-trading IG REST (DB-first + runtime sync)
XTB_IG_NON_TRADING_BUDGET_ENABLED=true
XTB_IG_NON_TRADING_BUDGET_RPM=24
# аварийный резерв на account/spec/runtime sync (рекомендуемый диапазон: 10-15)
XTB_IG_NON_TRADING_BUDGET_RESERVE_RPM=12
XTB_IG_NON_TRADING_BUDGET_BURST=4
XTB_IG_ACCOUNT_NON_TRADING_WARN_AT=24

# не синкать managed positions слишком часто при активных позициях
XTB_RUNTIME_BROKER_SYNC_ACTIVE_INTERVAL_SEC=8

# добирать factual close details у уже закрытых сделок без ожидания рестарта
XTB_RUNTIME_CLOSED_DETAILS_BACKFILL_INTERVAL_SEC=60
```

Опциональный proxy-слой поверх `IgApiClient` (дополнительный кеш read-only + token-bucket):

```bash
# По умолчанию выключен, чтобы не менять поведение существующих инстансов.
XTB_IG_PROXY_ENABLED=false

# Фоновый pre-refresh кешей account/news (обычно можно оставить false).
XTB_IG_PROXY_POLLERS_ENABLED=false

# Локальные бюджеты proxy (requests/min + burst)
XTB_IG_PROXY_ACCOUNT_NON_TRADING_PER_MIN=25
XTB_IG_PROXY_ACCOUNT_NON_TRADING_BURST=5
XTB_IG_PROXY_ACCOUNT_TRADING_PER_MIN=100
XTB_IG_PROXY_ACCOUNT_TRADING_BURST=10
```

DB-first стартовый prime (по умолчанию выключен, чтобы не создавать всплеск REST на старте):

```bash
# По умолчанию false: старт без массового symbol/tick prime в IG
XTB_DB_FIRST_PRIME_ENABLED=false

# Если включили prime — ограничить число символов (0 = без лимита)
XTB_DB_FIRST_PRIME_MAX_SYMBOLS=2

# Добавлять account snapshot в prime (по умолчанию false)
XTB_DB_FIRST_PRIME_ACCOUNT_ENABLED=false

# Ограничить частоту DB-first /markets запросов (запросов в минуту, default=24)
# Полезно против error.public-api.exceeded-api-key-allowance
XTB_DB_FIRST_TICK_TARGET_RPM=24

# Интервал между REST-запросами symbol spec в DB-first (по умолчанию 6s)
XTB_DB_FIRST_SYMBOL_SPEC_POLL_INTERVAL_SEC=6

# Возраст symbol spec в кеше, при котором DB-first воркер обновляет его из IG
XTB_DB_FIRST_SYMBOL_SPEC_REFRESH_AGE_SEC=900

# Retention для журнала обновлений открытых позиций (по ответам IG), default=7 дней
XTB_DB_POSITION_UPDATES_RETENTION_SEC=604800

# Жёсткий максимум возраста тиков в broker_ticks (старше удаляются из БД)
XTB_DB_FIRST_TICK_HARD_MAX_AGE_SEC=10

# Интервал очистки устаревших тиков из broker_ticks
XTB_DB_FIRST_TICK_PRUNE_INTERVAL_SEC=1
```

SQLite write-pipeline для снижения lock contention при большом количестве символов:

```bash
# По умолчанию async writer выключен: надёжность SQLite важнее микровыигрыша по lock contention.
XTB_MULTI_DB_ASYNC_WRITER_ENABLED=false
XTB_MULTI_DB_ASYNC_FLUSH_MS=200
XTB_MULTI_DB_ASYNC_BATCH_SIZE=1024

# То же самое для events: при high-frequency runtime лучше синхронная запись, чем риск битых индексов.
XTB_DB_EVENTS_ASYNC_WRITER_ENABLED=false
XTB_DB_EVENTS_ASYNC_FLUSH_MS=300
XTB_DB_EVENTS_ASYNC_BATCH_SIZE=512
```

Контроль remap EPIC (чтобы не переключаться с `CASH` на `DAILY/IFS`):

```bash
# По умолчанию true (авто-failover между candidate EPIC)
IG_EPIC_FAILOVER_ENABLED=true

# Если нужен строго один EPIC на символ без автопереключения:
IG_EPIC_FAILOVER_ENABLED=false

# Пин конкретных EPIC вручную:
IG_SYMBOL_EPICS={"US500":"IX.D.SPTRD.CASH.IP","DE40":"IX.D.DAX.CASH.IP","US30":"IX.D.DOW.CASH.IP","UK100":"IX.D.FTSE.CASH.IP","GOLD":"CS.D.GOLD.CFD.IP"}
```

Строгий старт без fallback в mock при ошибке подключения:

```bash
xtb-bot --strict-broker-connect
```

Показать последние алерты из SQLite:

```bash
xtb-bot --show-alerts --alerts-limit 20
```

Показать текущий баланс/equity из последнего account snapshot:

```bash
xtb-bot --show-balance
```

Показать последние сделки из SQLite:

```bash
# Последние 20 сделок (open + closed)
xtb-bot --show-trades

# Последние 50 сделок
xtb-bot --show-trades --trades-limit 50

# Только открытые сделки
xtb-bot --show-trades --trades-open-only
```

Детальный таймлайн последних закрытых сделок (с MFE/MAE, событиями и broker position updates):

```bash
# Последние 3 закрытые сделки
xtb-bot --show-trade-timeline

# Последние 5 сделок, расширенный лимит событий на сделку
xtb-bot --show-trade-timeline --trade-timeline-limit 5 --trade-timeline-events-limit 200
```

Выгрузить жизненный цикл сделок из таблицы `position_updates` (все IG-ответы по операциям `/positions*` и `/confirms*`, формат JSONL):

```bash
# Все записи
xtb-bot --export-position-updates

# Ограничить последними N записями
xtb-bot --export-position-updates --position-updates-limit 500
```

Показать агрегированный отчёт по причинам (закрытия/блокировки/hold):

```bash
# Топ причин из последних 1000 событий
xtb-bot --show-trade-reasons

# Увеличить окно анализа и ограничить вывод топ-10 групп
xtb-bot --show-trade-reasons --trade-reasons-window 5000 --trade-reasons-limit 10
```

Показать диагностику лимитов IG API (allowance/cooldown + stream usage):

```bash
# Сводка по последним 1000 событиям
xtb-bot --show-ig-allowance

# Расширить окно анализа и вывести топ-15 символов
xtb-bot --show-ig-allowance --ig-allowance-window 5000 --ig-allowance-symbols 15
```

Показать здоровье DB-first кеша по символам (tick age + worker state + последняя причина деградации):

```bash
# Сводка по последним 2000 событиям
xtb-bot --show-db-first-health

# Увеличить окно анализа и ограничить вывод топ-10 символов
xtb-bot --show-db-first-health --db-first-health-window 5000 --db-first-health-symbols 10
```

Сбросить якорь total drawdown (`risk.start_equity`) в SQLite:

```bash
# Взять последнее equity из account_snapshots
xtb-bot --reset-risk-anchor

# Задать значение вручную
xtb-bot --reset-risk-anchor --risk-anchor-value 8430
```

Очистить несовместимые открытые сделки в `execution` state (например `paper-*` после миграции/смешанного режима):

```bash
# Предпросмотр, без изменений в БД
xtb-bot --cleanup-incompatible-open-trades --cleanup-dry-run

# Архивировать найденные записи (перевести в closed)
xtb-bot --cleanup-incompatible-open-trades
```

## Тесты

Установка dev-зависимостей:

```bash
python -m pip install -e '.[dev]'
```

Запуск тестов:

```bash
pytest -q
```

## Хранилище состояния

По умолчанию: `./state/xtb_bot.db`

Таблицы:
- `worker_states` — heartbeat и состояние каждого потока
- `trades` — жизненный цикл каждой сделки
- `trade_performance` — экстремумы сделки (MFE/MAE в pips и pnl, `close_reason`) для пост-аналитики
- `position_updates` — детальный lifecycle-аудит по операциям IG для позиций (`open/modify/close/confirm/snapshot`, успешные и ошибочные ответы), хранится максимум 7 дней
- `events` — журнал событий/ошибок
- `account_snapshots` — снимки аккаунта и drawdown
- `kv` — служебные якоря риск-менеджера

## Важно

- Перед использованием `execution` проверьте параметры риска на demo.
- Размер позиции рассчитывается по `tickSize/tickValue/lotMin/lotMax/lotStep`, полученным из спецификации инструмента брокера.
- Для XTB клиент использует stream-кэш с fallback на command API; для IG используется Lightstreamer stream-кэш с fallback на REST.
- Для IG в `events` пишутся метрики использования потока: `Stream usage metrics` (`price_requests_total`, `stream_hits_total`, `rest_fallback_hits_total`, `stream_hit_rate_pct`).
- Для IG в событии `Loaded symbol specification` пишутся диагностические поля (`onePipMeans`, `scalingFactor`, `valueOfOnePip/valuePerPip`, `valueOfOnePoint/valuePerPoint`) и источники расчета `tick_size/tick_value`.
- Для `execution` с IG при закрытии позиции бот синхронизирует `close_price/pnl` из broker confirm (`/confirms`) и, при необходимости, из `/history/transactions` (fallback на случай `position missing` после серверного SL/TP).
- `--show-status` дополнительно печатает агрегаты по `trade_performance`: средние MFE/MAE и capture ratio по закрытым сделкам, плюс последнюю закрытую сделку с reason.
- По умолчанию фикс-лот отключен (`default_volume=0`), размер лота берется из риск-менеджера.
- Если нужен верхний лимит лота, можно задать `default_volume > 0` (это cap, а не фикс-лот логики входа).
- При срабатывании `max_daily_drawdown_pct` (по умолчанию `5%`) бот ставит дневной lock и не открывает новые сделки до смены даты.
- При активации дневного lock создается отдельный alert в `events` с причиной и `unlock_at` (начало следующего дня), плюс warning в логах.
- Лимит `max_total_drawdown_pct` считается от якоря `risk.start_equity` (хранится в таблице `kv`).
- Если после длительного paper-теста нужно продолжить торговлю от текущего баланса, используйте `xtb-bot --reset-risk-anchor`.
- Для CFD default лимит одновременных позиций = `5` (рекомендуемо `3..5`), жесткий верхний порог валидации конфига = `15`.
- При блокировке входа из-за `max_open_positions` создается `WARN`-alert в `events`.
- Выходы из позиции работают по независимым триггерам: `stop_loss`, `take_profit`, `reverse_signal` (обратный сигнал текущей стратегии).
- Для `g1` по умолчанию включен anti-whipsaw cooldown после закрытия сделки: `g1_trade_cooldown_sec=1800` (30 минут).
- В `g1` добавлены фильтры качества входа: гистерезис ADX (`g1_adx_hysteresis`), ADX-relief только для реально ускоряющегося fresh cross (`g1_cross_*` + `gap_acceleration_ratio`), минимальный наклон slow EMA (`g1_min_slow_slope_ratio`), ограничение удаленности цены от slow EMA c ATR-адаптацией (`g1_max_price_ema_gap_ratio`, `g1_max_price_ema_gap_atr_multiple`), минимальный разрыв EMA на cross (`g1_min_cross_gap_ratio`) и low-latency candle path (`g1_use_incomplete_candle_for_entry=true` по умолчанию поверх `g1_resample_mode=auto`).
- Для `g1` доступно подтверждение входа по объему (`g1_volume_*`): при spike confidence повышается на `g1_volume_confidence_boost`; при `g1_volume_require_spike=true` без spike вход блокируется (`reason=volume_not_confirmed`).
- Для `g1` можно исключать воскресные свечи (актуально для IG): `g1_ignore_sunday_candles=true`.
- Для `g1` есть прогрев ADX (`g1_adx_warmup_multiplier`, `g1_adx_warmup_extra_bars`, optional `g1_adx_warmup_cap_bars`) и гистерезис фильтра ADX (`g1_use_adx_hysteresis_state`), при этом состояние режима теперь поднимается из недавней ADX-истории и не зависит только от памяти процесса после рестарта.
- Веса confidence у `g1` вынесены в параметры (`g1_confidence_base`, `g1_confidence_adx_weight`, `g1_confidence_gap_weight`, `g1_confidence_velocity_weight`) вместо жёстко зашитых констант.
- Для `g1` есть режим ресемплинга `g1_resample_mode`: `auto` (по умолчанию), `always`, `off`.
- Для отладки индикаторов есть флаг `debug_indicators` (или `g1_debug_indicators`): бот пишет `Strategy indicator snapshot` на каждом цикле; частоту можно ограничить через `debug_indicators_interval_sec` / `g1_debug_indicators_interval_sec`.
- Поддерживается bot-side trailing stop: после выхода позиции в плюс SL переносится в безубыток и дальше подтягивается на `trailing_distance_pips`; `trailing_activation_ratio` (default `0.0`) позволяет задержать старт трейлинга по прогрессу к TP.
- Offset для безубытка можно задавать по профилям: `trailing_breakeven_offset_pips_fx`, `trailing_breakeven_offset_pips_index`, `trailing_breakeven_offset_pips_commodity`; `trailing_breakeven_offset_pips` остается общим fallback.
- Глобальный adaptive ATR overlay (`adaptive_trailing_enabled` / `XTB_ADAPTIVE_TRAILING_ENABLED`) теперь включен по умолчанию: worker берет `atr_pips` и extension metadata из текущего strategy signal, сжимает trail по мере роста move-extremity и при высокой heat фиксирует часть исходного `R` через `adaptive_trailing_profit_lock_r_at_full`.
- Если стратегия не прислала собственный trailing override, worker все равно выводит `adaptive_trailing_family` из `strategy_entry_component` позиции и использует исходный broker stop как базовый `R`, так что adaptive trailing работает не только для `mean_breakout_v2`, но и для `momentum`, `g1`, `g2`, `mean_reversion_bb`, `index_hybrid` и остальных execution-стратегий.
- Для тонкой настройки adaptive overlay доступны `adaptive_trailing_atr_base_multiplier`, `adaptive_trailing_heat_trigger`, `adaptive_trailing_heat_full`, `adaptive_trailing_distance_factor_at_full`, `adaptive_trailing_profit_lock_min_progress`, `adaptive_trailing_min_distance_stop_ratio`, `adaptive_trailing_min_distance_spread_multiplier`.
- Каждое подтягивание трейлинга пишет `WARN`-alert в `events` (`message=Trailing stop adjusted`).
- В `execution` trailing-modify выполняется через modify-эндпоинт брокера с подтверждением статуса.
- Для индексов (например, `US100`, `DE40`) бот закрывает позиции за `session_close_buffer_min` минут до close торговой сессии (защита от гэпов).
- Новостной фильтр проверяет high-impact события брокера (если доступны): за `news_event_buffer_min` минут до события бот блокирует новые входы; для уже открытой позиции либо закрывает её (`news_event_action=close`), либо переводит в безубыток (`breakeven`).
- Перед входом бот проверяет текущий спред (`ask-bid`) в пипсах; если он выше `средний_спред * spread_anomaly_multiplier` (default `3x`), вход блокируется и пишется `WARN`-alert.
- Перед входом бот делает connectivity-check к API брокера; при latency > `500ms` или проблемах соединения новые сделки не открываются.
- Для stream socket включен health-check с auto-reconnect и exponential backoff; деградации/восстановления пишутся в `events` (`Stream health degraded/recovered`), а новые входы блокируются при нездоровом стриме.
- Глобальный порог “свежести” тика для входа задается через `XTB_ENTRY_TICK_MAX_AGE_SEC` (или `risk.entry_tick_max_age_sec`): применяется ко всем стратегиям; `0` = auto-режим. Для IG/M1 лучше ставить жесткий лимит около `1.5..3.0`, чтобы не открываться по протухшему тику. При необходимости можно переопределить на стратегию через `*_entry_tick_max_age_sec`.
- Для маркировки ордеров используйте `bot_magic_prefix`; `bot_magic_instance` можно задать вручную или оставить авто-генерацию (сохранится в `kv`).
- Если брокер недоступен в `paper` или `signal_only`, бот автоматически переключится на mock-цены для непрерывного теста логики.
- Чтобы запретить fallback в mock, включите `BOT_STRICT_BROKER_CONNECT=true` (или CLI `--strict-broker-connect`).
