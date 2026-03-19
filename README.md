# IG Trading Bot (Python)

Многопоточный бот для IG.com (с совместимостью XTB) с изоляцией по инструментам, риск-контролем и персистентным состоянием.

## Что реализовано

- Работа с основными инструментами:
  - По умолчанию: `EURUSD, GBPUSD, USDJPY, US100, XAUUSD, WTI`
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
  - `donchian_breakout` (breakout-only по каналу Дончиана)
  - `index_hybrid` (гибрид для индексов: trend-following + mean-reversion + dynamic exit по каналу; неиндексные символы автоматически пропускаются)
  - `mean_breakout` (z-score + breakout уровней: подтвержденный пробой после отклонения от среднего)
  - `mean_breakout_v2` (EMA-based z-score + Donchian breakout + slope filter + exit hints)
  - `mean_breakout_session` (пробой opening range: коробка первых 15-30 минут сессии)
  - `mean_reversion_bb` (Bollinger Bands mean-reversion, опционально с RSI-фильтром)
  - `momentum`
  - `momentum_index` (alias `momentum` для отдельного пресета индексов в одном процессе)
  - `momentum_fx` (alias `momentum` для отдельного пресета FX в одном процессе)
  - `mean_reversion`
  - `trend_following` (EMA 20/80 + trend-filter по Donchian breakout и вход по pullback к EMA/середине канала)
  - `crypto_trend_following` (наследник `trend_following` с crypto-ориентированными дефолтами; поддерживает только crypto-symbols)
- Рекомендуемое соответствие стратегий и классов инструментов:
  - `index_hybrid` -> только индексы (`US100`, `US500`, `DE40`, `UK100`, `US30`, `AUS200`)
  - `crypto_trend_following` -> только crypto (`BTC`, `ETH`, `LTC`, `SOL`, `XRP`, `DOGE`)
  - `momentum` -> индексы/коммодити (`US*`, `DE40`, `WTI`, `BRENT`, `GOLD`)
  - `g1` -> FX + индексы (при необходимости можно добавить коммодити)
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
xtb-bot --mode execution --strategy mean_reversion
```

Запуск `g1`:

```bash
xtb-bot --strategy g1
```

`index_hybrid`: где торговать и какие символы ставить

- Стратегия рассчитана только на индексы.
- Рекомендуемые базовые 4 символа:
  - `US500` (S&P 500)
  - `US100`
  - `DE40`
  - `UK100`
- Неиндексные символы (`EURUSD`, `GBPUSD`, `USDJPY`, `WTI`, `XAUUSD`) будут автоматически пропущены для этой стратегии.

Пример `.env` для `index_hybrid`:

```bash
# Новый strategy-scoped формат (предпочтительно)
IG_SYMBOLS_INDEX_HYBRID=US500,US100,DE40,UK100
XTB_SYMBOLS_INDEX_HYBRID=US500,US100,DE40,UK100

# Либо общий список символов (если не используете strategy-specific ключи)
IG_SYMBOLS=US500,US100,DE40,UK100
XTB_SYMBOLS=US500,US100,DE40,UK100

# Legacy-ключи (оставлены для совместимости):
IG_INDEX_HYBRID_SYMBOLS=US500,US100,DE40,UK100
XTB_INDEX_HYBRID_SYMBOLS=US500,US100,DE40,UK100
```

Общий формат strategy-scoped символов:

- `IG_SYMBOLS_<STRATEGY>` / `XTB_SYMBOLS_<STRATEGY>`, где `<STRATEGY>` — верхний регистр с `_`, например:
  - `DONCHIAN_BREAKOUT`
  - `G1`
  - `INDEX_HYBRID`
  - `MEAN_BREAKOUT`
  - `MEAN_BREAKOUT_SESSION`
  - `MEAN_BREAKOUT_V2`
  - `MEAN_REVERSION`
  - `MEAN_REVERSION_BB`
  - `MOMENTUM`
  - `TREND_FOLLOWING`
- Приоритет выбора symbols: `*_SYMBOLS_<ACTIVE_STRATEGY>` -> `*_INDEX_HYBRID_SYMBOLS` (legacy только для index_hybrid) -> `*_SYMBOLS` -> defaults.

Пример наборов тикеров по стратегиям:

```bash
IG_SYMBOLS_MOMENTUM=US100,US500,DE40,US30,GOLD,WTI,BRENT
IG_SYMBOLS_MOMENTUM_INDEX=US100,US500,DE40,US30
IG_SYMBOLS_MOMENTUM_FX=EURUSD,GBPUSD,USDJPY
IG_SYMBOLS_MEAN_BREAKOUT_V2=US100,WTI,USDJPY,DE40,GOLD
IG_SYMBOLS_MEAN_BREAKOUT_SESSION=DE40,FR40,EU50,UK100
IG_SYMBOLS_DONCHIAN_BREAKOUT=GOLD,BRENT,USDCAD,USDJPY
IG_SYMBOLS_TREND_FOLLOWING=US500,US30,DE40,UK100,GOLD,WTI,BRENT,AAPL,MSFT
IG_SYMBOLS_MEAN_REVERSION_BB=EURGBP,EURCHF,GBPUSD,AUS200
IG_SYMBOLS_MEAN_REVERSION=EURUSD,UK100,AUDUSD,USDCHF
IG_SYMBOLS_MEAN_BREAKOUT=DE40,WTI,GOLD,US100
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

- `index_hybrid`: `safe`/`conservative`, `aggressive`
- `mean_breakout_v2`: `conservative` (`safe` alias), `aggressive`
- `mean_reversion_bb`: `conservative` (`safe` alias), `aggressive`

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
  {"label":"night_index_hybrid","strategy":"index_hybrid","symbols":["US100","US500","DE40","UK100"],"start":"23:00","end":"07:00","weekdays":"mon-fri","priority":20},
  {"label":"morning_g1_trend","strategy":"g1","symbols":["DE40","EURUSD","GBPUSD","USDJPY"],"start":"07:00","end":"13:00","weekdays":"mon-fri","priority":20},
  {"label":"day_momentum","strategy":"momentum","symbols":["US100","US500","DE40","US30","GOLD","WTI","BRENT"],"start":"13:00","end":"20:00","weekdays":"mon-fri","priority":20},
  {"label":"evening_crypto_trend","strategy":"crypto_trend_following","symbols":["BTC","ETH","LTC","SOL","XRP","DOGE"],"start":"20:00","end":"23:00","weekdays":"mon-fri","priority":20},
  {"label":"weekend_crypto","strategy":"crypto_trend_following","symbols":["BTC","ETH","LTC","SOL","XRP","DOGE"],"start":"00:00","end":"23:59","weekdays":"sat-sun","priority":30}
]}'
```

Примечание: `index_hybrid` используйте только для индексов, а `crypto_trend_following` только для crypto-symbols.
Для 24/7-инструментов оставляйте отдельный `sat-sun` слот (`BTC`, `ETH`, `LTC`, `SOL`, `XRP`, `DOGE`).
Если IG allowance ограничивает REST, лучше сократить число одновременно активных crypto-символов в вечернем окне.

Тюнинг `momentum` (рекомендуемый отдельный пресет):

```bash
# подтверждение кросса, ATR-стопы, slope-фильтр, лимит спреда и outcome-cooldown
XTB_STRATEGY_PARAMS_MOMENTUM={
  "fast_window":8,
  "slow_window":21,
  "momentum_ma_type":"ema",
  "momentum_entry_mode":"cross_only",
  "momentum_confirm_bars":2,
  "momentum_low_tf_min_confirm_bars":2,
  "momentum_low_tf_max_confirm_bars":2,
  "momentum_high_tf_max_confirm_bars":1,
  "momentum_auto_confirm_by_timeframe":true,
  "momentum_timeframe_sec":60,
  "momentum_session_filter_enabled":false,
  "momentum_session_start_hour_utc":6,
  "momentum_session_end_hour_utc":22,
  "momentum_max_spread_pips":1.5,
  "momentum_require_context_tick_size":true,
  "momentum_trade_cooldown_sec":300,
  "momentum_trade_cooldown_win_sec":60,
  "momentum_trade_cooldown_loss_sec":300,
  "momentum_trade_cooldown_flat_sec":120,
  "momentum_atr_window":14,
  "momentum_atr_multiplier":2.4,
  "momentum_risk_reward_ratio":2.0,
  "momentum_min_stop_loss_pips":20.0,
  "momentum_min_take_profit_pips":35.0,
  "momentum_min_relative_stop_pct":0.0010,
  "momentum_max_price_slow_gap_atr":3.0,
  "momentum_pullback_entry_max_gap_atr":3.0,
  "momentum_confirm_gap_relief_per_bar":0.4,
  "momentum_price_gap_mode":"wait_pullback",
  "momentum_min_slope_atr_ratio":0.05,
  "momentum_min_trend_gap_atr":0.0,
  "momentum_volume_confirmation":false,
  "momentum_volume_window":20,
  "momentum_min_volume_ratio":1.2,
  "momentum_volume_min_samples":5,
  "momentum_volume_allow_missing":true,
  "momentum_signal_only_min_confidence_for_entry":0.55,
  "momentum_paper_min_confidence_for_entry":0.55,
  "momentum_execution_min_confidence_for_entry":0.65
}
```

Раздельные боевые пресеты для IG M1 (чтобы не смешивать индексы и FX в одном наборе):

```bash
XTB_STRATEGY_PARAMS_MOMENTUM_INDEX={
  "fast_window":8,
  "slow_window":21,
  "momentum_ma_type":"ema",
  "momentum_entry_mode":"cross_only",
  "momentum_confirm_bars":1,
  "momentum_auto_confirm_by_timeframe":false,
  "momentum_timeframe_sec":60,
  "momentum_max_spread_pips":1.8,
  "momentum_require_context_tick_size":true,
  "momentum_trade_cooldown_sec":300,
  "momentum_trade_cooldown_win_sec":45,
  "momentum_trade_cooldown_loss_sec":300,
  "momentum_trade_cooldown_flat_sec":120,
  "momentum_atr_window":14,
  "momentum_atr_multiplier":2.5,
  "momentum_risk_reward_ratio":2.0,
  "momentum_min_stop_loss_pips":30.0,
  "momentum_min_take_profit_pips":60.0,
  "momentum_min_relative_stop_pct":0.0010,
  "momentum_max_price_slow_gap_atr":4.5,
  "momentum_pullback_entry_max_gap_atr":4.5,
  "momentum_confirm_gap_relief_per_bar":0.3,
  "momentum_price_gap_mode":"wait_pullback",
  "momentum_min_slope_atr_ratio":0.08,
  "momentum_signal_only_min_confidence_for_entry":0.55,
  "momentum_paper_min_confidence_for_entry":0.60,
  "momentum_execution_min_confidence_for_entry":0.70
}

XTB_STRATEGY_PARAMS_MOMENTUM_FX={
  "fast_window":8,
  "slow_window":21,
  "momentum_ma_type":"ema",
  "momentum_entry_mode":"cross_only",
  "momentum_confirm_bars":2,
  "momentum_timeframe_sec":60,
  "momentum_max_spread_pips":1.2,
  "momentum_require_context_tick_size":true,
  "momentum_trade_cooldown_sec":180,
  "momentum_trade_cooldown_win_sec":30,
  "momentum_trade_cooldown_loss_sec":240,
  "momentum_trade_cooldown_flat_sec":90,
  "momentum_atr_window":14,
  "momentum_atr_multiplier":2.1,
  "momentum_risk_reward_ratio":1.9,
  "momentum_min_stop_loss_pips":12.0,
  "momentum_min_take_profit_pips":23.0,
  "momentum_min_relative_stop_pct":0.0006,
  "momentum_max_price_slow_gap_atr":2.0,
  "momentum_pullback_entry_max_gap_atr":2.3,
  "momentum_confirm_gap_relief_per_bar":0.4,
  "momentum_min_slope_atr_ratio":0.05,
  "momentum_signal_only_min_confidence_for_entry":0.55,
  "momentum_paper_min_confidence_for_entry":0.58,
  "momentum_execution_min_confidence_for_entry":0.65
}
```

Параметры cooldown по исходу сделки:
- `*_trade_cooldown_win_sec` — задержка перед новым входом после прибыльного закрытия.
- `*_trade_cooldown_loss_sec` — задержка после убыточного закрытия.
- `*_trade_cooldown_flat_sec` — задержка после около-нулевого результата.
- Если outcome-поля не заданы, используется `*_trade_cooldown_sec` (обратная совместимость).

`momentum_entry_mode`:
- `cross_only` — вход только на свежем пересечении MA.
- `cross_or_trend` — разрешает вход по продолжению тренда (если MA уже разошлись и подтверждены).

`momentum_volume_confirmation`:
- если `true`, бот проверяет, что `current_volume >= avg(volume_window) * min_volume_ratio`;
- если объём недоступен у брокера, поведение управляется `momentum_volume_allow_missing` (`true` — не блокировать вход, `false` — блокировать).

Авто-правило `momentum_confirm_bars`:
- `<= M5` (`momentum_timeframe_sec <= 300`): подтверждение автоматически держится в диапазоне `2..3` (настраивается `momentum_low_tf_min_confirm_bars` / `momentum_low_tf_max_confirm_bars`)
- `>= H1` (`momentum_timeframe_sec >= 3600`): подтверждение ограничивается сверху `momentum_high_tf_max_confirm_bars` (по умолчанию `1`)
- отключить адаптацию можно через `momentum_auto_confirm_by_timeframe=false` (рекомендуется для индексного M1-профиля с `momentum_confirm_bars=1`)
- подтверждение ищет кросс **внутри окна**, а не только в одной фиксированной точке, поэтому бот не пропускает вход, если пересечение случилось на 1-2 свечи раньше.

`momentum_price_gap_mode`:
- `wait_pullback` — если цена слишком далеко от slow MA (по ATR), бот не входит и ждёт pullback (в metadata пишет целевую цену для лимитного входа).
- `block` — жестко блокирует вход при перерастяжении цены.

`momentum_pullback_entry_max_gap_atr`:
- мягкий лимит для входа в `wait_pullback`-режиме: даже если жесткий лимит не нарушен, вход выполняется только когда цена вернулась достаточно близко к slow MA.
- если не задан, используется `momentum_max_price_slow_gap_atr` (поведение совместимо с предыдущей версией).

`momentum_confirm_gap_relief_per_bar`:
- добавляет адаптивный запас к `momentum_max_price_slow_gap_atr` и `momentum_pullback_entry_max_gap_atr` для больших `confirm_bars`, чтобы подтверждение кросса и gap-фильтр не конфликтовали между собой.

`momentum_require_context_tick_size`:
- если `true`, стратегия не откроет вход без `ctx.tick_size` и вернет `HOLD` с причиной `tick_size_unavailable`.

Тюнинг `trend_following` (pullback entry + стоп по противоположной границе канала):

```bash
XTB_STRATEGY_PARAMS_TREND_FOLLOWING={
  "fast_ema_window":20,
  "slow_ema_window":80,
  "donchian_window":20,
  "use_donchian_filter":true,
  "trend_require_context_tick_size":false,
  "trend_breakout_lookback_bars":15,
  "trend_atr_window":14,
  "trend_pullback_max_distance_ratio":0.005,
  "trend_pullback_max_distance_atr":2.5,
  "trend_crypto_max_pullback_distance_atr":1.8,
  "trend_pullback_ema_tolerance_ratio":0.003,
  "trend_crypto_pullback_ema_tolerance_ratio":0.005,
  "trend_pullback_bounce_required":false,
  "trend_slope_mode":"fast_with_slow_tolerance",
  "trend_slow_slope_tolerance_ratio":0.0005,
  "trend_confidence_velocity_norm_ratio":0.0005,
  "trend_crypto_min_ema_gap_ratio":0.0012,
  "trend_crypto_min_fast_slope_ratio":0.0002,
  "trend_crypto_min_slow_slope_ratio":0.00005,
  "trend_crypto_min_atr_pct":0.18,
  "trend_volume_confirmation":true,
  "trend_volume_window":20,
  "trend_min_volume_ratio":1.2,
  "trend_volume_min_samples":8,
  "trend_volume_allow_missing":true,
  "trend_volume_require_spike":false,
  "trend_volume_confidence_boost":0.1,
  "trend_spread_buffer_factor":1.0,
  "trend_max_spread_to_stop_ratio":0.30,
  "trend_max_stop_loss_atr":0.0,
  "trend_crypto_max_stop_loss_atr":3.0,
  "trend_crypto_min_stop_pct":1.0,
  "trend_max_timestamp_gap_sec":3600.0,
  "trend_following_worker_manual_close_sync_interval_sec":60,
  "trend_following_hold_summary_enabled":true,
  "trend_following_hold_summary_interval_sec":60,
  "trend_following_hold_summary_window":120,
  "trend_following_hold_summary_min_samples":20,
  "trend_following_execution_min_confidence_for_entry":0.75,
  "trend_risk_reward_ratio":2.5,
  "trend_min_stop_loss_pips":30.0,
  "trend_min_take_profit_pips":75.0,
  "trend_index_min_stop_pct":0.25
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
  "use_donchian_filter":true,
  "trend_breakout_lookback_bars":10,
  "trend_pullback_max_distance_ratio":0.0025,
  "trend_pullback_max_distance_atr":1.5,
  "trend_crypto_max_pullback_distance_atr":1.5,
  "trend_pullback_ema_tolerance_ratio":0.0015,
  "trend_crypto_pullback_ema_tolerance_ratio":0.015,
  "trend_crypto_min_ema_gap_ratio":0.0015,
  "trend_crypto_min_fast_slope_ratio":0.00025,
  "trend_crypto_min_slow_slope_ratio":0.00008,
  "trend_crypto_min_atr_pct":0.20,
  "trend_pullback_bounce_required":false,
  "trend_spread_buffer_factor":1.0,
  "trend_max_spread_to_stop_ratio":0.50,
  "trend_crypto_min_stop_pct":1.0,
  "trend_crypto_max_stop_loss_atr":3.0,
  "trend_max_timestamp_gap_sec":3600.0,
  "trend_risk_reward_ratio":2.0,
  "trend_min_stop_loss_pips":150.0,
  "trend_min_take_profit_pips":120.0,
  "trend_strength_norm_ratio":0.0025
}
```

Для `crypto_trend_following` держите stop в процентах/ATR, иначе ночной spread и шум IG будут часто выбивать по короткому стопу.

Тюнинг `donchian_breakout` (фильтры ложных пробоев + ATR/volume + динамический SL/TP):

```bash
XTB_STRATEGY_PARAMS_DONCHIAN_BREAKOUT={
  "donchian_breakout_window":20,
  "donchian_breakout_atr_window":14,
  "donchian_breakout_atr_multiplier":2.0,
  "donchian_breakout_risk_reward_ratio":2.5,
  "donchian_breakout_min_stop_loss_pips":30.0,
  "donchian_breakout_min_take_profit_pips":75.0,
  "donchian_breakout_min_relative_stop_pct":0.0008,
  "donchian_breakout_min_breakout_atr_ratio":0.15,
  "donchian_breakout_max_breakout_atr_ratio":1.8,
  "donchian_breakout_min_channel_width_atr":0.6,
  "donchian_breakout_volume_confirmation":false,
  "donchian_breakout_volume_window":20,
  "donchian_breakout_min_volume_ratio":1.2,
  "donchian_breakout_volume_min_samples":5,
  "donchian_breakout_volume_allow_missing":true
}
```

Для IG:
- если брокер передаёт `tick_size`, стратегия использует его как источник `pip_size`;
- если `tick_size` недоступен для CFD/индексов/коммодити и включён `mb_require_context_tick_size_for_cfd=true`, вход блокируется (`reason=tick_size_unavailable`);
- symbol fallback используется только как резерв и в первую очередь для FX.

Тюнинг `mean_breakout_v2` (Z-Score + breakout + slope):

```bash
XTB_STRATEGY_PARAMS_MEAN_BREAKOUT_V2={
  "mb_zscore_window":40,
  "mb_breakout_window":15,
  "mb_slope_window":3,
  "mb_zscore_threshold":1.5,
  "mb_zscore_entry_mode":"directional_extreme",
  "mb_min_slope_ratio":0.0006,
  "mb_exit_z_level":0.5,
  "mb_stop_loss_pips":50.0,
  "mb_take_profit_pips":120.0,
  "mb_risk_reward_ratio":2.2,
  "mb_atr_window":14,
  "mb_atr_multiplier":1.3,
  "mb_min_stop_loss_pips":45.0,
  "mb_min_take_profit_pips":90.0,
  "mb_min_relative_stop_pct":0.0012,
  "mb_dynamic_tp_only":true,
  "mb_require_context_tick_size_for_cfd":true,
  "mb_volume_confirmation":true,
  "mb_volume_window":20,
  "mb_min_volume_ratio":1.4,
  "mb_volume_min_samples":8,
  "mb_volume_allow_missing":true,
  "mb_volume_require_spike":false,
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
  "mb_trailing_atr_multiplier":1.2,
  "mb_trailing_activation_stop_ratio":0.8,
  "mb_trailing_min_activation_pips":0.0
}
```

`mean_breakout_v2` сначала пытается определить ТФ по `timestamps` (медиана дельт), иначе использует `mb_timeframe_sec`.
Для импульсного режима используйте `mb_zscore_entry_mode=directional_extreme`: входы проходят только когда пробой подтвержден направленным экстремальным z-score.
Итоговый SL/TP:
- `SL = max(min_stop, base_sl * tf_multiplier, ATR_pips * mb_atr_multiplier, price * mb_min_relative_stop_pct / pip_size)`
- если `mb_dynamic_tp_only=true`, `TP = max(min_tp, SL * rr_ratio)` (без жёсткой привязки к `base_tp`);
- иначе `TP = max(min_tp, base_tp * tf_multiplier, SL * rr_ratio)`.

Если `mb_trailing_enabled=true`, стратегия добавляет в `Signal.metadata.trailing_stop` рекомендации для воркера:
- `trailing_distance_pips = ATR_pips * mb_trailing_atr_multiplier`
- `trailing_activation_pips = max(mb_trailing_min_activation_pips, SL * mb_trailing_activation_stop_ratio)`

`mb_volume_*`:
- опциональное подтверждение breakout по объему. При spike confidence повышается на `mb_volume_confidence_boost`; при `mb_volume_require_spike=true` без spike вход блокируется (`reason=volume_not_confirmed`).

Тюнинг `mean_reversion` (анти-тренд фильтр + ATR SL/TP + выход к среднему):

```bash
XTB_STRATEGY_PARAMS_MEAN_REVERSION={
  "mean_reversion_zscore_window":20,
  "mean_reversion_zscore_threshold":1.8,
  "mean_reversion_exit_zscore":0.15,
  "mean_reversion_trend_filter_enabled":true,
  "mean_reversion_trend_ma_window":200,
  "mean_reversion_use_atr_sl_tp":true,
  "mean_reversion_atr_window":14,
  "mean_reversion_atr_multiplier":2.0,
  "mean_reversion_risk_reward_ratio":1.8,
  "mean_reversion_min_stop_loss_pips":20.0,
  "mean_reversion_min_take_profit_pips":30.0
}
```

Логика:
- входы по Z-Score (`<= -threshold` для BUY, `>= threshold` для SELL);
- вход разрешается только по направлению long-term SMA (`mean_reversion_trend_ma_window`);
- `SL/TP` считаются от ATR (если включен `mean_reversion_use_atr_sl_tp`);
- для открытой позиции бот закрывает сделку по `exit_hint=close_on_mean_reversion`, когда Z-Score возвращается к зоне `abs(z) <= mean_reversion_exit_zscore`.

Тюнинг `mean_reversion_bb` (Bollinger re-entry + strict trend slope filter + volume spike confirmation):

```bash
XTB_STRATEGY_PARAMS_MEAN_REVERSION_BB={
  "mean_reversion_bb_window":20,
  "mean_reversion_bb_std_dev":2.2,
  "mean_reversion_bb_entry_mode":"reentry",
  "mean_reversion_bb_reentry_tolerance_sigma":0.05,
  "mean_reversion_bb_use_rsi_filter":true,
  "mean_reversion_bb_rsi_period":14,
  "mean_reversion_bb_rsi_history_multiplier":3.0,
  "mean_reversion_bb_rsi_method":"wilder",
  "mean_reversion_bb_rsi_overbought":70.0,
  "mean_reversion_bb_rsi_oversold":30.0,
  "mean_reversion_bb_min_std_ratio":0.00005,
  "mean_reversion_bb_min_band_extension_ratio":0.03,
  "mean_reversion_bb_max_band_extension_ratio":2.0,
  "mean_reversion_bb_trend_filter_enabled":true,
  "mean_reversion_bb_trend_ma_window":100,
  "mean_reversion_bb_trend_filter_mode":"strict",
  "mean_reversion_bb_trend_slope_lookback_bars":5,
  "mean_reversion_bb_trend_slope_strict_threshold":0.00015,
  "mean_reversion_bb_volume_confirmation":true,
  "mean_reversion_bb_volume_window":20,
  "mean_reversion_bb_min_volume_ratio":1.5,
  "mean_reversion_bb_volume_min_samples":10,
  "mean_reversion_bb_volume_allow_missing":true,
  "mean_reversion_bb_volume_confidence_boost":0.2,
  "mean_reversion_bb_exit_on_midline":true,
  "mean_reversion_bb_exit_midline_tolerance_sigma":0.15,
  "mean_reversion_bb_use_atr_sl_tp":true,
  "mean_reversion_bb_atr_window":14,
  "mean_reversion_bb_atr_multiplier":1.5,
  "mean_reversion_bb_take_profit_mode":"rr",
  "mean_reversion_bb_risk_reward_ratio":2.0,
  "mean_reversion_bb_min_stop_loss_pips":25.0,
  "mean_reversion_bb_min_take_profit_pips":30.0
}
```

Логика:
- вход по re-entry: сначала выход за полосу, затем возврат внутрь канала;
- RSI по умолчанию считается по Уайлдеру (`mean_reversion_bb_rsi_method="wilder"`), при этом используется расширенная история (`mean_reversion_bb_rsi_history_multiplier`) для стабильного сглаживания;
- при слишком низкой волатильности (`std/mean < mean_reversion_bb_min_std_ratio`) стратегия не входит;
- есть защита от экстремальных «ножей» через `mean_reversion_bb_max_band_extension_ratio`;
- в режиме `strict` стратегия блокирует контртрендовые входы при сильном наклоне трендовой MA (`mean_reversion_bb_trend_slope_strict_threshold`);
- опциональный volume spike фильтр/boost (`mean_reversion_bb_volume_*`) поднимает confidence при подтверждении объёмом;
- для открытой позиции бот закрывает сделку по `exit_hint=close_on_bb_midline`, когда цена возвращается к средней линии BB;
- `SL/TP` можно сделать ATR-адаптивными через `mean_reversion_bb_use_atr_sl_tp=true`, TP по умолчанию в режиме `rr`.

Тюнинг `index_hybrid` для IG (сбалансированный intraday-профиль):

```bash
XTB_STRATEGY_PARAMS_INDEX_HYBRID={
  "index_fast_ema_window":13,
  "index_slow_ema_window":55,
  "index_donchian_window":20,
  "index_atr_window":14,
  "index_trend_gap_threshold":0.0003,
  "index_mean_reversion_gap_threshold":0.0003,
  "index_trend_atr_pct_threshold":0.01,
  "index_mean_reversion_atr_pct_threshold":0.03,
  "index_regime_selection_mode":"fuzzy",
  "index_regime_trend_index_threshold":0.8,
  "index_regime_fallback_mode":"nearest",
  "index_zscore_window":30,
  "index_window_sync_mode":"warn",
  "index_zscore_donchian_ratio_target":1.5,
  "index_zscore_donchian_ratio_tolerance":0.2,
  "index_zscore_mode":"classic",
  "index_zscore_ema_window":55,
  "index_zscore_threshold":1.8,
  "index_auto_correct_regime_thresholds":true,
  "index_require_context_tick_size":true,
  "index_mean_reversion_allow_breakout":true,
  "index_mean_reversion_breakout_extreme_multiplier":1.2,
  "index_min_breakout_distance_ratio":0.00008,
  "index_min_channel_width_atr":0.45,
  "index_volume_confirmation":true,
  "index_volume_window":20,
  "index_min_volume_ratio":1.35,
  "index_volume_min_samples":8,
  "index_volume_allow_missing":true,
  "index_volume_require_spike":false,
  "index_volume_confidence_boost":0.08,
  "index_volume_as_bonus_only":true,
  "index_stop_loss_pips":30.0,
  "index_take_profit_pips":80.0,
  "index_stop_loss_pct":0.15,
  "index_take_profit_pct":0.45,
  "index_stop_atr_multiplier":2.0,
  "index_risk_reward_ratio":2.2,
  "index_session_filter_enabled":true,
  "index_trend_session_start_hour_utc":6,
  "index_trend_session_end_hour_utc":22,
  "index_mean_reversion_outside_trend_session":true,
  "index_hybrid_signal_only_min_confidence_for_entry":0.58,
  "index_hybrid_paper_min_confidence_for_entry":0.62,
  "index_hybrid_execution_min_confidence_for_entry":0.68
}
```

Почему этот профиль полезен:
- убирает «мертвую зону» между режимами через `index_regime_selection_mode="fuzzy"` и `index_regime_trend_index_threshold`;
- для совместимости оставляет `index_regime_fallback_mode="nearest"` (используется в `hard` режиме);
- снижает лаг по `zscore` в intraday через `index_zscore_mode="classic"`;
- отсекает микро-пробои и узкие каналы (`index_min_breakout_distance_ratio`, `index_min_channel_width_atr`);
- добавляет подтверждение объёмом (`index_volume_*`), но в `index_volume_as_bonus_only=true` не блокирует входы, а только бустит confidence;
- делает `%`-стопы для индексов практичными (`index_stop_loss_pct=0.15`, `index_stop_atr_multiplier=2.0`);
- разрешает mean-reversion вне тренд-сессии (`index_mean_reversion_outside_trend_session=true`);
- включает защиту от неверного масштаба через `index_require_context_tick_size=true`.

Важно:
- это still-агрессивный intraday пресет, он повышает частоту входов и потенциальную просадку;
- оставляйте `XTB_MAX_RISK_PER_TRADE_PCT <= 1.0` для live до отдельной проверки статистики.

`index_window_sync_mode`:
- `auto` — автоматически синхронизирует `index_zscore_window` с целевым отношением к `index_donchian_window`.
- `warn` — не меняет окно, только ставит статус рассинхронизации в metadata.
- `off` — выключает синхронизацию окон.

Тюнинг `g1` (anti-whipsaw + candle confirmation + cooldown):

```bash
XTB_STRATEGY_PARAMS_G1={
  "g1_trade_cooldown_sec":1800,
  "g1_candle_timeframe_sec":60,
  "g1_candle_confirm_bars":1,
  "g1_use_incomplete_candle_for_entry":true,
  "g1_ignore_sunday_candles":true,
  "g1_entry_mode":"cross_or_trend",
  "g1_min_trend_gap_ratio":0.0,
  "g1_min_cross_gap_ratio":0.00008,
  "g1_continuation_adx_multiplier":0.70,
  "g1_continuation_min_adx":18.0,
  "g1_adx_warmup_multiplier":3.0,
  "g1_adx_warmup_extra_bars":10,
  "g1_adx_warmup_cap_bars":48,
  "g1_use_adx_hysteresis_state":true,
  "g1_index_require_context_tick_size":true,
  "g1_resample_mode":"auto",
  "g1_debug_indicators":false,
  "g1_debug_indicators_interval_sec":0.0,
  "g1_adx_hysteresis":2.0,
  "g1_max_price_ema_gap_ratio":0.006,
  "g1_fx_max_price_ema_gap_ratio":0.006,
  "g1_index_max_price_ema_gap_ratio":0.010,
  "g1_index_low_vol_atr_pct_threshold":0.1,
  "g1_index_low_vol_multiplier":1.2,
  "g1_min_relative_stop_pct":0.0008,
  "g1_volume_confirmation":true,
  "g1_volume_window":20,
  "g1_min_volume_ratio":1.4,
  "g1_volume_min_samples":8,
  "g1_volume_allow_missing":true,
  "g1_volume_require_spike":false,
  "g1_volume_confidence_boost":0.08,
  "g1_confidence_velocity_norm_ratio":0.0005,
  "g1_fx_atr_multiplier":2.2,
  "g1_fx_min_stop_loss_pips":10.0,
  "g1_index_atr_multiplier":2.2,
  "g1_index_min_stop_loss_pips":30.0,
  "g1_signal_only_min_confidence_for_entry":0.58,
  "g1_paper_min_confidence_for_entry":0.60,
  "g1_execution_min_confidence_for_entry":0.70
}
```

Стартовые пороги `min_confidence_for_entry` по умолчанию:
- `momentum`: `signal_only=0.55`, `paper=0.55`, `execution=0.65`
- `g1`: `signal_only=0.60`, `paper=0.60`, `execution=0.70`
- `index_hybrid`: `signal_only=0.65`, `paper=0.65`, `execution=0.75`
- `crypto_trend_following`: `signal_only=0.65`, `paper=0.68`, `execution=0.75`
- для остальных стратегий фильтр выключен (`0.0`), пока вы не зададите его явно.

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
# держать stream-тик дольше перед fallback на REST
IG_STREAM_TICK_MAX_AGE_SEC=15

# глобально разнести /markets-запросы
IG_REST_MARKET_MIN_INTERVAL_SEC=1.0

# десинхронизировать циклы воркеров по символам
XTB_WORKER_POLL_JITTER_SEC=1.0
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
- В `g1` добавлены фильтры качества входа: гистерезис ADX (`g1_adx_hysteresis`), ограничение удаленности цены от slow EMA (`g1_max_price_ema_gap_ratio`), минимальный разрыв EMA на cross (`g1_min_cross_gap_ratio`) и подтверждение по закрытым свечам (`g1_candle_timeframe_sec`, `g1_candle_confirm_bars`).
- Для `g1` доступно подтверждение входа по объему (`g1_volume_*`): при spike confidence повышается на `g1_volume_confidence_boost`; при `g1_volume_require_spike=true` без spike вход блокируется (`reason=volume_not_confirmed`).
- Для `g1` можно исключать воскресные свечи (актуально для IG): `g1_ignore_sunday_candles=true`.
- Для `g1` есть прогрев ADX (`g1_adx_warmup_multiplier`, `g1_adx_warmup_extra_bars`, optional `g1_adx_warmup_cap_bars`) и stateful-гистерезис фильтра ADX (`g1_use_adx_hysteresis_state`).
- Для `g1` есть режим ресемплинга `g1_resample_mode`: `auto` (по умолчанию), `always`, `off`.
- Для отладки индикаторов есть флаг `debug_indicators` (или `g1_debug_indicators`): бот пишет `Strategy indicator snapshot` на каждом цикле; частоту можно ограничить через `debug_indicators_interval_sec` / `g1_debug_indicators_interval_sec`.
- Поддерживается bot-side trailing stop: после выхода позиции в плюс SL переносится в безубыток и дальше подтягивается на `trailing_distance_pips`; `trailing_activation_ratio` (default `0.0`) позволяет задержать старт трейлинга по прогрессу к TP.
- Offset для безубытка можно задавать по профилям: `trailing_breakeven_offset_pips_fx`, `trailing_breakeven_offset_pips_index`, `trailing_breakeven_offset_pips_commodity`; `trailing_breakeven_offset_pips` остается общим fallback.
- Каждое подтягивание трейлинга пишет `WARN`-alert в `events` (`message=Trailing stop adjusted`).
- В `execution` trailing-modify выполняется через modify-эндпоинт брокера с подтверждением статуса.
- Для индексов (например, `US100`, `DE40`) бот закрывает позиции за `session_close_buffer_min` минут до close торговой сессии (защита от гэпов).
- Новостной фильтр проверяет high-impact события брокера (если доступны): за `news_event_buffer_min` минут до события бот либо закрывает позицию (`news_event_action=close`), либо переводит её в безубыток (`breakeven`).
- Перед входом бот проверяет текущий спред (`ask-bid`) в пипсах; если он выше `средний_спред * spread_anomaly_multiplier` (default `3x`), вход блокируется и пишется `WARN`-alert.
- Перед входом бот делает connectivity-check к API брокера; при latency > `500ms` или проблемах соединения новые сделки не открываются.
- Для stream socket включен health-check с auto-reconnect и exponential backoff; деградации/восстановления пишутся в `events` (`Stream health degraded/recovered`), а новые входы блокируются при нездоровом стриме.
- Для маркировки ордеров используйте `bot_magic_prefix`; `bot_magic_instance` можно задать вручную или оставить авто-генерацию (сохранится в `kv`).
- Если брокер недоступен в `paper` или `signal_only`, бот автоматически переключится на mock-цены для непрерывного теста логики.
- Чтобы запретить fallback в mock, включите `BOT_STRICT_BROKER_CONNECT=true` (или CLI `--strict-broker-connect`).
