# Жизненный цикл сделки

**Status:** future contract · **Owner:** risk/model owner · **Related contracts:** `future_signal 0.2.0-draft`

Этот раздел описывает будущую аналитику сопровождения proposed/paper сценария. Он не является торговым движком, scheduler, Telegram-интеграцией или разрешением на автоматическое исполнение.

## Время и переоценка

Сигнал хранит горизонт, ориентировочную и максимальную длительность, дату истечения и следующую проверку. Пользователю показывается диапазон («4–12 часов»), а не обещание точного времени.

Переоценка бывает scheduled или event-driven: резкое движение, достижение цели, близость к стопу, смена regime/CVD/OI/funding, liquidation cascade, order-book break, event risk либо ухудшение качества данных. Каждая проверка создаёт новую immutable revision; предыдущая не редактируется.

## Stop, цели и состояния

`invalidation`, `stop_loss` и `take_profit` — разные понятия. Разрешены `KEEP_STOP`, `TIGHTEN_STOP`, `MOVE_TO_BREAK_EVEN`, `TRAIL_STOP`, `CANCEL_STOP_AND_EXIT`. Расширение stop в сторону большего риска по умолчанию запрещено.

Цели TP1/TP2/TP3 имеют ID, статус и отдельные probability/time/R:R, если метод проверен; иначе значение отсутствует. Возможны partial exit, break-even и trailing только как versioned research hypotheses.

Состояния: Новый сценарий, Активен, Сценарий усилился, Сценарий ослаб, Первая/Вторая/Третья цель достигнута, Сопровождение прибыли, Сценарий отменён, Рекомендуется выход, Закрыт, Истёк по времени. Terminal states не планируют следующую переоценку.

## Audit и будущие alerts

Paper audit сохраняет полную revision chain и сравнивает Static Stop/TP с Dynamic Stop/TP на одних и тех же сигналах, с одинаковыми costs. Будущие alerts возможны только при meaningful change: смене решения, существенном изменении probability/risk/stop/targets/срока, target reached, invalidation, exit recommendation или critical data/security event. Обычный пересчёт alert не создаёт.

См. также: [Инвалидация и стоп-лосс](stop_loss_and_invalidation.md), [Take Profit и выходы](take_profit_and_exits.md), [Управление риском](risk_management.md).
