# Сигналы

**Status:** skeleton · **Owner:** model owner

Simple view показывает актив, горизонт, направление/`NO_TRADE`, probabilities, confidence, trade risk, invalidation, position stop, Conservative/Base/Aggressive targets и основные причины. Stop/targets отсутствуют при `NO_TRADE` или недостаточной методологии.

Expert view добавляет feature values, OI/CVD/funding/order book, optional Polymarket context, DQ state, model/data versions, expected value и methodology. Confidence не равен trade risk; opportunity не равен safety. Ненадёжный показатель не выдумывается: используется «Недостаточно данных» либо блок скрывается.

Для proposed/paper сценария также показываются status, ориентировочный срок, следующая переоценка и ссылка на [жизненный цикл сделки](trade_lifecycle.md). Это будущий contract: он не означает, что сейчас существует автоматическое сопровождение позиции.
