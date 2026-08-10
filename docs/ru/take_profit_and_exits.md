# Take Profit и выходы

**Status:** future contract · **Owner:** risk owner

Take Profit — рациональный уровень/условие фиксации прибыли, а не обещание цены. Он не равен автоматически ближайшему support/resistance.

- `CONSERVATIVE`: ближайшая реалистичная цель, обычно выше вероятность и короче holding time.
- `BASE`: основной баланс probability × expected return × risk.
- `AGGRESSIVE`: более дальний сценарий с меньшей вероятностью, большим upside и риском возврата прибыли.

Для каждой цели отдельно оцениваются target-hit-before-stop probability, conditional time-to-target и cost-aware R:R. Directional confidence не является вероятностью достижения target. Если метод ещё не валидирован, значения остаются `null`, а интерфейс показывает «Вероятность пока не рассчитана».

Высокий R:R сам по себе не доказывает хорошую сделку: Decision Engine позднее использует multi-outcome expected value после fees, spread, slippage и funding. Partial exit percentages, break-even trigger и trailing method configurable и versioned. Paper research обязан сравнить single/staged TP, fixed/probability-based TP, partial on/off, break-even on/off и trailing on/off.

Изменения TP1/TP2/TP3 после входа относятся к [жизненному циклу сделки](trade_lifecycle.md): они создают новую immutable revision и требуют объяснения, evidence и сравнения со static baseline.
