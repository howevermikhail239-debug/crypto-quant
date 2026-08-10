# Управление риском

**Status:** future contract · **Owner:** risk owner

Risk Engine появляется только после проверяемых data/model baselines. Он разделяет hypothesis invalidation, position stop, take-profit targets, position sizing и portfolio limits. Все методы versioned, учитывают costs и сначала проходят backtest/paper testing. `NO_TRADE` — допустимое и часто предпочтительное решение.
