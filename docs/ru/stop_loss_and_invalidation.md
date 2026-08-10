# Инвалидация и стоп-лосс

**Status:** required before paper trading · **Owner:** risk owner

`invalidation` — аналитическое условие, при котором исходная гипотеза перестаёт быть валидной. `stop_loss` — уровень контроля фактического или paper-риска. Они не тождественны и могут совпасть только по обоснованной versioned risk methodology. У `NO_TRADE` может быть invalidation, но не должен появляться stop-loss позиции.
