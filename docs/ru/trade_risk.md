# Риск сделки

**Status:** future contract · **Owner:** risk owner

`trade_risk`, `model_confidence` и `opportunity_score` независимы. Уверенная модель может описывать крайне рискованную сделку.

Классы: `LOW` — Низкий, `MODERATE` — Умеренный, `HIGH` — Высокий, `VERY_HIGH` — Очень высокий, `EXTREME` — Экстремальный. Score учитывает market, liquidity, model, structural, data и asset risk; метод и thresholds versioned.

Hard gates (`DATA_QUALITY_FAILURE`, `SECURITY_HIGH_RISK`, `LIQUIDITY_TOO_LOW`, `EXPECTED_SLIPPAGE_TOO_HIGH`, `MODEL_OOD`) дают `NO_TRADE`. Для meme subsystem риск нельзя занижать из-за высокой confidence; `EXTREME` не является обычным trade candidate. Будущий position sizing уменьшает allocation с ростом риска, но конкретные проценты появятся только после paper testing.
