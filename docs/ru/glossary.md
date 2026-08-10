# Глоссарий

**Status:** skeleton · **Owner:** documentation owner · **Last reviewed:** 2026-08-10

Каждая запись обязана содержать: русское название; English name; abbreviation; простое объяснение; техническое определение; как рассчитывается; как интерпретируется; ограничения; где используется.

## Открытый интерес

- **English name:** Open Interest
- **Abbreviation:** OI
- **Простое объяснение:** объём открытых деривативных позиций; не является направленным сигналом сам по себе.
- **Техническое определение:** source-defined sum открытых контрактов/позиций с явно указанными contract semantics и units.
- **Как рассчитывается:** direct source observation либо versioned point-in-time conversion с provenance.
- **Как интерпретировать:** только вместе с price, volume, funding, regime и изменением OI.
- **Ограничения:** единицы и availability зависят от площадки и contract type.
- **Где используется:** derivatives features и Expert View после DQ/knowledge-time gate.
