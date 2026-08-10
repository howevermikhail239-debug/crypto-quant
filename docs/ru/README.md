# База знаний Crypto Quant

Это русскоязычный пользовательский слой. Идентификаторы, схемы, API-поля и код остаются английскими; пользовательские тексты идут через versioned localization keys.

## Ownership и обновления

- Владелец страницы: автор соответствующего data/feature/model/risk change.
- Новая feature не допускается к модели без записи в Feature Dictionary и mapping `feature_id → documentation_id → localization_key`.
- Изменение semantics требует обновления страницы в том же change set и ссылки на `data_contract_version`/`feature_version`.
- Каждая страница имеет `Status`, `Owner`, `Last reviewed` и `Related contracts`.

## Скелет разделов

- [Начало работы](getting_started.md)
- [Глоссарий](glossary.md)
- [Feature Dictionary](features.md)
- [Сигналы](signals.md)
- [Управление риском](risk_management.md)
- [Инвалидация и стоп-лосс](stop_loss_and_invalidation.md)
- [Take Profit и выходы](take_profit_and_exits.md)
- [Риск сделки](trade_risk.md)
- [Жизненный цикл сделки](trade_lifecycle.md)
- [Качество данных](data_quality.md)
- [Polymarket и внешние события](polymarket.md)
