# Качество данных

**Status:** evolving · **Owner:** data owner

Feature или signal недоступен при нарушении его coverage, freshness, knowledge-time, units или completeness contract. Missing intervals не удаляются молча. При недостаточной надёжности пользователь видит «Недостаточно данных», а hard DQ failure принудительно даёт `NO_TRADE`.
