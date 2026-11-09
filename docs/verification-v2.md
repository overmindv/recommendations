# Проверка practice-v2

Дата: 2026-11-09. Локальная проверка stateless сервиса; учебный эффект и production не измерялись.

| Проверка | Результат |
| --- | --- |
| `make test` | exit 0; все пакеты с `-race` |
| `make build` | exit 0; `CGO_ENABLED=0 go build ./...` |
| `make lint` с установленным golangci-lint v2.11.4 и Go 1.26 | exit 0; `0 issues.` |
| `make benchmark` | exit 0; 208 итераций, 5 660 021 ns/op, 1 446 917 B/op, 132 allocs/op |
| Сквозной контракт `infra/tests/recommendations_contract.py` | exit 0; реальные gateway и recommendations, synthetic owners |
| `npm run spec:check` в docs | exit 0; 5 passed, 0 failed |

Исходная команда `make lint` пыталась установить линтер через недоступный локальный
`goproxy.s.o3.ru` и завершилась ошибкой DNS. Повторная проверка использовала уже
установленный `/Users/alevasyukovv/Desktop/Overmindv/tasks/bin/golangci-lint` с
`GOTOOLCHAIN=go1.26.0`; настройки окружения не менялись.

Тесты подтверждают предпочтение новой задачи после ошибки, fallback на прежнюю
задачу, разнообразие типов, ограничения времени, prerequisites, детерминизм,
изоляцию actor и безопасность HTTP-ошибок. Benchmark — локальное CPU-измерение,
не p95 сервера. Сравнение учебного результата v1/v2 требует экспертных кейсов и
пилота на реальных пользователях; синтетические тесты такого вывода не дают.
