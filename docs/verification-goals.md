# Проверка сохранённой цели практики

Дата: 2026-11-09. Проверено в локальных изолированных checkout; production и
обучающая эффективность не проверялись.

| Проверка | Наблюдение |
| --- | --- |
| `make test` в recommendations | exit 0; все Go-пакеты с `-race` |
| `make ctest` с выделенной `goals_component` | exit 0; запись, revision, изоляция, удаление в PostgreSQL 17 |
| `make build` и `make lint` в recommendations | exit 0; lint `0 issues` |
| `docker build` recommendations | exit 0; бинарник и миграции включены в образ |
| `make test` в api-gateway | exit 0; Go race, integration и coverage-команда |
| `make lint`, `make build` в api-gateway | exit 0; lint `0 issues` |
| `npm run test:ci`, `npm run lint`, `npm run build` во frontend | exit 0; unit/UI, lint, TypeScript и Vite |
| `make lint` в infra | exit 0; Compose config и shell syntax |
| `recommendations_contract.py` с выделенной БД | exit 0; настоящие gateway/recommendations и synthetic tasks/entities |

Миграция применена к свежей временной БД через
`DATABASE_URL=.../goals_migration_test go run ./cmd/recommendations migrate up`:
exit 0, версия `202611090001`. Контрактный сценарий подтвердил отсутствие чужой цели,
сохранение, замену с увеличением revision, чтение, изоляцию другого actor и
идемпотентное удаление. База запускалась во временном
PostgreSQL-контейнере; общие БД workspace не очищались.

Общий стек с настоящими tasks/entities и live browser journey не запускался.
Сохранённая цель хранит темы и бюджет занятия; долговременную оценку знаний и
учебный эффект она не подтверждает. Интеграция удаления профиля вместе с аккаунтом
нужна до production deployment.
