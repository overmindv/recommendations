# OVM-205 — ограниченная учебная активность

## Решение и план

1. Изолировать self-reported engagement от learning evidence: новая таблица,
   ограничение частоты на actor, идемпотентность, серверный предел времени.
2. Добавить внутренний POST /v1/learner/study-activity с service token и actor
   только из X-User-ID; строгий JSON, без кода, ответов и скрытых тестов.
3. Включать сбор только явным RECOMMENDATIONS_ACTIVITY_TTL (1h–720h), без
   значения сбор выключен. Пакетное истечение относится только к новой таблице.
4. Проверить duplicate/conflict, огромное duration, offline replay, clock skew,
   скрытую/неактивную вкладку, гонки, TTL, lint/test/build/benchmark и review.

Проверенная ревизия каталога нужна для приёма нового события. Gateway/frontend
сбор активности реализуются в зависимой OVM-155: UI должен остановить таймер
при document.hidden и после 60 секунд без ввода, возобновить через start;
бэкенд дополнительно требует visible=true и active=true у обоих концов интервала.
Передача этих флагов не делает данные доверенными.

## Контракт

JSON: client_event_id, session_id, resource_kind (tasks/content), resource_id,
resource_revision_id (UUID), kind (view/start/heartbeat/finish), occurred_at
(RFC3339), duration_seconds (целое >=0), visible, active (boolean).
Ответ: client_event_id, accepted_seconds, trust=self_reported, expires_at.
Actor не передаётся в JSON. Смена payload при прежнем client_event_id — 409.

Минимум 5 секунд между новыми событиями одного actor независимо от session.
Не больше 600 событий за календарный час UTC. Повторы возвращают первый ответ.
Только heartbeat/finish после start/heartbeat в той же session/resource/revision
дают время: минимум из duration, 30 секунд, серверного и клиентского интервалов.
Разрыв больше 90 секунд, неактивность, скрытая вкладка, смена session или
ревизии дают 0. Событие старше 2 минут или из будущего также даёт 0;
принимаются события не старше min(TTL,24h) и не более 2 минут в будущем.
Пропуск событий не восстанавливается заявленным огромным duration.

TTL считается от occurred_at, повтор не продлевает срок. Допустимое опережение
часов клиента до 2 минут сдвигает срок на ту же величину: граница удаления
совпадает с проверкой возраста, поэтому очищенное событие нельзя воскресить.
Живой повтор возвращает receipt весь TTL, даже после окна новых offline событий.
После истечения старое событие отклоняется; очистка идёт пакетами по 1000.
Счётчик ограничения частоты (без payload) истекает через TTL после приёма;
очистка старых offline событий не сбрасывает ограничение частоты.
Сырые события и состояние интервала хранятся только в БД Recommendations;
не изменяют mastery, completion, оценки или readiness. Статистика engagement
доступна будущей адаптации отдельно от доказательств знаний.

OVM-128 (Леонид) остаётся владельцем общей политики хранения. Здесь нет нового
production default, согласования региона или запуска очистки реальных данных.
Соседний docs прочитан из основного workspace, в этом worktree его нет.
Следующая задача очереди — OVM-268.

## Проверки 22.11.2026

Go 1.26.11: make test (race), make lint (0 issues), make build, make benchmark,
go vet ./... — exit 0. make activity-ctest на выделенной PostgreSQL 17 — exit 0.
CI теперь также запускает PostgreSQL activity component tests.
Независимый review выявил два случая на границе replay/TTL: живой повтор после
24h и опережение часов клиента. Оба воспроизведены отрицательными тестами,
исправлены и проверены повторно. Время нормализуется до микросекунд PostgreSQL.
OpenSpec/harness: 73 проверки, tests 11, lint 0. CI и merge фиксируются в Jira.
Живой каталог Tasks/Content, UI/Gateway и deployment не проверялись;
каталог в component tests заменён контролируемой заглушкой.
