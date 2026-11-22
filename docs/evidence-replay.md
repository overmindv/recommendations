# Восстановление evidence (OVM-216)

Операторский `cmd/evidence-replay` использует только собственную БД Recommendations
и защищённый HTTP Tasks из OVM-253. Публичного endpoint восстановления нет.
Пороговая политика `weighted-v1` и понятие независимой попытки не меняются.

## Порядок

1. Применить миграции и заменить старые consumer-процессы: они не знают нового
   maintenance fence. Проверить доступ к Tasks manifest/replay отдельным токеном.
2. Задать `RECOMMENDATIONS_REPLAY_ENABLED=true`, `DATABASE_URL`, `TASKS_REPLAY_URL`
   и `TASKS_OUTCOME_REPLAY_TOKEN`. HTTPS обязателен кроме loopback. Credentials
   передаются окружением, не аргументами CLI и не сохраняются в version metadata.
3. `go run ./cmd/evidence-replay start` создаёт version ID и maintenance lease
   на 15 минут. Чтение работает на прежней модели; Kafka evidence временно не
   подтверждается. Старые offsets сохраняются, worker повторяет подключение.
4. `go run ./cmd/evidence-replay step VERSION_ID` применяет до 100 событий
   одной страницы. Повторять до `through == manifest.watermark`. Каждый commit
   сохраняет cursor и продлевает ещё действующий lease; можно продолжить другим
   процессом. Ошибка первоначального manifest оставляет `preparing`: повторить
   step или cancel по выведенному ID. После истечения lease — только cancel/start.
5. `go run ./cmd/evidence-replay activate VERSION_ID` повторно проверяет manifest
   Tasks, затем в одной транзакции проверяет count/hash, заменяет активную модель,
   пересчитывает baseline затронутых целей и ставит существующие планы в очередь
   rebuild. Нужна обычная конфигурация сервиса (включая service token) и та же
   явно заданная mastery policy, если baseline уже существуют. Повтор activate
   уже активной версии успешен и ничего не меняет.
6. `go run ./cmd/evidence-replay status VERSION_ID` показывает состояние;
   `cancel VERSION_ID` удаляет staging этой операции и снимает паузу без активации.
   При сетевой неопределённости после activate сначала проверить status.

Нельзя молча повторять start: одновременно существует только одна операция.
Истёкший lease возобновляет Kafka ingestion; live revision и сам deadline
запрещают переключение старого staging. Сетевых запросов внутри SQL transaction нет.

## Контроль полноты и границы

Manifest и event hashes следуют контракту Tasks: SHA256 канонического JSON, затем
SHA256 бинарных event hashes по journal position. При несовпадении count/hash,
privacy epoch, watermark либо при `legacy_missing>0` переключение запрещено.
Утраченная до введения journal история не реконструируется из последнего verdict.
Восстановление ограничено 100000 events и 1024 затронутыми active goals: большее
значение требует отдельной реализации, а не частичной публикации.

Коррекции выбираются по максимальной correction_version одного submission;
неизменяемые actor/task/version/topics сверяются. История содержит также старые
коррекции, доставленные после новых; отклонённые конфликтующие события не входят.
Отказ сохраняется по event ID и действует при повторе на другом Kafka offset.
У старых inbox без решения статус unknown: повтор не создаёт новый архивный proof;
сохранившийся текущий proof переносится миграцией, история восстанавливается Tasks replay.
Исторические версии, которых не было в старой БД и нет в Tasks journal, не создаются.
Закреплённые topic weights существующих попыток сохраняются. Если многотемной
попытки нет в локальной модели, replay отклоняется: Tasks outcome не содержит
исторических весов, поэтому равные веса не подставляются вместо утраченных.
Текущий Tasks публикует одну тему задачи. Сравнение выполняется для одинакового
набора evidence и зафиксированных локальных весов; неизвестные веса многотемной
истории требуют отдельного authoritative contract.

Старый baseline доступен до атомарного commit. Планы перестраиваются существующей
очередью после commit; мгновенная перестройка всех планов не заявляется.
Staging и correction history содержат идентификаторы evidence, без кода, hidden
tests и ответов. OVM-217 должен включить эти таблицы в owner deletion/retention.
Проверка epoch перед переключением не заменяет постоянного удаления у потребителя
и не даёт распределённой транзакции с Tasks. Production replay/cleanup/deployment
в рамках задачи не выполняются.

## Проверки

`make test`, `make build`, `make lint`, `make benchmark`; отдельная PostgreSQL 17
с `REPLAY_TEST_DSN` и миграциями для `go test -race ./internal/replay`; отдельно
`LEARNING_EVIDENCE_TEST_DSN` для `make evidence-ctest`. Replay tests не запускать
одновременно с другими тестами на той же БД: они заменяют её fixture read model.
CI выполняет activity, replay и evidence component suites последовательно.
