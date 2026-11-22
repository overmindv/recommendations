# Учебные планы: что проверить при сбое

## Сбор метрик

Включите учебные планы и задайте `RECOMMENDATIONS_STUDY_METRICS_TOKEN`.
Без токена маршрут не регистрируется. Используйте отдельный секрет для
Prometheus: токен внутренних API не подходит. Метрики учебного плана доступны
на `GET /internal/metrics/study`, стандартный `/metrics` Parker не меняется.
Маршрут не проксируется наружу; собирайте его по внутренней сети с TLS.

```yaml
scrape_configs:
  - job_name: recommendations-study
    metrics_path: /internal/metrics/study
    scheme: https
    authorization:
      credentials_file: /run/secrets/recommendations-study-metrics
    static_configs:
      - targets: ["recommendations.internal:443"]
```

Замените адрес на внутренний адрес сервиса в своем окружении. В метках нет
ID ученика, цели, ресурса, Kafka partition, URL или текста ошибок.
`version` и `slot` относятся к конфигурации процесса, по одной серии
`recommendations_study_ranking_policy_info` на реплику. Лог решения на уровне
`debug` содержит операцию, причину и полученный W3C trace ID, если он был
передан. Не включайте debug надолго.

## Диагностика

| Что смотреть | Как читать |
| --- | --- |
| `recommendations_study_plan_duration_seconds` | Длительность генерации и пересчета. Отдельно смотрите `saved`, `fallback_*`, `unavailable` и `failed`. Повтор ключа отмечен `replayed` |
| `recommendations_study_catalog_state` | `0` нет сводки, `1` проекция отстала, `2` свежая. Снимок проверяется раз в 15 секунд, независимо от запросов |
| `recommendations_study_catalog_synced_timestamp_seconds` | Когда последний раз синхронизировались Entities, Tasks и Content. `0` означает, что отметки нет |
| `recommendations_study_candidate_checks_total` | `empty` означает, что каталог ответил, но подходящих ресурсов нет; при ошибке владельца счетчик не растет |
| `recommendations_learning_evidence_lag_seconds` | Возраст сообщений Kafka в момент успешного commit offset. Считать по каждому источнику отдельно |
| `recommendations_learning_evidence_failures_total` | Рост `store` или `commit` указывает на повторы, `offset_lost` и `access_denied` останавливают прием |

Доля резервных ответов среди успешных генераций без повторов:

```promql
sum(rate(recommendations_study_plan_duration_seconds_count{operation="generate",result=~"fallback_.*"}[5m]))
/
sum(rate(recommendations_study_plan_duration_seconds_count{operation="generate",result=~"saved|fallback_.*"}[5m]))
```

Если сообщений нет, новый lag не измеряется. Пустая гистограмма сама по себе
не подтверждает, что Kafka догнала поток. При остановке проверьте
`recommendations_learning_evidence_committed_total`, ошибки и разницу между
offset группы `recommendations-learning-evidence-v1` и концом топика на брокере.
Отсутствие всего набора метрик проверяйте через `up`. Не сбрасывайте offsets
вручную при `offset_lost`: это пропустит часть истории.

## Отключение и откат

1. Чтобы остановить автоматический пересчет, задайте
   `RECOMMENDATIONS_STUDY_ADAPTATION_ENABLED=false` и обновите все реплики.
   Проверьте `recommendations_study_auto_adaptation_enabled 0` на каждой.
   После остановки старых реплик больше не берутся задания автоматики. Новые
   оценки сохраняются, ручной пересчет работает. Уже поставленные автоматические
   задания остаются в очереди; выключатель ничего не удаляет.
2. Храните последнюю проверенную конфигурацию ранжирования в
   `RECOMMENDATIONS_STUDY_RANKING_PREVIOUS_POLICY` с исходной версией.
   Для отката выставьте `RECOMMENDATIONS_STUDY_RANKING_POLICY_SLOT=previous`.
   По умолчанию используется `current` из `RECOMMENDATIONS_STUDY_RANKING_POLICY`.
   Если выбранный JSON отсутствует или невалиден, процесс не запускается.
3. Дождитесь завершения rollout и сверяйте версию и слот по
   `recommendations_study_ranking_policy_info`. Сгенерируйте план с новым
   ключом запроса на тестовом аккаунте. Старые версии плана и повторы принятого
   ключа не меняются. Если существующую цель нужно перевести на другую
   политику, явно создайте новую версию, а не исправляйте снимок в БД.
