# Проверка MVP recommendations

Дата: 2026-11-08. Локальная среда: macOS arm64, Apple M1, Go 1.27.1.
Область: сервис, политика `practice-v1`, HTTP-границы; без деплоя и учебного пилота.

| Проверка | Результат |
| --- | --- |
| `go test -race ./...` | exit 0; config, httpapi, planner проходят |
| `go vet ./...` | exit 0 |
| `gofmt` | исходники отформатированы |
| `make build` | exit 0; `CGO_ENABLED=0 go build ./...`, как в Dockerfile |
| golangci-lint v2.11.4 c `GOTOOLCHAIN=go1.26.0` | exit 0; `0 issues.` |
| `GOTOOLCHAIN=go1.26.0 make build` | exit 0 |
| `go test ./internal/planner -run '^$' -bench BenchmarkBuild -benchmem` | exit 0; 288 итераций, 4 165 979 ns/op, 1 445 849 B/op, 125 allocs/op |
| Проверка запущенного бинарника | `/health`, `/ready`, `/metrics`, authenticated `/v1/practice-plan`: HTTP 200 |
| Graceful shutdown | SIGTERM → exit 0; токен отсутствует в захваченных логах |

Benchmark использует 32 темы, 2000 задач в 5 выбранных темах, 2000 evidence,
бюджет 240 минут. Это CPU-измерение на одном компьютере, не p95 сервера и не
оценка памяти всего процесса. Среднее время — около 4,17 ms на snapshot.

Проверены холодный старт; prerequisite-граф и транзитивные блокировки; отсутствие
«открытия» тем самим планом; три разных accepted текущих версий; повторные попытки;
нейтральные infrastructure/checker/cancelled; ошибки решений; исключение solved;
diversity; бюджет; детерминизм при перестановке; лимиты; безопасные HTTP-ошибки;
service token/actor; параллельные запросы разных пользователей без общего состояния.

Ограничения окружения:

- Обычный `go build ./...` c CGO завершился ошибкой Apple linker из-за неподдерживаемой
  архитектуры `arm64e.x1` в MacOSX27 SDK. Production-профиль CGO=0 собирается и запускается.
- Полный golangci-lint v2.11.4 под системным Go 1.27 не читает новый формат export data.
  Команда с Go 1.26 завершилась успешно, `0 issues.`:
  `GOTOOLCHAIN=go1.26.0 GOPROXY=https://proxy.golang.org,direct GOSUMDB=sum.golang.org make lint GOLANGCI_LINT=../tasks/bin/golangci-lint`.
- Для загрузки отсутствовавших зависимостей использован command-local GOPROXY;
  глобальные настройки Go не менялись. Обычный goproxy.s.o3.ru не разрешался по DNS.
- Docker image build, CI GitHub, frontend/e2e и оценка learning gain этим отчётом
  не подтверждаются. Межсервисные проверки выполняются отдельно владельцем интеграции.

Следующий шаг: принять независимый review, проверить
gateway/frontend вертикальный сценарий, затем публиковать только согласованные файлы.
`tasks.md` не изменён. Коммиты/публикация относятся к интегрирующему агенту.
