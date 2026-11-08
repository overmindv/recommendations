# Исходники исполнителя для private CI

Неизменяемый git archive только tracked файлов sandbox на commit из sandbox-source.json.
Этот архив хранится только в закрытом recommendations: sandbox тоже private, а deploy
keys запрещены организацией. Личные PAT в Actions не передаются. Sandbox исходники
не изменены. Reusable tests workflow проверяет SHA commit и SHA256 архива до извлечения.

При обновлении sandbox заменить архив через git archive требуемого commit, пересчитать
SHA256 и одновременно обновить tests/ci/source-refs.json. Не включать dirty файлы,
.env или секреты. Долгосрочная альтернатива — approved GitHub App token с read-only
доступом к обоим репозиториям; эта задача не меняет org security policy.
