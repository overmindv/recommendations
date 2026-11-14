"""Показывает бесплатные кандидаты OpenRouter; не включает их автоматически."""
import json
from decimal import Decimal, InvalidOperation
from urllib.request import urlopen


def is_free(model):
    prices = model.get("pricing", {})
    try:
        return (model.get("id", "").endswith(":free")
                and {"prompt", "completion"} <= prices.keys()
                and all(Decimal(str(value)) == 0 for value in prices.values()))
    except (InvalidOperation, ValueError):
        return False


if __name__ == "__main__":
    with urlopen("https://openrouter.ai/api/v1/models", timeout=15) as response:
        data = response.read(8 * 1024 * 1024 + 1)
    if len(data) > 8 * 1024 * 1024:
        raise SystemExit("Catalog exceeds size limit")
    for model in json.loads(data)["data"]:
        if is_free(model):
            print(model["id"])
