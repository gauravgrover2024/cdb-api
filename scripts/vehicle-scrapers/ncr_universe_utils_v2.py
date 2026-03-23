import re
from typing import Dict, List, Tuple

from mongo_connection import prices_collection

NCR_CITIES = ("new-delhi", "gurgaon", "noida")


def normalize_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_key(value: str) -> str:
    value = normalize_spaces(value).lower()
    value = value.replace("&", " and ")
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return normalize_spaces(value)


def slugify(value: str) -> str:
    value = normalize_spaces(value).lower()
    value = value.replace("&", " and ")
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-{2,}", "-", value).strip("-")
    return value


def title_from_slug(slug: str) -> str:
    words = [w for w in str(slug or "").split("-") if w]
    return " ".join(
        w.upper() if len(w) <= 3 and w.isalpha() else w.title() for w in words
    )


def derive_model_display(brand_display: str, model_display_raw: str) -> str:
    brand = normalize_spaces(brand_display)
    model_raw = normalize_spaces(model_display_raw)
    if not model_raw:
        return ""

    if brand:
        pattern = re.compile(rf"^{re.escape(brand)}[\s\-]*", flags=re.IGNORECASE)
        stripped = normalize_spaces(pattern.sub("", model_raw)).strip("-")
        if stripped:
            return stripped

    return model_raw


def strip_variant_prefix(variant_name: str, brand_display: str, model_display: str) -> str:
    raw = normalize_spaces(variant_name)
    if not raw:
        return ""

    def _strip(text: str, phrase: str) -> str:
        phrase = normalize_spaces(phrase)
        if not phrase:
            return text
        escaped_phrase = re.escape(phrase).replace(r"\ ", r"[\s\-]*")
        pattern = re.compile(rf"^{escaped_phrase}[\s\-:]*", flags=re.IGNORECASE)
        return normalize_spaces(pattern.sub("", text))

    cleaned = raw
    combo = normalize_spaces(f"{brand_display} {model_display}")
    cleaned = _strip(cleaned, combo)
    cleaned = _strip(cleaned, model_display)
    cleaned = _strip(cleaned, brand_display)

    return cleaned or raw


def normalize_variant_key(
    variant_name: str, brand_display: str = "", model_display: str = ""
) -> str:
    cleaned = strip_variant_prefix(variant_name, brand_display, model_display)
    return normalize_key(cleaned)


def _active_only_query() -> Dict:
    return {
        "$or": [
            {"is_discontinued": {"$exists": False}},
            {"is_discontinued": False},
        ]
    }


def build_ncr_variant_universe(active_only: bool = True) -> Dict[Tuple[str, str], Dict]:
    query: Dict = {
        "city": {"$in": list(NCR_CITIES)},
        "brand": {"$exists": True, "$ne": ""},
        "model": {"$exists": True, "$ne": ""},
        "variant": {"$exists": True, "$ne": ""},
    }
    if active_only:
        query.update(_active_only_query())

    projection = {
        "brand": 1,
        "model": 1,
        "variant": 1,
        "city": 1,
        "is_discontinued": 1,
    }

    docs = list(prices_collection.find(query, projection))

    city_rank = {"new-delhi": 0, "noida": 1, "gurgaon": 2}
    universe: Dict[Tuple[str, str], Dict] = {}

    for doc in docs:
        brand_display = normalize_spaces(doc.get("brand"))
        model_display = derive_model_display(brand_display, doc.get("model"))
        variant_display = normalize_spaces(doc.get("variant"))
        city = normalize_spaces(doc.get("city")).lower()

        if not brand_display or not model_display or not variant_display:
            continue

        brand_slug = slugify(brand_display)
        model_slug = slugify(model_display)
        if not brand_slug or not model_slug:
            continue

        model_key = (brand_slug, model_slug)
        entry = universe.setdefault(
            model_key,
            {
                "brand_slug": brand_slug,
                "model_slug": model_slug,
                "brand_display": title_from_slug(brand_slug),
                "model_display": title_from_slug(model_slug),
                "variants": {},
            },
        )

        variant_key = normalize_variant_key(
            variant_display,
            entry["brand_display"],
            entry["model_display"],
        )
        rank = city_rank.get(city, 99)
        current = entry["variants"].get(variant_key)

        if not current or rank < current["rank"]:
            entry["variants"][variant_key] = {
                "variant_display": variant_display,
                "rank": rank,
                "source_city": city,
            }

    for entry in universe.values():
        entry["variant_list"] = sorted(
            [v["variant_display"] for v in entry["variants"].values()],
            key=normalize_key,
        )

    return universe


def list_active_models(active_only: bool = True) -> List[Dict]:
    universe = build_ncr_variant_universe(active_only=active_only)
    models = [
        {
            "brand_slug": value["brand_slug"],
            "model_slug": value["model_slug"],
            "brand_display": value["brand_display"],
            "model_display": value["model_display"],
            "variants": value["variant_list"],
        }
        for value in universe.values()
    ]
    models.sort(key=lambda x: (normalize_key(x["brand_display"]), normalize_key(x["model_display"])))
    return models
