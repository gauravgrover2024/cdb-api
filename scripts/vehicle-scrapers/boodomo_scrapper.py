import requests
import pandas as pd
from tqdm import tqdm
import time, re, os

# ----------------------------
# CONFIG
# ----------------------------
INPUT_FILE       = "variant_master.xlsx"
OUTPUT_MATCHED   = "mapped_output.csv"
OUTPUT_UNMATCHED = "unmatched.csv"
OUTPUT_MAPPING   = "model_line_mapping.csv"

# ----------------------------
# UPDATE THESE IF YOU GET 401
# ----------------------------
HEADERS = {
    "User-Agent":       "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:149.0) Gecko/20100101 Firefox/149.0",
    "Accept":           "application/json, text/plain, */*",
    "Accept-Language":  "en-US,en;q=0.9",
    "Referer":          "https://boodmo.com/catalog/3610-cabin_air_filter/m11248-hyundai-i20/?year=2024",
    "X-Client-Token":   "1874df8e-d27c-5042-a084-1c28f3366c67",
    "X-Client-App":     "web",
    "X-Client-Build":   "260408.1546",
    "X-Client-Version": "7.3.15",
    "X-Date":           "2026-04-13T07:43:51.063Z",    # <-- refresh if 401
    "X-Client-Id":      "357463e09538902f98da005ca51ef60c",
    "Accept-Version":   "v1",
    "X-Api":            "CustomerAPI",
    "X-Boo-Sign":       "9813cba07b91643346f96449811c8baf",  # <-- refresh if 401
    "Sec-Fetch-Dest":   "empty",
    "Sec-Fetch-Mode":   "cors",
    "Sec-Fetch-Site":   "same-origin",
    "Connection":       "keep-alive",
}
COOKIES = {
    "client_id": "357463e09538902f98da005ca51ef60c",
    "WZRK_G":    "bc328e05f9fd47bca97bf1a9e530fcda",
    "_ga":       "GA1.2.129713570.1775913706",
}

MAKES_URL = "https://boodmo.com/api/v1/customer/api/catalog/vehicle/car-maker-list"

# ----------------------------
# ENDPOINT CANDIDATES
# NOTE: model-line-list?maker=429 returned 400 (not 404) in last run.
# 400 = endpoint EXISTS but wrong param name. These variants target that base.
# ----------------------------
MODEL_ENDPOINT_CANDIDATES = [
    # --- model-line-list base (returned 400 = endpoint exists!) ---
    "https://boodmo.com/api/v1/customer/api/catalog/vehicle/model-line-list?carMaker={make_id}",
    "https://boodmo.com/api/v1/customer/api/catalog/vehicle/model-line-list?makerId={make_id}",
    "https://boodmo.com/api/v1/customer/api/catalog/vehicle/model-line-list?make_id={make_id}",
    "https://boodmo.com/api/v1/customer/api/catalog/vehicle/model-line-list?makeId={make_id}",
    "https://boodmo.com/api/v1/customer/api/catalog/vehicle/model-line-list?car_maker={make_id}",
    "https://boodmo.com/api/v1/customer/api/catalog/vehicle/model-line-list?id={make_id}",
    "https://boodmo.com/api/v1/customer/api/catalog/vehicle/model-line-list?make={make_id}",
    "https://boodmo.com/api/v1/customer/api/catalog/vehicle/model-line-list?maker_id={make_id}",
    # --- car-model-line-list with different params ---
    "https://boodmo.com/api/v1/customer/api/catalog/vehicle/car-model-line-list?carMaker={make_id}",
    "https://boodmo.com/api/v1/customer/api/catalog/vehicle/car-model-line-list?makerId={make_id}",
    "https://boodmo.com/api/v1/customer/api/catalog/vehicle/car-model-line-list?make_id={make_id}",
    "https://boodmo.com/api/v1/customer/api/catalog/vehicle/car-model-line-list?makeId={make_id}",
    "https://boodmo.com/api/v1/customer/api/catalog/vehicle/car-model-line-list?id={make_id}",
    "https://boodmo.com/api/v1/customer/api/catalog/vehicle/car-model-line-list?make={make_id}",
    # --- fallback: other paths ---
    "https://boodmo.com/api/v1/customer/api/catalog/vehicle/car-model-list?maker={make_id}",
    "https://boodmo.com/api/v1/customer/api/catalog/vehicle/car-model-list?carMaker={make_id}",
    "https://boodmo.com/api/v1/customer/api/catalog/vehicle/model-list?carMaker={make_id}",
    "https://boodmo.com/api/v1/customer/api/catalog/vehicle/model-list?makerId={make_id}",
]

# ----------------------------
# ALIASES
# ----------------------------
MAKE_ALIASES = {
    "maruti suzuki": "maruti",
    "force motors":  "force",
    "volkswagen":    "vw",
    "mg":            "morris garages",
}
MODEL_ALIASES = {
    ("bmw", "5 series gt"):            "5 series",
    ("bmw", "m3"):                     "3 series",
    ("bmw", "m5"):                     "5 series",
    ("bmw", "m6"):                     "6 series",
    ("bmw", "m7"):                     "7 series",
    ("hyundai", "santro xing"):        "santro",
    ("hyundai", "sonata transform"):   "sonata",
    ("hyundai", "verna transform"):    "verna",
    ("hyundai", "i10 grand"):          "grand i10",
    ("chevrolet", "aveo old"):         "aveo",
    ("chevrolet", "aveo u va"):        "aveo",
    ("chevrolet", "optra magnum"):     "optra",
    ("fiat", "grande punto"):          "punto",
    ("fiat", "palio stile"):           "palio",
    ("force", "force one"):            "one",
    ("force motors", "force one"):     "one",
    ("ford", "fiesta classic"):        "fiesta",
    ("maruti suzuki", "a star"):       "a star",
    ("maruti suzuki", "a-star"):       "a star",
    ("maruti suzuki", "alto"):         "alto",
    ("maruti suzuki", "alto 800"):     "alto",
    ("maruti suzuki", "zen estilo"):   "zen estilo",
    ("maruti suzuki", "sx4"):          "sx4",
    ("maruti suzuki", "swift dzire"):  "swift dzire",
    ("maruti suzuki", "wagon r"):      "wagon r",
    ("maruti suzuki", "s cross"):      "s cross",
    ("maruti suzuki", "s-cross"):      "s cross",
    ("maruti suzuki", "grand vitara"): "grand vitara",
    ("maruti suzuki", "brezza"):       "brezza",
    ("maruti suzuki", "vitara brezza"):"brezza",
    ("maruti suzuki", "baleno"):       "baleno",
    ("maruti suzuki", "ciaz"):         "ciaz",
    ("maruti suzuki", "celerio"):      "celerio",
    ("maruti suzuki", "ertiga"):       "ertiga",
    ("maruti suzuki", "ignis"):        "ignis",
    ("maruti suzuki", "swift"):        "swift",
    ("maruti suzuki", "ritz"):         "ritz",
    ("maruti suzuki", "omni"):         "omni",
    ("maruti suzuki", "eeco"):         "eeco",
    ("maruti suzuki", "xl6"):          "xl6",
    ("maruti suzuki", "fronx"):        "fronx",
    ("maruti suzuki", "jimny"):        "jimny",
    ("maruti suzuki", "invicto"):      "invicto",
    ("mahindra", "xuv500"):            "xuv 500",
    ("mahindra", "xuv 500"):           "xuv 500",
    ("mahindra", "xuv300"):            "xuv 300",
    ("mahindra", "xuv 300"):           "xuv 300",
    ("mahindra", "tuv300"):            "tuv 300",
    ("mahindra", "tuv 300"):           "tuv 300",
    ("mahindra", "kuv100"):            "kuv 100",
    ("mahindra", "kuv 100"):           "kuv 100",
    ("mahindra", "xuv700"):            "xuv 700",
    ("mahindra", "xuv 700"):           "xuv 700",
    ("mahindra", "xuv400"):            "xuv 400",
    ("mahindra", "xuv 400"):           "xuv 400",
    ("land rover", "discovery 4"):     "discovery",
    ("land rover", "freelander 2"):    "freelander",
    ("jaguar", "xj l"):                "xj",
    ("jaguar", "xjl"):                 "xj",
    ("tata", "indica vista"):          "indica",
    ("tata", "indigo cs"):             "indigo",
    ("tata", "indigo ecs"):            "indigo",
    ("tata", "safari storme"):         "safari",
    ("tata", "sumo gold"):             "sumo",
    ("tata", "sumo grande"):           "sumo",
    ("nissan", "micra active"):        "micra",
    ("honda", "city zx"):              "city",
    ("skoda", "rapid spaceback"):      "rapid",
}

# ----------------------------
# HELPERS
# ----------------------------

def normalize(text):
    if not text:
        return ""
    text = str(text).lower().strip()
    text = re.sub(r"[^a-z0-9\\s]", " ", text)
    text = re.sub(r"\\s+", " ", text).strip()
    return text

def resolve_make(make_raw):
    return MAKE_ALIASES.get(normalize(make_raw), normalize(make_raw))

def resolve_model(make_raw, model_raw):
    mk = normalize(make_raw)
    md = normalize(model_raw)
    return (MODEL_ALIASES.get((mk, md))
            or MODEL_ALIASES.get((MAKE_ALIASES.get(mk, mk), md))
            or md)

def smart_key(make, model):
    return f"{normalize(make)}|{normalize(model)}"

def resolved_key(make_raw, model_raw):
    return f"{resolve_make(make_raw)}|{resolve_model(make_raw, model_raw)}"

def unwrap(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("data", "items", "results", "list", "models",
                    "makes", "payload", "response", "body"):
            val = data.get(key)
            if isinstance(val, list):
                return val
            if isinstance(val, dict):
                for k2 in ("data", "items", "list", "results"):
                    if isinstance(val.get(k2), list):
                        return val[k2]
    raise ValueError(f"Cannot unwrap: {str(data)[:300]}")

def make_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    s.cookies.update(COOKIES)
    return s

# ----------------------------
# AUTO-DETECT (prioritises 400 responses as "endpoint exists")
# ----------------------------

def detect_model_endpoint(session, test_make_id):
    print(f"\\n  Auto-detecting model endpoint (test make_id={test_make_id})...")
    got_400 = []
    for template in MODEL_ENDPOINT_CANDIDATES:
        url = template.replace("{make_id}", str(test_make_id))
        try:
            res = session.get(url, timeout=15)
            code = res.status_code
            if code == 200:
                try:
                    data = unwrap(res.json())
                    if len(data) > 0:
                        print(f"  ✅ Working endpoint: {template}\\n")
                        return template
                    else:
                        print(f"    200 empty  {url}")
                except Exception:
                    print(f"    200 bad-json  {url}")
            elif code == 400:
                print(f"    400 (exists, wrong param?)  {url}")
                got_400.append(template)
            else:
                print(f"    {code}  {url}")
        except Exception as e:
            print(f"    ERR  {url}  ({e})")

    if got_400:
        print(f"\\n  ⚠️  {len(got_400)} endpoint(s) returned 400 — the path exists but")
        print("  the correct query-param name is still unknown.")
        print("  Please check DevTools and paste the working cURL in chat.\\n")

    return None

# ----------------------------
# FETCH MAKES
# ----------------------------

def get_all_makes(session):
    res = session.get(MAKES_URL, timeout=30)
    if res.status_code == 401:
        raise Exception(
            "\\n401 — refresh X-Boo-Sign + X-Date from a fresh DevTools cURL."
        )
    res.raise_for_status()
    return unwrap(res.json())

# ----------------------------
# BUILD MODEL MAP
# ----------------------------

def build_model_line_map(session):
    print("Fetching makes from boodmo...")
    makes = get_all_makes(session)
    print(f"  Found {len(makes)} makes")

    # Pick a well-known test make_id
    test_id = next(
        (m.get("id") for m in makes
         if (m.get("name") or "").upper() in ("TATA", "MARUTI", "HYUNDAI", "HONDA")),
        makes[0].get("id")
    )

    url_template = detect_model_endpoint(session, test_id)

    if not url_template:
        print("""
╔════════════════════════════════════════════════════════════╗
║  ENDPOINT NOT FOUND — ACTION REQUIRED                      ║
╠════════════════════════════════════════════════════════════╣
║  1. Open Chrome → boodmo.com/vehicles/                     ║
║  2. DevTools (F12) → Network tab → clear log               ║
║  3. Click any maker (e.g. HYUNDAI)                         ║
║  4. Filter network requests by: model                      ║
║  5. Find the call that returns a list of model names        ║
║  6. Right-click → Copy → Copy as cURL (bash)               ║
║  7. Paste the cURL in chat — we will hardcode the URL       ║
╚════════════════════════════════════════════════════════════╝
        """)
        return {}

    mapping = {}
    skipped = 0

    for make in tqdm(makes, desc="Fetching model lines"):
        make_name = make.get("name") or make.get("title") or ""
        make_id   = make.get("id") or make.get("slug") or ""
        if not make_id or not make_name:
            continue

        url = url_template.replace("{make_id}", str(make_id))
        try:
            res = session.get(url, timeout=30)
            if res.status_code in (404, 400):
                skipped += 1
                continue
            if res.status_code == 401:
                raise Exception("Token expired — update X-Boo-Sign + X-Date")
            res.raise_for_status()
            models = unwrap(res.json())
        except requests.exceptions.HTTPError:
            skipped += 1
            continue
        except Exception as e:
            tqdm.write(f"  Skipped {make_name}: {e}")
            continue

        for m in models:
            model_name = (m.get("name") or m.get("title") or
                          m.get("model") or m.get("modelName") or "")
            model_id   = (m.get("id") or m.get("slug") or
                          m.get("modelLineId") or m.get("model_id") or "")
            if not model_name or not model_id:
                continue
            key = smart_key(make_name, model_name)
            mapping[key] = {
                "make":        make_name,
                "model":       model_name,
                "modelLineId": model_id,
            }
        time.sleep(0.3)

    if skipped:
        tqdm.write(f"  ({skipped} makes skipped)")
    return mapping

# ----------------------------
# LOAD / MATCH / SAVE
# ----------------------------

def load_dataset():
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"'{INPUT_FILE}' not found in {os.getcwd()}")
    df = pd.read_excel(INPUT_FILE)
    df.columns = [c.strip().lower() for c in df.columns]
    missing = [c for c in ["make", "model"] if c not in df.columns]
    if missing:
        raise Exception(f"Missing columns: {missing}")
    before = len(df)
    df = df.dropna(subset=["make", "model"])
    if before - len(df):
        print(f"  Dropped {before - len(df)} null rows")
    return df

def match_dataset(df, mapping):
    matched, unmatched = [], []
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Matching rows"):
        rec = row.to_dict()
        exact = smart_key(row["make"], row["model"])
        if exact in mapping:
            rec["modelLineId"] = mapping[exact]["modelLineId"]
            rec["match_type"]  = "exact"
            matched.append(rec)
            continue
        alias = resolved_key(row["make"], row["model"])
        if alias in mapping:
            rec["modelLineId"] = mapping[alias]["modelLineId"]
            rec["match_type"]  = "alias"
            matched.append(rec)
            continue
        unmatched.append(rec)
    return pd.DataFrame(matched), pd.DataFrame(unmatched)

def save_outputs(matched_df, unmatched_df, mapping):
    print("\\nSaving outputs...")
    if not matched_df.empty:
        matched_df.to_csv(OUTPUT_MATCHED, index=False)
        exact = (matched_df["match_type"] == "exact").sum()
        alias = (matched_df["match_type"] == "alias").sum()
        print(f"  → {OUTPUT_MATCHED}  ({len(matched_df)} rows: {exact} exact + {alias} alias)")
    if not unmatched_df.empty:
        unmatched_df.to_csv(OUTPUT_UNMATCHED, index=False)
        print(f"  → {OUTPUT_UNMATCHED}  ({len(unmatched_df)} rows)")
        print("\\n  Sample unmatched:")
        print(unmatched_df[["make","model"]].drop_duplicates().head(20).to_string(index=False))
    pd.DataFrame(mapping.values()).to_csv(OUTPUT_MAPPING, index=False)
    print(f"  → {OUTPUT_MAPPING}  ({len(mapping)} model lines)")

# ----------------------------
# MAIN
# ----------------------------

def main():
    print("=" * 55)
    print("  Boodmo Model-Line Mapping Pipeline  v7")
    print("=" * 55 + "\\n")
    session = make_session()
    mapping = build_model_line_map(session)
    if not mapping:
        return
    print(f"\\nTotal model lines mapped: {len(mapping)}\\n")
    df = load_dataset()
    print(f"Dataset loaded: {len(df)} rows\\n")
    matched_df, unmatched_df = match_dataset(df, mapping)
    total = len(df)
    pct = len(matched_df) / total * 100 if total else 0
    print(f"\\n✅ Matched:   {len(matched_df)} / {total}  ({pct:.1f}%)")
    print(f"❌ Unmatched: {len(unmatched_df)} / {total}  ({100-pct:.1f}%)")
    save_outputs(matched_df, unmatched_df, mapping)
    print("\\nDone!")

if __name__ == "__main__":
    main()