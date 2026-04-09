import json

INPUT_FILE = "cars_dataset.json"
OUTPUT_FILE = "cars_dataset_corrected_v2.json"


# -------------------------
# HIGH ACCURACY PARSER
# -------------------------
def extract_specs(name):
    n = name.lower()

    tokens = n.replace("-", " ").replace("(", " ").replace(")", " ").split()

    # -------------------------
    # MARUTI SPECIAL CASE
    # -------------------------
    if any(t in tokens for t in ["ldi", "vdi", "zdi"]):
        fuel = "Diesel"

    elif any(t in tokens for t in ["lxi", "vxi", "zxi"]):
        fuel = "Petrol"

    else:
        diesel_keywords = [
            "diesel", "tdi", "crdi", "mjd", "multijet", "dci",
            "ddi", "cdti", "idtec", "did", "ddis", "dicor",
            "hdi", "revotorq", "mhawk", "di"
        ]

        petrol_keywords = [
            "petrol", "mpfi", "gdi", "fsi", "tfsi", "tsi",
            "vtvt", "vvt", "ivtec", "kappa", "smartstream",
            "revotron"
        ]

        if any(x in n for x in diesel_keywords):
            fuel = "Diesel"

        elif any(x in n for x in ["cng", "lpg", "s-cng", "ecng"]):
            fuel = "CNG"

        elif any(x in n for x in ["electric", "ev", "bev"]):
            fuel = "Electric"

        elif any(x in n for x in ["hybrid", "phev"]):
            fuel = "Hybrid"

        elif any(x in n for x in petrol_keywords):
            fuel = "Petrol"

        else:
            fuel = "Petrol"

    # -------------------------
    # TRANSMISSION
    # -------------------------
    auto_keywords = [
        "automatic", "amt", "cvt", "ivt", "dct",
        "tiptronic", "stronic", "steptronic",
        "torque converter"
    ]

    if any(t in tokens for t in ["at", "amt", "cvt", "dct", "ivt"]):
        transmission = "Automatic"

    elif any(x in n for x in auto_keywords):
        transmission = "Automatic"

    elif any(x in n for x in ["quattro", "xdrive", "awd", "4x4"]):
        transmission = "Automatic"

    else:
        transmission = "Manual"

    return fuel, transmission


# -------------------------
# LOAD DATA
# -------------------------
with open(INPUT_FILE, "r") as f:
    data = json.load(f)


# -------------------------
# CLEAN + CORRECT
# -------------------------
corrected = []

for row in data:
    variant = row.get("variant", "")

    fuel, transmission = extract_specs(variant)

    new_row = row.copy()
    new_row["fuel"] = fuel
    new_row["transmission"] = transmission

    corrected.append(new_row)


# -------------------------
# SAVE OUTPUT
# -------------------------
with open(OUTPUT_FILE, "w") as f:
    json.dump(corrected, f, indent=2)

print(f"\n✅ Corrected dataset saved as {OUTPUT_FILE}")