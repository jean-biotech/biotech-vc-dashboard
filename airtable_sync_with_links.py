import os
import csv
import time
from dotenv import load_dotenv
from pyairtable import Api

load_dotenv()

AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
CSV_PATH = os.getenv("CSV_PATH", "Biotech_VC_All_polished_one_sheet.csv")

if not AIRTABLE_TOKEN or not AIRTABLE_BASE_ID:
    raise SystemExit("Missing credentials in .env")

api = Api(AIRTABLE_TOKEN)
base = api.base(AIRTABLE_BASE_ID)

firms_table = base.table("Venture Capital Firms")
therapeutic_table = base.table("Therapeutic Areas")
geography_table = base.table("Geographic Regions")

FIELD_MAP = {
    "Firm": "Firm Name",
    "Website": "Website",
    "HQ City/State": "Headquarters City/State",
    "AUM (USD)": "Assets Under Management (USD)",
    "Typical Check Size (USD)": "Typical Check Size (USD)",
    "Description": "Description",
    "Verified": "Verified",
}

def sanitize(v):
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return None if s == "" or s.lower() in ("tbd", "unknown", "n/a") else s
    return v

def compute_verified(row):
    w = (row.get("Website") or "").strip().lower()
    city = (row.get("HQ City/State") or "").strip().lower()
    return bool(w.startswith("http") and city not in ("", "tbd", "unknown", "n/a"))

def load_csv_rows(path):
    rows = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rr = {c: r.get(c, "") for c in reader.fieldnames or []}
            if not rr.get("Verified"):
                rr["Verified"] = "true" if compute_verified(rr) else "false"
            rows.append(rr)
    return rows

def parse_list_field(value):
    if not value or not isinstance(value, str):
        return []
    cleaned = value.replace(";", ",")
    items = [item.strip() for item in cleaned.split(",")]
    return [item for item in items if item and item.lower() not in ("tbd", "unknown", "n/a", "")]

def get_or_create_lookup_table(table, name_field, items):
    mapping = {}
    print(f"  Fetching existing {table.name} records...")
    existing = table.all()
    for rec in existing:
        name = rec.get("fields", {}).get(name_field)
        if name:
            mapping[name.strip().lower()] = rec["id"]
    print(f"  Found {len(mapping)} existing records")
    to_create = []
    for item in items:
        if item.lower() not in mapping:
            to_create.append(item)
    if to_create:
        print(f"  Creating {len(to_create)} new records...")
        for item in to_create:
            try:
                rec = table.create({name_field: item})
                mapping[item.lower()] = rec["id"]
                print(f"    + Created '{item}'")
                time.sleep(0.21)
            except Exception as e:
                print(f"    ERROR creating '{item}': {e}")
    return mapping

def setup_linked_records(rows):
    print("\n=== Setting up Linked Record Tables ===")
    all_therapeutics = set()
    all_geographies = set()
    all_countries = set()
    for r in rows:
        all_therapeutics.update(parse_list_field(r.get("Therapeutic Areas")))
        all_geographies.update(parse_list_field(r.get("Geography Focus")))
        all_countries.update(parse_list_field(r.get("HQ Country")))
    print(f"\nFound {len(all_therapeutics)} unique therapeutic areas")
    print(f"Found {len(all_geographies)} unique geography focuses")
    print(f"Found {len(all_countries)} unique countries")
    
    # Use correct field names
    therapeutic_map = get_or_create_lookup_table(therapeutic_table, "Therapeutic Area Name", all_therapeutics)
    geography_map = get_or_create_lookup_table(geography_table, "Region Name", all_geographies | all_countries)
    
    return therapeutic_map, geography_map

def to_payload(csv_row, therapeutic_map, geography_map):
    payload = {}
    for csv_name, airtable_name in FIELD_MAP.items():
        v = sanitize(csv_row.get(csv_name))
        if airtable_name == "Verified" and isinstance(v, str):
            v_lower = v.strip().lower()
            v = True if v_lower in ("true", "1", "yes", "y") else False if v_lower in ("false", "0", "no", "n", "") else v
        payload[airtable_name] = v
    therapeutic_items = parse_list_field(csv_row.get("Therapeutic Areas"))
    therapeutic_ids = [therapeutic_map.get(item.lower()) for item in therapeutic_items if therapeutic_map.get(item.lower())]
    if therapeutic_ids:
        payload["Therapeutic Areas of Focus"] = therapeutic_ids
    geo_items = parse_list_field(csv_row.get("Geography Focus"))
    geo_ids = [geography_map.get(item.lower()) for item in geo_items if geography_map.get(item.lower())]
    if geo_ids:
        payload["Geography Focus"] = geo_ids
    country_items = parse_list_field(csv_row.get("HQ Country"))
    country_ids = [geography_map.get(item.lower()) for item in country_items if geography_map.get(item.lower())]
    if country_ids:
        payload["Headquarters Country"] = country_ids
    return payload

def index_existing_firms():
    existing = {}
    print("Fetching existing VC firms...")
    all_records = firms_table.all(fields=["Firm Name"])
    for rec in all_records:
        firm = rec.get("fields", {}).get("Firm Name")
        if firm:
            existing[str(firm).strip().lower()] = rec["id"]
    print(f"Found {len(existing)} existing firms")
    return existing

def upsert_firms(rows, therapeutic_map, geography_map):
    print("\n=== Syncing VC Firms ===")
    existing = index_existing_firms()
    creates = 0
    updates = 0
    errors = 0
    for idx, r in enumerate(rows, start=1):
        firm_name = (r.get("Firm") or "").strip()
        if not firm_name:
            continue
        key_val = firm_name.lower()
        payload = to_payload(r, therapeutic_map, geography_map)
        try:
            if key_val in existing:
                firms_table.update(existing[key_val], payload)
                updates += 1
                if idx % 20 == 0:
                    print(f"  Updated {updates} records...")
            else:
                rec = firms_table.create(payload)
                existing[key_val] = rec["id"]
                creates += 1
                print(f"  + Created '{firm_name}'")
            time.sleep(0.21)
        except Exception as e:
            errors += 1
            print(f"  ERROR Row {idx} '{firm_name}': {str(e)[:80]}")
    return updates, creates, errors

def main():
    if not os.path.exists(CSV_PATH):
        raise SystemExit(f"CSV not found: {CSV_PATH}")
    print(f"\n{'='*60}")
    print(f"AIRTABLE SYNC WITH LINKED RECORDS")
    print(f"{'='*60}")
    print(f"\nLoading CSV: {CSV_PATH}")
    rows = load_csv_rows(CSV_PATH)
    print(f"Loaded {len(rows)} rows\n")
    therapeutic_map, geography_map = setup_linked_records(rows)
    updates, creates, errors = upsert_firms(rows, therapeutic_map, geography_map)
    print(f"\n{'='*60}")
    print(f"SYNC COMPLETE!")
    print(f"{'='*60}")
    print(f"  VC Firms Updated:  {updates}")
    print(f"  VC Firms Created:  {creates}")
    print(f"  Errors:            {errors}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
