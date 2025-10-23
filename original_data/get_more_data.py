import requests, pandas as pd, time, os, json, datetime

BASE_URL = "https://api.nsf.gov/services/v1/awards.json"
OUTPUT_FILE = "nsf_awards_us_2019_2024.csv"
CHECKPOINT_FILE = "checkpoint.json"
RPP = 25
PAUSE = 0.2
MAX_RETRIES = 3
TIMEOUT = 40

# ---- YEAR RANGE ----
YEAR_START = 2019
CURRENT_YEAR = datetime.datetime.now().year   # e.g. 2025
YEAR_END = CURRENT_YEAR

# ✅ Do NOT reset (we're continuing!)
RESET = False

# --- Fields you want ---
PRINT_FIELDS = ",".join([
    "agency",
    "awardeeCountryCode",
    "awardeeStateCode",
    "awardeeName",
    "startDate",
    "expDate",
    "date",
    "title",
    "abstractText"
])

def reset_files():
    """Delete CSV and checkpoint only if RESET=True."""
    if RESET:
        if os.path.exists(OUTPUT_FILE):
            os.remove(OUTPUT_FILE)
            print(f"🗑️ Deleted old file: {OUTPUT_FILE}")
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
            print(f"🗑️ Deleted old checkpoint: {CHECKPOINT_FILE}")

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
        if state.get("year", YEAR_END) > YEAR_END or state["year"] < YEAR_START:
            state["year"] = YEAR_END
            state["offset"] = 1
        return state
    return {"year": YEAR_END, "offset": 1, "written_header": os.path.exists(OUTPUT_FILE), "total_saved": 0}

def save_checkpoint(state):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)

def fetch_page(y, offset):
    params = {
        "rpp": RPP,
        "offset": offset,
        "startDateStart": f"01/01/{y}",
        "startDateEnd": f"12/31/{y}",
        "agency": "NSF",
        "awardeeCountryCode": "US",
        "printFields": PRINT_FIELDS
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(BASE_URL, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
            return data.get("response", {}).get("award", [])
        except Exception as e:
            print(f"⚠️  [Year {y} offset {offset} attempt {attempt}] {e}")
            time.sleep(2 * attempt)
    print(f"❌  Skipping [Year {y} offset {offset}] after {MAX_RETRIES} retries.")
    return []

def append_chunk(df, state, existing_ids):
    # Filter duplicates
    if "id" in df.columns:
        df = df[~df["id"].isin(existing_ids)]
    if df.empty:
        return 0

    write_header = not state["written_header"]
    df.to_csv(OUTPUT_FILE, mode="a", index=False, header=write_header, encoding="utf-8")
    state["written_header"] = True
    state["total_saved"] += len(df)
    return len(df)

def load_existing_ids():
    """Load existing grant IDs from the CSV (if any) to avoid duplicates."""
    if not os.path.exists(OUTPUT_FILE):
        return set()
    try:
        existing = pd.read_csv(OUTPUT_FILE, usecols=["id"])
        return set(existing["id"].dropna().astype(str))
    except Exception:
        print("⚠️ Could not load existing IDs — continuing without duplicate filter.")
        return set()

def main():
    print("🚀 Updating NSF Awards (continue → today)")
    print(f"Fields: {PRINT_FIELDS}")

    # Only reset if explicitly asked
    reset_files()

    # Load checkpoint
    state = load_checkpoint()
    print(f"🔁 Resuming from year {state['year']} offset {state['offset']} (saved so far: {state['total_saved']})")

    existing_ids = load_existing_ids()
    print(f"🧮 Loaded {len(existing_ids)} existing award IDs.")

    y = state["year"]
    start_ts = time.time()

    while y >= YEAR_START:
        offset = max(state["offset"], 1)
        page = 1 + (offset - 1) // RPP
        print(f"\n📆 Year {y} — offset {offset} (page {page})")

        while True:
            awards = fetch_page(y, offset)
            if not awards:
                print(f"✅ Finished year {y}.")
                y -= 1
                state["year"] = y
                state["offset"] = 1
                save_checkpoint(state)
                break

            df = pd.DataFrame(awards)
            added = append_chunk(df, state, existing_ids)
            save_checkpoint(state)

            count = len(df)
            print(f"   ✓ Fetched {count}, saved {added} new (total: {state['total_saved']})")

            if count < RPP:
                print(f"🎯 End of results for year {y}.")
                y -= 1
                state["year"] = y
                state["offset"] = 1
                save_checkpoint(state)
                break

            offset += RPP
            state["offset"] = offset
            save_checkpoint(state)
            time.sleep(PAUSE)

    mins = (time.time() - start_ts) / 60.0
    print("\n🏁 Completed update.")
    print(f"💾 Total awards saved: {state['total_saved']}")
    print(f"📄 File: {OUTPUT_FILE}")
    print(f"⏱️ Elapsed: {mins:.2f} min")

if __name__ == "__main__":
    main()
