"""
build.py — Fetches 3 Google Sheets (Reddit, Patreon, SubscribeStar) as CSV,
normalizes the data, and outputs audios.json for the Lurky Masterlist site.

Categories are fully dynamic: whatever is in the "Category" column of each
spreadsheet will appear in the JSON. The HTML reads them from the JSON.

Run manually:   python build.py
Run via GitHub Actions: see .github/workflows/build.yml
"""

import csv
import json
import re
import io
import os
import urllib.request
from datetime import datetime

# ─── Google Sheets published CSV URLs ────────────────────────────────
# Replace these with your actual published CSV URLs.
# To get them: Google Sheets → File → Share → Publish to web → CSV
# Use the format:
#   https://docs.google.com/spreadsheets/d/e/XXXXX/pub?gid=0&single=true&output=csv

REDDIT_CSV_URL   = os.environ.get("REDDIT_CSV_URL",   "YOUR_REDDIT_SHEET_CSV_URL_HERE")
PATREON_CSV_URL  = os.environ.get("PATREON_CSV_URL",  "YOUR_PATREON_SHEET_CSV_URL_HERE")
SUBSTAR_CSV_URL  = os.environ.get("SUBSTAR_CSV_URL",  "YOUR_SUBSTAR_SHEET_CSV_URL_HERE")

OUTPUT_FILE = "audios.json"


# ─── Category normalization ──────────────────────────────────────────
# Fixes typos and inconsistencies. Add more as needed.
CATEGORY_FIXES = {
    "ASME":       "ASMR",
    "Sadsim":     "Sadism",
    "Day-To-Day": "Day-to-Day",
    "Inteview":   "Interview",
    "Sci-Fi":     "Sci-fi",
    "iNCEST":     "Incest",
    "Beast ":     "Beast",
    "Sci-fi ":    "Sci-fi",
    "Fdom ":      "Fdom",
    "Incest ":    "Incest",
    "Fantasy ":   "Fantasy",
    "Sadism ":    "Sadism",
}

# Categories to exclude from filters (they still appear on entries)
# Remove or edit as you like
CATEGORY_EXCLUDE_FROM_FILTERS = set()


# ─── Portuguese month map (for SubscribeStar dates) ──────────────────
PT_MONTHS = {
    "jan": "01", "fev": "02", "mar": "03", "abr": "04",
    "mai": "05", "jun": "06", "jul": "07", "ago": "08",
    "set": "09", "out": "10", "nov": "11", "dez": "12",
}


# ─── Helpers ─────────────────────────────────────────────────────────

def fetch_csv(url):
    """Download a CSV from a URL and return list of dicts."""
    print(f"  Fetching {url[:80]}...")
    req = urllib.request.Request(url, headers={"User-Agent": "LurkyBuildBot/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        text = resp.read().decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


def normalize_category(raw):
    """Fix known typos/case issues."""
    raw = raw.strip()
    return CATEGORY_FIXES.get(raw, raw)


def parse_categories(raw):
    """Split comma-separated categories, normalize each."""
    if not raw or not raw.strip():
        return []
    parts = [normalize_category(c) for c in raw.split(",")]
    return [c for c in parts if c]


def parse_date_reddit(d):
    """Parse YYYY-MM-DD."""
    d = (d or "").strip()
    if re.match(r"\d{4}-\d{2}-\d{2}", d):
        return d[:10]
    return ""


def parse_date_patreon(d):
    """Parse YYYY-MM-DD (same format)."""
    return parse_date_reddit(d)


def parse_date_substar(d):
    """Parse Portuguese format: '21 jul., 2025' → '2025-07-21'."""
    d = (d or "").strip()
    m = re.match(r"(\d{1,2})\s+(\w+)\.?,?\s*(\d{4})", d)
    if m:
        day = m.group(1).zfill(2)
        month_abbr = m.group(2).lower()
        year = m.group(3)
        month = PT_MONTHS.get(month_abbr, "01")
        return f"{year}-{month}-{day}"
    # Fallback: try YYYY-MM-DD
    return parse_date_reddit(d)


def parse_duration(raw):
    """
    Normalize various duration formats to 'M:SS' or 'H:MM:SS'.
    Inputs:
      '20:36'      → '20:36'
      '32:00:00'   → '32:00'   (Google Sheets mangled MM:SS → HH:MM:SS)
      '01:44:47'   → '1:44:47' (actual long audio)
      '105:23:00'  → '105:23'  (mangled)
      '22m 52s'    → '22:52'
      '54m 27s'    → '54:27'
      '28m'        → '28:00'
      '186m 39s'   → '186:39'
    """
    raw = (raw or "").strip()
    if not raw:
        return ""

    # Format: NNm NNs or NNm
    m = re.match(r"(\d+)m\s*(?:(\d+)s)?", raw)
    if m:
        mins = int(m.group(1))
        secs = int(m.group(2) or 0)
        return f"{mins}:{secs:02d}"

    # Format: HH:MM:SS — could be real or mangled
    m = re.match(r"(\d+):(\d{2}):(\d{2})", raw)
    if m:
        h, mm, ss = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if ss == 0 and h < 200:
            # Likely mangled: original was MM:SS, Sheets added :00
            # e.g. 32:00:00 was really 32:00, 105:23:00 was really 105:23
            return f"{h}:{mm:02d}"
        else:
            # Genuine long audio (e.g. 1:44:47)
            return f"{h}:{mm:02d}:{ss:02d}"

    # Format: MM:SS
    m = re.match(r"(\d+):(\d{2})", raw)
    if m:
        return f"{int(m.group(1))}:{int(m.group(2)):02d}"

    return raw  # Return as-is if unrecognized


def parse_tags(raw):
    """Extract tags, strip formatting brackets, limit to 15."""
    raw = (raw or "").strip()
    if not raw:
        return []

    # If tags are in [bracket] format
    bracket_tags = re.findall(r"\[([^\]]+)\]", raw)
    if bracket_tags:
        tags = [t.strip() for t in bracket_tags]
    else:
        # Comma-separated
        tags = [t.strip() for t in raw.split(",")]

    # Filter out audience tags (F4M etc), empties, very long ones
    filtered = []
    for t in tags:
        t = t.strip()
        if not t or len(t) > 60:
            continue
        if re.match(r"^[FMA]\d?4[FMATFNB]+$", t, re.I):
            continue
        filtered.append(t)

    return filtered[:15]


def make_slug(title, index, prefix):
    """Create a URL-friendly ID."""
    slug = re.sub(r"[^\w\s-]", "", title.lower())
    slug = re.sub(r"[\s_]+", "-", slug).strip("-")
    slug = slug[:50]
    return f"{slug}-{prefix}{index}"


def build_writer_link(writer):
    """Generate a Reddit profile link if writer looks like a username."""
    if not writer:
        return ""
    writer = writer.strip()
    if writer.startswith("u/"):
        writer = writer[2:]
    # Only link if it looks like a reddit username
    if re.match(r"^[\w_-]+$", writer) and len(writer) > 1:
        return f"https://reddit.com/u/{writer}"
    return ""


# ─── Process each source ─────────────────────────────────────────────

def process_reddit(rows):
    """Process Reddit CSV rows into normalized audio entries."""
    entries = []
    for i, r in enumerate(rows):
        title = (r.get("Title") or "").strip()
        if not title:
            continue

        categories = parse_categories(r.get("Category", ""))
        writer = (r.get("Writer") or "").strip()

        entry = {
            "id": make_slug(title, i, "r"),
            "title": title,
            "categories": categories,
            "description": (r.get("Description") or "").strip(),
            "date": parse_date_reddit(r.get("Date", "")),
            "tags": parse_tags(r.get("Tags", "")),
            "exclusive": False,
            "platform": "reddit",
            "writer": writer,
            "writerLink": build_writer_link(writer),
            "largeCollab": False,
            "collabPartners": (r.get("Collab Partners") or "").strip(),
            "duration": parse_duration(r.get("Duration", "")),
            "links": {},
        }

        post_link = (r.get("Post Link") or "").strip()
        script_link = (r.get("Script Link") or "").strip()
        if post_link:
            entry["links"]["reddit"] = post_link
        if script_link:
            entry["links"]["script"] = script_link

        # Mark large collabs (4+ partners)
        partners = entry["collabPartners"]
        if partners and partners.count(",") >= 3:
            entry["largeCollab"] = True

        entries.append(entry)

    return entries


def process_patreon(rows):
    """Process Patreon CSV rows into normalized audio entries."""
    entries = []
    for i, r in enumerate(rows):
        title = (r.get("Title") or "").strip()
        if not title:
            continue

        # Patreon may or may not have a Category column
        categories = parse_categories(r.get("Category", ""))
        writer = (r.get("Writer") or "").strip()

        entry = {
            "id": make_slug(title, i, "p"),
            "title": title,
            "categories": categories,
            "description": (r.get("Description") or "").strip(),
            "date": parse_date_patreon(r.get("Date", "")),
            "tags": parse_tags(r.get("Tags", "")),
            "exclusive": True,
            "platform": "patreon",
            "writer": writer,
            "writerLink": build_writer_link(writer),
            "largeCollab": False,
            "collabPartners": (r.get("Collab Partners") or "").strip(),
            "duration": parse_duration(r.get("Duration", "")),
            "links": {},
        }

        post_link = (r.get("Post Link") or "").strip()
        script_link = (r.get("Script Link") or "").strip()
        if post_link:
            entry["links"]["patreon"] = post_link
        if script_link:
            entry["links"]["script"] = script_link

        if entry["collabPartners"] and entry["collabPartners"].count(",") >= 3:
            entry["largeCollab"] = True

        entries.append(entry)

    return entries


def process_substar(rows):
    """Process SubscribeStar CSV rows into normalized audio entries."""
    entries = []
    for i, r in enumerate(rows):
        title = (r.get("Title") or "").strip()
        if not title:
            continue

        categories = parse_categories(r.get("Category", ""))
        writer = (r.get("Writer") or "").strip()

        entry = {
            "id": make_slug(title, i, "s"),
            "title": title,
            "categories": categories,
            "description": (r.get("Description") or "").strip(),
            "date": parse_date_substar(r.get("Date", "")),
            "tags": parse_tags(r.get("Tags", "")),
            "exclusive": True,
            "platform": "subscribestar",
            "writer": writer,
            "writerLink": build_writer_link(writer),
            "largeCollab": False,
            "collabPartners": (r.get("Collab Partners") or "").strip(),
            "duration": parse_duration(r.get("Duration", "")),
            "links": {},
        }

        post_link = (r.get("Post Link") or "").strip()
        script_link = (r.get("Script Link") or "").strip()
        if post_link:
            entry["links"]["subscribestar"] = post_link
        if script_link:
            entry["links"]["script"] = script_link

        if entry["collabPartners"] and entry["collabPartners"].count(",") >= 3:
            entry["largeCollab"] = True

        entries.append(entry)

    return entries


# ─── Main ────────────────────────────────────────────────────────────

def main():
    print("Building audios.json...")

    # Fetch CSVs
    print("\n1. Fetching spreadsheets...")
    reddit_rows  = fetch_csv(REDDIT_CSV_URL)
    patreon_rows = fetch_csv(PATREON_CSV_URL)
    substar_rows = fetch_csv(SUBSTAR_CSV_URL)

    print(f"   Reddit: {len(reddit_rows)} rows")
    print(f"   Patreon: {len(patreon_rows)} rows")
    print(f"   SubscribeStar: {len(substar_rows)} rows")

    # Process
    print("\n2. Processing entries...")
    all_entries = []
    all_entries.extend(process_reddit(reddit_rows))
    all_entries.extend(process_patreon(patreon_rows))
    all_entries.extend(process_substar(substar_rows))

    print(f"   Total entries: {len(all_entries)}")

    # Sort by date descending (newest first)
    all_entries.sort(key=lambda e: e.get("date", ""), reverse=True)

    # Collect all unique categories dynamically
    all_categories = set()
    for entry in all_entries:
        for cat in entry["categories"]:
            all_categories.add(cat)

    # Remove excluded categories from filters (but keep on entries)
    filter_categories = sorted(all_categories - CATEGORY_EXCLUDE_FROM_FILTERS)

    print(f"   Categories found: {len(filter_categories)}")
    print(f"   {filter_categories}")

    # Remove empty fields to keep JSON lean
    print("\n3. Cleaning up...")
    for entry in all_entries:
        # Remove empty optional fields
        for key in ["writer", "writerLink", "collabPartners", "duration", "description"]:
            if not entry.get(key):
                entry.pop(key, None)
        if not entry.get("largeCollab"):
            entry.pop("largeCollab", None)

    # Build final JSON
    output = {
        "lastUpdated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "totalEntries": len(all_entries),
        "categories": filter_categories,
        "audios": all_entries,
    }

    # Write
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Done! Wrote {len(all_entries)} entries to {OUTPUT_FILE}")
    print(f"   Categories: {', '.join(filter_categories)}")

    # Stats
    platforms = {}
    for e in all_entries:
        p = e.get("platform", "unknown")
        platforms[p] = platforms.get(p, 0) + 1
    for p, c in sorted(platforms.items()):
        print(f"   {p}: {c}")

    exclusive = sum(1 for e in all_entries if e.get("exclusive"))
    with_dur = sum(1 for e in all_entries if e.get("duration"))
    print(f"   Exclusive: {exclusive}")
    print(f"   With duration: {with_dur}")


if __name__ == "__main__":
    main()
