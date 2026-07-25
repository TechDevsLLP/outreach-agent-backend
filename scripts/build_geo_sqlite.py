"""
build_geo_sqlite.py

Build the local SQLite gazetteer (data/geo_places.sqlite) from GeoNames files.
Replaces the Mongo `geo_places` collection (~200MB Atlas, 5.5h load, regex scans)
with a ~60MB read-only file: sub-ms indexed lookups, zero Atlas storage.

Usage:
    python3 scripts/build_geo_sqlite.py --geonames-dir ./geonames [--out data/geo_places.sqlite]

GeoNames inputs (download from https://download.geonames.org/export/dump/):
    cities500.zip  -> cities500.txt
    admin1CodesASCII.txt
"""

import argparse
import logging
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.geo_resolver import parse_geonames_line

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
logger = logging.getLogger(__name__)

# Keep primary + ascii name always, plus all Latin-script alternate names —
# location strings from LinkedIn/Apollo are Latin, and CJK/Cyrillic/etc.
# aliases would only bloat the file without ever being queried.
def _is_latin(s: str) -> bool:
    return all(ord(ch) < 0x250 for ch in s)

SCHEMA = """
CREATE TABLE places (
    place_id     TEXT PRIMARY KEY,
    city         TEXT,
    region       TEXT,
    country      TEXT,
    country_code TEXT,
    continent    TEXT,
    population   INTEGER,
    lat          REAL,
    lng          REAL
);
CREATE TABLE names (
    name_lower   TEXT NOT NULL,
    country_code TEXT,
    population   INTEGER,
    place_id     TEXT NOT NULL REFERENCES places(place_id)
);
"""

INDEXES = """
CREATE INDEX idx_names_lookup ON names(name_lower, country_code, population DESC);
"""


def build(geonames_dir: str, out_path: str) -> None:
    cities = os.path.join(geonames_dir, "cities500.txt")
    admin1 = os.path.join(geonames_dir, "admin1CodesASCII.txt")
    for f in (cities, admin1):
        if not os.path.exists(f):
            raise SystemExit(f"Required GeoNames file missing: {f}")

    admin1_map = {}
    with open(admin1, encoding="utf-8") as fh:
        for line in fh:
            cols = line.rstrip("\n\r").split("\t")
            if len(cols) >= 2 and cols[0].strip() and cols[1].strip():
                admin1_map[cols[0].strip()] = cols[1].strip()
    logger.info("Loaded %d admin1 codes", len(admin1_map))

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    tmp_path = out_path + ".tmp"
    if os.path.exists(tmp_path):
        os.remove(tmp_path)
    conn = sqlite3.connect(tmp_path)
    conn.executescript(SCHEMA)
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA synchronous = OFF")

    n_places = 0
    n_names = 0
    place_rows, name_rows = [], []
    with open(cities, encoding="utf-8") as fh:
        for line in fh:
            doc = parse_geonames_line(line, admin1_map)
            if doc is None:
                continue
            coords = (doc.get("geo") or {}).get("coordinates") or [None, None]
            place_rows.append((
                doc["place_id"], doc.get("city") or doc.get("name"), doc.get("region"),
                doc.get("country"), doc.get("country_code"), doc.get("continent"),
                doc.get("population") or 0, coords[1], coords[0],
            ))
            seen = set()
            names = [doc.get("name"), doc.get("ascii_name")]
            names += [n for n in (doc.get("alt_names") or []) if _is_latin(n)]
            for nm in names:
                if not nm:
                    continue
                key = nm.lower()
                if key in seen:
                    continue
                seen.add(key)
                name_rows.append((key, doc.get("country_code"),
                                  doc.get("population") or 0, doc["place_id"]))
            n_places += 1
            if len(place_rows) >= 5000:
                conn.executemany("INSERT OR REPLACE INTO places VALUES (?,?,?,?,?,?,?,?,?)", place_rows)
                conn.executemany("INSERT INTO names VALUES (?,?,?,?)", name_rows)
                n_names += len(name_rows)
                place_rows, name_rows = [], []
    if place_rows:
        conn.executemany("INSERT OR REPLACE INTO places VALUES (?,?,?,?,?,?,?,?,?)", place_rows)
        conn.executemany("INSERT INTO names VALUES (?,?,?,?)", name_rows)
        n_names += len(name_rows)

    logger.info("Inserted %d places, %d name rows; building index...", n_places, n_names)
    conn.executescript(INDEXES)
    conn.execute("ANALYZE")
    conn.commit()
    conn.execute("VACUUM")
    conn.close()
    os.replace(tmp_path, out_path)
    logger.info("Done: %s (%.1f MB)", out_path, os.path.getsize(out_path) / 1e6)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--geonames-dir", required=True)
    ap.add_argument("--out", default=os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "geo_places.sqlite"))
    args = ap.parse_args()
    build(args.geonames_dir, args.out)
