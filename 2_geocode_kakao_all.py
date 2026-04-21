# -*- coding: utf-8 -*-
"""
Kakao Local API: raw_stores + raw_schools -> processed_data.csv (full, fast).

Requires REST API key: KAKAO_REST_API_KEY or .streamlit/secrets.toml kakao_rest_api_key.

  python import_excel_to_csv.py
  python 2_geocode_kakao_all.py

  # Nominatim(2_geocode.py)으로 매장 좌표가 거의 안 나온 경우, 매장만 다시:
  python 2_geocode_kakao_all.py --stores-only
"""

from __future__ import annotations

import argparse
import os
import re
import time
from pathlib import Path

import pandas as pd
import requests

BASE = Path(__file__).resolve().parent
RAW_STORES = BASE / "raw_stores.csv"
RAW_SCHOOLS = BASE / "raw_schools.csv"
OUT = BASE / "processed_data.csv"
OUT_ALT = BASE / "processed_data_kakao_output.csv"
ADDRESS_URL = "https://dapi.kakao.com/v2/local/search/address.json"
KEYWORD_URL = "https://dapi.kakao.com/v2/local/search/keyword.json"

SCHOOL_EXTRA_COLS = (
    "campus_kind",
    "contact_office",
    "contact_name",
    "contact_phone",
    "contact_email",
)
STORE_EXTRA_COLS = ("ops_team", "store_region")

PROCESSED_COLUMN_ORDER = [
    "entity_type",
    "name",
    "school_type",
    "address",
    "latitude",
    "longitude",
    "ops_team",
    "store_region",
    "campus_kind",
    "contact_office",
    "contact_name",
    "contact_phone",
    "contact_email",
]


def _school_extras(r: pd.Series) -> dict[str, str]:
    out: dict[str, str] = {}
    for k in SCHOOL_EXTRA_COLS:
        if k not in r.index:
            out[k] = ""
            continue
        v = r[k]
        out[k] = str(v).strip() if pd.notna(v) else ""
    return out


def _store_extras(r: pd.Series) -> dict[str, str]:
    out: dict[str, str] = {}
    for k in STORE_EXTRA_COLS:
        if k not in r.index:
            out[k] = ""
            continue
        v = r[k]
        out[k] = str(v).strip() if pd.notna(v) else ""
    return out


def load_rest_key() -> str:
    k = os.environ.get("KAKAO_REST_API_KEY", "").strip()
    if k:
        return k
    sec = BASE / ".streamlit" / "secrets.toml"
    if sec.is_file():
        try:
            text = sec.read_text(encoding="utf-8")
            m = re.search(r'kakao_rest_api_key\s*=\s*"([^"]*)"', text)
            if m:
                return m.group(1).strip()
        except OSError:
            pass
    return ""


def _normalize_addr(s: str) -> str:
    s = str(s or "").strip()
    if s.lower() in ("nan", "none", ""):
        return ""
    s = re.sub(r"\s+", " ", s)
    return s


def geocode_address(
    session: requests.Session,
    headers: dict[str, str],
    address: str,
) -> tuple[float | None, float | None]:
    addr = _normalize_addr(address)
    if not addr:
        return None, None
    for attempt in range(3):
        try:
            r = session.get(
                ADDRESS_URL,
                headers=headers,
                params={"query": addr, "size": 1},
                timeout=20,
            )
            if r.status_code == 429:
                time.sleep(1.0 + attempt)
                continue
            r.raise_for_status()
            data = r.json()
            docs = data.get("documents") or []
            if not docs:
                return None, None
            d0 = docs[0]
            return float(d0["y"]), float(d0["x"])
        except (requests.RequestException, KeyError, ValueError, TypeError):
            time.sleep(0.3 * (attempt + 1))
    return None, None


def geocode_keyword(
    session: requests.Session,
    headers: dict[str, str],
    query: str,
) -> tuple[float | None, float | None]:
    q = _normalize_addr(query)[:100]
    if not q:
        return None, None
    for attempt in range(2):
        try:
            r = session.get(
                KEYWORD_URL,
                headers=headers,
                params={"query": q, "size": 5},
                timeout=20,
            )
            if r.status_code == 429:
                time.sleep(1.0 + attempt)
                continue
            r.raise_for_status()
            data = r.json()
            docs = data.get("documents") or []
            if not docs:
                return None, None
            d0 = docs[0]
            return float(d0["y"]), float(d0["x"])
        except (requests.RequestException, KeyError, ValueError, TypeError):
            time.sleep(0.3 * (attempt + 1))
    return None, None


def resolve_store_coords(
    session: requests.Session,
    headers: dict[str, str],
    name: str,
    address: str,
    pause: float,
) -> tuple[float | None, float | None]:
    lat, lon = geocode_address(session, headers, address)
    if lat is not None and lon is not None:
        return lat, lon
    time.sleep(pause)
    lat, lon = geocode_keyword(session, headers, name)
    if lat is not None and lon is not None:
        return lat, lon
    time.sleep(pause)
    short = _normalize_addr(address)[:40]
    if short and short != _normalize_addr(address):
        return geocode_address(session, headers, short)
    return None, None


def resolve_school_coords(
    session: requests.Session,
    headers: dict[str, str],
    name: str,
    address: str,
    pause: float,
) -> tuple[float | None, float | None]:
    lat, lon = geocode_address(session, headers, address)
    if lat is not None and lon is not None:
        return lat, lon
    time.sleep(pause)
    lat, lon = geocode_keyword(session, headers, name)
    if lat is not None and lon is not None:
        return lat, lon
    time.sleep(pause)
    short = _normalize_addr(address)[:40]
    if short and short != _normalize_addr(address):
        return geocode_address(session, headers, short)
    return None, None


def write_csv_atomic(df: pd.DataFrame, path: Path) -> Path:
    tmp = path.with_suffix(".tmp.csv")
    df.to_csv(tmp, index=False, encoding="utf-8-sig")
    try:
        os.replace(tmp, path)
    except PermissionError:
        if tmp.is_file():
            tmp.unlink(missing_ok=True)
        df.to_csv(OUT_ALT, index=False, encoding="utf-8-sig")
        print(
            f"\n[note] {path.name} is locked. Saved to {OUT_ALT.name}.",
            flush=True,
        )
        return OUT_ALT
    return path


def main() -> None:
    ap = argparse.ArgumentParser(description="Kakao: full geocode stores+schools -> processed_data.csv")
    ap.add_argument("--limit", type=int, default=None, help="Test: first N stores and N schools only")
    ap.add_argument(
        "--stores-only",
        action="store_true",
        help="매장만 카카오 지오코딩. 기존 processed_data.csv 의 학교 행은 그대로 둡니다(API·시간 절약).",
    )
    ap.add_argument(
        "--pause",
        type=float,
        default=0.09,
        help="Seconds between requests (default 0.09)",
    )
    args = ap.parse_args()

    rest_key = load_rest_key()
    if not rest_key:
        raise SystemExit(
            "Missing KAKAO_REST_API_KEY or kakao_rest_api_key in .streamlit/secrets.toml"
        )

    if not RAW_STORES.is_file() or not RAW_SCHOOLS.is_file():
        raise SystemExit("Run: python import_excel_to_csv.py")

    stores_only = bool(args.stores_only)
    school_keep: pd.DataFrame | None = None
    if stores_only:
        if not OUT.is_file():
            raise SystemExit(
                f"{OUT.name} 이 없습니다. 처음 한 번은 전체 실행: python 2_geocode_kakao_all.py"
            )
        prev = pd.read_csv(OUT, encoding="utf-8-sig")
        if "entity_type" not in prev.columns:
            raise SystemExit("processed_data 형식이 맞지 않습니다. 전체 실행하세요.")
        school_keep = prev[prev["entity_type"] == "school"].copy()
        if school_keep.empty:
            raise SystemExit("기존 파일에 학교 행이 없습니다. 전체 실행: python 2_geocode_kakao_all.py")

    stores = pd.read_csv(RAW_STORES, encoding="utf-8-sig")
    schools = pd.read_csv(RAW_SCHOOLS, encoding="utf-8-sig")
    if args.limit is not None:
        n = max(0, int(args.limit))
        stores = stores.head(n)
        if not stores_only:
            schools = schools.head(n)
        print(f"[--limit] stores {len(stores)}, schools {len(schools)}", flush=True)

    pause = max(0.03, float(args.pause))
    total = len(stores) + (0 if stores_only else len(schools))
    print(
        f"Kakao geocode: stores {len(stores)}"
        + (f" + schools {len(schools)} = {total}" if not stores_only else f" (학교 {len(school_keep)}행 유지)")
        + f", pause {pause}s",
        flush=True,
    )

    headers = {"Authorization": f"KakaoAK {rest_key}"}
    session = requests.Session()
    rows: list[dict] = []
    failed: list[str] = []
    done = 0

    for _, r in stores.iterrows():
        name = str(r["name"]).strip()
        addr = str(r["address"]).strip()
        lat, lon = resolve_store_coords(session, headers, name, addr, pause)
        row = {
            "entity_type": "store",
            "name": name,
            "school_type": "",
            "address": addr,
            "latitude": lat,
            "longitude": lon,
        }
        row.update(_store_extras(r))
        for k in SCHOOL_EXTRA_COLS:
            row[k] = ""
        rows.append(row)
        if lat is None or lon is None:
            failed.append(f"store | {name} | {addr[:80]}")
        done += 1
        if done % 50 == 0 or done == total:
            print(f"  progress {done}/{total}", flush=True)
        time.sleep(pause)

    if not stores_only:
        for _, r in schools.iterrows():
            name = str(r["name"]).strip()
            stype = str(r["school_type"]).strip()
            addr = str(r["address"]).strip()
            lat, lon = resolve_school_coords(session, headers, name, addr, pause)
            row = {
                "entity_type": "school",
                "name": name,
                "school_type": stype,
                "address": addr,
                "latitude": lat,
                "longitude": lon,
            }
            row.update(_school_extras(r))
            for k in STORE_EXTRA_COLS:
                row[k] = ""
            rows.append(row)
            if lat is None or lon is None:
                failed.append(f"school | {name} | {addr[:80]}")
            done += 1
            if done % 50 == 0 or done == total:
                print(f"  progress {done}/{total}", flush=True)
            time.sleep(pause)

    store_df = pd.DataFrame(rows).reindex(columns=PROCESSED_COLUMN_ORDER)
    if stores_only and school_keep is not None:
        before_st = len(store_df)
        store_df = store_df.dropna(subset=["latitude", "longitude"])
        dropped_st = before_st - len(store_df)
        if dropped_st:
            print(f"Warning: 매장 중 좌표 없음 제외: {dropped_st}", flush=True)
        out_df = pd.concat(
            [store_df, school_keep.reindex(columns=PROCESSED_COLUMN_ORDER)],
            ignore_index=True,
            sort=False,
        )
    else:
        out_df = store_df
        before = len(out_df)
        out_df = out_df.dropna(subset=["latitude", "longitude"])
        dropped = before - len(out_df)
        if dropped:
            print(f"Warning: dropped rows without coords: {dropped}", flush=True)

    saved = write_csv_atomic(out_df, OUT)
    n_store_out = int((out_df["entity_type"] == "store").sum())
    n_school_out = int((out_df["entity_type"] == "school").sum())
    print(
        f"Saved {saved} rows={len(out_df)} (매장 {n_store_out}, 학교 {n_school_out})",
        flush=True,
    )
    if failed:
        fp = BASE / "kakao_geocode_failed.txt"
        fp.write_text("\n".join(failed), encoding="utf-8")
        print(f"Failed coords: {len(failed)} -> {fp.name}", flush=True)
    print("Next: streamlit run 3_app.py", flush=True)


if __name__ == "__main__":
    main()
