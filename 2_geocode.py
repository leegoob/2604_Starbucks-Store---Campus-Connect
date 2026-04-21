# -*- coding: utf-8 -*-
"""Geocode addresses with geopy/Nominatim -> processed_data.csv. Run after import_excel_to_csv.py."""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd
from geopy.exc import GeocoderRateLimited, GeocoderTimedOut, GeocoderUnavailable
from geopy.geocoders import Nominatim

BASE = Path(__file__).resolve().parent
RAW_STORES = BASE / "raw_stores.csv"
RAW_SCHOOLS = BASE / "raw_schools.csv"
OUT = BASE / "processed_data.csv"
CHECKPOINT = BASE / "processed_data.checkpoint.csv"
USER_AGENT = "starbucks_school_finder_edu_demo/1.0"

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


def geocode_one(geolocator: Nominatim, address: str) -> tuple[float | None, float | None]:
    """Nominatim 호출. 429(속도 제한)이면 서버가 알려준 시간만큼 쉬었다가 재시도."""
    max_attempts = 12
    for attempt in range(max_attempts):
        try:
            loc = geolocator.geocode(address, country_codes="kr", language="ko")
            if loc is None:
                return None, None
            return float(loc.latitude), float(loc.longitude)
        except GeocoderRateLimited as e:
            wait = float(e.retry_after) if e.retry_after is not None else 60.0
            wait = max(wait, 5.0)
            print(
                f"  [429 속도제한] {wait:.0f}초 대기 후 재시도… ({attempt + 1}/{max_attempts})",
                flush=True,
            )
            time.sleep(wait)
        except (GeocoderTimedOut, GeocoderUnavailable):
            time.sleep(1.0 + attempt * 0.5)
    return None, None


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


def _write_checkpoint(df: pd.DataFrame, path: Path) -> None:
    tmp = path.with_suffix(".tmp.csv")
    df.to_csv(tmp, index=False, encoding="utf-8-sig")
    tmp.replace(path)


def main() -> None:
    ap = argparse.ArgumentParser(description="Nominatim 지오코딩 -> processed_data.csv")
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="테스트용: 매장·학교 각각 앞에서 N행만 처리 (생략 시 전체)",
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=None,
        help=f"출력 CSV 경로 (기본: {OUT.name})",
    )
    ap.add_argument(
        "--pause",
        type=float,
        default=2.0,
        metavar="초",
        help="주소 한 건 처리 후 대기 시간(초). Nominatim 무료 서버는 초당 1회 이하 권장. 기본 2초.",
    )
    ap.add_argument(
        "--stores-only",
        action="store_true",
        help="매장(raw_stores)만 지오코딩. 기존 processed_data.csv의 학교 행은 유지.",
    )
    ap.add_argument(
        "--schools-only",
        action="store_true",
        help="학교(raw_schools)만 지오코딩. 기존 processed_data.csv의 매장 행은 유지. 매장은 나중에 반영할 때 사용.",
    )
    args = ap.parse_args()
    out_path = Path(args.out).resolve() if args.out else OUT

    if not RAW_STORES.is_file() or not RAW_SCHOOLS.is_file():
        raise SystemExit("Missing raw CSVs. Run: python import_excel_to_csv.py")

    stores_only = bool(args.stores_only)
    schools_only = bool(args.schools_only)
    if stores_only and schools_only:
        raise SystemExit("--stores-only 과 --schools-only 는 함께 쓸 수 없습니다.")

    geolocator = Nominatim(user_agent=USER_AGENT, timeout=15)

    stores = pd.read_csv(RAW_STORES, encoding="utf-8-sig")
    schools = pd.read_csv(RAW_SCHOOLS, encoding="utf-8-sig")
    if args.limit is not None:
        n = max(0, int(args.limit))
        if stores_only:
            stores = stores.head(n)
        elif schools_only:
            schools = schools.head(n)
        else:
            stores = stores.head(n)
            schools = schools.head(n)
        if stores_only:
            lim_msg = f"매장 {len(stores)}행만"
        elif schools_only:
            lim_msg = f"학교 {len(schools)}행만"
        else:
            lim_msg = f"매장 {len(stores)}행, 학교 {len(schools)}행"
        print(f"[--limit {n}] {lim_msg} → {out_path}", flush=True)

    pause = max(1.0, float(args.pause))
    store_keep: pd.DataFrame | None = None
    school_keep: pd.DataFrame | None = None

    if stores_only:
        if not out_path.is_file():
            raise SystemExit(
                f"{out_path.name} 이 없습니다. 처음 한 번은 전체 실행: python 2_geocode.py"
            )
        prev = pd.read_csv(out_path, encoding="utf-8-sig")
        if "entity_type" not in prev.columns:
            raise SystemExit("processed_data 형식이 올바르지 않습니다. 전체 실행하세요.")
        school_keep = prev[prev["entity_type"] == "school"].copy()
        if school_keep.empty:
            raise SystemExit("저장된 파일에 학교 행이 없습니다. 전체 실행: python 2_geocode.py")
        total_n = len(stores)
        print(
            f"[매장만] 매장 {total_n}건 지오코딩, 학교 {len(school_keep)}건은 기존 파일 유지, 간격 {pause}초",
            flush=True,
        )
    elif schools_only:
        if not out_path.is_file():
            raise SystemExit(
                f"{out_path.name} 이 없습니다. 매장 데이터가 있어야 합니다. "
                "한 번이라도 전체 지오코딩을 했거나, 매장만 먼저 반영하세요."
            )
        prev = pd.read_csv(out_path, encoding="utf-8-sig")
        if "entity_type" not in prev.columns:
            raise SystemExit("processed_data 형식이 올바르지 않습니다.")
        store_keep = prev[prev["entity_type"] == "store"].copy()
        if store_keep.empty:
            raise SystemExit("저장된 파일에 매장 행이 없습니다. 먼저 매장이 포함되도록 지오코딩하세요.")
        total_n = len(schools)
        print(
            f"[학교만] 매장 {len(store_keep)}건은 기존 파일 유지, 학교 {total_n}건 지오코딩, 간격 {pause}초",
            flush=True,
        )
    else:
        total_n = len(stores) + len(schools)
        print(
            f"총 {total_n}건 지오코딩, 요청 간격 {pause}초 (끊기면 나중에 같은 명령으로 다시 실행)",
            flush=True,
        )
    done = 0

    rows: list[dict] = []
    checkpoint_every = 50
    since_checkpoint = 0
    cp_store_done: set[tuple[str, str]] = set()
    cp_school_done: set[tuple[str, str, str]] = set()
    cp_df = pd.DataFrame(columns=PROCESSED_COLUMN_ORDER)

    # 전체 모드에서만 체크포인트 자동 이어하기
    if (not stores_only) and (not schools_only) and CHECKPOINT.is_file():
        try:
            cp_df = pd.read_csv(CHECKPOINT, encoding="utf-8-sig").reindex(columns=PROCESSED_COLUMN_ORDER)
            cp_df = cp_df.dropna(subset=["latitude", "longitude"])
            cp_st = cp_df[cp_df["entity_type"] == "store"].copy()
            cp_sc = cp_df[cp_df["entity_type"] == "school"].copy()
            cp_store_done = {
                (str(r["name"]).strip(), str(r["address"]).strip())
                for _, r in cp_st.iterrows()
            }
            cp_school_done = {
                (str(r["name"]).strip(), str(r["school_type"]).strip(), str(r["address"]).strip())
                for _, r in cp_sc.iterrows()
            }
            rows.extend(cp_df.to_dict(orient="records"))
            done = len(cp_store_done) + len(cp_school_done)
            if done:
                print(f"[resume] 체크포인트 복구: {done}/{total_n} 완료 상태", flush=True)
        except Exception:
            print("[resume] 체크포인트를 읽지 못해 처음부터 다시 진행합니다.", flush=True)
            cp_store_done = set()
            cp_school_done = set()
            cp_df = pd.DataFrame(columns=PROCESSED_COLUMN_ORDER)
            rows = []
            done = 0

    if not schools_only:
        for _, r in stores.iterrows():
            name = str(r["name"]).strip()
            addr = str(r["address"]).strip()
            if (not stores_only) and (not schools_only) and (name, addr) in cp_store_done:
                continue
            lat, lon = geocode_one(geolocator, addr)
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
            if lat is not None and lon is not None:
                rows.append(row)
                since_checkpoint += 1
                if (not stores_only) and (not schools_only):
                    cp_store_done.add((name, addr))
            done += 1
            if done % 25 == 0 or done == total_n:
                print(f"  진행 {done}/{total_n}", flush=True)
            if (not stores_only) and (not schools_only) and since_checkpoint >= checkpoint_every:
                _write_checkpoint(pd.DataFrame(rows).reindex(columns=PROCESSED_COLUMN_ORDER), CHECKPOINT)
                since_checkpoint = 0
            time.sleep(pause)

    if not stores_only:
        for _, r in schools.iterrows():
            name = str(r["name"]).strip()
            stype = str(r["school_type"]).strip()
            addr = str(r["address"]).strip()
            if (not stores_only) and (not schools_only) and (name, stype, addr) in cp_school_done:
                continue
            lat, lon = geocode_one(geolocator, addr)
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
            if lat is not None and lon is not None:
                rows.append(row)
                since_checkpoint += 1
                if (not stores_only) and (not schools_only):
                    cp_school_done.add((name, stype, addr))
            done += 1
            if done % 25 == 0 or done == total_n:
                print(f"  진행 {done}/{total_n}", flush=True)
            if (not stores_only) and (not schools_only) and since_checkpoint >= checkpoint_every:
                _write_checkpoint(pd.DataFrame(rows).reindex(columns=PROCESSED_COLUMN_ORDER), CHECKPOINT)
                since_checkpoint = 0
            time.sleep(pause)

    store_df = pd.DataFrame(rows)
    if stores_only and school_keep is not None:
        out_df = pd.concat(
            [
                store_df.reindex(columns=PROCESSED_COLUMN_ORDER),
                school_keep.reindex(columns=PROCESSED_COLUMN_ORDER),
            ],
            ignore_index=True,
            sort=False,
        )
    elif schools_only and store_keep is not None:
        school_df = store_df
        out_df = pd.concat(
            [
                store_keep.reindex(columns=PROCESSED_COLUMN_ORDER),
                school_df.reindex(columns=PROCESSED_COLUMN_ORDER),
            ],
            ignore_index=True,
            sort=False,
        )
    else:
        out_df = store_df
    out_df = out_df.reindex(columns=PROCESSED_COLUMN_ORDER)
    before = len(out_df)
    out_df = out_df.dropna(subset=["latitude", "longitude"])
    dropped = before - len(out_df)
    if dropped:
        print("Warning: dropped", dropped, "rows without coordinates.", flush=True)

    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print("Saved:", out_path, "rows:", len(out_df), flush=True)
    if (not stores_only) and (not schools_only) and CHECKPOINT.is_file():
        CHECKPOINT.unlink(missing_ok=True)
        print("체크포인트 정리 완료.", flush=True)
    print("Next: streamlit run 3_app.py", flush=True)


if __name__ == "__main__":
    main()
