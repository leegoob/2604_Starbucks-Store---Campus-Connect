# -*- coding: utf-8 -*-
"""
새 stores.xlsx (또는 raw_stores.csv) 를 기준으로 매장 마스터를 안전하게 동기화한다.

처리 흐름:
  1) 백업: 현재 raw_stores.csv, processed_data.csv 를 _backups/ 로 자동 백업
  2) 비교: 새 마스터 vs 기존 processed_data.csv(store) 를 매장명 기준으로 매칭
     - 신규(added)        : 새 파일에는 있는데 기존에 없음 → 지오코딩 필요
     - 삭제(removed)      : 기존에는 있는데 새 파일에 없음 → 제거
     - 주소변경(changed)  : 매장명 동일, 주소 다름 → 좌표 재계산
     - 동일(unchanged)    : 좌표 그대로 유지. 운영팀/권역만 새 파일 값으로 갱신
  3) 카카오 API 로 신규+주소변경 매장만 지오코딩
  4) raw_stores.csv 와 processed_data.csv(store 부분) 갱신, 학교 행은 그대로 유지

사용법 (PowerShell/Git Bash):
    # 새 stores.xlsx 를 폴더에 두고 실행
    python 2_sync_stores.py

    # dry-run: 무엇이 바뀔지만 미리 보기 (API 호출 X, 파일 변경 X)
    python 2_sync_stores.py --dry-run

    # 원본 CSV 를 직접 갈아끼웠을 때 (xlsx 없이)
    python 2_sync_stores.py --source raw_stores.csv
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

BASE = Path(__file__).resolve().parent
STORES_XLSX = BASE / "stores.xlsx"
RAW_STORES = BASE / "raw_stores.csv"
PROCESSED = BASE / "processed_data.csv"
BACKUP_DIR = BASE / "_backups"
FAIL_LOG = BASE / "kakao_sync_stores_failed.csv"

ADDRESS_URL = "https://dapi.kakao.com/v2/local/search/address.json"
KEYWORD_URL = "https://dapi.kakao.com/v2/local/search/keyword.json"

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
]

STORE_RENAME = {
    "매장명": "name",
    "주소": "address",
    "운영팀": "ops_team",
    "권역": "store_region",
}


def load_rest_key() -> str:
    k = os.environ.get("KAKAO_REST_API_KEY", "").strip()
    if k:
        return k
    sec = BASE / ".streamlit" / "secrets.toml"
    if sec.is_file():
        text = sec.read_text(encoding="utf-8")
        m = re.search(r'kakao_rest_api_key\s*=\s*"([^"]*)"', text)
        if m:
            return m.group(1).strip()
    return ""


def _norm(s: object) -> str:
    s = str(s or "").strip()
    if s.lower() in ("nan", "none", ""):
        return ""
    return re.sub(r"\s+", " ", s)


def strip_parens(addr: str) -> str:
    out = re.sub(r"\([^)]*\)", "", addr)
    return re.sub(r"\s+", " ", out).strip()


def _kakao_get(session: requests.Session, url: str, headers: dict, params: dict, retry: int = 3) -> Optional[dict]:
    for attempt in range(retry):
        try:
            r = session.get(url, headers=headers, params=params, timeout=20)
            if r.status_code == 429:
                time.sleep(1.0 + attempt)
                continue
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError, TypeError):
            time.sleep(0.3 * (attempt + 1))
    return None


def geocode_address(session, headers, address: str):
    addr = _norm(address)
    if not addr:
        return None, None
    data = _kakao_get(session, ADDRESS_URL, headers, {"query": addr, "size": 1})
    if not data:
        return None, None
    docs = data.get("documents") or []
    if not docs:
        return None, None
    d0 = docs[0]
    try:
        return float(d0["y"]), float(d0["x"])
    except (KeyError, ValueError, TypeError):
        return None, None


def geocode_keyword(session, headers, query: str):
    q = _norm(query)[:100]
    if not q:
        return None, None
    data = _kakao_get(session, KEYWORD_URL, headers, {"query": q, "size": 5})
    if not data:
        return None, None
    docs = data.get("documents") or []
    if not docs:
        return None, None
    d0 = docs[0]
    try:
        return float(d0["y"]), float(d0["x"])
    except (KeyError, ValueError, TypeError):
        return None, None


def resolve_store(session, headers, name: str, address: str, pause: float):
    """다단계 전략: 원본주소 → 괄호제거 → 매장명 키워드 → 매장명+주소 앞부분."""
    lat, lon = geocode_address(session, headers, address)
    if lat is not None:
        return lat, lon

    time.sleep(pause)
    stripped = strip_parens(address)
    if stripped and stripped != _norm(address):
        lat, lon = geocode_address(session, headers, stripped)
        if lat is not None:
            return lat, lon

    time.sleep(pause)
    lat, lon = geocode_keyword(session, headers, name)
    if lat is not None:
        return lat, lon

    time.sleep(pause)
    head_tokens = " ".join(strip_parens(address).split()[:3])
    if head_tokens:
        lat, lon = geocode_keyword(session, headers, f"{head_tokens} {name}")
        if lat is not None:
            return lat, lon

    return None, None


def make_backup(src: Path) -> Optional[Path]:
    if not src.is_file():
        return None
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = BACKUP_DIR / f"{src.stem}.{ts}.bak.csv"
    shutil.copy2(src, dst)
    return dst


def read_new_master(source: Optional[Path]) -> pd.DataFrame:
    """새 매장 마스터를 읽어 표준 4컬럼 (name, address, ops_team, store_region) 반환."""
    if source is None:
        if not STORES_XLSX.is_file():
            raise SystemExit(
                f"{STORES_XLSX.name} 이 없습니다. 새 매장 엑셀을 이 폴더에 두고 다시 실행하거나, "
                f"--source raw_stores.csv 로 CSV 를 지정하세요."
            )
        source = STORES_XLSX

    if source.suffix.lower() in (".xlsx", ".xls"):
        df = pd.read_excel(source, sheet_name=0, engine="openpyxl")
    else:
        df = pd.read_csv(source, encoding="utf-8-sig")

    df = df.dropna(how="all")
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str).str.strip()
    df = df.rename(columns=STORE_RENAME)

    for must in ("name", "address"):
        if must not in df.columns:
            raise SystemExit(
                f"필수 컬럼 '{must if must=='name' else '주소'}' 가 없습니다. "
                f"현재 컬럼: {list(df.columns)} (필요 컬럼: 매장명, 주소, 운영팀, 권역)"
            )
    for opt in ("ops_team", "store_region"):
        if opt not in df.columns:
            df[opt] = ""

    df = df[["name", "address", "ops_team", "store_region"]].copy()
    df["name"] = df["name"].map(_norm)
    df["address"] = df["address"].map(_norm)
    df["ops_team"] = df["ops_team"].map(_norm)
    df["store_region"] = df["store_region"].map(_norm)
    df = df[(df["name"] != "") & (df["address"] != "")]

    dup = df[df["name"].duplicated(keep=False)]
    if not dup.empty:
        print("[경고] 새 파일에 중복된 매장명이 있습니다 — 첫 번째 행만 사용합니다.")
        print(dup[["name", "address"]].to_string(index=False))
        df = df.drop_duplicates(subset=["name"], keep="first").reset_index(drop=True)

    return df


def main() -> None:
    ap = argparse.ArgumentParser(description="매장 마스터 동기화 (추가/삭제/주소변경 자동 처리)")
    ap.add_argument("--dry-run", action="store_true", help="변경사항만 미리 보기 (저장·API 호출 X)")
    ap.add_argument("--source", type=str, default=None, help="새 마스터 파일 경로 (기본: stores.xlsx)")
    ap.add_argument("--pause", type=float, default=0.09, help="API 호출 간 대기(초)")
    args = ap.parse_args()

    if not PROCESSED.is_file():
        raise SystemExit(f"{PROCESSED.name} 이 없습니다. 먼저 전체 지오코딩이 한 번 끝나 있어야 합니다.")

    source = Path(args.source) if args.source else None
    new_df = read_new_master(source)
    print(f"[입력] 새 매장 마스터: {len(new_df)} 행", flush=True)

    proc = pd.read_csv(PROCESSED, encoding="utf-8-sig")
    cur_stores = proc[proc["entity_type"] == "store"].copy()
    cur_stores["name"] = cur_stores["name"].map(_norm)
    cur_stores["address"] = cur_stores["address"].map(_norm)
    print(f"[현재] processed_data.csv 매장: {len(cur_stores)} 행", flush=True)

    cur_by_name = {r["name"]: r for _, r in cur_stores.iterrows()}
    new_names = set(new_df["name"].tolist())
    cur_names = set(cur_by_name.keys())

    added_names = sorted(new_names - cur_names)
    removed_names = sorted(cur_names - new_names)

    changed_addr: list[str] = []
    unchanged: list[str] = []
    for nm in new_names & cur_names:
        new_row = new_df[new_df["name"] == nm].iloc[0]
        cur_row = cur_by_name[nm]
        if _norm(new_row["address"]) != _norm(cur_row["address"]):
            changed_addr.append(nm)
        else:
            unchanged.append(nm)

    print()
    print(f"[차이] 추가 신규    : {len(added_names)} 곳")
    print(f"[차이] 삭제 예정    : {len(removed_names)} 곳")
    print(f"[차이] 주소 변경    : {len(changed_addr)} 곳")
    print(f"[차이] 동일 매장    : {len(unchanged)} 곳 (좌표 유지, 운영팀/권역만 갱신)")

    def _preview(label: str, names: list[str]):
        if not names:
            return
        sample = names[:10]
        more = f"  …외 {len(names)-10}건" if len(names) > 10 else ""
        print(f"\n  ▸ {label} 미리보기:")
        for n in sample:
            if label == "추가":
                row = new_df[new_df["name"] == n].iloc[0]
                print(f"    + {n} | {row['address']}")
            elif label == "삭제":
                row = cur_by_name[n]
                print(f"    - {n} | {row['address']}")
            elif label == "주소변경":
                old = cur_by_name[n]["address"]
                new = new_df[new_df["name"] == n].iloc[0]["address"]
                print(f"    ~ {n}\n        OLD: {old}\n        NEW: {new}")
        if more:
            print(f"    {more}")

    _preview("추가", added_names)
    _preview("삭제", removed_names)
    _preview("주소변경", changed_addr)

    if args.dry_run:
        print("\n[dry-run] 미리보기만 출력. 파일 변경 없음.")
        return

    need_geocode = list(added_names) + list(changed_addr)
    if not (added_names or removed_names or changed_addr):
        print("\n변경 사항이 없습니다. 운영팀/권역 갱신만 진행하고 종료할게요.")

    bkp1 = make_backup(RAW_STORES)
    bkp2 = make_backup(PROCESSED)
    print(f"\n[백업] {bkp1.name if bkp1 else '(raw_stores.csv 없음, skip)'} / {bkp2.name}")

    geocoded: dict[str, tuple[float, float]] = {}
    failures: list[dict] = []

    if need_geocode:
        rest_key = load_rest_key()
        if not rest_key:
            raise SystemExit("KAKAO_REST_API_KEY 또는 secrets.toml 의 kakao_rest_api_key 가 필요합니다.")
        headers = {"Authorization": f"KakaoAK {rest_key}"}
        session = requests.Session()
        pause = max(0.03, float(args.pause))

        print(f"\n[지오코딩] 신규+주소변경 {len(need_geocode)} 곳 처리 (pause={pause}s)")
        for i, nm in enumerate(need_geocode, start=1):
            row = new_df[new_df["name"] == nm].iloc[0]
            lat, lon = resolve_store(session, headers, nm, row["address"], pause)
            if lat is not None and lon is not None:
                geocoded[nm] = (lat, lon)
            else:
                failures.append({"name": nm, "address": row["address"]})
            if i % 20 == 0 or i == len(need_geocode):
                print(f"  진행 {i}/{len(need_geocode)} | 성공 {len(geocoded)} | 실패 {len(failures)}", flush=True)
            time.sleep(pause)

    new_store_rows: list[dict] = []
    for _, r in new_df.iterrows():
        nm = r["name"]
        if nm in unchanged:
            cur = cur_by_name[nm]
            lat = cur["latitude"]
            lon = cur["longitude"]
        elif nm in changed_addr or nm in added_names:
            if nm in geocoded:
                lat, lon = geocoded[nm]
            else:
                continue
        else:
            continue
        new_store_rows.append({
            "entity_type": "store",
            "name": nm,
            "school_type": "",
            "address": r["address"],
            "latitude": lat,
            "longitude": lon,
            "ops_team": r["ops_team"],
            "store_region": r["store_region"],
            "campus_kind": "",
        })

    school_rows = proc[proc["entity_type"] == "school"]
    new_store_df = pd.DataFrame(new_store_rows).reindex(columns=PROCESSED_COLUMN_ORDER)
    out_df = pd.concat([new_store_df, school_rows.reindex(columns=PROCESSED_COLUMN_ORDER)], ignore_index=True, sort=False)

    tmp = PROCESSED.with_suffix(".tmp.csv")
    out_df.to_csv(tmp, index=False, encoding="utf-8-sig")
    os.replace(tmp, PROCESSED)

    raw_new = new_df[["name", "address", "ops_team", "store_region"]].copy()
    raw_new.to_csv(RAW_STORES, index=False, encoding="utf-8-sig")

    n_store_out = int((out_df["entity_type"] == "store").sum())
    n_school_out = int((out_df["entity_type"] == "school").sum())
    print(
        f"\n[저장] processed_data.csv (매장 {n_store_out}, 학교 {n_school_out}) / raw_stores.csv ({len(raw_new)})",
        flush=True,
    )

    if failures:
        pd.DataFrame(failures).to_csv(FAIL_LOG, index=False, encoding="utf-8-sig")
        print(f"[실패] 지오코딩 실패 {len(failures)} 곳 → {FAIL_LOG.name} 참고")
        print("   해당 매장은 좌표 없이 누락된 상태입니다. 주소 확인 후 다시 실행하세요.")

    print("\n[다음 단계] git add raw_stores.csv processed_data.csv && git commit -m '...' && git push")


if __name__ == "__main__":
    main()
