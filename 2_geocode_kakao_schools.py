# -*- coding: utf-8 -*-
"""
카카오 로컬 API(주소 검색)로 학교 주소만 빠르게 좌표 반영 → processed_data.csv
기존 매장 행은 그대로 두고, 학교 행만 raw_schools.csv 기준으로 갱신합니다.

필요: REST API 키 (JavaScript 키와 다름)
  - 환경변수: KAKAO_REST_API_KEY
  - 또는 `.streamlit/secrets.toml` 에 kakao_rest_api_key = "..." 추가

실행 (import 후):
  python 2_geocode_kakao_schools.py

테스트 (앞 20건만):
  python 2_geocode_kakao_schools.py --limit 20
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
    """주소 검색. 좌표는 y=위도, x=경도."""
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
    """키워드 검색(학교명 등). 주소 검색 실패 시 보조."""
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


def resolve_school_coords(
    session: requests.Session,
    headers: dict[str, str],
    name: str,
    address: str,
    pause: float,
) -> tuple[float | None, float | None]:
    """주소 검색 → 실패 시 학교명 키워드 검색."""
    lat, lon = geocode_address(session, headers, address)
    if lat is not None and lon is not None:
        return lat, lon
    time.sleep(pause)
    lat, lon = geocode_keyword(session, headers, name)
    if lat is not None and lon is not None:
        return lat, lon
    time.sleep(pause)
    # 주소 앞부분만으로 재시도 (시·군·도로명만 있는 경우)
    short = _normalize_addr(address)[:40]
    if short and short != _normalize_addr(address):
        return geocode_address(session, headers, short)
    return None, None


def write_csv_atomic(df: pd.DataFrame, path: Path) -> Path:
    """저장. 권한 오류 시 대체 파일명으로 저장."""
    tmp = path.with_suffix(".tmp.csv")
    df.to_csv(tmp, index=False, encoding="utf-8-sig")
    try:
        os.replace(tmp, path)
    except PermissionError:
        if tmp.is_file():
            tmp.unlink(missing_ok=True)
        df.to_csv(OUT_ALT, index=False, encoding="utf-8-sig")
        print(
            f"\n[안내] '{path.name}' 을(를) 다른 프로그램이 사용 중입니다 "
            f"(Streamlit 실행 중, 엑셀 등).\n"
            f"  결과를 '{OUT_ALT.name}' 에 저장했습니다.\n"
            f"  Streamlit·엑셀을 모두 닫은 뒤, '{OUT_ALT.name}' 을(를) '{path.name}' 으로 "
            f"이름만 바꾸면 됩니다.\n"
        )
        return OUT_ALT
    return path


def main() -> None:
    ap = argparse.ArgumentParser(description="카카오 주소 API로 학교 좌표 일괄 반영")
    ap.add_argument("--limit", type=int, default=None, help="테스트용 앞 N행만")
    ap.add_argument(
        "--pause",
        type=float,
        default=0.09,
        help="요청 간격(초). 기본 0.09 (초당 약 10회 이하 권장)",
    )
    args = ap.parse_args()

    rest_key = load_rest_key()
    if not rest_key:
        raise SystemExit(
            "REST API 키가 없습니다. developers.kakao.com 에서 REST API 키를 발급한 뒤\n"
            "  환경변수 KAKAO_REST_API_KEY 로 설정하거나\n"
            "  .streamlit/secrets.toml 에 kakao_rest_api_key = \"...\" 를 넣으세요.\n"
            "(지도에 쓰는 JavaScript 키와는 다른 키입니다.)"
        )

    if not RAW_SCHOOLS.is_file():
        raise SystemExit("raw_schools.csv 가 없습니다. python import_excel_to_csv.py 먼저 실행하세요.")

    if not OUT.is_file():
        raise SystemExit(
            f"{OUT.name} 이 없습니다. 매장 행이 필요합니다. "
            "한 번이라도 매장이 포함된 상태를 만든 뒤 이 스크립트를 실행하세요."
        )

    prev = pd.read_csv(OUT, encoding="utf-8-sig")
    if "entity_type" not in prev.columns:
        raise SystemExit("processed_data 형식이 올바르지 않습니다.")
    store_keep = prev[prev["entity_type"] == "store"].copy()
    if store_keep.empty:
        raise SystemExit("저장된 파일에 매장 행이 없습니다.")

    schools = pd.read_csv(RAW_SCHOOLS, encoding="utf-8-sig")
    if args.limit is not None:
        schools = schools.head(max(0, int(args.limit)))
        print(f"[--limit] 학교 {len(schools)}행만 처리")

    headers = {"Authorization": f"KakaoAK {rest_key}"}
    session = requests.Session()
    pause = max(0.03, float(args.pause))
    rows: list[dict] = []
    failed: list[str] = []
    n = len(schools)
    print(f"학교 {n}건 카카오 주소 검색, 요청 간격 {pause}초")

    for i, (_, r) in enumerate(schools.iterrows(), start=1):
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
            failed.append(f"{name} | {addr[:80]}")
        if i % 50 == 0 or i == n:
            print(f"  진행 {i}/{n}")
        time.sleep(pause)

    school_df = pd.DataFrame(rows)
    out_df = pd.concat(
        [
            store_keep.reindex(columns=PROCESSED_COLUMN_ORDER),
            school_df.reindex(columns=PROCESSED_COLUMN_ORDER),
        ],
        ignore_index=True,
        sort=False,
    )
    out_df = out_df.reindex(columns=PROCESSED_COLUMN_ORDER)
    before = len(out_df)
    out_df = out_df.dropna(subset=["latitude", "longitude"])
    dropped = before - len(out_df)
    if dropped:
        print("Warning: 좌표 없음으로 제외:", dropped, "행")
    saved = write_csv_atomic(out_df, OUT)
    print("저장:", saved, "총", len(out_df), "행 (매장", len(store_keep), "+ 학교", len(out_df) - len(store_keep), ")")

    if failed:
        fail_path = BASE / "kakao_geocode_failed_schools.txt"
        fail_path.write_text("\n".join(failed), encoding="utf-8")
        print("주소 검색 실패 (좌표 없음):", len(failed), "건 →", fail_path)
    print("다음: python -m streamlit run 3_app.py")


if __name__ == "__main__":
    main()
