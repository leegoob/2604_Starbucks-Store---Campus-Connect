# -*- coding: utf-8 -*-
"""
카카오 로컬 API로 스타벅스 매장 -> starbucks_kakao_export.xlsx

- 주차장(PK6)·이름에 주차 포함·비카페 업종은 제외하고 CE7(카페) 등만 허용합니다.
- 이미 받은 엑셀 정리만: python clean_stores_xlsx.py

기본: 전국 초촘촘 격자(약 2193점). 시간이 매우 오래 걸릴 수 있음.
최대한 촘촘(누락 최소화 목적): python fetch_starbucks_kakao.py --ultra  (약 4440점, 수 시간~)
이전(덜 촘촘) 격자: python fetch_starbucks_kakao.py --legacy
빠른 테스트만: python fetch_starbucks_kakao.py --quick

실행:
  $env:KAKAO_REST_API_KEY="REST키"
  python fetch_starbucks_kakao.py
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from store_filters import kakao_doc_is_starbucks_store

BASE = Path(__file__).resolve().parent
OUT = BASE / "starbucks_kakao_export.xlsx"

KEYWORD_URL = "https://dapi.kakao.com/v2/local/search/keyword.json"

# 기본: 전국을 덮는 소수 격자 (반경 20km, 약간 겹침)
def grid_coarse() -> list[tuple[float, float]]:
    pts: list[tuple[float, float]] = []
    lats = [33.2, 34.2, 35.2, 36.2, 37.2, 38.2]
    lons = [124.4, 125.6, 126.8, 128.0, 129.2, 130.4, 131.5]
    for lat in lats:
        for lon in lons:
            pts.append((lon, lat))
    return pts


# 이전 기본 격자(약 396점) — 빠르지만 누락 가능성 큼
def grid_dense_legacy() -> list[tuple[float, float]]:
    pts: list[tuple[float, float]] = []
    lat = 33.05
    while lat <= 38.85:
        lon = 124.15
        while lon <= 131.95:
            pts.append((lon, lat))
            lon += 0.36
        lat += 0.34
    return pts


# 전국 초촘촘 격자 (위·경도 간격 축소 → 격자점 2000개 전후, 누적 행 2000+ 목표에 유리)
def grid_extra_dense() -> list[tuple[float, float]]:
    pts: list[tuple[float, float]] = []
    lat = 33.0
    lat_step = 0.14
    lon_step = 0.16
    while lat <= 39.0:
        lon = 124.0
        while lon <= 132.05:
            pts.append((lon, lat))
            lon += lon_step
        lat += lat_step
    return pts


# 초극촘 격자 (위·경도 간격 추가 축소, 반경 20km와 겹침 극대화 — API·시간 부담 큼)
def grid_ultra_dense() -> list[tuple[float, float]]:
    pts: list[tuple[float, float]] = []
    lat = 33.0
    lat_step = 0.10
    lon_step = 0.11
    while lat <= 39.0:
        lon = 124.0
        while lon <= 132.05:
            pts.append((lon, lat))
            lon += lon_step
        lat += lat_step
    return pts


REQUEST_DELAY = 0.15
# 저장 후 고유 매장 행이 이 값 이하이면 경고 (목표 개수가 아니라 하한 기준)
MIN_STORE_ROWS_WARN = 2000


def pick_address(doc: dict[str, Any]) -> str:
    road = str(doc.get("road_address_name") or "").strip()
    jibun = str(doc.get("address_name") or "").strip()
    return road if road else jibun


def fetch_keyword(
    rest_key: str,
    lon: float,
    lat: float,
    page: int,
) -> dict[str, Any] | None:
    headers = {"Authorization": f"KakaoAK {rest_key}"}
    params = {
        "query": "스타벅스",
        "x": str(lon),
        "y": str(lat),
        "radius": 20000,
        "page": page,
        "size": 15,
        "sort": "distance",
    }
    r = requests.get(KEYWORD_URL, headers=headers, params=params, timeout=30)
    if r.status_code == 401:
        print("401: REST API 키가 잘못되었습니다.", file=sys.stderr)
        return None
    if r.status_code == 429:
        print("429: 호출 한도. 60초 대기 후 재시도...", flush=True)
        time.sleep(60)
        r = requests.get(KEYWORD_URL, headers=headers, params=params, timeout=30)
    if r.status_code != 200:
        print(f"HTTP {r.status_code}: {r.text[:300]}", file=sys.stderr)
        return None
    return r.json()


def run_grid(rest_key: str, grid: list[tuple[float, float]]) -> dict[str, dict[str, str]]:
    seen: dict[str, dict[str, str]] = {}
    n_calls = 0
    total = len(grid)

    for gi, (lon, lat) in enumerate(grid, 1):
        page = 1
        while page <= 45:
            data = fetch_keyword(rest_key, lon, lat, page)
            n_calls += 1
            time.sleep(REQUEST_DELAY)
            if data is None:
                break
            docs = data.get("documents") or []
            meta = data.get("meta") or {}
            for doc in docs:
                if not isinstance(doc, dict):
                    continue
                if not kakao_doc_is_starbucks_store(doc):
                    continue
                pid = str(doc.get("id") or "")
                if not pid:
                    continue
                name = str(doc.get("place_name") or "").strip()
                addr = pick_address(doc)
                if pid not in seen:
                    seen[pid] = {"매장명": name, "주소": addr}

            if meta.get("is_end"):
                break
            if not docs:
                break
            page += 1

        print(f"  [{gi}/{total}] 중심({lon:.2f},{lat:.2f}) -> 누적 {len(seen)}매장 (API {n_calls}회)", flush=True)

    return seen


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--key", default=os.environ.get("KAKAO_REST_API_KEY", ""))
    parser.add_argument(
        "--quick",
        action="store_true",
        help="격자 소수만 사용(테스트용, 몇 분 안에 끝남). 기본은 전국 초촘촘.",
    )
    parser.add_argument(
        "--legacy",
        action="store_true",
        help="이전 기본 격자(약 396점, 더 빠름·누락 가능).",
    )
    parser.add_argument(
        "--ultra",
        action="store_true",
        help="초극촘 격자(약 4440점). 누락 최소화에 유리하나 실행 시간·API 호출이 매우 큼.",
    )
    args = parser.parse_args()
    rest_key = (args.key or "").strip()
    if not rest_key:
        print(
            "KAKAO_REST_API_KEY 없음. 예: $env:KAKAO_REST_API_KEY='여기에_콘솔에서_복사한_키'",
            file=sys.stderr,
        )
        raise SystemExit(1)
    try:
        rest_key.encode("ascii")
    except UnicodeEncodeError:
        print(
            "REST API 키는 카카오 개발자 콘솔의 영문·숫자 문자열만 사용할 수 있습니다.\n"
            "문서의 'REST키' 같은 한글 예시를 그대로 넣으면 HTTP 헤더 오류가 납니다.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    if args.quick:
        grid = grid_coarse()
        mode = "QUICK(테스트)"
    elif args.ultra:
        grid = grid_ultra_dense()
        mode = "전국 초극촘(--ultra)"
    elif args.legacy:
        grid = grid_dense_legacy()
        mode = "LEGACY(이전 촘촘)"
    else:
        grid = grid_extra_dense()
        mode = "전국 초촘촘(기본)"
    print(f"모드: {mode} 격자 {len(grid)}곳", flush=True)
    if not args.quick:
        print("전국 모드는 수 시간 걸릴 수 있습니다. 터미널을 닫지 마세요.", flush=True)
    print("시작... (잠시 기다리세요)", flush=True)

    seen = run_grid(rest_key, grid)

    if not seen:
        print("수집 0건. 키·한도·네트워크 확인.", file=sys.stderr)
        raise SystemExit(1)

    df = pd.DataFrame(list(seen.values()))
    df = df.sort_values("매장명").drop_duplicates(subset=["매장명", "주소"]).reset_index(drop=True)
    df.to_excel(OUT, index=False, engine="openpyxl")
    print("--- 완료 ---", flush=True)
    print(f"저장: {OUT}", flush=True)
    print(f"총 {len(df)}행", flush=True)
    if len(df) <= MIN_STORE_ROWS_WARN:
        print(
            f"주의: 고유 매장 {len(df)}행으로 {MIN_STORE_ROWS_WARN}행 이하입니다. "
            "카카오 키워드 검색은 구·페이지 한계가 있어 전 매장을 보장하지 않습니다. "
            "다른 시점에 재실행하거나, 결과를 stores.xlsx에 반영한 뒤 수동 보완을 검토하세요.",
            flush=True,
        )


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        raise
