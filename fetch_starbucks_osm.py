# -*- coding: utf-8 -*-
"""
전국 스타벅스 매장 목록 -> stores.xlsx (매장명, 주소)

- OpenStreetMap Overpass (여러 미러·작은 구역·재시도로 안정화)
- OSM은 비공식 데이터라 공식 매장 수와 다를 수 있습니다.
- 수집 후 주차장 등 비매장 POI는 store_filters 로 걸러 냅니다.

실행: python fetch_starbucks_osm.py

※ 고등학교(NEIS)처럼 공식 전수 API는 없어, 자동 수집은 항상 '근사치'입니다.
   정확히 맞추려면 스타벅스에서 배포하는 명단·지도 API를 별도로 쓰는 것이 좋습니다.
"""

from __future__ import annotations

import random
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from store_filters import osm_tags_skip_non_store

BASE = Path(__file__).resolve().parent
OUT = BASE / "stores.xlsx"

# Overpass 미러 (앞에서부터 시도)
OVERPASS_URLS = [
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass-api.de/api/interpreter",
    "https://overpass.openstreetmap.fr/api/interpreter",
]

# 한반도 6x2 격자 (작을수록 서버 부담 감소)
def _cells() -> list[tuple[float, float, float, float]]:
    lats = [33.0, 34.2, 35.4, 36.6, 37.8, 39.0]
    lons = [(124.0, 127.5), (127.5, 132.0)]
    out: list[tuple[float, float, float, float]] = []
    for i in range(len(lats) - 1):
        s, n = lats[i], lats[i + 1]
        for w, e in lons:
            out.append((s, w, n, e))
    return out


KR_CELLS = _cells()


def build_address(tags: dict[str, Any]) -> str:
    if not tags:
        return ""
    full = str(tags.get("addr:full") or "").strip()
    if full:
        return full
    parts: list[str] = []
    for key in (
        "addr:province",
        "addr:city",
        "addr:district",
        "addr:street",
        "addr:housenumber",
    ):
        v = str(tags.get(key) or "").strip()
        if v:
            parts.append(v)
    return " ".join(parts).strip()


def query_for_bbox(s: float, w: float, n: float, e: float) -> str:
    return f"""[out:json][timeout:120];
(
  nwr["brand"="Starbucks"]({s},{w},{n},{e});
  nwr["amenity"="cafe"]["name"~"스타벅스",i]({s},{w},{n},{e});
);
out center;
"""


def parse_elements(data: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for el in data.get("elements", []):
        tags = el.get("tags") or {}
        if osm_tags_skip_non_store(tags):
            continue
        name = str(tags.get("name") or tags.get("name:ko") or "").strip()
        if not name:
            name = "스타벅스"

        lat = el.get("lat")
        lon = el.get("lon")
        if lat is None and "center" in el:
            lat = el["center"].get("lat")
            lon = el["center"].get("lon")
        if lat is None or lon is None:
            continue

        addr = build_address(tags)
        if not addr:
            addr = f"(주소없음 OSM {float(lat):.5f},{float(lon):.5f})"

        rows.append(
            {
                "매장명": name,
                "주소": addr,
                "_lat": f"{float(lat):.5f}",
                "_lon": f"{float(lon):.5f}",
            }
        )
    return rows


def post_overpass(url: str, query: str) -> requests.Response:
    return requests.post(
        url,
        data={"data": query},
        headers={"User-Agent": "starbucks_school_finder/1.0 (education demo)"},
        timeout=150,
    )


def fetch_cell(s: float, w: float, n: float, e: float, cell_idx: int, total: int) -> list[dict[str, str]]:
    q = query_for_bbox(s, w, n, e)
    last_err: str | None = None
    for attempt in range(3):
        random.shuffle(OVERPASS_URLS)
        for base in OVERPASS_URLS:
            try:
                r = post_overpass(base, q)
                if r.status_code == 504:
                    last_err = "504"
                    time.sleep(2 + attempt * 2)
                    continue
                r.raise_for_status()
                data = r.json()
                return parse_elements(data)
            except Exception as ex:
                last_err = str(ex)
                time.sleep(1.5)
    print(f"  [구역 {cell_idx}/{total}] 실패: {last_err}", file=sys.stderr)
    return []


def main() -> None:
    total = len(KR_CELLS)
    all_rows: list[dict[str, str]] = []
    print(f"Overpass {total}구역 순차 호출 (실패 시 빈 구역은 건너뜀)...", flush=True)

    for i, (s, w, n, e) in enumerate(KR_CELLS, 1):
        print(f"구역 {i}/{total} ({s},{w})~({n},{e})", flush=True)
        part = fetch_cell(s, w, n, e, i, total)
        all_rows.extend(part)
        print(f"  누적: {len(all_rows)}", flush=True)
        time.sleep(3)

    if not all_rows:
        print(
            "수집 실패(서버 타임아웃 등). 나중에 다시 실행하거나, "
            "스타벅스 매장 주소를 엑셀로 직접 만드세요.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    df = pd.DataFrame(all_rows)
    df = df.drop_duplicates(subset=["매장명", "_lat", "_lon"], keep="first")
    df = df.drop(columns=["_lat", "_lon"], errors="ignore")
    df = df.sort_values("매장명").reset_index(drop=True)
    df.to_excel(OUT, index=False, engine="openpyxl")

    print(f"저장: {OUT} ({len(df)}행)", flush=True)


if __name__ == "__main__":
    t0 = time.time()
    try:
        main()
    except requests.RequestException as ex:
        print(f"네트워크 오류: {ex}", file=sys.stderr)
        raise SystemExit(1)
    print(f"소요: {time.time() - t0:.1f}초", flush=True)
