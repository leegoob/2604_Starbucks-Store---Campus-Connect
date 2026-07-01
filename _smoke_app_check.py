# -*- coding: utf-8 -*-
"""앱 데이터·API·핵심 로직 스모크 테스트 (Streamlit UI 없이)."""
from __future__ import annotations

import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import requests

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))

import importlib

app = importlib.import_module("3_app")
geocode_utils = importlib.import_module("geocode_utils")

DATA = BASE / "processed_data.csv"
SECRETS = BASE / ".streamlit" / "secrets.toml"

FAILURES: list[str] = []
WARNINGS: list[str] = []
PASSED: list[str] = []


def ok(msg: str) -> None:
    PASSED.append(msg)


def warn(msg: str) -> None:
    WARNINGS.append(msg)


def fail(msg: str) -> None:
    FAILURES.append(msg)


def load_secrets() -> dict[str, str]:
    out: dict[str, str] = {}
    if not SECRETS.is_file():
        fail("secrets.toml 없음")
        return out
    text = SECRETS.read_text(encoding="utf-8")
    for key in ("kakao_js_key", "kakao_rest_api_key", "store_reference_date", "app_last_updated"):
        m = re.search(rf'{key}\s*=\s*"([^"]*)"', text)
        if m:
            out[key] = m.group(1).strip()
    return out


def main() -> int:
    print("=== Starbucks School Finder smoke check ===\n")

    # --- secrets / dates ---
    sec = load_secrets()
    if sec.get("store_reference_date") == "2026-06-22":
        ok("매장 기준일 secrets: 2026-06-22")
    else:
        fail(f"매장 기준일 기대 2026-06-22, 실제 {sec.get('store_reference_date')}")
    if sec.get("app_last_updated") == "2026-06-22":
        ok("데이터 갱신일 secrets: 2026-06-22")
    else:
        fail(f"데이터 갱신일 기대 2026-06-22, 실제 {sec.get('app_last_updated')}")

    rest_key = sec.get("kakao_rest_api_key", "")
    js_key = sec.get("kakao_js_key", "")
    if rest_key:
        ok("kakao_rest_api_key 설정됨")
    else:
        fail("kakao_rest_api_key 없음")
    if js_key:
        ok("kakao_js_key 설정됨")
    else:
        fail("kakao_js_key 없음")

    # --- processed_data ---
    if not DATA.is_file():
        fail("processed_data.csv 없음")
        print_report()
        return 1

    df = pd.read_csv(DATA, encoding="utf-8-sig")
    stores = df[df["entity_type"] == "store"].copy()
    schools = df[df["entity_type"] == "school"].copy()
    ok(f"매장 {len(stores):,} · 학교 {len(schools):,}")

    miss_st = stores[stores["latitude"].isna() | stores["longitude"].isna()]
    miss_sc = schools[schools["latitude"].isna() | schools["longitude"].isna()]
    if miss_st.empty:
        ok("매장 좌표 100%")
    else:
        fail(f"매장 좌표 누락 {len(miss_st)}건")
    if len(miss_sc) <= 5:
        ok(f"학교 좌표 누락 {len(miss_sc)}건 (허용 범위)")
    elif len(miss_sc) / max(1, len(schools)) < 0.01:
        warn(f"학교 좌표 누락 {len(miss_sc)}건 ({100*len(miss_sc)/len(schools):.2f}%)")
    else:
        fail(f"학교 좌표 누락 {len(miss_sc)}건")

    dup_st = stores.duplicated(subset=["name"], keep=False)
    if not dup_st.any():
        ok("매장명 중복 없음")
    else:
        fail(f"매장명 중복 {int(dup_st.sum())}행")

    for col in ("ops_team", "store_region"):
        empty = int((stores[col].fillna("").astype(str).str.strip() == "").sum()) if col in stores.columns else len(stores)
        if empty == 0:
            ok(f"매장 {col} 전건 채움")
        elif empty / len(stores) < 0.02:
            warn(f"매장 {col} 비어 있음 {empty}건")
        else:
            fail(f"매장 {col} 비어 있음 {empty}건")

    # --- school filters ---
    schools = schools.copy()
    schools["school_type"] = schools["school_type"].map(app.normalize_school_type_value)
    schools_f = app.filter_out_corporate_campus_universities(schools)
    removed = len(schools) - len(schools_f)
    ok(f"사내대학 제외 후 학교 {len(schools_f):,} (제외 {removed})")

    all_keys = {k for k, _ in app.SCHOOL_FILTER_DEF}
    hs_pool, uv_pool = app.campaign_school_pools_for_summary(schools_f, all_keys)
    if not hs_pool.empty and not uv_pool.empty:
        ok(f"캠페인 학교 풀: 고등 {len(hs_pool):,} · 대학 {len(uv_pool):,}")
    else:
        fail("캠페인 학교 풀이 비어 있음")

    filtered = app.filter_schools_by_keys(schools_f, {"hs_general", "univ_4year"})
    if not filtered.empty:
        ok(f"학교 유형 필터 샘플: {len(filtered):,}행")
    else:
        fail("학교 유형 필터 결과 비어 있음")

    # --- store tags ---
    tagged = app.apply_store_tags(stores)
    for c in app.STORE_TAG_COLUMNS:
        if c not in tagged.columns:
            fail(f"매장 특성 열 없음: {c}")
    ok("매장 특성 태그 병합")

    # --- neighbor batch (지도·요약 탭 핵심) ---
    st_view = stores.dropna(subset=["latitude", "longitude"]).head(5)
    sch_use = schools_f.dropna(subset=["latitude", "longitude"]).head(200)
    if len(st_view) >= 1 and len(sch_use) >= 10:
        idx, dist, sub = app._batch_topn_school_indices(st_view, sch_use, 5)
        if idx.shape[0] == len(st_view) and np.isfinite(dist).any():
            ok("매장-학교 근접 배치 계산")
        else:
            fail("근접 배치 계산 결과 이상")
    else:
        warn("근접 배치 샘플 스킵 (데이터 부족)")

    # --- campaign execution sample ---
    if len(st_view) >= 1:
        exec_hs = app.build_campaign_execution_table(st_view, hs_pool.head(500), n_near=5)
        dedup = app.build_school_dedup_table(exec_hs)
        if not exec_hs.empty:
            ok(f"산학연계 실행표 샘플 {len(exec_hs)}행 · 중복제거 {len(dedup)}행")
        else:
            warn("산학연계 실행표 샘플 비어 있음 (풀/매장 조합)")

    # --- map HTML ---
    if js_key:
        html_out = app.kakao_map_html(
            37.5,
            127.0,
            js_key,
            [
                {"lat": 37.5, "lng": 127.0, "kind": "store", "label": "테스트매장"},
                {"lat": 37.51, "lng": 127.01, "kind": "school", "label": "테스트학교"},
            ],
            map_height_px=200,
        )
        if "kakao.maps" in html_out and js_key in html_out:
            ok("카카오 지도 HTML 생성")
        else:
            fail("카카오 지도 HTML 생성 실패")

    # --- Kakao REST API live ping ---
    if rest_key:
        session = requests.Session()
        headers = {"Authorization": f"KakaoAK {rest_key}"}
        lat, lon, q = geocode_utils.resolve_store_coords_kakao(
            session,
            headers,
            "스타벅스 강남역",
            "서울특별시 강남구 강남대로 396",
            pause=0.05,
        )
        if lat is not None and lon is not None:
            ok(f"카카오 REST API 지오코딩 OK (쿼리: {q[:40]}…)")
        else:
            fail("카카오 REST API 지오코딩 실패")

        # 층 포함 주소 정규화 샘플
        lat2, lon2, _ = geocode_utils.resolve_store_coords_kakao(
            session,
            headers,
            "광주문화전당",
            "광주광역시 동구 서석로 44 (광산동) 1~2층",
            pause=0.05,
        )
        if lat2 is not None:
            ok("층 포함 주소 정규화 지오코딩 OK")
        else:
            fail("층 포함 주소 지오코딩 실패")

    # --- sidebar filter simulation ---
    team_vals = sorted(
        {str(x).strip() for x in stores["ops_team"].tolist() if str(x).strip()},
        key=app._team_sort_key,
    )
    region_vals = sorted(
        {str(x).strip() for x in stores["store_region"].tolist() if str(x).strip()}
    )
    if team_vals and region_vals:
        ok(f"운영팀 필터 옵션 {len(team_vals)} · 권역 {len(region_vals)}")
        mask = stores["ops_team"].astype(str).isin(team_vals[:1])
        if mask.any():
            ok("운영팀 필터 마스크 동작")
    else:
        fail("운영팀/권역 필터 옵션 없음")

    sc_keys = {"hs_general", "hs_special", "univ_4year", "univ_junior"}
    schools_sel = app.filter_schools_by_keys(schools_f, sc_keys)
    stores_sel = stores  # tags default pass
    if not schools_sel.empty:
        ok(f"학교 유형 멀티필터 후 {len(schools_sel):,}교")

    print_report()
    return 1 if FAILURES else 0


def print_report() -> None:
    print(f"\n--- PASS ({len(PASSED)}) ---")
    for p in PASSED:
        print(f"  [OK] {p}")
    if WARNINGS:
        print(f"\n--- WARN ({len(WARNINGS)}) ---")
        for w in WARNINGS:
            print(f"  [!!] {w}")
    if FAILURES:
        print(f"\n--- FAIL ({len(FAILURES)}) ---")
        for f in FAILURES:
            print(f"  [XX] {f}")
    print()
    if FAILURES:
        print("RESULT: FAILED")
    else:
        print("RESULT: ALL CHECKS PASSED" + (f" ({len(WARNINGS)} warnings)" if WARNINGS else ""))


if __name__ == "__main__":
    raise SystemExit(main())
