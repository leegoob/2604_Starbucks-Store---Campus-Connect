from __future__ import annotations

import math
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests


BASE = Path(__file__).resolve().parent
PROCESSED = BASE / "processed_data.csv"
BACKUP = BASE / "processed_data.before_full_school_kakao_sync.csv"
REPORT = BASE / "school_full_kakao_sync_report.csv"
ADDR_URL = "https://dapi.kakao.com/v2/local/search/address.json"
KEYWORD_URL = "https://dapi.kakao.com/v2/local/search/keyword.json"
MAX_WORKERS = 8
TIMEOUT = 6


def read_rest_key() -> str:
    sec = (BASE / ".streamlit" / "secrets.toml").read_text(encoding="utf-8")
    m = re.search(r'kakao_rest_api_key\s*=\s*"([^"]+)"', sec)
    if not m:
        raise ValueError("kakao_rest_api_key not found")
    return m.group(1).strip()


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    d_lat = lat2 - lat1
    d_lon = lon2 - lon1
    a = math.sin(d_lat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(d_lon / 2) ** 2
    return 6371.0 * 2 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))


def best_doc_address(doc: dict) -> str:
    road = (doc.get("road_address") or {}).get("address_name")
    jibun = (doc.get("address") or {}).get("address_name")
    return str(road or jibun or "").strip()


def geocode_one(idx: int, name: str, address: str, lat: float, lon: float, key: str) -> dict:
    headers = {"Authorization": f"KakaoAK {key}"}
    out = {
        "idx": idx,
        "학교명": name,
        "원본주소": address,
        "원본위도": lat,
        "원본경도": lon,
        "카카오주소": "",
        "카카오위도": None,
        "카카오경도": None,
        "거리차_km": None,
        "매칭방식": "",
        "상태": "",
    }

    # 1) address 검색 우선
    try:
        r = requests.get(ADDR_URL, headers=headers, params={"query": address, "size": 1}, timeout=TIMEOUT)
        if r.status_code == 200:
            docs = (r.json() or {}).get("documents") or []
            if docs:
                d = docs[0]
                k_lat = float(d["y"])
                k_lon = float(d["x"])
                k_addr = best_doc_address(d) or address
                out["카카오주소"] = k_addr
                out["카카오위도"] = k_lat
                out["카카오경도"] = k_lon
                out["매칭방식"] = "address"
                out["상태"] = "ok"
                if pd.notna(lat) and pd.notna(lon):
                    out["거리차_km"] = round(haversine_km(float(lat), float(lon), k_lat, k_lon), 3)
                return out
    except Exception:
        pass

    # 2) keyword fallback
    try:
        r2 = requests.get(KEYWORD_URL, headers=headers, params={"query": name, "size": 1}, timeout=TIMEOUT)
        if r2.status_code == 200:
            docs2 = (r2.json() or {}).get("documents") or []
            if docs2:
                d2 = docs2[0]
                k_lat = float(d2["y"])
                k_lon = float(d2["x"])
                k_addr = str(d2.get("road_address_name") or d2.get("address_name") or address).strip()
                out["카카오주소"] = k_addr
                out["카카오위도"] = k_lat
                out["카카오경도"] = k_lon
                out["매칭방식"] = "keyword"
                out["상태"] = "ok_fallback"
                if pd.notna(lat) and pd.notna(lon):
                    out["거리차_km"] = round(haversine_km(float(lat), float(lon), k_lat, k_lon), 3)
                return out
    except Exception:
        pass

    out["상태"] = "not_found"
    return out


def main() -> None:
    if not PROCESSED.exists():
        raise FileNotFoundError(PROCESSED)
    key = read_rest_key()
    df = pd.read_csv(PROCESSED, encoding="utf-8-sig")
    schools = df[df["entity_type"] == "school"].copy()
    schools = schools.reset_index()  # 원본 df index 보존
    total = len(schools)
    print(f"학교 전체 동기화 시작: {total}건")

    # 백업
    if not BACKUP.exists():
        df.to_csv(BACKUP, index=False, encoding="utf-8-sig")
        print(f"백업 생성: {BACKUP}")
    else:
        print(f"기존 백업 사용: {BACKUP}")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = []
        for _, r in schools.iterrows():
            futures.append(
                ex.submit(
                    geocode_one,
                    int(r["index"]),
                    str(r.get("name", "") or "").strip(),
                    str(r.get("address", "") or "").strip(),
                    r.get("latitude"),
                    r.get("longitude"),
                    key,
                )
            )
        done = 0
        for fut in as_completed(futures):
            results.append(fut.result())
            done += 1
            if done % 200 == 0 or done == total:
                print(f"progress {done}/{total}")

    rep = pd.DataFrame(results).sort_values(["상태", "거리차_km"], ascending=[True, False], na_position="last")
    rep.to_csv(REPORT, index=False, encoding="utf-8-sig")

    # 실제 반영
    ok = rep[rep["상태"].isin(["ok", "ok_fallback"])].copy()
    for _, r in ok.iterrows():
        i = int(r["idx"])
        df.at[i, "latitude"] = float(r["카카오위도"])
        df.at[i, "longitude"] = float(r["카카오경도"])
        if str(r["카카오주소"]).strip():
            df.at[i, "address"] = str(r["카카오주소"]).strip()

    df.to_csv(PROCESSED, index=False, encoding="utf-8-sig")
    print("=== 반영 요약 ===")
    print(rep["상태"].value_counts(dropna=False).to_string())
    print(f"좌표/주소 반영 건수: {len(ok)}")
    print(f"미해결 건수: {int((rep['상태'] == 'not_found').sum())}")
    print(f"리포트: {REPORT}")
    print(f"저장: {PROCESSED}")


if __name__ == "__main__":
    main()
