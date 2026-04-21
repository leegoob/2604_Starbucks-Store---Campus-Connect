from __future__ import annotations

import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests


BASE = Path(__file__).resolve().parent
CSV_PATH = BASE / "processed_data.csv"
OUT_PATH = BASE / "school_address_kakao_audit.csv"
KAKAO_REST_KEY = "323477313bf9f88927e24948ecda8758"
API_URL = "https://dapi.kakao.com/v2/local/search/address.json"
MAX_WORKERS = 8
TIMEOUT_SEC = 5


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    d_lat = lat2 - lat1
    d_lon = lon2 - lon1
    a = math.sin(d_lat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(d_lon / 2) ** 2
    return 6371.0 * 2 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))


def classify(diff_km: float) -> str:
    if diff_km <= 0.3:
        return "일치"
    if diff_km <= 1.0:
        return "경미불일치"
    return "불일치"


def geocode_one(row: dict) -> dict:
    headers = {"Authorization": f"KakaoAK {KAKAO_REST_KEY}"}
    name = str(row.get("name", "") or "").strip()
    address = str(row.get("address", "") or "").strip()
    lat = row.get("latitude")
    lon = row.get("longitude")

    out = {
        "학교명": name,
        "주소": address,
        "src_lat": lat,
        "src_lon": lon,
        "kakao_lat": None,
        "kakao_lon": None,
        "거리차_km": None,
        "상태": "",
        "상세": "",
    }
    if not address or pd.isna(lat) or pd.isna(lon):
        out["상태"] = "원본누락"
        out["상세"] = "주소/좌표 없음"
        return out

    try:
        resp = requests.get(API_URL, headers=headers, params={"query": address, "size": 1}, timeout=TIMEOUT_SEC)
        if resp.status_code != 200:
            out["상태"] = f"api_{resp.status_code}"
            return out
        docs = (resp.json() or {}).get("documents") or []
        if not docs:
            out["상태"] = "주소결과없음"
            return out
        kakao_lat = float(docs[0]["y"])
        kakao_lon = float(docs[0]["x"])
        diff_km = haversine_km(float(lat), float(lon), kakao_lat, kakao_lon)
        out["kakao_lat"] = kakao_lat
        out["kakao_lon"] = kakao_lon
        out["거리차_km"] = round(diff_km, 3)
        out["상태"] = classify(diff_km)
        return out
    except Exception as e:  # noqa: BLE001
        out["상태"] = "api_error"
        out["상세"] = str(e)[:120]
        return out


def main() -> None:
    df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
    schools = df[df["entity_type"] == "school"][["name", "address", "latitude", "longitude"]].copy()
    rows = schools.to_dict("records")
    total = len(rows)
    print(f"학교 전수조사 시작: {total}건")

    results = []
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(geocode_one, r) for r in rows]
        for fut in as_completed(futures):
            results.append(fut.result())
            done += 1
            if done % 200 == 0 or done == total:
                print(f"progress {done}/{total}")

    out = pd.DataFrame(results)
    out = out.sort_values(["상태", "거리차_km", "학교명"], ascending=[True, False, True], na_position="last")
    out.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")

    print("=== 상태 요약 ===")
    print(out["상태"].value_counts(dropna=False).to_string())
    mismatch = out[out["상태"] == "불일치"].copy()
    print(f"불일치(>1.0km): {len(mismatch)}")

    seoul = out[out["학교명"].astype(str).str.contains("서울고", na=False)]
    print("=== 서울고 점검 ===")
    if seoul.empty:
        print("서울고 매칭 행 없음")
    else:
        print(seoul[["학교명", "주소", "거리차_km", "상태"]].to_string(index=False))

    print(f"저장 완료: {OUT_PATH}")


if __name__ == "__main__":
    main()
