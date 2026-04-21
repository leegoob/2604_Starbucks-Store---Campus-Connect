from __future__ import annotations

import math
import re
from pathlib import Path

import pandas as pd
import requests


BASE = Path(__file__).resolve().parent
PROCESSED = BASE / "processed_data.csv"
AUDIT = BASE / "school_address_kakao_audit.csv"
OUT = BASE / "school_coord_update_verification.csv"
ADDR_URL = "https://dapi.kakao.com/v2/local/search/address.json"


def read_key() -> str:
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


def topn_schools(store_name: str, n: int = 5) -> pd.DataFrame:
    df = pd.read_csv(PROCESSED, encoding="utf-8-sig")
    stores = df[df["entity_type"] == "store"].copy()
    schools = df[df["entity_type"] == "school"].copy().dropna(subset=["latitude", "longitude"])
    s = stores[stores["name"].astype(str).str.strip() == store_name].head(1)
    if s.empty:
        return pd.DataFrame(columns=["학교명", "주소", "직선거리(km)"])
    slat = float(s.iloc[0]["latitude"])
    slon = float(s.iloc[0]["longitude"])
    lat = schools["latitude"].astype(float).to_numpy()
    lon = schools["longitude"].astype(float).to_numpy()
    out_rows = []
    for i, (la, lo) in enumerate(zip(lat, lon)):
        d = haversine_km(slat, slon, float(la), float(lo))
        out_rows.append((i, d))
    out_rows.sort(key=lambda x: x[1])
    idx = [i for i, _ in out_rows[:n]]
    res = schools.iloc[idx][["name", "address"]].copy().reset_index(drop=True)
    res["직선거리(km)"] = [round(out_rows[i][1], 3) for i in range(min(n, len(out_rows)))]
    res = res.rename(columns={"name": "학교명", "address": "주소"})
    return res


def main() -> None:
    key = read_key()
    src = pd.read_csv(PROCESSED, encoding="utf-8-sig")
    audit = pd.read_csv(AUDIT, encoding="utf-8-sig")
    tgt = audit[audit["상태"].isin(["불일치", "주소결과없음"])][["학교명", "주소"]].copy()
    merged = src.merge(
        tgt,
        how="inner",
        left_on=["name", "address"],
        right_on=["학교명", "주소"],
    )
    merged = merged[merged["entity_type"] == "school"].copy()

    headers = {"Authorization": f"KakaoAK {key}"}
    rows = []
    for _, r in merged.iterrows():
        addr = str(r["address"])
        lat = float(r["latitude"])
        lon = float(r["longitude"])
        st = "api_error"
        diff = None
        k_lat = None
        k_lon = None
        resp = requests.get(ADDR_URL, headers=headers, params={"query": addr, "size": 1}, timeout=8)
        if resp.status_code == 200:
            docs = (resp.json() or {}).get("documents") or []
            if docs:
                k_lat = float(docs[0]["y"])
                k_lon = float(docs[0]["x"])
                diff = haversine_km(lat, lon, k_lat, k_lon)
                st = "ok" if diff <= 1.0 else "still_mismatch"
            else:
                st = "no_docs"
        else:
            st = f"api_{resp.status_code}"
        rows.append(
            {
                "학교명": r["name"],
                "주소": addr,
                "현재lat": lat,
                "현재lon": lon,
                "kakao_lat": k_lat,
                "kakao_lon": k_lon,
                "거리차_km": None if diff is None else round(diff, 3),
                "검증상태": st,
            }
        )

    out = pd.DataFrame(rows)
    out.to_csv(OUT, index=False, encoding="utf-8-sig")
    print(f"검증대상: {len(out)}")
    if not out.empty:
        print(out["검증상태"].value_counts().to_string())

    seoul = src[(src["entity_type"] == "school") & (src["name"].astype(str).str.contains("서울고", na=False))]
    if not seoul.empty:
        row = seoul.iloc[0]
        print(f"서울고 좌표: {row['latitude']}, {row['longitude']}")

    top5 = topn_schools("이수역사거리", n=5)
    print("이수역사거리 인근학교(5)")
    if top5.empty:
        print("매장 없음")
    else:
        print(top5.to_string(index=False))
    print(f"저장: {OUT}")


if __name__ == "__main__":
    main()
