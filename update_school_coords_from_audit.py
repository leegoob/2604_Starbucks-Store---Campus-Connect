from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import requests


BASE = Path(__file__).resolve().parent
PROCESSED = BASE / "processed_data.csv"
AUDIT = BASE / "school_address_kakao_audit.csv"
BACKUP = BASE / "processed_data.before_school_coord_fix.csv"
API_URL = "https://dapi.kakao.com/v2/local/search/keyword.json"


def read_kakao_rest_key() -> str:
    sec = (BASE / ".streamlit" / "secrets.toml").read_text(encoding="utf-8")
    m = re.search(r'kakao_rest_api_key\s*=\s*"([^"]+)"', sec)
    if not m:
        raise ValueError("kakao_rest_api_key not found in .streamlit/secrets.toml")
    return m.group(1).strip()


def kakao_keyword_geocode(name: str, key: str) -> tuple[float | None, float | None, str]:
    q = str(name or "").strip()
    if not q:
        return None, None, "empty_name"
    headers = {"Authorization": f"KakaoAK {key}"}
    try:
        r = requests.get(API_URL, headers=headers, params={"query": q, "size": 1}, timeout=8)
        if r.status_code != 200:
            return None, None, f"api_{r.status_code}"
        docs = (r.json() or {}).get("documents") or []
        if not docs:
            return None, None, "no_docs"
        return float(docs[0]["y"]), float(docs[0]["x"]), "ok"
    except Exception as e:  # noqa: BLE001
        return None, None, f"error:{str(e)[:80]}"


def main() -> None:
    if not PROCESSED.exists():
        raise FileNotFoundError(PROCESSED)
    if not AUDIT.exists():
        raise FileNotFoundError(AUDIT)

    key = read_kakao_rest_key()
    src = pd.read_csv(PROCESSED, encoding="utf-8-sig")
    audit = pd.read_csv(AUDIT, encoding="utf-8-sig")

    target_status = {"불일치", "주소결과없음"}
    target = audit[audit["상태"].isin(target_status)].copy()
    if target.empty:
        print("갱신 대상 없음")
        return

    # 백업 1회 생성
    if not BACKUP.exists():
        src.to_csv(BACKUP, index=False, encoding="utf-8-sig")
        print(f"백업 생성: {BACKUP}")
    else:
        print(f"기존 백업 사용: {BACKUP}")

    src["name_key"] = src["name"].astype(str).str.strip()
    src["address_key"] = src["address"].astype(str).str.strip()
    mask_school = src["entity_type"].astype(str).str.strip().eq("school")

    replaced_mismatch = 0
    replaced_notfound = 0
    unresolved_notfound = 0

    for _, r in target.iterrows():
        nm = str(r.get("학교명", "") or "").strip()
        ad = str(r.get("주소", "") or "").strip()
        st = str(r.get("상태", "") or "").strip()
        if not nm or not ad:
            continue

        idx = src.index[mask_school & src["name_key"].eq(nm) & src["address_key"].eq(ad)]
        if len(idx) == 0:
            continue

        new_lat = r.get("kakao_lat")
        new_lon = r.get("kakao_lon")

        if st == "불일치":
            if pd.notna(new_lat) and pd.notna(new_lon):
                src.loc[idx, "latitude"] = float(new_lat)
                src.loc[idx, "longitude"] = float(new_lon)
                replaced_mismatch += len(idx)
            continue

        if st == "주소결과없음":
            lat2, lon2, msg = kakao_keyword_geocode(nm, key)
            if lat2 is not None and lon2 is not None:
                src.loc[idx, "latitude"] = lat2
                src.loc[idx, "longitude"] = lon2
                replaced_notfound += len(idx)
            else:
                unresolved_notfound += len(idx)
                print(f"주소결과없음 미해결: {nm} | {ad} | {msg}")

    src = src.drop(columns=["name_key", "address_key"], errors="ignore")
    src.to_csv(PROCESSED, index=False, encoding="utf-8-sig")

    print("=== 갱신 결과 ===")
    print(f"불일치 반영 행수: {replaced_mismatch}")
    print(f"주소결과없음 반영 행수: {replaced_notfound}")
    print(f"주소결과없음 미해결 행수: {unresolved_notfound}")
    print(f"저장 완료: {PROCESSED}")


if __name__ == "__main__":
    main()
