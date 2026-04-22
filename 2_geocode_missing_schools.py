# -*- coding: utf-8 -*-
"""
누락된 학교만 재지오코딩하여 processed_data.csv 를 보완한다.

- raw_schools.csv 에 있는데 processed_data.csv 에 (좌표 포함) 없는 학교만 대상.
- 실패 복구를 위해 여러 쿼리 전략을 순차 시도:
    1) 원본 주소
    2) 괄호 () 제거한 주소
    3) "학교명 + 시/구" 키워드
    4) 학교명 단독 키워드
    5) 주소 앞부분만 (토큰 N개 컷)

사용법 (PowerShell/Git Bash):
    python 2_geocode_missing_schools.py            # 실제 실행(처리+저장)
    python 2_geocode_missing_schools.py --dry-run  # API 호출 안 하고 누락 목록만 리포트
    python 2_geocode_missing_schools.py --limit 10 # 테스트: 10건만
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
RAW_SCHOOLS = BASE / "raw_schools.csv"
PROCESSED = BASE / "processed_data.csv"
BACKUP_DIR = BASE / "_backups"
FAIL_LOG = BASE / "kakao_missing_schools_failed.csv"
RECOVERED_LOG = BASE / "kakao_missing_schools_recovered.csv"

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
    "contact_office",
    "contact_name",
    "contact_phone",
    "contact_email",
]

SCHOOL_EXTRA_COLS = (
    "campus_kind",
    "contact_office",
    "contact_name",
    "contact_phone",
    "contact_email",
)


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
    """주소의 괄호 () 와 그 안 내용 제거."""
    out = re.sub(r"\([^)]*\)", "", addr)
    return re.sub(r"\s+", " ", out).strip()


def extract_city_gu(addr: str) -> str:
    """주소에서 '시/도 + 시/군/구' 부분만 뽑아 키워드 보강용으로 사용.
    예) '서울특별시 서초구 방배9길 23' -> '서울 서초구'
    """
    a = strip_parens(addr)
    tokens = a.split()
    if not tokens:
        return ""
    sido = tokens[0]
    sido_short = {
        "서울특별시": "서울",
        "부산광역시": "부산",
        "대구광역시": "대구",
        "인천광역시": "인천",
        "광주광역시": "광주",
        "대전광역시": "대전",
        "울산광역시": "울산",
        "세종특별자치시": "세종",
        "제주특별자치도": "제주",
        "강원특별자치도": "강원",
        "전북특별자치도": "전북",
        "경기도": "경기",
        "강원도": "강원",
        "충청북도": "충북",
        "충청남도": "충남",
        "전라북도": "전북",
        "전라남도": "전남",
        "경상북도": "경북",
        "경상남도": "경남",
    }.get(sido, sido)
    gu = ""
    for t in tokens[1:4]:
        if t.endswith(("시", "군", "구")):
            gu = t
            break
    return f"{sido_short} {gu}".strip()


def first_n_tokens(addr: str, n: int = 3) -> str:
    a = strip_parens(addr)
    tokens = a.split()
    return " ".join(tokens[:n])


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
        return None, None, ""
    data = _kakao_get(session, ADDRESS_URL, headers, {"query": addr, "size": 1})
    if not data:
        return None, None, ""
    docs = data.get("documents") or []
    if not docs:
        return None, None, ""
    d0 = docs[0]
    try:
        return float(d0["y"]), float(d0["x"]), "address"
    except (KeyError, ValueError, TypeError):
        return None, None, ""


def geocode_keyword(session, headers, query: str, size: int = 5):
    q = _norm(query)[:100]
    if not q:
        return None, None, ""
    data = _kakao_get(session, KEYWORD_URL, headers, {"query": q, "size": size})
    if not data:
        return None, None, ""
    docs = data.get("documents") or []
    if not docs:
        return None, None, ""
    d0 = docs[0]
    try:
        return float(d0["y"]), float(d0["x"]), "keyword"
    except (KeyError, ValueError, TypeError):
        return None, None, ""


def resolve_school(session, headers, name: str, address: str, pause: float):
    """다단계 전략으로 학교 좌표 탐색. (lat, lon, method_used) 반환."""
    attempts = []

    lat, lon, m = geocode_address(session, headers, address)
    attempts.append(("원본 주소", m))
    if lat is not None:
        return lat, lon, attempts

    time.sleep(pause)
    stripped = strip_parens(address)
    if stripped and stripped != _norm(address):
        lat, lon, m = geocode_address(session, headers, stripped)
        attempts.append(("괄호 제거 주소", m))
        if lat is not None:
            return lat, lon, attempts

    time.sleep(pause)
    region = extract_city_gu(address)
    if region:
        q = f"{region} {name}".strip()
        lat, lon, m = geocode_keyword(session, headers, q)
        attempts.append((f"키워드:{q}", m))
        if lat is not None:
            return lat, lon, attempts

    time.sleep(pause)
    lat, lon, m = geocode_keyword(session, headers, name)
    attempts.append((f"키워드:{name}", m))
    if lat is not None:
        return lat, lon, attempts

    time.sleep(pause)
    head = first_n_tokens(address, 3)
    if head and head != stripped:
        lat, lon, m = geocode_address(session, headers, head)
        attempts.append((f"주소 앞부분:{head}", m))
        if lat is not None:
            return lat, lon, attempts

    return None, None, attempts


def make_backup(src: Path) -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = BACKUP_DIR / f"{src.stem}.{ts}.bak.csv"
    shutil.copy2(src, dst)
    return dst


def main() -> None:
    ap = argparse.ArgumentParser(description="누락 학교만 재지오코딩해서 processed_data.csv 보완")
    ap.add_argument("--dry-run", action="store_true", help="API 호출 없이 누락 목록만 표시")
    ap.add_argument("--limit", type=int, default=None, help="테스트용: 상위 N건만 처리")
    ap.add_argument("--pause", type=float, default=0.09, help="요청 간 대기(초), 기본 0.09")
    args = ap.parse_args()

    if not RAW_SCHOOLS.is_file():
        raise SystemExit(f"{RAW_SCHOOLS.name} 이 없습니다.")
    if not PROCESSED.is_file():
        raise SystemExit(f"{PROCESSED.name} 이 없습니다. 먼저 전체 지오코딩을 실행하세요.")

    raw = pd.read_csv(RAW_SCHOOLS, encoding="utf-8-sig")
    proc = pd.read_csv(PROCESSED, encoding="utf-8-sig")
    proc_schools = proc[proc["entity_type"] == "school"].copy()

    def key_of(n: object, a: object) -> str:
        return f"{_norm(n)}||{_norm(a)}"

    existing_keys = set(
        proc_schools.apply(lambda r: key_of(r["name"], r["address"]), axis=1)
    )
    existing_names = set(proc_schools["name"].map(lambda x: _norm(x)))

    missing_rows = []
    for _, r in raw.iterrows():
        k = key_of(r.get("name", ""), r.get("address", ""))
        if k in existing_keys:
            continue
        if _norm(r.get("name", "")) in existing_names:
            continue
        missing_rows.append(r)

    missing = pd.DataFrame(missing_rows)
    print(
        f"[진단] raw 학교 {len(raw)} / processed 학교 {len(proc_schools)} "
        f"=> 누락 추정 {len(missing)} 건",
        flush=True,
    )

    if args.limit is not None:
        missing = missing.head(max(0, int(args.limit)))
        print(f"[--limit] 처리 대상: 상위 {len(missing)} 건", flush=True)

    if args.dry_run:
        preview = missing[["name", "school_type", "address"]].head(30)
        print("=== 누락 샘플 (상위 30건) ===")
        print(preview.to_string(index=False))
        return

    if missing.empty:
        print("누락 학교 없음. 종료.", flush=True)
        return

    rest_key = load_rest_key()
    if not rest_key:
        raise SystemExit("KAKAO_REST_API_KEY 또는 .streamlit/secrets.toml 의 kakao_rest_api_key 가 필요합니다.")

    bkp = make_backup(PROCESSED)
    print(f"[백업] {bkp.name} 생성 완료.", flush=True)

    headers = {"Authorization": f"KakaoAK {rest_key}"}
    session = requests.Session()
    pause = max(0.03, float(args.pause))

    recovered: list[dict] = []
    failures: list[dict] = []
    total = len(missing)

    print(f"[시작] 누락 {total} 건 재지오코딩 (pause={pause}s)", flush=True)
    for i, (_, r) in enumerate(missing.iterrows(), start=1):
        name = _norm(r.get("name", ""))
        addr = _norm(r.get("address", ""))
        stype = _norm(r.get("school_type", ""))
        lat, lon, attempts = resolve_school(session, headers, name, addr, pause)
        attempt_str = " | ".join([f"{t}:{m or 'fail'}" for t, m in attempts])

        if lat is not None and lon is not None:
            row = {
                "entity_type": "school",
                "name": name,
                "school_type": stype,
                "address": addr,
                "latitude": lat,
                "longitude": lon,
                "ops_team": "",
                "store_region": "",
            }
            for k in SCHOOL_EXTRA_COLS:
                v = r.get(k, "")
                row[k] = _norm(v) if pd.notna(v) else ""
            recovered.append(row)
        else:
            failures.append({
                "name": name,
                "school_type": stype,
                "address": addr,
                "attempts": attempt_str,
            })

        if i % 25 == 0 or i == total:
            print(f"  진행 {i}/{total} | 복구 {len(recovered)} | 실패 {len(failures)}", flush=True)
        time.sleep(pause)

    print(
        f"\n[결과] 복구 성공 {len(recovered)} / 실패 {len(failures)} (총 {total})",
        flush=True,
    )

    if recovered:
        new_rows = pd.DataFrame(recovered).reindex(columns=PROCESSED_COLUMN_ORDER)
        merged = pd.concat([proc, new_rows], ignore_index=True, sort=False)
        merged = merged.reindex(columns=PROCESSED_COLUMN_ORDER)
        tmp = PROCESSED.with_suffix(".tmp.csv")
        merged.to_csv(tmp, index=False, encoding="utf-8-sig")
        os.replace(tmp, PROCESSED)
        pd.DataFrame(recovered).to_csv(RECOVERED_LOG, index=False, encoding="utf-8-sig")
        print(f"[저장] {PROCESSED.name} 업데이트 (총 {len(merged)} 행). 복구 목록: {RECOVERED_LOG.name}", flush=True)
    else:
        print("[저장] 복구 성공 건이 없어 processed_data.csv 는 변경되지 않았습니다.", flush=True)

    if failures:
        pd.DataFrame(failures).to_csv(FAIL_LOG, index=False, encoding="utf-8-sig")
        print(f"[실패 로그] {FAIL_LOG.name} ({len(failures)} 건)", flush=True)

    print("다음 단계: streamlit run 3_app.py 로 확인", flush=True)


if __name__ == "__main__":
    main()
