# -*- coding: utf-8 -*-
"""
전국 고등학교 목록 CSV 생성 (교육부 나이스 NEIS Open API - 학교기본정보)

- 데이터 출처: 나이스 교육정보 개방 포털 '학교기본정보(schoolInfo)' API
  https://open.neis.go.kr/ → 인증키 신청 후 사용

- 출력 열: 학교명, 주소, 학교구분(일반/특성화/특목), 고등학교구분_원문

※ 학교알리미(schoolinfo.go.kr) Open API는 별도 회원·인증키가 필요하며,
  이 스크립트는 교육부 NEIS API만 구현합니다.

사용 예:
  set NEIS_API_KEY=발급받은키
  python fetch_highschools_neis.py

  또는:
  python fetch_highschools_neis.py --key 발급받은키

  출력 파일: highschools_nationwide.csv (기본)
  엑셀만 필요하면: -o 고등학교개황.xlsx --simple

※ 채팅으로 엑셀 파일을 보내줄 수는 없고, 이 스크립트를 한 번 실행해 PC에 저장합니다.
   (NEIS 인증키 필요: https://open.neis.go.kr)
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

BASE = Path(__file__).resolve().parent
DEFAULT_OUT = BASE / "highschools_nationwide.csv"

NEIS_URL = "https://open.neis.go.kr/hub/schoolInfo"

# 시도교육청 코드 (17개) — 나이스 API 명세 기준
ATPT_OFCDC_SC_CODES = [
    ("B10", "서울"),
    ("C10", "부산"),
    ("D10", "대구"),
    ("E10", "인천"),
    ("F10", "광주"),
    ("G10", "대전"),
    ("H10", "울산"),
    ("I10", "세종"),
    ("J10", "경기"),
    ("K10", "강원"),
    ("M10", "충북"),
    ("N10", "충남"),
    ("P10", "전북"),
    ("Q10", "전남"),
    ("R10", "경북"),
    ("S10", "경남"),
    ("T10", "제주"),
]

PAGE_SIZE = 1000
REQUEST_SLEEP_SEC = 0.05


def normalize_hs_type(hs_raw: str) -> str:
    """NEIS HS_SC_NM(고등학교구분) 값을 일반/특성화/특목으로 정리."""
    s = (hs_raw or "").strip()
    if not s or s in ("", " "):
        return "기타"

    # 특성화·직업계열
    if "특성화" in s or "마이스터" in s:
        return "특성화"

    # 특목·전문계 고등학교류
    specials = ("특목", "과학고", "외국어고", "국제고", "예술고", "체육고", "국제물류고")
    if any(k in s for k in specials):
        return "특목"

    # 일반·자율 등
    if "일반" in s or "자율" in s:
        return "일반"

    return s


def build_address(row: dict[str, Any]) -> str:
    """도로명주소 + 상세."""
    zipc = str(row.get("ORG_RDNZC") or "").strip()
    main = str(row.get("ORG_RDNMA") or "").strip()
    detail = str(row.get("ORG_RDNDA") or "").strip()
    parts = []
    if zipc:
        parts.append(f"[{zipc}]")
    if main:
        parts.append(main)
    if detail:
        parts.append(detail)
    return " ".join(parts).strip()


def extract_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """NEIS JSON에서 schoolInfo[].row 레코드 추출."""
    out: list[dict[str, Any]] = []
    si = payload.get("schoolInfo")
    if not si or not isinstance(si, list):
        return out
    for block in si:
        if not isinstance(block, dict):
            continue
        if "row" not in block:
            continue
        r = block["row"]
        if isinstance(r, list):
            for item in r:
                if isinstance(item, dict):
                    out.append(item)
        elif isinstance(r, dict):
            out.append(r)
    return out


def check_api_error(payload: dict[str, Any]) -> str | None:
    """RESULT.CODE가 오류면 메시지 반환."""
    si = payload.get("schoolInfo")
    if not si or not isinstance(si, list):
        return None
    for block in si:
        if not isinstance(block, dict):
            continue
        head = block.get("head")
        if not isinstance(head, list):
            continue
        for h in head:
            if not isinstance(h, dict):
                continue
            res = h.get("RESULT")
            if isinstance(res, dict):
                code = str(res.get("CODE", ""))
                msg = str(res.get("MESSAGE", ""))
                if code and not code.startswith("INFO-0"):
                    return f"{code}: {msg}"
    return None


def fetch_region(
    api_key: str,
    atpt_code: str,
    p_index: int,
) -> dict[str, Any]:
    params = {
        "KEY": api_key,
        "Type": "json",
        "pIndex": str(p_index),
        "pSize": str(PAGE_SIZE),
        "ATPT_OFCDC_SC_CODE": atpt_code,
        "SCHUL_KND_SC_NM": "고등학교",
    }
    r = requests.get(NEIS_URL, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def collect_all(api_key: str) -> list[dict[str, str]]:
    rows_out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for atpt_code, region_name in ATPT_OFCDC_SC_CODES:
        p_index = 1
        while True:
            payload = fetch_region(api_key, atpt_code, p_index)
            err = check_api_error(payload)
            if err:
                print(f"[경고] {region_name}({atpt_code}) pIndex={p_index}: {err}", file=sys.stderr)
                break

            batch = extract_rows(payload)
            if not batch:
                break

            for row in batch:
                name = str(row.get("SCHUL_NM") or "").strip()
                if not name:
                    continue
                # 고등학교만 (이중 안전)
                kind = str(row.get("SCHUL_KND_SC_NM") or "").strip()
                if kind and kind != "고등학교":
                    continue

                hs_raw = str(row.get("HS_SC_NM") or "").strip()
                addr = build_address(row)
                label = normalize_hs_type(hs_raw)

                key = (name, addr)
                if key in seen:
                    continue
                seen.add(key)

                rows_out.append(
                    {
                        "학교명": name,
                        "주소": addr,
                        "학교구분": label,
                        "고등학교구분_원문": hs_raw,
                        "시도교육청": region_name,
                    }
                )

            if len(batch) < PAGE_SIZE:
                break
            p_index += 1
            time.sleep(REQUEST_SLEEP_SEC)

        time.sleep(REQUEST_SLEEP_SEC)
        print(f"완료: {region_name} ({atpt_code}) - 누적 {len(rows_out)}건", flush=True)

    return rows_out


def save_table(rows: list[dict[str, str]], out_path: Path, *, simple: bool) -> None:
    """CSV 또는 xlsx 저장. simple이면 학교명·주소·학교구분만."""
    full_cols = ["학교명", "주소", "학교구분", "고등학교구분_원문", "시도교육청"]
    simple_cols = ["학교명", "주소", "학교구분"]
    cols = simple_cols if simple else full_cols
    df = pd.DataFrame(rows)
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df = df[cols]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    suf = out_path.suffix.lower()
    if suf == ".xlsx":
        df.to_excel(out_path, index=False, engine="openpyxl")
    else:
        df.to_csv(out_path, index=False, encoding="utf-8-sig")


def main() -> None:
    parser = argparse.ArgumentParser(description="NEIS 학교기본정보로 전국 고등학교 CSV/엑셀 생성")
    parser.add_argument("--key", default=os.environ.get("NEIS_API_KEY", ""), help="NEIS 인증키 (또는 환경변수 NEIS_API_KEY)")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=DEFAULT_OUT,
        help="출력 경로 (.csv 또는 .xlsx)",
    )
    parser.add_argument(
        "--simple",
        action="store_true",
        help="학교명·주소·학교구분(일반/특성화/특목) 3열만 저장",
    )
    args = parser.parse_args()

    api_key = (args.key or "").strip()
    if not api_key:
        print(
            "NEIS 인증키가 없습니다.\n"
            "1) https://open.neis.go.kr → 로그인 → 인증키 신청\n"
            "2) PowerShell: $env:NEIS_API_KEY='키'\n"
            "3) python fetch_highschools_neis.py --key 키 -o 고등학교개황.xlsx --simple",
            file=sys.stderr,
        )
        raise SystemExit(1)

    print("NEIS API 호출 중... (시간이 다소 걸릴 수 있습니다)", flush=True)
    all_rows = collect_all(api_key)

    out_path: Path = args.output
    save_table(all_rows, out_path, simple=args.simple)

    print(f"저장 완료: {out_path} (총 {len(all_rows)}행)", flush=True)


if __name__ == "__main__":
    main()
