# -*- coding: utf-8 -*-
"""
이미 저장된 매장 엑셀에서 이름 기준으로 주차장·주차 POI 행만 제거합니다.
(category 정보가 없을 때는 이름 휴리스틱만 적용)

사용:
  python clean_stores_xlsx.py
  python clean_stores_xlsx.py --input starbucks_kakao_export.xlsx --output stores.xlsx

※ 카페(CE7) 여부까지 반영하려면 fetch_starbucks_kakao.py 를 다시 실행하는 편이 좋습니다.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from store_filters import is_parking_like_text, place_name_is_starbucks

BASE = Path(__file__).resolve().parent


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="stores.xlsx", help="입력 엑셀 (매장명, 주소 열)")
    p.add_argument("--output", default="stores_clean.xlsx", help="저장 파일명")
    args = p.parse_args()

    inp = BASE / args.input
    if not inp.is_file():
        raise SystemExit(f"파일 없음: {inp}")

    df = pd.read_excel(inp, engine="openpyxl")
    # 열 이름 정규화 (매장명/주소)
    cols = {c: str(c).strip() for c in df.columns}
    df = df.rename(columns=cols)
    name_col = None
    for cand in ("매장명", "name", "place_name", "상호"):
        if cand in df.columns:
            name_col = cand
            break
    if name_col is None:
        raise SystemExit(f"매장명 열을 찾을 수 없습니다. 열: {list(df.columns)}")

    before = len(df)

    def keep_row(name: str) -> bool:
        n = str(name or "").strip()
        if not place_name_is_starbucks(n):
            return False
        if is_parking_like_text(n, ""):
            return False
        return True

    mask = df[name_col].map(keep_row)
    out = df[mask].copy()
    after = len(out)

    outp = BASE / args.output
    out.to_excel(outp, index=False, engine="openpyxl")
    print(f"입력: {inp} ({before}행) -> 출력: {outp} ({after}행), 제거 {before - after}행")


if __name__ == "__main__":
    main()
