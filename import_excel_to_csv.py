# -*- coding: utf-8 -*-
"""
엑셀 -> raw_stores.csv / raw_schools.csv
자세한 규칙: 엑셀_형식_안내.txt

파일명: stores.xlsx, schools.xlsx (이 폴더에 둠)
시트: 기본 첫 번째 시트(인덱스 0)
필수 열 이름(1행 헤더):
  stores:  매장명, 주소
  schools: 학교명, 학교구분, 주소 (필수)
         우측에 선택: 담당부서, 담당자명, 전화번호, 이메일 등 — 엑셀_형식_안내.txt 참고

실행: python import_excel_to_csv.py
다음: python 2_geocode.py
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parent

# --- 엑셀 파일 경로 (필요하면 파일명만 바꾸세요) ---
STORES_XLSX = BASE / "stores.xlsx"
SCHOOLS_XLSX = BASE / "schools.xlsx"

# --- 엑셀 시트 이름 (첫 시트만 쓰려면 None) ---
STORE_SHEET = 0
SCHOOL_SHEET = 0

# --- 본인 엑셀 '열 이름' -> 내부 표준 열 이름 ---
# 표준: 매장은 name, address / 학교는 name, school_type, address
# 예: 엑셀에 '매장명', '도로명주소' 가 있으면 아래처럼 적습니다.
STORE_RENAME = {
    "매장명": "name",
    "주소": "address",
}

# 매장 엑셀 선택 열 (헤더 → raw_stores / processed_data)
STORE_OPTIONAL_RENAME = {
    "운영팀": "ops_team",
    "권역": "store_region",
}

SCHOOL_RENAME = {
    "학교명": "name",
    "학교구분": "school_type",
    "주소": "address",
}

# 학교 엑셀 우측에 붙이는 선택 열 (헤더 이름 -> raw_schools / processed_data 컬럼명)
SCHOOL_OPTIONAL_RENAME = {
    "캠퍼스구분": "campus_kind",
    "캠퍼스 구분": "campus_kind",
    "담당부서": "contact_office",
    "부서명": "contact_office",
    "취업진로부서": "contact_office",
    "담당자명": "contact_name",
    "전화번호": "contact_phone",
    "이메일": "contact_email",
}


def _read_excel(path: Path, sheet) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"파일이 없습니다: {path}")
    return pd.read_excel(path, sheet_name=sheet, engine="openpyxl")


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all")
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str).str.strip()
    return df


def main() -> None:
    stores = _read_excel(STORES_XLSX, STORE_SHEET)
    stores = _clean(stores)
    stores = stores.rename(columns={**STORE_RENAME, **STORE_OPTIONAL_RENAME})
    if stores.columns.duplicated().any():
        stores = stores.loc[:, ~stores.columns.duplicated()]
    for col in ("name", "address"):
        if col not in stores.columns:
            raise ValueError(
                f"매장 엑셀에 표준 열 '{col}' 이 없습니다. "
                f"현재 열: {list(stores.columns)} / import_excel_to_csv.py 의 STORE_RENAME 을 수정하세요."
            )
    store_opt_order = ["ops_team", "store_region"]
    store_opt_present = [c for c in store_opt_order if c in stores.columns]
    stores = stores[["name", "address"] + store_opt_present]
    stores = stores[stores["name"].str.len() > 0]
    stores = stores[stores["address"].str.len() > 0]

    schools = _read_excel(SCHOOLS_XLSX, SCHOOL_SHEET)
    schools = _clean(schools)
    schools = schools.rename(columns={**SCHOOL_RENAME, **SCHOOL_OPTIONAL_RENAME})
    for col in ("name", "school_type", "address"):
        if col not in schools.columns:
            raise ValueError(
                f"학교 엑셀에 표준 열 '{col}' 이 없습니다. "
                f"현재 열: {list(schools.columns)} / SCHOOL_RENAME 을 수정하세요."
            )
    # 동일 내부 키가 두 열에서 오면 pandas가 중복 열을 만들 수 있음 → 첫 열만 유지
    if schools.columns.duplicated().any():
        schools = schools.loc[:, ~schools.columns.duplicated()]
    optional_keys_order = list(dict.fromkeys(SCHOOL_OPTIONAL_RENAME.values()))
    optional_present = [c for c in optional_keys_order if c in schools.columns]
    school_cols = ["name", "school_type", "address"] + optional_present
    schools = schools[school_cols]
    schools = schools[schools["name"].str.len() > 0]
    schools = schools[schools["address"].str.len() > 0]

    out_stores = BASE / "raw_stores.csv"
    out_schools = BASE / "raw_schools.csv"
    stores.to_csv(out_stores, index=False, encoding="utf-8-sig")
    schools.to_csv(out_schools, index=False, encoding="utf-8-sig")

    print("저장:", out_stores, f"({len(stores)}행)")
    print("저장:", out_schools, f"({len(schools)}행)")
    print("다음: python 2_geocode.py")


if __name__ == "__main__":
    main()
