# -*- coding: utf-8 -*-
"""
[Step 1] 샘플 CSV 생성 + 공공 데이터 확보 가이드
========================================
■ 실제 데이터를 쓰고 싶을 때 (요약)
  1) 학교(고등학교·대학교)
     - 공공데이터포털 https://www.data.go.kr 에서
       "학교기본정보", "학교알리미", "고등학교", "대학교" 등으로 검색
     - CSV/엑셀을 받아 '학교명', '주소' 열만 골라 이 스크립트와 같은 형식으로 맞춤
  2) 스타벅스 매장
     - 공식 매장 정보·지도 서비스 이용약관을 확인한 뒤 수동 정리하거나,
       카카오/네이버 지도 API 등으로 주소·좌표를 수집하는 방식이 일반적
  3) 크롤링
     - 대상 사이트 robots.txt·이용약관 준수 필수

■ 이 스크립트가 만드는 파일 (실습용 가상 매장명 + 서울 도로명 주소)
  - raw_stores.csv    열: name, address
  - raw_schools.csv   열: name, school_type, address

실행: python 1_data_prep.py
다음: python 2_geocode.py
"""

from __future__ import annotations

import csv
import random
from pathlib import Path

OUT_DIR = Path(__file__).resolve().parent

STORE_NAME_PREFIXES = ["스타벅스", "Starbucks"]
STORE_SUFFIXES = [
    "강남역점",
    "역삼역점",
    "선릉점",
    "삼성점",
    "잠실점",
    "홍대입구점",
    "신촌점",
    "명동점",
    "을지로입구점",
    "건대입구점",
    "건대스타시티점",
    "왕십리역점",
    "성수역점",
    "여의도IFC점",
    "영등포타임스퀘어점",
    "노원점",
    "수유역점",
    "신림점",
    "관악점",
    "사당역점",
]

SEOUL_STREETS = [
    ("서울특별시 강남구 강남대로 396", "강남"),
    ("서울특별시 강남구 테헤란로 152", "역삼"),
    ("서울특별시 강남구 테헤란로 513", "삼성"),
    ("서울특별시 송파구 올림픽로 300", "잠실"),
    ("서울특별시 마포구 양화로 188", "홍대"),
    ("서울특별시 서대문구 신촌로 134", "신촌"),
    ("서울특별시 중구 명동8길 27", "명동"),
    ("서울특별시 중구 을지로 281", "을지로"),
    ("서울특별시 광진구 능동로 120", "건대"),
    ("서울특별시 성동구 아차산로 111", "왕십리"),
    ("서울특별시 성동구 아차산로 15", "성수"),
    ("서울특별시 영등포구 국제금융로 10", "여의도"),
    ("서울특별시 영등포구 영중로 15", "영등포"),
    ("서울특별시 노원구 동일로 1344", "노원"),
    ("서울특별시 강북구 도봉로 338", "수유"),
    ("서울특별시 관악구 관악로 1", "관악"),
    ("서울특별시 동작구 남부순환로 1614", "사당"),
    ("서울특별시 서초구 서초대로 411", "서초"),
    ("서울특별시 종로구 종로 78", "종로"),
    ("서울특별시 용산구 한강대로 23", "용산"),
]

HIGH_SCHOOL_SAMPLES = [
    ("가상고등학교", "고등학교"),
    ("한빛고등학교", "고등학교"),
    ("서울샘고등학교", "고등학교"),
    ("미래고등학교", "고등학교"),
    ("중앙고등학교", "고등학교"),
    ("동명여자고등학교", "고등학교"),
    ("개포고등학교", "고등학교"),
    ("대치고등학교", "고등학교"),
    ("반포고등학교", "고등학교"),
    ("한강고등학교", "고등학교"),
]

UNIV_SAMPLES = [
    ("가상대학교", "대학교"),
    ("서울샘대학교", "대학교"),
    ("한국과학기술원", "대학교"),
    ("연세대학교", "대학교"),
    ("고려대학교", "대학교"),
    ("서울대학교", "대학교"),
    ("성균관대학교", "대학교"),
    ("이화여자대학교", "대학교"),
    ("한양대학교", "대학교"),
    ("중앙대학교", "대학교"),
]


def _pick_addresses(n: int, rng: random.Random) -> list[str]:
    streets = [s[0] for s in SEOUL_STREETS]
    if n <= len(streets):
        return rng.sample(streets, n)
    out = streets[:]
    while len(out) < n:
        out.append(rng.choice(streets))
    rng.shuffle(out)
    return out[:n]


def write_stores_csv(path: Path, count: int, rng: random.Random) -> None:
    names = []
    for suf in STORE_SUFFIXES:
        prefix = rng.choice(STORE_NAME_PREFIXES)
        names.append(f"{prefix} {suf}")
    rng.shuffle(names)
    names = names[:count]
    addresses = _pick_addresses(len(names), rng)

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["name", "address"])
        w.writeheader()
        for name, addr in zip(names, addresses):
            w.writerow({"name": name, "address": addr})


def write_schools_csv(path: Path, count: int, rng: random.Random) -> None:
    hs = HIGH_SCHOOL_SAMPLES[:]
    uv = UNIV_SAMPLES[:]
    rng.shuffle(hs)
    rng.shuffle(uv)
    pool = [(t[0], t[1]) for t in hs] + [(t[0], t[1]) for t in uv]
    rng.shuffle(pool)
    pool = pool[:count]
    addresses = _pick_addresses(len(pool), rng)
    rows = []
    for (school_name, school_type), addr in zip(pool, addresses):
        rows.append(
            {
                "name": school_name,
                "school_type": school_type,
                "address": addr,
            }
        )

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["name", "school_type", "address"])
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> None:
    rng = random.Random(42)
    stores_path = OUT_DIR / "raw_stores.csv"
    schools_path = OUT_DIR / "raw_schools.csv"

    write_stores_csv(stores_path, count=min(20, len(STORE_SUFFIXES)), rng=rng)
    write_schools_csv(schools_path, count=20, rng=rng)

    print("Created:", stores_path)
    print("Created:", schools_path)
    print("Next: python 2_geocode.py")


if __name__ == "__main__":
    main()
