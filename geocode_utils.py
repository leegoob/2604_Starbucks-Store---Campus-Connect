# -*- coding: utf-8 -*-
"""매장 주소 지오코딩용 정규화·카카오 다단계 조회."""

from __future__ import annotations

import re
import time
from typing import Optional

import requests

ADDRESS_URL = "https://dapi.kakao.com/v2/local/search/address.json"
KEYWORD_URL = "https://dapi.kakao.com/v2/local/search/keyword.json"

_FLOOR_UNIT = (
    r"(?:"
    r"(?:지하\s*)?"
    r"(?:B)?\d+(?:~\d+)?층"
    r"|\d+F"
    r"|B\d+F"
    r"|L\d+(?:\s+\S+)?"
    r"|RF\b"
    r"|일부"
    r"|\d+(?:,\s*\d+)*호(?:\s*,\s*\d+호)*"
    r")"
)


def normalize_whitespace(s: str) -> str:
    t = str(s or "").strip()
    if t.lower() in ("nan", "none", ""):
        return ""
    return re.sub(r"\s+", " ", t)


def strip_parens(addr: str) -> str:
    out = re.sub(r"\([^)]*\)", "", addr)
    return normalize_whitespace(out)


def strip_floor_suffix_for_geocode(addr: str) -> str:
    """도로명·지번 뒤 층·호·키오스크 등 상세는 제거해 카카오 주소 검색에 맞춘다."""
    s = normalize_whitespace(addr)
    if not s:
        return ""
    changed = True
    while changed:
        changed = False
        for pat in (
            rf",\s*{_FLOOR_UNIT}.*$",
            rf"\s+{_FLOOR_UNIT}.*$",
            r",\s*키오스크\d+호.*$",
            r"\s+키오스크\d+호.*$",
            r",\s*제\d+층.*$",
            r"\s+제\d+층.*$",
        ):
            ns = re.sub(pat, "", s, flags=re.IGNORECASE)
            if ns != s:
                s = normalize_whitespace(ns).rstrip(",")
                changed = True
    return s


def store_geocode_query_variants(name: str, address: str) -> list[str]:
    """중복 없이 지오코딩 시도 순서대로 주소·키워드 후보."""
    name = normalize_whitespace(name)
    raw = normalize_whitespace(address)
    seen: set[str] = set()
    out: list[str] = []

    def _add(q: str) -> None:
        q = normalize_whitespace(q)
        if q and q not in seen:
            seen.add(q)
            out.append(q)

    if raw:
        _add(raw)
    stripped = strip_parens(raw)
    if stripped:
        _add(stripped)
    no_floor = strip_floor_suffix_for_geocode(raw)
    if no_floor:
        _add(no_floor)
    no_floor_stripped = strip_floor_suffix_for_geocode(stripped)
    if no_floor_stripped:
        _add(no_floor_stripped)
    if stripped:
        _add(stripped[:40])
    if no_floor:
        _add(no_floor[:40])
    if name:
        _add(name)
    head = " ".join(stripped.split()[:3]) if stripped else ""
    if head and name:
        _add(f"{head} {name}")
    return out


def _kakao_get(
    session: requests.Session,
    url: str,
    headers: dict[str, str],
    params: dict,
    retry: int = 3,
) -> Optional[dict]:
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


def kakao_geocode_address(
    session: requests.Session,
    headers: dict[str, str],
    address: str,
) -> tuple[float | None, float | None]:
    addr = normalize_whitespace(address)
    if not addr:
        return None, None
    data = _kakao_get(session, ADDRESS_URL, headers, {"query": addr, "size": 1})
    if not data:
        return None, None
    docs = data.get("documents") or []
    if not docs:
        return None, None
    d0 = docs[0]
    try:
        return float(d0["y"]), float(d0["x"])
    except (KeyError, ValueError, TypeError):
        return None, None


def kakao_geocode_keyword(
    session: requests.Session,
    headers: dict[str, str],
    query: str,
) -> tuple[float | None, float | None]:
    q = normalize_whitespace(query)[:100]
    if not q:
        return None, None
    data = _kakao_get(session, KEYWORD_URL, headers, {"query": q, "size": 5})
    if not data:
        return None, None
    docs = data.get("documents") or []
    if not docs:
        return None, None
    d0 = docs[0]
    try:
        return float(d0["y"]), float(d0["x"])
    except (KeyError, ValueError, TypeError):
        return None, None


def resolve_store_coords_kakao(
    session: requests.Session,
    headers: dict[str, str],
    name: str,
    address: str,
    pause: float = 0.09,
) -> tuple[float | None, float | None, str]:
    """좌표와 성공에 사용된 쿼리 문자열을 반환."""
    variants = store_geocode_query_variants(name, address)
    for i, q in enumerate(variants):
        if i > 0:
            time.sleep(pause)
        # 짧은 쿼리·매장명은 키워드, 긴 주소형은 주소 검색 우선
        if len(q) <= 35 or q == normalize_whitespace(name):
            lat, lon = kakao_geocode_keyword(session, headers, q)
            if lat is not None:
                return lat, lon, q
            lat, lon = kakao_geocode_address(session, headers, q)
            if lat is not None:
                return lat, lon, q
        else:
            lat, lon = kakao_geocode_address(session, headers, q)
            if lat is not None:
                return lat, lon, q
            lat, lon = kakao_geocode_keyword(session, headers, q)
            if lat is not None:
                return lat, lon, q
    return None, None, ""
