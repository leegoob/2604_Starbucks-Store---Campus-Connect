# -*- coding: utf-8 -*-
"""
스타벅스 '매장'만 남기기 위한 공통 필터.
- 주차장·주차 관련 POI 제외
- 카카오: category_group_code CE7(카페) 위주
- OSM: amenity·이름 기준 제외
"""

from __future__ import annotations

from typing import Any


def is_parking_like_text(name: str, category_name: str = "") -> bool:
    """이름·업종 문자열에 주차 관련이면 True (제외 대상)."""
    n = (name or "").strip()
    c = (category_name or "").strip()
    combined = f"{n} {c}"
    bad = ("주차장", "주차", "Parking", "PARKING", "주차장입구", "발렛")
    return any(b in combined for b in bad)


def place_name_is_starbucks(place_name: str) -> bool:
    n = place_name or ""
    return "스타벅스" in n or "starbucks" in n.lower()


def kakao_doc_is_starbucks_store(doc: dict[str, Any]) -> bool:
    """
    카카오 로컬 keyword 응답 1건이 '스타벅스 매장(카페)'으로 볼 수 있는지.
    주차·비카페 업종은 False.
    """
    name = str(doc.get("place_name") or "")
    if not place_name_is_starbucks(name):
        return False
    cat_name = str(doc.get("category_name") or "")
    code = str(doc.get("category_group_code") or "").strip()

    if is_parking_like_text(name, cat_name):
        return False
    if code == "PK6":  # 주차장
        return False

    # 카페 그룹이 가장 안전
    if code == "CE7":
        return True

    # 일부 문서에서 코드 누락 시: 업종에 카페·커피가 명시된 경우만 허용
    if not code:
        if "카페" in cat_name or "커피" in cat_name:
            return "주차" not in cat_name
        return False

    # FD6 음식점이어도 세부가 카페/커피인 경우만
    if code == "FD6":
        return ("카페" in cat_name or "커피" in cat_name) and "주차" not in cat_name

    return False


def osm_tags_skip_non_store(tags: dict[str, Any]) -> bool:
    """
    OSM tags에서 이 요소를 건너뛸지(True = 스킵).
    주차 시설·주차 안내 등은 True.
    """
    if not tags:
        return True
    amenity = str(tags.get("amenity") or "").strip().lower()
    if amenity in ("parking", "parking_space", "parking_entrance", "motorcycle_parking"):
        return True

    name = str(tags.get("name") or tags.get("name:ko") or "").strip()
    if is_parking_like_text(name, ""):
        return True

    if amenity in ("fuel", "charging_station"):
        return True

    return False
