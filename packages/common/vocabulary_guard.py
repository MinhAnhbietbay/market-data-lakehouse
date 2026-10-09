"""Sáu fence của bộ từ vựng. Mỗi hàm thuần, trả list[str] -- rỗng là sạch.

Khuôn theo packages/common/catalog_curated_guard.py: không raise, không log,
chỉ trả về mô tả vi phạm để test quyết định.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping as MappingABC

from common.vocabulary import Area, Concept, Mapping, RawField, VALID_GRAINS, VALID_ROLES


def check_every_field_mapped_once(
    fields: Iterable[RawField], mappings: Iterable[Mapping]
) -> list[str]:
    """Fence 1 -- mỗi field có đúng một dòng ánh xạ, và không ánh xạ field ma.

    Field bắt đầu bằng "@" (vd. "@run_date") là pseudo-field metadata landing --
    `pipelines/raw/mapper.py::source_expression` và
    `scripts/dev/metadata/contract_live_smoke.py::compare_contract_to_payload` đã dùng
    cùng quy ước này cho `source:` của contract raw. Field này không do FMP trả về
    nên không có -- và không cần có -- dòng tương ứng trong `fmp_raw_fields.yaml` (kho
    field đóng băng theo response API thật); loại khỏi chiều "ánh xạ trỏ vào field
    không có trong kho" để không báo ma một field cố ý không thuộc payload
    (F11b, spec 2026-08-18 -- observation_date của Price Target Consensus tổng hợp từ
    @run_date vì endpoint gốc không có cột date).
    """
    inventory = {(f.endpoint, f.field) for f in fields}
    counts = Counter((m.endpoint, m.field) for m in mappings)

    violations = [
        f"field chưa được ánh xạ: {endpoint} :: {field}"
        for endpoint, field in sorted(inventory - set(counts))
    ]
    violations += [
        f"field được ánh xạ {counts[key]} lần: {key[0]} :: {key[1]}"
        for key in sorted(counts)
        if counts[key] > 1
    ]
    violations += [
        f"ánh xạ trỏ vào field không có trong kho: {endpoint} :: {field}"
        for endpoint, field in sorted(set(counts) - inventory)
        if not field.startswith("@")
    ]
    return violations


def check_every_concept_has_fields(
    concepts: Iterable[Concept], mappings: Iterable[Mapping]
) -> list[str]:
    """Fence 2 -- concept không hút được field nào thì không có lý do tồn tại.

    Ngoại lệ: concept khai `no_source: true` (vd. Trading Day) được MIỄN -- nó
    đã được thiết kế có chủ đích không có field nguồn nào, suy từ concept khác
    ở tầng downstream. Đây là ngoại lệ duy nhất, có gõ tường minh trong YAML.
    """
    claimed = {m.concept for m in mappings}
    return [
        f"concept không có field nào: {c.name} ({c.area_code})"
        for c in sorted(concepts, key=lambda c: c.name)
        if c.name not in claimed and not c.no_source
    ]


def check_concept_areas_exist(
    areas: Iterable[Area], concepts: Iterable[Concept]
) -> list[str]:
    """Fence 3 -- area phải tồn tại VÀ đang active."""
    by_code = {a.code: a for a in areas}
    violations = []
    for concept in sorted(concepts, key=lambda c: c.name):
        area = by_code.get(concept.area_code)
        if area is None:
            violations.append(f"concept {concept.name} trỏ vào area không tồn tại: {concept.area_code}")
        elif area.status != "active":
            violations.append(f"concept {concept.name} nằm trong area rỗng: {concept.area_code}")
    return violations


def check_raw_name_collisions(mappings: Iterable[Mapping]) -> list[str]:
    """Fence 4 -- hai NGHĨA khác nhau không được mang chung một raw_name.

    Hai lớp kiểm, vì "trùng tên" có hai loại rất khác nhau:

    a) Trong CÙNG một endpoint, hai field không được cùng raw_name -- chúng
       sẽ thành hai cột trùng tên của một bảng raw. Đây là đụng độ thật.

    b) Với role `attribute`, một raw_name chỉ được thuộc MỘT concept. Đây là
       lớp bắt ca netIncome: cùng tên, hai endpoint, hai con số khác nhau.

    Các role identity/event_date/known_from_date/reference được MIỄN lớp (b):
    tên của chúng là từ vựng dùng chung có chủ đích -- `symbol`, `period_end_date`,
    `filing_date`, `country_code` lặp qua nhiều concept là đúng ý đồ, không phải lỗi.
    """
    violations: list[str] = []

    per_endpoint: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for mapping in mappings:
        per_endpoint[mapping.endpoint][mapping.raw_name].add(mapping.field)
    for endpoint in sorted(per_endpoint):
        for name, source_fields in sorted(per_endpoint[endpoint].items()):
            if len(source_fields) > 1:
                violations.append(
                    f"{endpoint}: raw_name '{name}' bị {len(source_fields)} field "
                    f"dùng chung: {sorted(source_fields)}"
                )

    owners: dict[str, set[str]] = defaultdict(set)
    for mapping in mappings:
        if mapping.role == "attribute":
            owners[mapping.raw_name].add(mapping.concept)
    for name, concepts in sorted(owners.items()):
        if len(concepts) > 1:
            violations.append(
                f"raw_name '{name}' (role attribute) bị {len(concepts)} concept "
                f"dùng chung: {sorted(concepts)}"
            )

    return violations


def check_vocabulary_reaches_raw(
    fields: Iterable[RawField],
    raw_columns: MappingABC[str, set[str]],
    exempt_endpoints: frozenset[str],
) -> list[str]:
    """Fence 5 (đảo chiều) -- mọi field trong kho đóng băng phải có cột raw.

    Chiều CŨ (raw ⊆ kho) đã nghỉ hưu: kiến trúc full-fidelity cố ý cho contract
    chứa field ngoài kho curated, nên tiền đề của nó đã lỗi thời (spec §2.1). Chiều
    này mới là chiều còn đáng canh.

    exempt_endpoints là các endpoint CHƯA dựng contract vì còn khoá theo gói FMP. Nó
    được SUY RA (tắt VÀ chưa có contract), không khai tay -- nên không phình được như
    KNOWN_DRIFT cũ: muốn né fence thì phải tắt endpoint, một hành vi nhìn thấy được.
    """
    by_endpoint: dict[str, set[str]] = defaultdict(set)
    for field in fields:
        by_endpoint[field.endpoint].add(field.field)

    violations = []
    for endpoint in sorted(by_endpoint):
        if endpoint in exempt_endpoints:
            continue
        if endpoint not in raw_columns:
            violations.append(f"{endpoint}: endpoint đang bật nhưng chưa có contract raw nào")
            continue
        for field in sorted(by_endpoint[endpoint] - raw_columns[endpoint]):
            violations.append(f"{endpoint}: field '{field}' không tới được raw")
    return violations


def check_grains_valid(concepts: Iterable[Concept]) -> list[str]:
    """Fence 6 -- grain phải là một trong 5 dạng của KAFS §4."""
    return [
        f"concept {c.name} khai grain không hợp lệ: {c.grain}"
        for c in sorted(concepts, key=lambda c: c.name)
        if c.grain not in VALID_GRAINS
    ]


def check_roles_valid(mappings: Iterable[Mapping]) -> list[str]:
    """Bổ trợ fence 1 -- role phải thuộc bộ 6 giá trị của spec §6."""
    return [
        f"{m.endpoint} :: {m.field} khai role không hợp lệ: {m.role}"
        for m in mappings
        if m.role not in VALID_ROLES
    ]
