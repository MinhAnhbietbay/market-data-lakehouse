# dbt Models: Standardize

## Purpose
Spark + dbt canonical integrated models. This layer is normalized (3NF), source-independent, and organized by concept area. Current areas: `accounting`, `arrangement`, `business_direction`, `common`, `documentation`, `event`, `group`, `involved_party`, `location`, `product`, `property`, `transaction` (area codes `ac`, `ar`, `bd`, `cm`, `do`, `ev`, `gr`, `ip`, `lo`, `pd`, `pr`, `tx`). See `docs/superpowers/specs/` for each slice's design doc.

Các vi phạm còn lại (nợ kỹ thuật cố ý):
- Ánh xạ venue → market composite trong `std_ar_listing` là proxy, chưa có bảng tra thật.
- Ba FK còn tautological: `std_ip_research_firm`, `std_gr_sector`, `std_gr_industry` (đã tự khai trong `schema.yml`).

## What belongs here
- Reusable business entities
- source-independent keys
- point-in-time-safe models

## What does NOT belong here
- UI-specific tables
- API response shaping
- field-oriented alpha stores

## Inputs
dbt raw models and sometimes other standardize entities.

## Outputs
Canonical entities consumed by curated models.

## Naming conventions
`std_<area_code>_<entity>.sql` for canonical integrated entities, where `<area_code>` is the two-letter area code of the containing directory (`ac`, `ar`, `bd`, `cm`, `do`, `ev`, `gr`, `ip`, `lo`, `pd`, `pr`, `tx`).

## Ownership
dbt / data modeling owner

## Examples
- `involved_party/std_ip_legal_entity.sql`
- `arrangement/std_ar_listing.sql`
- `event/std_ev_delisting_event.sql`

## Modeling Priority

Standardize models should prioritize correctness, entity consistency, and point-in-time safety over query convenience. They are not designed to be consumed directly by the alpha expression runtime.

## Bitemporal shape (spec 2026-08-20 §3.1 & 2026-08-22)

Every versioned model carries `known_from` (date, always a trading day) + `known_from_basis` (`'business_date'` | `'system_time'`) + `pit_basis` (`'filing'` | `'observation'`), and its composite key includes `known_from`.

- `known_from_basis`: Nguồn gốc của known_from Ở TỪNG DÒNG. 'business_date' = lấy từ mốc thời gian nghiệp vụ (ngày SEC chấp nhận, ngày ex-dividend...). 'system_time' = lấy từ run_date, tức thời điểm pipeline của ta bắt được, nên nó MANG trọn độ trễ ingest. pit_basis là nhãn cấp bảng và không phân biệt được hai ca này -- mọi bản đính chính (V2+) đều là 'system_time' kể cả ở bảng khai pit_basis='filing'.

Các model bitemporal được chuẩn hoá thông qua macro dùng chung `bitemporal_version_chain` (`macros/bitemporal_version_chain.sql`). Macro quản lý logic fingerprint, change-point detection và bọc timeline.

### Hư cấu backfill (spec §2.1)
`known_from` của bản V1 là **hư cấu backfill** (ngày nghiệp vụ / `least(ngày nghiệp vụ, run_date)`): nó nói "thông tin tồn tại từ ngày nghiệp vụ", không nói "hệ thống ta biết từ ngày đó". Từ bản V2+ trở đi, `known_from` phản ánh system time thật (run_date của lần đầu tiên nhìn thấy giá trị mới), bảo đảm as-of query as-of thời điểm trong quá khứ trả về đúng trạng thái lịch sử đã biết lúc bấy giờ.

Two business-time shapes:

- **A — period-anchored**: the natural key already carries the business period (e.g. `symbol, fiscal_year, period`); `known_from` is the only extra time column needed.
- **B — continuously-changing**: entities without a natural period use `valid_from`/`valid_to` (unrounded business dates) alongside `known_from` (D14-rounded to the next trading day). Reference model: `involved_party/std_ip_legal_entity.sql`.

Static lookup dimensions and the 13 empty tables awaiting an FMP tier upgrade are exempt from `known_from` — see the comment on each model in its `schema.yml` for why.
