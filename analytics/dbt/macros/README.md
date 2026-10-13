# dbt Macros

## Purpose
Reusable SQL helpers for dbt models.

## What belongs here
- naming helpers
- date/calendar helpers
- quality helper macros

## What does NOT belong here
- Python pipeline code
- service logic
- secrets

## Inputs
dbt model needs.

## Outputs
Reusable macros imported by dbt models/tests.

## Ownership
dbt / data modeling owner

## Examples
- `generate_schema_name.sql`
- `safe_divide.sql`

## Macro reference

### resolve_security_at(event_alias, symbol_column, date_column)
Phân giải `symbol + ngày -> share_class_figi` qua phiên bản niêm yết CÓ HIỆU LỰC tại ngày đó (join
theo khoảng `[valid_from, valid_to)` trên `std_ar_listing`). Spec 2026-08-19 Đ5: bảng sự kiện giữ
`symbol`, không mang figi — macro này trả giá đó một lần, ở một chỗ.

Ví dụ gọi:
```sql
select
    e.symbol,
    e.announcement_date,
    _sec.share_class_figi
from {{ ref('std_ev_merger_acquisition') }} e
{{ resolve_security_at('e', 'symbol', 'announcement_date') }}
```

**NULL nghĩa là gì:** sự kiện rơi vào khe giữa hai lần niêm yết (không phiên bản nào có hiệu lực tại
ngày đó) — đây là câu trả lời ĐÚNG, không phải lỗi cần vá. Người gọi tự quyết bỏ dòng đó hay giữ, và
phải quyết tường minh (không coalesce âm thầm về một figi "gần đúng").

### resolve_entity_at(entity_alias, cik_column, date_column)
Song sinh của `resolve_security_at`, cho trục pháp nhân (`std_ip_legal_entity`). Phân giải
`cik + ngày -> entity SCD2 version` CÓ HIỆU LỰC tại ngày đó (join theo khoảng `[valid_from, valid_to)`
trên `std_ip_legal_entity`). PK của `std_ip_legal_entity` là `(cik, valid_from, known_from)` nên join
trần theo `cik` nhân bản dòng theo số phiên bản.

Ví dụ gọi:
```sql
select
    t.symbol,
    t.company_cik,
    t.transaction_date,
    _ent.company_name
from {{ ref('std_tx_insider_transaction') }} t
{{ resolve_entity_at('t', 'company_cik', 'transaction_date') }}
```

