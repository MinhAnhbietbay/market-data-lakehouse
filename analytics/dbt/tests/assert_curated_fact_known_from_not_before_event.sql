-- Bất biến "không thể biết một việc trước khi nó xảy ra". Áp cho mọi bảng có đủ
-- cả known_from và một cột ngày sự kiện.
--
-- Test này KHÔNG thay thế việc đọc spec §5: nếu ai đó nối known_from vào
-- transaction_date thay vì filing_date, test vẫn xanh (vì hai cột bằng nhau).
-- Nó chỉ bắt trường hợp thô hơn: known_from nằm TRƯỚC ngày sự kiện.

select 'analyst_grade_action' as t, cast(symbol as string) as entity, grade_date as event_date, known_from
from {{ ref('curated_fact_analyst_grade_action') }}
where known_from < grade_date
union all
select 'market_cap_observation' as t, cast(symbol as string) as entity, trade_date as event_date, known_from
from {{ ref('curated_fact_market_cap_observation') }}
where known_from < trade_date
union all
select 'security_rating' as t, cast(symbol as string) as entity, rating_date as event_date, known_from
from {{ ref('curated_fact_security_rating') }}
where known_from < rating_date
union all
select 'security_rating_score' as t, cast(symbol as string) as entity, rating_date as event_date, known_from
from {{ ref('curated_fact_security_rating_score') }}
where known_from < rating_date
union all
select 'enterprise_value' as t, cast(symbol as string) as entity, period_end_date as event_date, known_from
from {{ ref('curated_fact_enterprise_value') }}
where known_from < period_end_date
union all
select 'owner_earnings' as t, cast(symbol as string) as entity, period_end_date as event_date, known_from
from {{ ref('curated_fact_owner_earnings') }}
where known_from < period_end_date
union all
select 'revenue_segment' as t, cast(symbol as string) as entity, period_end_date as event_date, known_from
from {{ ref('curated_fact_revenue_segment') }}
where known_from < period_end_date
union all
select 'workforce_observation' as t, cast(cik as string) as entity, period_end_date as event_date, known_from
from {{ ref('curated_fact_workforce_observation') }}
where known_from < period_end_date
union all
select 'ratio' as t, cast(symbol as string) as entity, period_end_date as event_date, known_from
from {{ ref('curated_fact_ratio') }}
where known_from < period_end_date
union all
select 'index_membership' as t, cast(symbol as string) as entity, valid_from as event_date, known_from
from {{ ref('curated_bridge_index_membership') }}
where known_from < valid_from
union all
select 'merger_acquisition' as t, cast(symbol as string) as entity, announcement_date as event_date, known_from
from {{ ref('curated_fact_merger_acquisition') }}
where known_from < announcement_date
union all
select 'symbol_change' as t, cast(new_symbol as string) as entity, change_date as event_date, known_from
from {{ ref('curated_fact_symbol_change') }}
where known_from < change_date
union all
select 'economic_data_release' as t, cast(event_key as string) as entity, release_date as event_date, known_from
from {{ ref('curated_fact_economic_data_release') }}
where known_from < release_date
union all
select 'ipo_event' as t, cast(symbol as string) as entity, effectiveness_date as event_date, known_from
from {{ ref('curated_fact_ipo_event') }}
where known_from < effectiveness_date
union all
select 'beneficial_ownership_position' as t, cast(symbol as string) as entity, filing_date as event_date, known_from
from {{ ref('curated_fact_beneficial_ownership_position') }}
where known_from < filing_date
union all
select 'insider_transaction' as t, cast(symbol as string) as entity, transaction_date as event_date, known_from
from {{ ref('curated_fact_insider_transaction') }}
where known_from < transaction_date
