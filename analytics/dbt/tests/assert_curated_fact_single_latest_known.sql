-- Bất biến PIT: NHIỀU NHẤT một dòng is_latest_known = true cho mỗi business key.
--
-- "Nhiều nhất", KHÔNG phải "đúng một": một thực thể có thể không có dòng is_latest_known nào
-- (ví dụ khi bảng rỗng hoặc không có dữ liệu hợp lệ).
--
-- unique trên khoá ghép (business key, known_from) KHÔNG bắt được lỗi này: hai dòng
-- khác known_from nhưng cùng is_latest_known = true vẫn lọt qua khoá ghép.

select 'analyst_grade_action' as t,
       concat_ws('|', symbol, cast(grade_date as string), grading_company_key) as bk,
       count(*) as n
from {{ ref('curated_fact_analyst_grade_action') }}
where is_latest_known
group by 1, 2
having count(*) > 1
union all
select 'market_cap_observation' as t,
       concat_ws('|', symbol, cast(trade_date as string)) as bk,
       count(*) as n
from {{ ref('curated_fact_market_cap_observation') }}
where is_latest_known
group by 1, 2
having count(*) > 1
union all
select 'security_rating' as t,
       concat_ws('|', symbol, cast(rating_date as string)) as bk,
       count(*) as n
from {{ ref('curated_fact_security_rating') }}
where is_latest_known
group by 1, 2
having count(*) > 1
union all
select 'security_rating_score' as t,
       concat_ws('|', symbol, cast(rating_date as string), score_name) as bk,
       count(*) as n
from {{ ref('curated_fact_security_rating_score') }}
where is_latest_known
group by 1, 2
having count(*) > 1
union all
select 'enterprise_value' as t,
       concat_ws('|', symbol, cast(period_end_date as string)) as bk,
       count(*) as n
from {{ ref('curated_fact_enterprise_value') }}
where is_latest_known
group by 1, 2
having count(*) > 1
union all
select 'owner_earnings' as t,
       concat_ws('|', symbol, cast(fiscal_year as string), period) as bk,
       count(*) as n
from {{ ref('curated_fact_owner_earnings') }}
where is_latest_known
group by 1, 2
having count(*) > 1
union all
select 'revenue_segment' as t,
       concat_ws('|', symbol, cast(fiscal_year as string), period, segment_name) as bk,
       count(*) as n
from {{ ref('curated_fact_revenue_segment') }}
where is_latest_known
group by 1, 2
having count(*) > 1
union all
select 'workforce_observation' as t,
       concat_ws('|', cik, cast(period_end_date as string)) as bk,
       count(*) as n
from {{ ref('curated_fact_workforce_observation') }}
where is_latest_known
group by 1, 2
having count(*) > 1
union all
select 'financial_report_document' as t,
       concat_ws('|', symbol, cast(fiscal_year as string), period) as bk,
       count(*) as n
from {{ ref('curated_dim_financial_report_document') }}
where is_latest_known
group by 1, 2
having count(*) > 1
union all
select 'ratio' as t,
       concat_ws('|', symbol, cast(fiscal_year as string), period) as bk,
       count(*) as n
from {{ ref('curated_fact_ratio') }}
where is_latest_known
group by 1, 2
having count(*) > 1
union all
select 'index_membership' as t,
       concat_ws('|', index_symbol, symbol, cast(valid_from as string)) as bk,
       count(*) as n
from {{ ref('curated_bridge_index_membership') }}
where is_latest_known
group by 1, 2
having count(*) > 1
union all
select 'merger_acquisition' as t,
       concat_ws('|', symbol, cik, cast(announcement_date as string), targeted_party_key) as bk,
       count(*) as n
from {{ ref('curated_fact_merger_acquisition') }}
where is_latest_known
group by 1, 2
having count(*) > 1
union all
select 'symbol_change' as t,
       concat_ws('|', old_symbol, new_symbol, cast(change_date as string)) as bk,
       count(*) as n
from {{ ref('curated_fact_symbol_change') }}
where is_latest_known
group by 1, 2
having count(*) > 1
union all
select 'insider_activity_summary' as t,
       concat_ws('|', symbol, cik, cast(fiscal_year as string), fiscal_quarter) as bk,
       count(*) as n
from {{ ref('curated_fact_insider_activity_summary') }}
where is_latest_known
group by 1, 2
having count(*) > 1
union all
select 'economic_data_release' as t,
       concat_ws('|', event_key, country_code, cast(release_date as string)) as bk,
       count(*) as n
from {{ ref('curated_fact_economic_data_release') }}
where is_latest_known
group by 1, 2
having count(*) > 1
union all
select 'ipo_event' as t,
       concat_ws('|', symbol, cast(effectiveness_date as string)) as bk,
       count(*) as n
from {{ ref('curated_fact_ipo_event') }}
where is_latest_known
group by 1, 2
having count(*) > 1
union all
select 'beneficial_ownership_position' as t,
       concat_ws('|', symbol, cik) as bk,
       count(*) as n
from {{ ref('curated_fact_beneficial_ownership_position') }}
where is_latest_known
group by 1, 2
having count(*) > 1
union all
select 'insider_transaction' as t,
       concat_ws('|', reporting_cik, symbol, cast(transaction_date as string), transaction_type) as bk,
       count(*) as n
from {{ ref('curated_fact_insider_transaction') }}
where is_latest_known
group by 1, 2
having count(*) > 1
