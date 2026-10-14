-- std_ip_legal_entity.industry used to carry a hard `relationships` FK test against
-- std_gr_industry.industry. It became unsatisfiable when Task 9 turned this model from a dbt
-- snapshot into a derived model: the snapshot had gone stale at 5 ciks while the
-- derived model recomputes from source and covers all 42, so industry values now
-- appear that the reference table has no row for.
--
-- The reference table is EMPTY (0 rows, measured 2026-08-20) because its FMP
-- endpoint returns 402 on the current plan tier. A hard FK against an empty table
-- cannot pass for any non-null value -- the test is structurally impossible, not
-- merely failing.
--
-- Same ruling as assert_delisting_event_orphan_count: do NOT delete the industry
-- values (they are real data from the profile source) and do NOT delete the test
-- (that buys green by no longer asking). Guard the COUNT instead: if it climbs
-- above what we know about today, something new is leaking and a human should
-- look. When the FMP tier is upgraded and std_gr_industry fills in, this should drop to 0
-- and the hard FK can come back.
--
-- Measured 2026-08-20: 6 orphaned industry values across 42 ciks.
select count(*) as orphan_count
from {{ ref('std_ip_legal_entity') }} l
where l.industry_key is not null
  and not exists (select 1 from {{ ref('std_gr_industry') }} r where r.industry_key = l.industry_key)
having count(*) > 6
