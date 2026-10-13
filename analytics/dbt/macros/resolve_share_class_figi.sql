{% macro resolve_share_class_figi() %}
-- OpenFIGI /mapping returns every FIGI record worldwide that shares an input
-- identifier's ISIN (4657 rows for 105 distinct symbols measured 2026-08-19),
-- so this collapses to exactly one row per input_id_value: ISIN-path matches
-- outrank CUSIP-path, which outrank TICKER-path (measured reliability
-- order). security_type = 'PUBLIC' (baby bond/preferred) is excluded here by
-- design -- those aren't equity share classes and have no share_class_figi
-- to give. Shared by std_pd_security and std_ar_listing (Task 6/7) so the
-- priority ladder can't drift between the two callers -- each caller is
-- responsible for its own join key(s) against input_id_value.
--
-- Note: The flat subquery below is deliberate and not an oversight; it is a
-- documented exception to the CTE-only rule in spec §6.4. This macro is expanded
-- inside a CTE at all three call sites (std_pd_security.sql, std_ar_listing.sql,
-- and analytics/dbt/tests/assert_unresolved_share_class_figi_count.sql), so using
-- `with` here would create invalid nested `with` blocks.
select share_class_figi, input_id_value, input_id_type
from (
    select
        share_class_figi, input_id_value, input_id_type,
        row_number() over (
            partition by input_id_value
            order by case input_id_type
                when 'ID_ISIN' then 1 when 'ID_CUSIP' then 2 else 3 end
        ) as rn
    from {{ source('raw', 'raw_openfigi_mapping') }}
    where share_class_figi is not null
      and not (security_type <=> 'PUBLIC')
) ranked
where rn = 1
{% endmacro %}
