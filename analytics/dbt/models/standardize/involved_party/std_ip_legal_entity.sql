{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_ip_legal_entity',
        tags=['layer::standardize', 'domain::involved_party']
    )
}}

with profile_ranked as (
    select
        cik,
        company_name,
        website,
        description,
        ceo,
        full_time_employees,
        phone,
        address,
        city,
        state,
        zip,
        image,
        default_image,
        country_code,
        currency_code,
        industry,
        sector,
        ipo_date,
        run_date,
        row_number() over (partition by cik, run_date order by ingested_at desc, symbol) as rn
    from {{ source('raw', 'raw_fmp_profile') }}
    where cik is not null
),

profile_latest as (
    select
        cik,
        company_name,
        website,
        description,
        ceo,
        full_time_employees,
        phone,
        address,
        city,
        state,
        zip,
        image,
        default_image,
        country_code,
        currency_code,
        industry,
        sector,
        ipo_date,
        run_date
    from profile_ranked
    where rn = 1
),

sec_profile_ranked as (
    select
        cik,
        registrant_name,
        business_address,
        mailing_address,
        phone_number,
        postal_code,
        city as sec_city,
        state as sec_state,
        country_code as sec_country_code,
        description as sec_description,
        ceo as sec_ceo,
        website as sec_website,
        state_location,
        state_of_incorporation,
        fiscal_year_end,
        fifty_two_week_range,
        asset_type,
        market_sector,
        tax_identification_number,
        price_currency,
        sic_code,
        sic_description,
        sic_group,
        ipo_date as sec_ipo_date,
        run_date,
        row_number() over (partition by cik, run_date order by ingested_at desc, symbol) as rn
    from {{ source('raw', 'raw_fmp_sec_profile') }}
    where cik is not null
),

sec_profile_latest as (
    select
        cik,
        registrant_name,
        business_address,
        mailing_address,
        phone_number,
        postal_code,
        sec_city,
        sec_state,
        sec_country_code,
        sec_description,
        sec_ceo,
        sec_website,
        state_location,
        state_of_incorporation,
        fiscal_year_end,
        fifty_two_week_range,
        asset_type,
        market_sector,
        tax_identification_number,
        price_currency,
        sic_code,
        sic_description,
        sic_group,
        sec_ipo_date,
        run_date
    from sec_profile_ranked
    where rn = 1
),

-- KHÔNG ép full outer join trên run_date vì các nguồn có thể chạy lệch ngày nhau.
-- Ví dụ: profile có 2026-08-18 nhưng không có 08-19, sec_profile có cả hai.
-- Nối trực tiếp bằng run_date khiến 42 dòng ở 08-19 bị rớt toàn bộ company_name thành null.
-- Ta lấy tập hợp (cik, run_date) có thật từ cả hai nguồn làm mốc thời gian quan sát.
observations as (
    select cik, run_date from profile_latest
    union
    select cik, run_date from sec_profile_latest
),

profile_asof_ranked as (
    select 
        o.cik as obs_cik,
        o.run_date as obs_run_date,
        src.*,
        row_number() over (partition by o.cik, o.run_date order by src.run_date desc) as rn
    from observations o
    left join profile_latest src 
      on o.cik = src.cik and src.run_date <= o.run_date
),

profile_asof as (
    select * from profile_asof_ranked where rn = 1
),

sec_profile_asof_ranked as (
    select 
        o.cik as obs_cik,
        o.run_date as obs_run_date,
        src.*,
        row_number() over (partition by o.cik, o.run_date order by src.run_date desc) as rn
    from observations o
    left join sec_profile_latest src 
      on o.cik = src.cik and src.run_date <= o.run_date
),

sec_profile_asof as (
    select * from sec_profile_asof_ranked where rn = 1
),

merged_by_run as (
    -- sec_profile wins over profile for every overlapping field (more authoritative, SEC-sourced)
    select
        coalesce(p.cik, s.cik) as cik,
        coalesce(p.obs_run_date, s.obs_run_date) as run_date,
        coalesce(s.sec_ipo_date, p.ipo_date) as ipo_date,
        p.company_name,
        coalesce(s.sec_website, p.website) as website,
        coalesce(s.sec_description, p.description) as description,
        coalesce(s.sec_ceo, p.ceo) as ceo,
        p.full_time_employees,
        -- Hai nguồn (profile của vendor, sec_profile của SEC) chưa bao giờ được
        -- hoà giải, nên bảng mang cả hai tên cho cùng một khái niệm. Ưu tiên
        -- SEC: đó là bản khai pháp lý, không phải bản làm giàu của vendor.
        coalesce(s.phone_number, p.phone) as phone,
        coalesce(s.business_address, p.address) as address,
        coalesce(s.sec_city, p.city) as city,
        coalesce(s.state_location, p.state) as state,
        coalesce(s.postal_code, p.zip) as postal_code,
        s.mailing_address,          -- KHÁC address: địa chỉ gửi thư
        s.state_of_incorporation,   -- KHÁC state: bang thành lập
        coalesce(s.sec_country_code, p.country_code) as country_code,
        p.currency_code,
        p.industry,
        p.sector,
        s.registrant_name,
        s.fiscal_year_end,
        s.tax_identification_number,
        nullif(s.sic_code, '') as sic_code
    from profile_asof p
    join sec_profile_asof s on p.obs_cik = s.obs_cik and p.obs_run_date = s.obs_run_date
),

-- industry_key/sector_key là dạng chuẩn hoá (upper/trim) của industry/sector, VỐN ĐÃ nằm trong
-- fingerprint_cols -- đổi industry_key/sector_key luôn kéo theo đổi industry/sector nên đã được
-- fingerprint đó bắt, không cần fingerprint riêng cho bản chuẩn hoá.
{{ bitemporal_version_chain(
    source_cte='merged_by_run',
    key_cols=['cik'],
    fingerprint_cols=['company_name', 'website', 'description', 'ceo', 'full_time_employees',
                      'phone', 'address', 'city', 'state', 'postal_code', 'mailing_address',
                      'state_of_incorporation', 'country_code', 'currency_code', 'industry',
                      'sector', 'registrant_name', 'fiscal_year_end',
                      'tax_identification_number', 'sic_code'],
    fingerprint_excluded_cols=['industry_key', 'sector_key'],
    first_known_from_expr='ipo_date',
    pit_basis='observation',
    first_known_from_basis_label='assumed_since_ipo',
    order_by='run_date desc'
) }},

versioned_intervals as (
    -- valid_from của phiên bản ĐẦU là ngày IPO (ngày niêm yết bắt đầu thật),
    -- không phải run_date đầu tiên (đó chỉ là ngày ta bắt đầu thu thập).
    -- Phiên bản sau lấy run_date của change-point: đó là thời điểm sớm nhất
    -- ta biết symbol/venue đã đổi.
    -- Macro bitemporal_version_chain đã sinh các cột as known_from và as pit_basis.
    select
        cik,
        company_name,
        website,
        description,
        ceo,
        full_time_employees,
        phone,
        address,
        city,
        state,
        postal_code,
        mailing_address,
        state_of_incorporation,
        country_code,
        currency_code,
        industry,
        sector,
        registrant_name,
        fiscal_year_end,
        tax_identification_number,
        sic_code,
        case when prev_hash is null then coalesce(ipo_date, run_date) else run_date end as valid_from,
        lead(run_date) over (partition by cik order by run_date) as valid_to,
        source_run_date,
        known_from,
        known_from_basis,
        pit_basis
    from _bvc_versioned
)

select
    cik,
    company_name,
    website,
    description,
    ceo,
    full_time_employees,
    phone,
    address,
    city,
    state,
    postal_code,
    mailing_address,
    state_of_incorporation,
    country_code,
    currency_code,
    upper(trim(industry)) as industry_key,
    industry,
    upper(trim(sector)) as sector_key,
    sector,
    registrant_name,
    fiscal_year_end,
    tax_identification_number,
    sic_code,
    valid_from,
    valid_to,
    (valid_to is null) as is_current,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from versioned_intervals

