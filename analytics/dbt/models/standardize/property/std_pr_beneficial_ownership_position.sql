{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_pr_beneficial_ownership_position',
        tags=['layer::standardize', 'domain::property']
    )
}}

-- analytics/dbt/models/standardize/property/std_pr_beneficial_ownership_position.sql
-- Empty by design: /acquisition-of-beneficial-ownership chưa có raw contract.
--
-- filing_date là cột THÊM MỚI ở đợt 3b và là chỗ duy nhất đợt này đoán trước
-- hình dạng dữ liệu chưa thấy: hồ sơ SC 13D/G luôn có ngày nộp, nhưng stub gốc
-- bỏ sót nên bảng không neo được vào thời gian. Phải verify lại tên cột thật khi
-- endpoint mở khoá (nợ N-f). first_known_from_expr='filing_date',
-- pit_basis='filing'. (spec 3b §5)
select
    cast(null as string) as symbol,
    cast(null as string) as cik,
    cast(null as string) as cusip,
    cast(null as decimal(18,8)) as sole_voting_power,
    cast(null as decimal(18,8)) as shared_voting_power,
    cast(null as decimal(18,8)) as sole_dispositive_power,
    cast(null as decimal(18,8)) as shared_dispositive_power,
    cast(null as decimal(18,8)) as amount_beneficially_owned,
    cast(null as date) as filing_date,
    cast(null as date) as source_run_date,
    cast(null as date) as known_from,
    cast(null as string) as known_from_basis,
    cast(null as string) as pit_basis
where false
