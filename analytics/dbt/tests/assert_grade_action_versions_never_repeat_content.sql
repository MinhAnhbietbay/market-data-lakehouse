-- Singular test: trong std_bd_analyst_grade_action, một khoá không được có hai
-- version mang CÙNG nội dung nghiệp vụ (previous_grade, new_grade, action).
--
-- Đo 2026-09-16 sau 3 ngày ingest: raw có 31 khoá trùng trong cùng landing
-- file, tiebreaker hoà -> Spark chọn tuỳ ý khi partition mới xuất hiện -> chuỗi
-- version flip-flop upgrade/downgrade/upgrade (AMZN 2018-05-05 Argus), và
-- grading_company thô trong fingerprint sinh version chỉ khác hoa/thường. Kết
-- quả: 19 khoá / 26 version thừa, mỗi cái là một "sự kiện" ma neo vào
-- known_from = run_date rồi chảy vào analyst_upgrade_count của mart engine.
--
-- Vì sao không phải "build 2 lần so checksum": đo cùng ngày, code cũ build 3
-- lần vẫn cho kết quả y hệt -- bất định chỉ lộ khi input đổi. Test này đo
-- TRỰC TIẾP hậu quả (nội dung lặp) nên đỏ trên code cũ ngay với dữ liệu hiện
-- có, không cần chờ thêm một ngày ingest. Với grade action, một hãng "đổi ý
-- rồi đổi lại" về đúng nội dung cũ trong cùng (symbol, grade_date) không có
-- nghĩa nghiệp vụ, nên lặp nội dung = lỗi chuỗi, không phải restatement thật.
with versions as (
    select
        symbol,
        grade_date,
        grading_company_key,
        {{ attr_fingerprint(['previous_grade', 'new_grade', 'action']) }} as content_hash
    from {{ ref('std_bd_analyst_grade_action') }}
),

per_key as (
    select
        symbol,
        grade_date,
        grading_company_key,
        count(*) as version_count,
        count(distinct content_hash) as distinct_content_count
    from versions
    group by symbol, grade_date, grading_company_key
)

select
    symbol,
    grade_date,
    grading_company_key,
    version_count,
    distinct_content_count
from per_key
where version_count > distinct_content_count
