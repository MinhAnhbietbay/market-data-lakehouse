{#
    Chuỗi CTE bitemporal dùng chung. Trước macro này, idiom được chép tay ở
    mỗi model, và kết quả là std_ac_ratio.sql:22 đúng còn 3 anh em cùng thư
    mục accounting/ sai -- partition thiếu run_date nên bản đính chính bị
    vứt vĩnh viễn trước khi kịp có phiên bản thứ hai.

    Model gọi như sau:

        with src as ( ... select cột từ raw ... ),

        {{ bitemporal_version_chain(
            source_cte='src',
            key_cols=['symbol', 'fiscal_year', 'period'],
            fingerprint_cols=['period_end_date', 'revenue'],
            first_known_from_expr='coalesce(cast(accepted_date as date), filing_date)',
            pit_basis='filing'
        ) }}

        select ... from _bvc_versioned

    fingerprint_excluded_cols KHÔNG sinh SQL. Nó là lời khai: "cột này phơi ở
    select cuối nhưng CỐ Ý không vào fingerprint". Có nó thì việc chọn subset
    là một quyết định đọc được và kiểm được (tests/unit/analytics/
    test_fingerprint_accounting.py); không có nó thì subset im lặng, và đó
    chính là lỗi đã đóng băng 37/32/19 cột ở ba bảng accounting.

    first_known_from_is_system_time=true cho model truyền
    first_known_from_expr='run_date' -- ở đó ngay cả V1 cũng là system time.

    Tiền tố _bvc_ để tên CTE của macro không đụng tên CTE của model.
#}
{% macro bitemporal_version_chain(
    source_cte,
    key_cols,
    fingerprint_cols,
    first_known_from_expr,
    pit_basis,
    fingerprint_excluded_cols=[],
    first_known_from_is_system_time=false,
    first_known_from_basis_label='business_date',
    order_by='run_date desc, landing_key desc',
    round_to_trading_day=true
) %}
_bvc_ranked as (
    -- run_date NẰM TRONG partition: mỗi lần raw chạy giữ lại một ứng viên,
    -- thay vì cả lịch sử bị nén xuống còn bản mới nhất.
    select
        *,
        row_number() over (
            partition by {{ key_cols | join(', ') }}, run_date
            order by {{ order_by }}
        ) as _bvc_rn
    from {{ source_cte }}
),

_bvc_latest as (
    select * from _bvc_ranked where _bvc_rn = 1
),

_bvc_fingerprinted as (
    select
        *,
        {{ attr_fingerprint(fingerprint_cols) }} as attr_hash
    from _bvc_latest
),

_bvc_with_previous as (
    -- partition ở đây KHÔNG có run_date: ta đang so phiên bản này với phiên
    -- bản trước của CÙNG một khoá qua thời gian.
    select
        *,
        lag(attr_hash) over (
            partition by {{ key_cols | join(', ') }}
            order by run_date
        ) as prev_hash
    from _bvc_fingerprinted
),

_bvc_change_points as (
    select *
    from _bvc_with_previous
    where prev_hash is null or attr_hash != prev_hash
),

_bvc_with_raw_known_from as (
    select
        *,
        -- Bản đầu tiên: ngày nghiệp vụ, KẸP bởi run_date (spec §2.2). Nguồn
        -- earnings trả cả lịch công bố tương lai; không kẹp thì một dòng ta
        -- đã biết hôm nay lại tự khai "chỉ biết được từ 2026-10-29", và bản
        -- đính chính sau đó nhận known_from NHỎ HƠN bản gốc.
        -- Bản thứ hai trở đi: run_date của change-point, đúng nghĩa system time.
        case
            when prev_hash is null then least({{ first_known_from_expr }}, run_date)
            else run_date
        end as known_from_raw,
        '{{ pit_basis }}' as pit_basis,
        -- F2: pit_basis là hằng số CẤP BẢNG, nên nó không mô tả được dòng nào
        -- lấy known_from từ ngày nghiệp vụ và dòng nào lấy từ run_date. Bản
        -- đính chính (V2+) luôn mang run_date -- tức lịch cron của ta, không
        -- phải ngày hồ sơ -- mà vẫn dán nhãn 'filing'. Downstream không tự suy
        -- ngược được: prev_hash chết ở đây, và so known_from = source_run_date
        -- cũng không phân biệt được vì least() kẹp V1 về run_date khi ngày
        -- nghiệp vụ nằm ở tương lai.
{% if first_known_from_is_system_time %}
        -- first_known_from_expr chính là run_date: cả V1 cũng là system time.
        'system_time' as known_from_basis
{% else %}
        -- Nhãn của bản đầu tiên do call-site khai, không suy từ biểu thức: cùng
        -- một dạng biểu thức có thể là sự thật đo được (filing_date) hoặc một
        -- giả định (ipo_date của Lớp 3, spec §3.3), và macro không phân biệt được.
        case
            when prev_hash is null and {{ first_known_from_expr }} <= run_date
                then '{{ first_known_from_basis_label }}'
            else 'system_time'
        end as known_from_basis
{% endif %}
    from _bvc_change_points
),

{% if round_to_trading_day %}
_bvc_known_from as (
    -- D14: đẩy tới ngày giao dịch kế tiếp. LEFT JOIN + coalesce chứ không
    -- INNER: một known_from muộn hơn ngày giao dịch cuối cùng ta biết thì
    -- KHÔNG có dòng trong bảng tra, và inner join sẽ XOÁ ÂM THẦM đúng những
    -- dòng mới nhất -- chế độ hỏng tệ nhất có thể, vì nó chỉ đụng dữ liệu gần đây.
    select
        r.*,
        r.run_date as source_run_date,
        coalesce(n.next_trading_day, r.known_from_raw) as known_from
    from _bvc_with_raw_known_from r
    left join {{ ref('std_cm_next_trading_day') }} n
      on n.calendar_date = r.known_from_raw
),

_bvc_collision_ranked as (
    -- F3: làm tròn D14 ép nhiều change-point vào cùng một ngày giao dịch. Hai
    -- change-point rơi vào hai ngày nghỉ liên tiếp (Bảy+CN, hoặc lễ+cuối tuần)
    -- đều làm tròn tới cùng thứ Hai -> hai dòng cùng (key..., known_from) ->
    -- 31/35 unique_combination test của tầng đỏ.
    --
    -- Giữ change-point MUỘN hơn: bản sau supersede bản trước, và vì cả hai cùng
    -- visible đúng một ngày nên bản sớm không mang thông tin PIT nào mà bản
    -- muộn không mang. Giữ bản sớm mới là mất thông tin.
    select
        *,
        row_number() over (
            partition by {{ key_cols | join(', ') }}, known_from
            order by known_from_raw desc, source_run_date desc
        ) as _bvc_collision_rn
    from _bvc_known_from
),

_bvc_versioned as (
    select * from _bvc_collision_ranked where _bvc_collision_rn = 1
)
{% else %}
_bvc_versioned as (
    -- Không làm tròn: known_from ở đây theo cấu tạo ĐÃ là ngày giao dịch, và
    -- ref() tới bảng lịch sẽ tạo chu trình DAG -- bảng lịch dựng TỪ model này
    -- (std_cm_trading_day.sql:12).
    --
    -- KHÔNG dedupe F3 ở nhánh này, và đó là điều CHỨNG MINH ĐƯỢC chứ không phải
    -- bỏ sót: V1 có known_from = least(expr, run_date_1) <= run_date_1, còn mọi
    -- bản sau có known_from = run_date_k với run_date_k > run_date_1
    -- (change-point sắp theo run_date, và _bvc_latest chỉ giữ MỘT dòng mỗi
    -- (khoá, run_date)). Suy ra known_from đôi một khác nhau, va chạm là BẤT
    -- KHẢ. Thêm row_number ở đây chỉ tốn một window function trên
    -- std_tx_daily_price_bar -- bảng lớn nhất của tầng.
    select
        r.*,
        r.run_date as source_run_date,
        r.known_from_raw as known_from
    from _bvc_with_raw_known_from r
)
{% endif %}
{% endmacro %}

