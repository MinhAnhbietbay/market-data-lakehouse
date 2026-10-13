{#
    Thân logic của __fam_price.sql, tách ra macro để __pit_test_price.sql (Task 18)
    gọi lại NGUYÊN VẸN thay vì copy tay -- tránh đúng loại lỗi "cùng công thức
    tồn tại ở hai nơi rồi trôi khỏi nhau" mà chính comment gốc của model này đã
    cảnh báo cho universe (curated_universe_membership.sql). Trước đợt này,
    __pit_test_price.sql là bản copy tay giống hệt __fam_price.sql byte-for-byte
    trừ config/ref() -- nếu ai sửa công thức thật mà quên mirror sang test,
    fixture vẫn xanh trên logic cũ. Quyết định 2026-08-26, sau review cuối plan
    2026-08-25 phát hiện việc này không có drift guard nào.

    3 tham số là 3 nguồn dữ liệu (relation): __fam_price.sql truyền các std_*
    thật, __pit_test_price.sql truyền các pit_seed_* fixture (Task 18). Toàn bộ
    công thức tính adj_factor/adj_close/returns... giữ NGUYÊN,
    không đổi một dòng so với bản trước khi tách macro. (market_cap đã chuyển
    sang fam_ratios_body, đợt 3c -- xem comment dưới chữ ký macro.)
#}
{% macro fam_price_body(bars_rel, split_rel, dividend_rel, next_trading_day_rel) %}

{#
    share_count_rel đã bỏ (đợt 3c, spec §3.4.3): market_cap từng dựng ở đây chỉ vì
    họ giá là nơi DUY NHẤT từng có một nguồn share count -- và chính sự sắp đặt đó
    là nguyên nhân lỗi. std_pd_share_count_observation là endpoint SNAPSHOT (5 dòng,
    1 quan sát/symbol), nên market_cap chỉ non-null 4/32.966 dòng và kéo 9 ratio
    chết theo. market_cap giờ ở fam_ratios_body, nền close * shares_basic.
#}

-- KHÔNG dùng spine: dùng spine ở đây sẽ tạo vòng phụ thuộc
-- spine -> universe -> giá -> spine. Chạy trên TOÀN BỘ mã, chưa lọc universe.
--
-- Nhưng KHÔNG dùng spine không có nghĩa là không cần collapse version. Bản cũ
-- của macro này viết "bảng bar đã ở đúng grain (date, symbol) ... nên không cần
-- collapse" -- sai, và schema.yml của chính bảng nguồn nói ngược lại: test grain
-- ở đó là (symbol, trade_date, known_from). std_tx_daily_price_bar đi qua
-- bitemporal_version_chain với volume nằm trong fingerprint, nên một thanh giá
-- được vendor đính chính sinh version THỨ HAI ở cùng (symbol, trade_date).
-- Đo 2026-09-15: FMP làm tròn lại volume 2026-09-11 của AAPL (50.716.865 ->
-- 50.716.900) và META (16.924.953 -> 16.925.000) giữa hai lần ingest.
--
-- Không collapse thì hỏng NHÂN TẦNG, không chỉ nhân đôi: `bars` giữ 2 dòng, và
-- `cumulative` (dựng từ cùng nguồn qua daily_factor) cũng giữ 2 dòng, nên phép
-- join cumulative bên dưới cho 2 x 2 = 4 dòng/ngày. Từ đó chảy thẳng xuống
-- curated_universe_membership -- một mã một ngày nhận 4 dollar_volume_rank khác
-- nhau -- và làm đỏ test grain của bảng đó, khiến dbt SKIP cả nhánh
-- __spine_daily -> __fam_* -> curated_alpha_fields_daily.
--
-- Chọn known_from lớn nhất = bản đính chính mới nhất ta biết. Đây là họ giá
-- "latest known", không có trục as-of (macro không nhận as_of), nên đúng ngữ
-- nghĩa mà phần còn lại của macro vẫn luôn giả định. Tiebreak source_run_date
-- chỉ để row_number tất định nếu hai version từng trùng known_from; khoá tự
-- nhiên (symbol, trade_date, known_from) đã là duy nhất nên nhánh đó không xảy
-- ra trên dữ liệu hợp lệ.
with bars_versioned as (
    select
        symbol,
        trade_date as date,
        open,
        high,
        low,
        close,
        volume,
        -- Spark không có QUALIFY (AGENTS.md:44) -- xếp hạng ở đây, lọc rn = 1 ở CTE sau.
        row_number() over (
            partition by symbol, trade_date
            order by known_from desc, source_run_date desc
        ) as __bar_rn
    from {{ bars_rel }}
),

bars as (
    select symbol, date, open, high, low, close, volume
    from bars_versioned
    where __bar_rn = 1
),

-- Hệ số split và hệ số cổ tức TÁCH BIỆT, không gộp: adj_volume chỉ dùng hệ số
-- split (cổ tức không làm đổi số cổ phiếu), còn adj_close dùng cả hai nên
-- returns từ nó là total return.
--
-- Ngày sự kiện của vendor KHÔNG luôn là ngày giao dịch: NVDA có split 2:1 ex-date
-- 2001-09-12, đúng đợt thị trường đóng cửa sau 11/9 (phiên cuối 09-10, phiên kế
-- tiếp 09-17). Join thẳng ex_date = bars.date làm hệ số biến mất im lặng -- đo
-- 2026-09-02: adj_factor hai bên khe bằng hệt nhau và returns bịa ra -55,1%.
-- Snap sang ngày giao dịch kế tiếp, cùng quy ước mà bitemporal_version_chain đã
-- dùng cho known_from của chính hàng đó (nó ghi 2001-09-17).
--
-- Cửa chặn 7 ngày lịch: sự kiện cách phiên kế tiếp quá xa thì KHÔNG snap (điều
-- kiện join không thoả -> next_trading_day NULL -> coalesce trả ex_date gốc ->
-- không có bar nào khớp -> bỏ qua, đúng hành vi cũ). Không có cửa chặn này, khi
-- lịch sử giá được backfill lùi thì một ex_date hỏng sẽ bị đẩy đi hàng tháng.
split_snapped as (
    select
        s.symbol,
        coalesce(n.next_trading_day, s.split_effective_date) as ex_date,
        s.numerator,
        s.denominator
    from {{ split_rel }} s
    left join {{ next_trading_day_rel }} n
        on  n.calendar_date = s.split_effective_date
        and datediff(n.next_trading_day, s.split_effective_date) <= 7
    where s.numerator > 0 and s.denominator > 0
),

-- Gộp TRƯỚC khi join: snap không đơn ánh (hai ex_date khác nhau rơi về cùng một
-- phiên), nên không gộp thì bars bị nhân bản -- và adv20 đếm cửa sổ theo DÒNG nên
-- một ngày trùng đẩy lệch 20 ngày kế tiếp.
--
-- HAI ĐẠI LƯỢNG NGHỊCH ĐẢO NHAU, đừng dùng chung một công thức:
--   price_factor = denominator/numerator  (2:1 -> 0,5) -- hệ số điều chỉnh giá
--   split_ratio  = numerator/denominator  (2:1 -> 2,0) -- field `split` của BRAIN
-- Dùng nhầm chiều thì giá đáng lẽ nhân 0,5 lại nhân 2.
split_factor as (
    select
        symbol,
        ex_date,
        exp(sum(ln(cast(denominator as double) / cast(numerator as double)))) as price_factor,
        exp(sum(ln(cast(numerator as double) / cast(denominator as double)))) as split_ratio
    from split_snapped
    group by symbol, ex_date
),

dividend_snapped as (
    select
        d.symbol,
        coalesce(n.next_trading_day, d.ex_dividend_date) as ex_date,
        d.dividend
    from {{ dividend_rel }} d
    left join {{ next_trading_day_rel }} n
        on  n.calendar_date = d.ex_dividend_date
        and datediff(n.next_trading_day, d.ex_dividend_date) <= 7
),

dividend_factor as (
    select symbol, ex_date, sum(dividend) as dividend
    from dividend_snapped
    group by symbol, ex_date
),

-- Thứ tự khi trùng ngày: SPLIT TRƯỚC, cổ tức sau, và cổ tức quy đổi theo giá
-- đã điều chỉnh split. Không khai thứ tự này thì hai người implement ra hai
-- chuỗi giá khác nhau mà cả hai đều "đúng công thức".
--
-- CẢNH BÁO cạnh biên (Ø chưa fix, ghi lại để không quên): split_factor/
-- dividend_factor join vào bars theo (symbol, ex_date) = (symbol, date) --
-- một sự kiện có ex_date KHÔNG trùng ngày nào trong bars (vd dữ liệu vendor
-- lỗi ghi ex_date vào cuối tuần/ngày lễ) sẽ bị BỎ QUA HOÀN TOÀN, không có
-- dòng nào đại diện cho nó để báo lỗi. Hiếm khi xảy ra vì ex_date thật luôn
-- là ngày giao dịch theo quy ước sàn Mỹ, nhưng nếu xảy ra, hệ số của sự kiện
-- đó biến mất khỏi adj_factor mà không ai biết. Cũng lưu ý: nếu div_f từng
-- trở thành NULL (chỉ xảy ra khi prev_close*split_f = 0, một ca dữ liệu hỏng
-- nặng), sum() ở cumulative bên dưới ÂM THẦM bỏ qua NULL đó như thể hệ số
-- ngày đó là 1 (không nhân, không lỗi) -- SQL SUM() mặc định bỏ qua NULL.
-- Thiệt hại lan: ln(split_f * div_f) là MỘT số hạng duy nhất, nên nếu đúng
-- ngày đó CŨNG có một split hợp lệ (split_f khác 1), hệ số split đó cũng mất
-- theo luôn -- không chỉ hệ số cổ tức bị NULL mới biến mất, mà cả ngày.
daily_factor as (
    select b.symbol, b.date,
           coalesce(s.price_factor, 1.0) as split_f,
           1.0 - coalesce(d.dividend, 0) /
                 nullif(lag(b.close) over (partition by b.symbol order by b.date)
                        * coalesce(s.price_factor, 1.0), 0) as div_f
    from bars b
    left join split_factor s    on s.symbol = b.symbol and s.ex_date = b.date
    left join dividend_factor d on d.symbol = b.symbol and d.ex_date = b.date
),

-- Hệ số tích luỹ lùi: bằng 1 ở ngày mới nhất, nhân dồn ngược về quá khứ.
cumulative as (
    select symbol, date,
           exp(sum(ln(split_f)) over (
               partition by symbol order by date
               rows between 1 following and unbounded following)) as split_adj_factor,
           exp(sum(ln(split_f * div_f)) over (
               partition by symbol order by date
               rows between 1 following and unbounded following)) as adj_factor
    from daily_factor
),

bars_with_factors as (
    select
        b.symbol,
        b.date,
        b.open,
        b.high,
        b.low,
        b.close,
        b.volume,
        coalesce(c.adj_factor, 1.0)                                 as adj_factor,
        b.open  * coalesce(c.adj_factor, 1.0)                       as adj_open,
        b.high  * coalesce(c.adj_factor, 1.0)                       as adj_high,
        b.low   * coalesce(c.adj_factor, 1.0)                       as adj_low,
        b.close * coalesce(c.adj_factor, 1.0)                       as adj_close,
        b.volume / nullif(coalesce(c.split_adj_factor, 1.0), 0)     as adj_volume,
        lag(b.close * coalesce(c.adj_factor, 1.0)) over (
            partition by b.symbol order by b.date
        )                                                           as prev_close,
        b.close * b.volume                                          as dollar_volume,
        sf.split_ratio                                              as split,
        df.dividend                                                 as dividend
    from bars b
    left join cumulative c
        on c.symbol = b.symbol and c.date = b.date
    left join split_factor sf
        on sf.symbol = b.symbol and sf.ex_date = b.date
    left join dividend_factor df
        on df.symbol = b.symbol and df.ex_date = b.date
),

-- adv20 và typical_price phải nằm ở tầng sau bars_with_factors: cả hai đọc các cột
-- adj_* mà Spark chưa cho tham chiếu alias trong cùng một SELECT.
--
-- adv20 = "average daily volume in past 20 days" (BRAIN). Nền là adj_volume, KHÔNG
-- phải volume thô: trung bình 20 ngày trên khối lượng thô nhảy bậc qua mọi ranh giới
-- split. Đòi ĐỦ 20 quan sát non-null -- count() đếm non-null nên điều kiện = 20 bao
-- luôn cả "đủ 20 dòng" lẫn "không dòng nào NULL"; 19 ngày đầu mỗi mã là NULL có chủ ý.
with_windows as (
    select
        *,
        (adj_high + adj_low + adj_close) / 3                        as typical_price,
        avg(adj_volume) over (
            partition by symbol order by date
            rows between 19 preceding and current row
        )                                                           as __adv20_avg,
        count(adj_volume) over (
            partition by symbol order by date
            rows between 19 preceding and current row
        )                                                           as __adv20_n
    from bars_with_factors
)

select
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,
    typical_price,
    adj_factor,
    adj_open,
    adj_high,
    adj_low,
    adj_close,
    adj_volume,
    adj_close / nullif(prev_close, 0) - 1                       as returns,
    ln(adj_close / nullif(prev_close, 0))                       as log_return,
    prev_close,
    (adj_high - adj_low) / nullif(prev_close, 0)                as amplitude,
    (adj_open - prev_close) / nullif(prev_close, 0)             as gap_pct,
    dollar_volume,
    case when __adv20_n = 20 then __adv20_avg end                as adv20,
    adj_close * adj_volume                                      as adj_dollar_volume,
    split,
    dividend,
    current_timestamp                                           as loaded_at
from with_windows

{% endmacro %}
