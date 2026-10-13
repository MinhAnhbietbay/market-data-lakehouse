{#
    Thân logic của curated_universe_membership.sql, tách ra macro để
    __pit_test_universe.sql gọi lại NGUYÊN VẸN thay vì mirror tay -- cùng lý
    do/quyết định như fam_price_body.sql/fam_group_body.sql (drift risk giữa
    model thật và model test). Khác 2 macro đó ở chỗ `window_days`/`top_n`
    CŨNG là tham số, không chỉ relation: fixture cần cửa sổ nhỏ (vd 3) để
    hand-computable, khác hẳn hằng số thật (63/3000) -- đây là lý do universe
    chưa được gộp macro cùng đợt với price/group khi macro đó mới viết.
    Quyết định 2026-08-26 (backlog #3 từ review ngoài, xử lý ngay sau khi
    được chỉ ra logic vẫn còn mirror tay).

    `include_adv_debug_columns` (mặc định false, giữ schema bảng thật không
    đổi): model thật KHÔNG expose adv_63 (không phải field catalog, spec §3
    chỉ liệt kê dollar_volume_rank/in_top3000/adv_sessions là cột mask), nhưng
    test cần thấy giá trị adv_63 thô để so khớp số tính tay chính xác (vd
    600.0 chứ không phải 366.67) -- test model truyền true để lộ cột này ra,
    không đụng gì tới bảng curated thật.
#}
{% macro universe_membership_body(price_rel, dim_date_rel, window_days, top_n, include_adv_debug_columns=false) %}

-- Đọc dollar_volume từ __fam_price, KHÔNG tự tính lại từ std_tx_daily_price_bar:
-- cùng một công thức tồn tại ở hai nơi sẽ trôi khỏi nhau, đúng loại lỗi mà
-- nguyên tắc "derive một lần, sau collapse" sinh ra để chặn.

-- Chỉ số phiên giao dịch TOÀN THỊ TRƯỜNG, độc lập với việc một mã cụ thể có
-- dòng ở ngày đó hay không -- đây là trục cửa sổ ADV phải đếm theo. Trước đây
-- dùng row_number() trên chính dòng của mã (= đếm số DÒNG, không phải số
-- PHIÊN), và join vào curated_dim_date không lọc gì (curated_dim_date phủ mọi
-- ngày lịch, không riêng ngày giao dịch) -- mã bị treo giao dịch N phiên sẽ có
-- cửa sổ kéo dài âm thầm ra 63+N phiên thật. Phát hiện ở lượt review cuối
-- plan 2026-08-25 (spec: "Cửa sổ ADV đếm theo ngày giao dịch, không theo số
-- dòng"). Hiện chưa lộ trên dữ liệu dev vì chưa mã nào có khoảng trống giao
-- dịch, nhưng công thức phải đúng trước khi universe mở rộng.
with trading_days as (
    select date, row_number() over (order by date) as trading_day_no
    from {{ dim_date_rel }}
    where is_trading_day
),

sessions as (
    select p.symbol, p.date, p.dollar_volume,
           t.trading_day_no
    from {{ price_rel }} p
    join trading_days t on t.date = p.date
),

-- range trên trading_day_no (chỉ số phiên TOÀN THỊ TRƯỜNG) thay vì rows trên
-- session_no (chỉ số dòng RIÊNG của mã) -- đây là sửa lỗi chính.
--
-- adv_sessions ĐẾM SỐ QUAN SÁT THẬT TRONG ĐÚNG CỬA SỔ range ở trên (count()
-- cùng range với avg() của adv_63), KHÔNG dùng least(session_no, N) như bản
-- trước -- session_no là tổng số dòng CẢ ĐỜI của mã, đúng cho mã IPO mới
-- (chưa có khoảng trống nên hai cách cho cùng kết quả) nhưng SAI cho mã đã
-- từng bị treo giao dịch giữa chừng rồi giao dịch lại: least(session_no, N)
-- sẽ báo "đủ N phiên" dù một phần cửa sổ hiện tại thật ra là khoảng trống,
-- che mất đúng cái spec §3 muốn adv_sessions cảnh báo ("cửa sổ thực tế dài
-- bao nhiêu"). Đếm trong đúng range window vừa đúng cho ca IPO (session_no
-- và count-trong-range trùng nhau khi không có gap) vừa đúng cho ca gap.
--
-- Tên cột LUÔN là adv_63 dù `window_days` truyền vào là gì (kể cả fixture
-- dùng window nhỏ để hand-computable) -- một macro dùng chung thì tên cột
-- đầu ra phải cố định, không phụ thuộc tham số.
adv as (
    select symbol, date,
           avg(dollar_volume) over (
               partition by symbol order by trading_day_no
               range between {{ window_days - 1 }} preceding and current row) as adv_63,
           count(dollar_volume) over (
               partition by symbol order by trading_day_no
               range between {{ window_days - 1 }} preceding and current row) as adv_sessions
    from sessions
),

-- row_number() + tiebreak symbol asc thay vì rank(): hoà hạng ở biên phải
-- cắt ĐÚNG N mã theo quy tắc XÁC ĐỊNH ĐƯỢC, nếu không rebuild sau có thể lật
-- membership của các mã hoà hạng mà không có nguyên nhân truy được -- đúng
-- loại violation freeze test sinh ra để bắt (spec §7.2.1). Cắt theo symbol là
-- tùy ý về kinh tế nhưng bất biến qua rebuild; nhóm biên vốn là các mã kém
-- thanh khoản nhất nên sai số vô hại. (Quyết định 2026-08-26, gap từ review
-- cuối plan 2026-08-25 -- rank() thuần có thể cho >N mã cùng vào top khi hoà
-- hạng, spec §3 chốt lại chính sách này.)
ranked as (
    select
        date, symbol, adv_63, adv_sessions,
        row_number() over (
            partition by date
            order by adv_63 desc, symbol asc
        ) as dollar_volume_rank
    from adv
)

select
    date,
    symbol,
    dollar_volume_rank,
    dollar_volume_rank <= {{ top_n }} as in_top3000,
    adv_sessions,
    {%- if include_adv_debug_columns %}
    adv_63,
    {%- endif %}
    current_timestamp as loaded_at
from ranked

{% endmacro %}
