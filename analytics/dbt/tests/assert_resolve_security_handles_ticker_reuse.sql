-- Ticker tái sử dụng là ca duy nhất mà macro này tồn tại để xử lý: mã huỷ niêm
-- yết năm X rồi được sàn cấp lại cho một công ty KHÁC năm Y. Join trần theo symbol
-- sẽ nối liền hai công ty thành một chuỗi liên tục và không test nào hiện có bắt
-- được. Join đúng phải chọn phiên bản niêm yết CÓ HIỆU LỰC tại ngày sự kiện.
--
-- Test dựng dữ liệu giả ngay trong câu lệnh: nó kiểm LOGIC của macro, không phụ
-- thuộc dữ liệu thật có sẵn ca đó hay chưa (hiện chưa có -- 105 mã, chưa ai tái
-- sử dụng ticker). Không có bước này thì test xanh vì rỗng, và xanh vì rỗng
-- không chứng minh được gì.
with fake_listing as (
    select 'FIGI_OLD' as share_class_figi, 'ZZZZ' as symbol,
           date '2015-01-01' as valid_from, date '2019-06-30' as valid_to
    union all
    select 'FIGI_NEW', 'ZZZZ', date '2021-03-01', cast(null as date)
),

fake_events as (
    select 'ZZZZ' as symbol, date '2017-05-10' as event_date, 'FIGI_OLD' as expected
    union all
    select 'ZZZZ', date '2022-08-01', 'FIGI_NEW'
    union all
    -- Sự kiện rơi vào KHE GIỮA hai lần niêm yết: không phiên bản nào có hiệu lực.
    -- Đáp án đúng là null, KHÔNG phải "lấy đại phiên bản gần nhất".
    select 'ZZZZ', date '2020-01-15', cast(null as string)
),

resolved_inline as (
    select
        e.symbol,
        e.event_date,
        e.expected,
        l.share_class_figi as actual
    from fake_events e
    left join fake_listing l
      on l.symbol = e.symbol
     and e.event_date >= l.valid_from
     and (l.valid_to is null or e.event_date < l.valid_to)
),

probe as (
    -- CTE có tên vì repo cấm subquery lồng ẩn danh.
    select '__NO_SUCH_SYMBOL__' as symbol, current_date as event_date
),

macro_compiles as (
    select _sec.share_class_figi as actual
    from probe
    {{ resolve_security_at('probe', 'symbol', 'event_date') }}
),

violations as (
    select symbol, event_date, expected, actual
    from resolved_inline
    where not (actual <=> expected)

    union all

    select
        cast(null as string),
        cast(null as date),
        cast(null as string),
        actual
    from macro_compiles
    where actual is not null
)

select * from violations
