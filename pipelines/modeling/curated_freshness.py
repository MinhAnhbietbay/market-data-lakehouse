"""Độ tươi của các mart engine đọc, đo trên snapshot ĐÃ GHIM.

Tách khỏi DAG để test được mà không cần dựng Airflow: `pool_runtime_daily.gate_ready`
là `@task` in-process, chạy trong môi trường riêng của Airflow nơi chỉ
alpha_engine/pyiceberg/pipelines import được (xem docstring của is_realloc_day).

Sự cố 2026-09-15 là lý do file này tồn tại. `gate_ready` hỏi
`select max(date) from curated_universe_membership` trên bảng LIVE rồi lấy đó làm `ds`,
trong khi engine đọc theo `data_build_id` đã ghim. Hai thứ đó lệch nhau khi một build hỏng:
`dbt_build_curated` đỏ -> `stamp_build_id` không chạy -> build ghim được vẫn là bản cũ
(dữ liệu tới 09-11), nhưng bảng live đã có 09-14 nhờ những model build xong trước khi dbt
dừng nhánh. Gate xanh, pool tính PnL cho ngày 09-14 trên field của 09-11.

Hai luật, cả hai đều cần:
  1. đọc ngày từ snapshot ghim, không phải trạng thái hiện thời của bảng;
  2. mọi mart engine đọc phải cùng ngày -- không chỉ riêng curated_universe_membership,
     vì chính curated_alpha_fields_daily mới là nơi field sống, và nó nằm cuối nhánh
     __spine_daily -> __fam_* nên là thứ bị dbt SKIP đầu tiên khi có test đỏ.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable


def resolve_pinned_ds(latest_dates: dict[str, dt.date | None]) -> dt.date:
    """Ngày dữ liệu dùng chung cho cả run, hoặc ValueError nêu đích danh bảng lệch.

    Cố tình KHÔNG lùi về min(): một mart tụt lại là dấu hiệu build hỏng, và chạy tiếp trên
    ngày cũ chỉ làm sự cố lặng đi thêm một tầng. Thà không có điểm còn hơn có điểm sai --
    cùng nguyên tắc gate_ready đã theo từ đầu.
    """
    if not latest_dates:
        raise ValueError("resolve_pinned_ds: không có bảng nào để đo")

    empty = sorted(name for name, date in latest_dates.items() if date is None)
    if empty:
        raise ValueError(f"mart rỗng ở snapshot đã ghim: {', '.join(empty)} -- chưa sẵn sàng")

    distinct = {date for date in latest_dates.values()}
    if len(distinct) > 1:
        newest = max(distinct)
        lagging = sorted(
            f"{name}={date.isoformat()}"
            for name, date in latest_dates.items()
            if date != newest
        )
        raise ValueError(
            f"mart lệch ngày ở snapshot đã ghim (mới nhất {newest.isoformat()}): "
            f"{', '.join(lagging)} -- build hỏng, không chạy pool"
        )

    return distinct.pop()


def trading_days_behind(
    ds: dt.date, today: dt.date, trading_days: Iterable[dt.date]
) -> int:
    """Số PHIÊN mà mart đang tụt so với hôm nay.

    `trading_days` là lịch ĐÃ BIẾT, và nó thường dừng sớm -- đó là điểm mấu chốt của hàm
    này, không phải chi tiết phụ. curated_dim_date được sinh TỪ chính dữ liệu giá: đo
    2026-09-15 nó dừng đúng ở 2026-09-14, ngày cuối cùng có giá. Nên khi mart tụt lại,
    lịch tụt theo y hệt, và một phép đếm chỉ dựa vào lịch sẽ luôn ra 0 -- hàng rào tự làm
    mù chính nó. Đo sống: mart đứng yên 8 ngày vẫn báo XANH.

    Vì vậy đếm hai đoạn:
      1. trong tầm lịch đã biết -- chính xác, ngày lễ được tôn trọng;
      2. sau đó -- suy bằng thứ trong tuần (T2-T6).

    Đoạn 2 có thể đếm dư nếu khoảng tụt vướng ngày lễ, và như thế là ĐÚNG hướng: thà kêu
    hơi to khi pipeline đã chết vài ngày, còn hơn im lặng. Chiều ngược lại không rủi ro:
    khi mart khoẻ, lịch đã biết phủ tới phiên gần nhất nên đoạn 2 rỗng, không có báo oan
    ngày lễ.

    Không tính phiên hôm nay: EOD của một phiên chỉ có sau khi phiên đó đóng, nên mart
    khoẻ vào sáng nay đúng ra chỉ có tới phiên GẦN NHẤT ĐÃ ĐÓNG.
    """
    known = sorted(trading_days)
    in_range = sum(1 for day in known if ds < day < today)

    horizon = max(known[-1], ds) if known else ds
    extrapolated = 0
    day = horizon + dt.timedelta(days=1)
    while day < today:
        if day.weekday() < 5:  # T2-T6; ngày lễ ngoài tầm lịch thì chịu đếm dư, xem docstring
            extrapolated += 1
        day += dt.timedelta(days=1)

    return in_range + extrapolated


def assert_fresh(
    ds: dt.date,
    today: dt.date,
    trading_days: Iterable[dt.date],
    max_behind: int,
) -> None:
    """Hàng rào ĐỘC LẬP với chuỗi asset: nó chạy theo cron kể cả khi modeling không chạy.

    Đó là toàn bộ lý do nó tồn tại. Mọi hàng rào khác trong hệ đều nằm TRONG chuỗi
    landing -> raw -> modeling -> pool, nên khi chuỗi đứng im (DAG paused, scheduler chết,
    build đỏ ở giữa) thì không cái nào lên tiếng. Sự cố 2026-09-15 trôi 3 phiên đúng vì
    thế.

    `max_behind=0` là mặc định đúng, không phải chặt quá: với phép đếm ở trên, một mart
    khoẻ cho ra 0. Ngưỡng 2 (đề xuất ban đầu 2026-09-15) đo ra là quá lỏng -- chính sự cố
    hôm đó chỉ ra 1 phiên tụt, nên hàng rào đặt ở 2 sẽ im lặng đúng lúc cần kêu.
    """
    behind = trading_days_behind(ds, today, trading_days)
    if behind > max_behind:
        raise ValueError(
            f"mart đang tụt {behind} phiên (ds={ds.isoformat()}, hôm nay={today.isoformat()}, "
            f"ngưỡng={max_behind}) -- modeling không tiến, kiểm tra market_data_modeling"
        )


def assert_macro_content(
    latest_value_dates: dict[str, dt.date | None],
    ds: dt.date,
) -> None:
    """Hàng rào NỘI DUNG cho curated_macro_daily -- mart duy nhất mà `max(date)` nói dối.

    Hai mart kia (`curated_alpha_fields_daily`, `curated_universe_membership`) chỉ có
    dòng ở những ngày thật sự có dữ liệu, nên biên ngày của chúng LÀ độ tươi.
    `curated_macro_daily` thì dựng `from spine s left join observations o on o.date =
    s.date`, tức mọi ngày trong lịch đều có một dòng kể cả khi mọi giá trị đều NULL.
    `max(date)` của nó vì thế đo lịch, không đo dữ liệu.

    Đo sống 2026-09-22, đúng cái khoảng mù đó: `max(date)` = 2026-09-21 -- trùng khít
    hai mart kia, `resolve_pinned_ds` xanh, `assert_fresh` xanh, guard catalog<->curated
    xanh (nó chỉ kiểm cột TỒN TẠI), `test_macro_field_is_broadcast_on_date` xanh (chỉ
    kiểm hình dạng frame) -- trong khi 14/14 field macro NULL tại chính ngày đó và 6
    field `yield_*` NULL suốt 3.661 dòng. Mart nằm chết 6,5 tháng (mốc cuối còn giá trị:
    2026-03-01) và không có gì kêu.

    Hai phép kiểm, mỗi cái cho một chế độ hỏng đã ĐO ĐƯỢC:

      1. Không field nào được rỗng hoàn toàn. Đó là hình dạng của 6 field `yield_*`:
         cột tồn tại, catalog khai là field dùng được, không có một giá trị nào.
      2. ÍT NHẤT MỘT field phải có giá trị tại `ds`. Đó là hình dạng của đợt chết 6,5
         tháng: nguồn ngừng chảy nên CẢ 14 field cùng dừng ở 2026-03-01.

    Phép (2) cố ý KHÔNG đòi mọi field đều có giá trị tại `ds` -- bản đầu của hàm này
    đòi thế và đo ra là báo oan. `collapse_asof.sql:113` chặn staleness bằng
    `s.date <= date_add(v.known_from, 90)`, mà `std_cm_macro_observation` đặt
    `known_from = observation_date`; nên một series QUÝ như GDP (quan sát mới nhất
    2026-04-01, cách ds 173 ngày) NULL ở phần cuối mỗi quý do đúng thiết kế hiện tại,
    không phải do sự cố. Một hàng rào kêu mỗi quý sẽ bị bỏ qua, và một hàng rào bị bỏ
    qua thì không khác gì không có -- chính bài học của đợt 6,5 tháng này.

    Đánh đổi được ghi rõ: nếu MỘT series ngừng chảy trong khi các series khác vẫn chảy,
    phép (2) im lặng. Bù lại phép (1) vẫn bắt được trường hợp nó ngừng hẳn, và người đọc
    log luôn thấy mốc cuối của từng field (DAG in ra cả bảng). Cái này nên siết lại khi
    `known_from` của macro được sửa sang ngày CÔNG BỐ thật -- lúc đó mọi field đều phải
    có giá trị tại `ds` và ngưỡng chặt mới không báo oan.
    """
    if not latest_value_dates:
        raise ValueError(
            "assert_macro_content: không có field nào để đo -- truy vấn hỏng hoặc catalog "
            "macro rỗng, không phải 'mọi thứ ổn'"
        )

    empty = sorted(name for name, date in latest_value_dates.items() if date is None)
    if empty:
        raise ValueError(
            "curated_macro_daily có cột nhưng không có giá trị nào trong toàn snapshot: "
            f"{', '.join(empty)} -- kiểm cửa sổ ngày của treasury_rates/"
            "economic_indicators trong configs/sources/data_sources.yaml"
        )

    current = [name for name, date in latest_value_dates.items() if date >= ds]
    if not current:
        newest = max(latest_value_dates.values())
        detail = ", ".join(
            f"{name}={date.isoformat()}"
            for name, date in sorted(latest_value_dates.items(), key=lambda kv: kv[1])
        )
        raise ValueError(
            f"curated_macro_daily: KHÔNG field macro nào có giá trị tại ds={ds.isoformat()} "
            f"-- mốc mới nhất của cả {len(latest_value_dates)} field chỉ tới "
            f"{newest.isoformat()}, tức nguồn macro đã ngừng chảy, không phải nhịp công bố "
            f"của một series. Chi tiết: {detail}. Kiểm cửa sổ ngày của treasury_rates/"
            "economic_indicators trong configs/sources/data_sources.yaml"
        )
