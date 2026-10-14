-- Kỳ được __fam_earnings chọn cho một ngày phải là kỳ ĐÃ CÔNG BỐ tính đến ngày đó.
--
-- `std_ev_earnings_announcement` chứa cả LỊCH quý sau: dòng có `announcement_date` ở
-- tương lai, `eps_actual` NULL, và `known_from` là ngày cái lịch được công bố (basis
-- `system_time`). Dòng đó hợp lệ về PIT -- ta thật sự biết trước -- nhưng nó KHÔNG phải
-- một kỳ báo cáo, và `period_window_body` xếp hạng kỳ theo `announcement_date` nên nó
-- giành mất `__rank = 0`.
--
-- Đo sống 2026-09-22: AAPL có (2026-10-29, eps_actual NULL, known_from 2026-09-14) nên
-- cả 4 field earnings_surprise_*/revenue_surprise_pct/SUE NULL đúng 6 phiên cuối
-- (2026-09-14..09-21) của cả 5 mã. Sửa ở `fam_earnings_body` (lọc `eps_actual is not
-- null` trước khi vào cửa sổ kỳ).
--
-- Vì sao cần test này DÙ ĐÃ có `__pit_test_earnings` + fixture SCHED: fixture chỉ chứng
-- minh macro xử lý đúng hình dạng đó trên 3 dòng tự dựng. Test này đo trên CHÍNH dữ liệu
-- production, nơi hình dạng ấy tái sinh mỗi quý khi lịch quý sau được ingest -- đúng loại
-- hồi quy mà một oracle tĩnh không thấy. Seed cũ đặt known_from = announcement_date ở mọi
-- dòng, tức chưa từng có hình dạng này, và vì thế lỗi sống 6 phiên mà mọi test đều xanh.
--
-- Không tautological: bộ lọc ở fam_earnings_body là `eps_actual is not null`, KHÔNG phải
-- `announcement_date <= date`. Nếu nhà cung cấp từng trả actual trước ngày công bố thì
-- đó là lookahead thật và test này là chỗ bắt được.
select
    date,
    symbol,
    announcement_date,
    earnings_surprise_pct
from {{ ref('__fam_earnings') }}
where announcement_date > date
