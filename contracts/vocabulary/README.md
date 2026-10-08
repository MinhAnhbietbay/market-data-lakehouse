# Bộ từ vựng nghiệp vụ

Nguồn sự thật cho tầng `standardize` và cho bảng đổi tên `raw`.
Spec: [`docs/superpowers/specs/2026-08-10-concept-model-44-api-design.md`](../../docs/superpowers/specs/2026-08-10-concept-model-44-api-design.md)

| File | Nội dung |
|---|---|
| `areas.yaml` | 15 area — 12 active, 3 rỗng (`ba`, `cn`, `cd`) |
| `concepts/*.yaml` | 44 concept, mỗi cái có `grain` thuộc 5 dạng của KAFS §4. Spec gốc liệt kê 46 — 2 concept cha trừu tượng (`Corporate Action`, `Financial Statement`) đã bị gỡ sau khi chạy fence 2 thật (Task 8): cả hai không hút được field nào vì con của chúng (6 sự kiện của `Corporate Action`; `Income Statement`/`Balance Sheet Statement`/`Cash Flow Statement` của `Financial Statement`) đã giữ hết field thật. Đây đúng luật §5.3 mà spec đã tự dùng để gộp `Issuer` vào `Legal Entity`: concept trừu tượng không có field riêng thì không cần bảng vật lý của riêng nó — chỉ là spec chưa áp dụng nhất quán cho 2 cái này lúc viết §5.5. `Executive Compensation` (1 concept) bị gỡ tiếp sau đó khi `/governance-executive-compensation` bị loại khỏi scope (xem bảng dưới). |
| `fmp_raw_fields.yaml` | 424 field / 44 API (mở rộng từ 410/42 khi F11b, spec 2026-08-18, thêm /price-target-consensus + /price-target-summary cho concept "Price Target Consensus" — extension mang tính cơ học, cùng tên `api`/`endpoint`/`field`/`type` như script sinh ra, không phải hand-craft field mới), sinh bằng `scripts/data/export_fmp_raw_fields.py` — **không sửa tay ngoại trừ đợt mở rộng có ghi chú ở đây** |
| `field_concept_map.yaml` | 426 dòng: field → concept + role + raw_name |

## Năm API đã bị loại — đừng thêm lại

| API | Lý do |
|---|---|
| `/key-metrics` | Sau khi tách phần `Derived` chỉ còn field định danh — 0 field dữ liệu. Các chỉ số vẫn được **tính ở `curated`** |
| `/actively-trading-list` | Mâu thuẫn luật N3: danh sách của hôm nay; dùng lọc universe quá khứ là survivorship bias |
| `/batch-aftermarket-quote` | Ảnh chụp tại thời điểm gọi, không có chiều lịch sử. Dự án chỉ xử lý EOD |
| `/key-executives` | Dữ liệu ở mức cá nhân lãnh đạo, không phải mức symbol/company như phần còn lại của vocabulary — không phù hợp bài toán alpha nghiên cứu định lượng cross-sectional theo công ty của dự án |
| `/governance-executive-compensation` | Dữ liệu ở mức cá nhân lãnh đạo, không phải mức symbol/company như phần còn lại của vocabulary — không phù hợp bài toán alpha nghiên cứu định lượng cross-sectional theo công ty của dự án |

## Nguồn sự thật: `field_concept_map.yaml`

`field_concept_map.yaml` (426 dòng: field → concept + role + raw_name) là **nguồn sự
thật duy nhất** ở cấp field. `concepts/*.yaml`'s `source_endpoints` chỉ là metadata mô tả
— suy ra **từ** `field_concept_map.yaml` (đếm endpoint distinct theo concept), không phải
ngược lại. Khi hai bên lệch nhau, `field_concept_map.yaml` thắng; sửa `source_endpoints`
cho khớp, không sửa ngược.

> **Phạm vi (từ 2026-08-18).** `field_concept_map.yaml` là nguồn sự thật cho tập field
> **được mô hình hoá** — tức chặng raw→standardize. Nó **không còn** định nghĩa tập
> cột của bảng raw: từ đợt raw full-fidelity
> ([spec](../../docs/superpowers/specs/2026-08-18-raw-full-fidelity-design.md)),
> contract raw giữ **mọi** field FMP trả về (424 field curated + 122 field ngoài kho),
> còn tầng standardize mới là chỗ áp bộ từ vựng này.
>
> Ràng buộc `contract ⊆ từ vựng` đã **nghỉ hưu** — nó chống lại chính thiết kế trên. Chiều
> còn được canh là chiều **ngược lại**: mọi field trong `fmp_raw_fields.yaml` phải tới được
> một cột raw (`check_vocabulary_reaches_raw`, fence 5 đảo chiều). Miễn trừ duy nhất
> được suy ra, không khai tay — endpoint vừa `enabled: false` vừa chưa có contract, hiện là
> 12 endpoint / 81 field bị khoá theo gói FMP.

## Sáu giá trị `role`

| `role` | Ví dụ | Ý nghĩa |
|---|---|---|
| `identity` | `symbol`, `cik` | khoá của concept |
| `event_date` | `date` trên `/dividends` | thời điểm **sự việc** xảy ra |
| `known_from_date` | `filingDate`, `acceptedDate` | thời điểm **thị trường biết** — mọi truy vấn PIT lọc bằng cột này (KAFS §5) |
| `attribute` | `revenue`, `volume` | thuộc tính thường |
| `reference` | `sector`, `exchange` | khoá ngoại sang bảng tra |
| `unknown` | `daa` của `/ipos-calendar` | chưa xác định nghĩa — chặn không cho lên `standardize` |

Định nghĩa gốc: [`docs/superpowers/specs/2026-08-10-concept-model-44-api-design.md`](../../docs/superpowers/specs/2026-08-10-concept-model-44-api-design.md) §6.

## Luật đặt `raw_name` (L1–L4)

Sống ở [`docs/superpowers/plans/2026-08-10-concept-model-44-api.md`](../../docs/superpowers/plans/2026-08-10-concept-model-44-api.md), mục "Luật đặt `raw_name`" — không lặp lại ở đây để tránh hai bản lệch nhau.

## Cảnh báo: `/revenue-geographic-segmentation`

Field `data.Americas Segment` (và các key `data.<Tên vùng>` khác cùng dạng) là tên cột
**động theo từng công ty** — FMP trả về một key khác nhau tuỳ danh mục phân khúc doanh thu
mà công ty đó tự đặt, không phải một cột ổn định như các field còn lại. Ai sinh lại
contract `raw` từ model này cần xử lý riêng field này (ví dụ pivot/unpivot), không thể
áp L1 (`snake_case` trực tiếp) như các field khác.

## Sáu fence

Chạy: `PYTHONPATH=packages python -m pytest tests/metadata -v`

Repo **không có CI** — các fence chỉ cắn khi có người chạy lệnh trên.

Fence 2 (`check_every_concept_has_fields`) có đúng một ngoại lệ: `Trading Day`
(area `cm`) khai `no_source: true` trong `concepts/cm_common.yaml`. Nó không có
field nguồn nào theo thiết kế — FMP không có endpoint lịch giao dịch — và được
suy ra ở tầng downstream (`standardize`/`curated`) từ tập ngày phân biệt của
`Daily Price Bar`: ngày nào có dữ liệu giá thì đó là ngày giao dịch. Khác với
`Corporate Action`/`Financial Statement` (gỡ hẳn vì trừu tượng, không có lý do
tồn tại như concept riêng), `Trading Day` là một dimension concept thật, chỉ
là chưa có field mapping ở bước này — cờ `no_source` ghi nhận đúng khoản nợ
đó thay vì để fence im lặng bỏ qua nó.

## Cái này KHÔNG chứa gì

- Property term đầy đủ theo chuẩn KAFS (cardinality, data type) — bước P2, chưa làm.
- Measure phái sinh — 154 field sheet `Derived` tính ở **`curated`**, không ở `standardize`.
- DDL bảng `standardize` — cần P2 xong trước.
