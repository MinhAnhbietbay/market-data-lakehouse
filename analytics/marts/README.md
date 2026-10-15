# Analytics Marts

Mục này chứa mã nguồn và các định nghĩa SQL dùng để tạo các bảng mart tính toán trước phục vụ báo cáo và phân tích chuyên sâu trên dữ liệu thị trường.

## 1. Mục đích
- Chuyển đổi và tổng hợp dữ liệu từ tầng `lakehouse.curated` thành các bảng nhỏ gọn trong schema `lakehouse.mart` (lưu trữ dưới định dạng Iceberg trên MinIO).
- Tính trước các phép tính phức tạp (tích lũy lợi suất kép, xếp hạng vốn hoá, chỉ số phân vị trung vị theo ngành) để tăng tốc độ truy vấn khi hiển thị biểu đồ và chạy notebook phân tích.

## 2. Cách chạy
Sử dụng môi trường Python `.venv-viz`:

- **Chạy toàn bộ các bảng mart:**
  ```bash
  .venv-viz/bin/python analytics/marts/build_marts.py
  ```

- **Chạy một bảng cụ thể:**
  ```bash
  .venv-viz/bin/python analytics/marts/build_marts.py <tên_bảng>
  ```
  *Ví dụ:*
  ```bash
  .venv-viz/bin/python analytics/marts/build_marts.py size_growth
  ```

## 3. Danh sách 6 bảng mart và câu hỏi trả lời

1. **`mart_concentration`** (từ `sql/concentration.sql`):
   - *Câu hỏi trả lời:* Mức độ tập trung vốn hoá của thị trường biến động ra sao theo thời gian? Top 10, Top 50, Top 500 cổ phiếu lớn nhất chiếm bao nhiêu phần trăm tổng vốn hoá thị trường?
2. **`mart_macro_daily`** (từ `sql/macro_daily.sql`):
   - *Câu hỏi trả lời:* Các chỉ số vĩ mô hàng ngày (lãi suất Fed, đường cong lợi suất 10Y-2Y, chỉ số VIX, chênh lệch tín dụng High Yield, S&P 500) biến động như thế nào qua các giai đoạn kinh tế?
3. **`mart_sector_latest`** (từ `sql/sector_latest.sql`):
   - *Câu hỏi trả lời:* Bức tranh định giá và hiệu quả kinh doanh hiện tại của từng ngành ra sao (số mã, tổng vốn hoá, định giá P/E trung vị, tỷ suất ROE trung vị và biên lợi nhuận ròng trung vị)?
4. **`mart_sector_year_return`** (từ `sql/sector_year_return.sql`):
   - *Câu hỏi trả lời:* Lợi suất bình quân hàng năm (2017–2025) của từng ngành kinh tế là bao nhiêu? Ngành nào dẫn đầu hoặc tụt hậu qua từng năm?
5. **`mart_size_growth`** (từ `sql/size_growth.sql`):
   - *Câu hỏi trả lời:* Đầu tư theo phân lớp vốn hoá (Large Cap > 10B, Mid Cap 2–10B, Small Cap 0.3–2B) mang lại mức tăng trưởng tài sản lũy kế (1 USD tăng thành bao nhiêu) ra sao từ cuối 2016?
6. **`mart_style_growth`** (từ `sql/style_growth.sql`):
   - *Câu hỏi trả lời:* Phong cách đầu tư theo định giá P/E (Value P/E < 12, Core 12–30, Growth >= 30, Loss-making) mang lại hiệu suất lũy kế thế nào theo thời gian?
