-- capex là dòng tiền RA, báo cáo lưu chuyển tiền tệ theo GAAP trình bày nó là
-- số âm. Curated dùng abs(capital_expenditure) trong công thức FCF (spec §5.2),
-- và abs() sẽ im lặng nuốt một lỗi dữ liệu nếu nguồn đổi quy ước dấu. Test này
-- là thứ duy nhất phát hiện được việc đó.
select symbol, fiscal_year, period, capital_expenditure
from {{ ref('std_ac_cash_flow_statement') }}
where capital_expenditure > 0
