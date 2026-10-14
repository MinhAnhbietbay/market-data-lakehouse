-- Test đối chiếu vendor (spec 2026-08-25 §7.3, plan Task 19).
--
-- Chỉ áp cho nhóm A (spec §5.2) -- 14 field tái lập chính xác được từ báo cáo
-- tài chính raw, không có cách nào tính khác. Lệch là ĐỎ, KHÔNG có ngưỡng dung
-- sai -- xem không dùng round()/khoảng sai số nào ở dưới, so tuyệt đối trên
-- kiểu decimal(28,4) của raw (cộng/trừ decimal không có sai số làm tròn nhị
-- phân, nên "=" là đủ, không cần epsilon).
--
-- Đây KHÔNG phải test PIT: raw không có known_from, nên phép so này không bảo
-- đảm cùng phiên bản với bất kỳ ngày cụ thể nào trong curated. Nó chỉ kiểm
-- công thức ở __fam_fundamental.sql có trung thành với con số vendor tự báo
-- cáo hay không, tại chính mức raw -- vì mọi cột atomic curated dùng
-- (revenue, cost_of_revenue, account_payables, ...) đi qua standardize tới
-- curated KHÔNG bị biến đổi (thuần passthrough, xem std_ac_*.sql), nên nếu
-- công thức đúng ở raw thì nó đúng ở mọi ngày curated chọn ra từ đúng dòng đó.
--
-- Công thức dưới đây độc lập với __fam_fundamental.sql (không query bảng đó),
-- nên nó KHÔNG bắt được một regression sửa sai lại __fam_fundamental.sql --
-- phạm vi hẹp này đúng như spec §7.3 đã nói trước ("đừng dùng nó thay cho
-- test 2 hay test 3"). Giá trị của nó là: lần chạy đầu tiên trên dữ liệu
-- thật (2026-08-26) đã bắt được 2 lỗi thật trong __fam_fundamental.sql --
-- tax_payables bị cộng thừa vào current_liabilities/total_liabilities, và
-- capital_lease_obligations_current bị cộng thừa lần nữa vào total_liabilities
-- (capital_lease_obligations không có _current đã là tổng current+non-current),
-- và treasury_stock (đã âm sẵn trong raw) bị trừ thay vì cộng ở common_equity/
-- total_equity -- công thức dưới đây phản ánh phiên bản ĐÃ SỬA, khớp với
-- __fam_fundamental.sql hiện tại. Nếu ai sửa công thức ở đó, nhớ đối chiếu
-- lại file này bằng tay.
--
-- Dùng IS DISTINCT FROM (null-safe) chứ không phải "<>" -- một vế NULL còn vế
-- kia có giá trị PHẢI được coi là lệch (dữ liệu vendor không nhất quán), còn
-- cả hai vế cùng NULL thì không phải lỗi công thức.

with income_mismatches as (
    select
        symbol, period_end_date, fiscal_year, period, run_date,
        'gross_profit' as field,
        (revenue - cost_of_revenue) as computed,
        gross_profit as vendor
    from {{ source('raw', 'raw_fmp_income_statement') }}
    where (revenue - cost_of_revenue) is distinct from gross_profit

    union all

    select
        symbol, period_end_date, fiscal_year, period, run_date,
        'operating_income' as field,
        (revenue - cost_of_revenue
            - (selling_general_and_administrative_expenses
               + research_and_development_expenses
               + other_expenses)) as computed,
        operating_income as vendor
    from {{ source('raw', 'raw_fmp_income_statement') }}
    where (revenue - cost_of_revenue
            - (selling_general_and_administrative_expenses
               + research_and_development_expenses
               + other_expenses)) is distinct from operating_income

    union all

    select
        symbol, period_end_date, fiscal_year, period, run_date,
        'net_income_common' as field,
        (net_income - net_income_deductions) as computed,
        bottom_line_net_income as vendor
    from {{ source('raw', 'raw_fmp_income_statement') }}
    where (net_income - net_income_deductions) is distinct from bottom_line_net_income
),

balance_mismatches as (
    select
        symbol, period_end_date, fiscal_year, period, run_date,
        'current_assets' as field,
        (cash_and_cash_equivalents + short_term_investments + net_receivables
            + inventory + prepaids + other_current_assets) as computed,
        total_current_assets as vendor
    from {{ source('raw', 'raw_fmp_balance_sheet') }}
    where (cash_and_cash_equivalents + short_term_investments + net_receivables
            + inventory + prepaids + other_current_assets)
          is distinct from total_current_assets

    union all

    select
        symbol, period_end_date, fiscal_year, period, run_date,
        'non_current_assets' as field,
        (property_plant_equipment_net + goodwill + intangible_assets
            + long_term_investments + tax_assets + other_non_current_assets) as computed,
        total_non_current_assets as vendor
    from {{ source('raw', 'raw_fmp_balance_sheet') }}
    where (property_plant_equipment_net + goodwill + intangible_assets
            + long_term_investments + tax_assets + other_non_current_assets)
          is distinct from total_non_current_assets

    union all

    select
        symbol, period_end_date, fiscal_year, period, run_date,
        'current_liabilities' as field,
        (account_payables + other_payables + accrued_expenses + short_term_debt
            + capital_lease_obligations_current + deferred_revenue
            + other_current_liabilities) as computed,
        total_current_liabilities as vendor
    from {{ source('raw', 'raw_fmp_balance_sheet') }}
    where (account_payables + other_payables + accrued_expenses + short_term_debt
            + capital_lease_obligations_current + deferred_revenue
            + other_current_liabilities)
          is distinct from total_current_liabilities

    union all

    select
        symbol, period_end_date, fiscal_year, period, run_date,
        'cash_equivalents' as field,
        (cash_and_cash_equivalents + short_term_investments) as computed,
        cash_and_short_term_investments as vendor
    from {{ source('raw', 'raw_fmp_balance_sheet') }}
    where (cash_and_cash_equivalents + short_term_investments)
          is distinct from cash_and_short_term_investments

    union all

    select
        symbol, period_end_date, fiscal_year, period, run_date,
        'common_equity' as field,
        (common_stock + additional_paid_in_capital + retained_earnings
            + accumulated_other_comprehensive_income_loss
            + other_total_stockholders_equity + treasury_stock) as computed,
        total_stockholders_equity as vendor
    from {{ source('raw', 'raw_fmp_balance_sheet') }}
    where (common_stock + additional_paid_in_capital + retained_earnings
            + accumulated_other_comprehensive_income_loss
            + other_total_stockholders_equity + treasury_stock)
          is distinct from total_stockholders_equity

    union all

    select
        symbol, period_end_date, fiscal_year, period, run_date,
        'total_assets' as field,
        (cash_and_cash_equivalents + short_term_investments + net_receivables
            + inventory + prepaids + other_current_assets
            + property_plant_equipment_net + goodwill + intangible_assets
            + long_term_investments + tax_assets + other_non_current_assets) as computed,
        total_assets as vendor
    from {{ source('raw', 'raw_fmp_balance_sheet') }}
    where (cash_and_cash_equivalents + short_term_investments + net_receivables
            + inventory + prepaids + other_current_assets
            + property_plant_equipment_net + goodwill + intangible_assets
            + long_term_investments + tax_assets + other_non_current_assets)
          is distinct from total_assets

    union all

    select
        symbol, period_end_date, fiscal_year, period, run_date,
        'total_liabilities' as field,
        (account_payables + other_payables + accrued_expenses + short_term_debt
            + deferred_revenue
            + other_current_liabilities + long_term_debt + deferred_revenue_non_current
            + deferred_tax_liabilities_non_current + other_non_current_liabilities
            + other_liabilities + capital_lease_obligations) as computed,
        total_liabilities as vendor
    from {{ source('raw', 'raw_fmp_balance_sheet') }}
    where (account_payables + other_payables + accrued_expenses + short_term_debt
            + deferred_revenue
            + other_current_liabilities + long_term_debt + deferred_revenue_non_current
            + deferred_tax_liabilities_non_current + other_non_current_liabilities
            + other_liabilities + capital_lease_obligations)
          is distinct from total_liabilities

    union all

    select
        symbol, period_end_date, fiscal_year, period, run_date,
        'total_equity' as field,
        (common_stock + preferred_stock + additional_paid_in_capital
            + retained_earnings + accumulated_other_comprehensive_income_loss
            + other_total_stockholders_equity + minority_interest
            + treasury_stock) as computed,
        total_equity as vendor
    from {{ source('raw', 'raw_fmp_balance_sheet') }}
    where (common_stock + preferred_stock + additional_paid_in_capital
            + retained_earnings + accumulated_other_comprehensive_income_loss
            + other_total_stockholders_equity + minority_interest
            + treasury_stock)
          is distinct from total_equity
),

cashflow_mismatches as (
    select
        symbol, period_end_date, fiscal_year, period, run_date,
        'net_change_cash' as field,
        (operating_cash_flow + net_cash_provided_by_investing_activities
            + net_cash_provided_by_financing_activities
            + effect_of_forex_changes_on_cash) as computed,
        net_change_in_cash as vendor
    from {{ source('raw', 'raw_fmp_cash_flow') }}
    where (operating_cash_flow + net_cash_provided_by_investing_activities
            + net_cash_provided_by_financing_activities
            + effect_of_forex_changes_on_cash)
          is distinct from net_change_in_cash

    union all

    select
        symbol, period_end_date, fiscal_year, period, run_date,
        'free_cash_flow' as field,
        (operating_cash_flow - abs(capital_expenditure)) as computed,
        free_cash_flow as vendor
    from {{ source('raw', 'raw_fmp_cash_flow') }}
    where (operating_cash_flow - abs(capital_expenditure)) is distinct from free_cash_flow

    union all

    select
        symbol, period_end_date, fiscal_year, period, run_date,
        'dividends_paid' as field,
        (common_dividends_paid + preferred_dividends_paid) as computed,
        net_dividends_paid as vendor
    from {{ source('raw', 'raw_fmp_cash_flow') }}
    where (common_dividends_paid + preferred_dividends_paid) is distinct from net_dividends_paid
)

select * from income_mismatches
union all
select * from balance_mismatches
union all
select * from cashflow_mismatches
