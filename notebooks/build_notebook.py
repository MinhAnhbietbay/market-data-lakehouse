#!/usr/bin/env python3
"""
Script to build, execute, and export the Market Insights Jupyter Notebook.
Uses nbformat to construct notebooks/market_insights.ipynb,
then executes it in place and exports notebooks/market_insights.html.
"""

import os
import sys
import subprocess
import nbformat as nbf
from nbconvert.preprocessors import ExecutePreprocessor
from nbconvert import HTMLExporter


def create_notebook():
    nb = nbf.v4.new_notebook()

    # Cell 0: Markdown Intro
    intro_md = """# Báo cáo Phân tích Chuyên sâu Thị trường Chứng khoán (Market Insights)

## 1. Mục tiêu
Báo cáo này cung cấp cái nhìn toàn diện và có hệ thống về cấu trúc thị trường chứng khoán Mỹ, tương quan kinh tế vĩ mô và hiệu suất đầu tư qua 8 góc nhìn chuyên sâu:
1. **Lãi suất Fed & Đường cong lợi suất 10Y-2Y:** Đánh giá chu kỳ thắt chặt tiền tệ và hiện tượng đường cong lợi suất đảo chiều.
2. **Biến động VIX & Chênh lệch tín dụng High Yield:** Đo lường tâm lý thị trường và phần bù rủi ro tín dụng doanh nghiệp.
3. **Mức độ tập trung vốn hoá (Top 10, Top 50, Top 500):** Khảo sát xu hướng tập trung tỷ trọng vốn hoá vào nhóm các cổ phiếu dẫn đầu thị trường.
4. **Hiệu suất đầu tư theo Quy mô vốn hoá (Equal-Weight):** So sánh giá trị tích lũy của $1 giữa các phân khúc Large Cap, Mid Cap và Small Cap.
5. **Phong cách đầu tư (Value vs Growth):** Phân tích hiệu quả sinh lời giữa các nhóm định giá P/E (Value, Core, Growth, Loss-making).
6. **Bản đồ nhiệt Lợi suất ngành theo Năm (2017–2025):** Theo dõi chu kỳ luân chuyển ngành qua từng giai đoạn kinh tế.
7. **Định giá P/E trung vị theo Ngành:** Đánh giá mức định giá tương đối của 11 nhóm ngành kinh tế tại thời điểm hiện tại.
8. **Tương quan ROE vs P/E:** Nhận diện các ngành có hiệu quả sinh lời vượt trội trên mức định giá tương ứng.

## 2. Nguồn dữ liệu
Dữ liệu được trích xuất và tính toán trước từ kiến trúc Lakehouse lưu trữ tại schema `lakehouse.mart`:
- `mart_macro_daily`: Chuỗi thời gian hàng ngày về các chỉ báo vĩ mô (lãi suất điều hành Fed, lợi suất trái phiếu 2Y và 10Y, độ dốc 10Y-2Y, VIX, High Yield spread, S&P 500).
- `mart_concentration`: Tỷ trọng vốn hoá tích lũy của Top 10, Top 50, Top 500 cổ phiếu vào ngày cuối mỗi tháng.
- `mart_size_growth`: Giá trị tích luỹ danh mục ($1 đầu tư ban đầu) phân loại theo quy mô vốn hoá (Large, Mid, Small Cap).
- `mart_style_growth`: Giá trị tích luỹ danh mục ($1 đầu tư ban đầu) phân loại theo trường phái định giá P/E (Value, Core, Growth, Loss-making).
- `mart_sector_year_return`: Lợi suất bình quân hàng năm (2017–2025) của 11 ngành kinh tế.
- `mart_sector_latest`: Dữ liệu tổng hợp mới nhất của 11 ngành (số lượng mã, vốn hoá, P/E trung vị, ROE trung vị, biên lợi nhuận ròng trung vị).

## 3. Cảnh báo phương pháp luận & Giới hạn dữ liệu
- **Chiến lược Equal-Weight:** Các danh mục quy mô và phong cách đầu tư được tính toán theo phương pháp tỷ trọng đều (equal-weight) và tái cân bằng định kỳ, tránh bị chi phối bởi một vài cổ phiếu có quy mô vốn hóa cá biệt.
- **Tiêu chuẩn lọc vốn hoá:** Chỉ tính toán trên các cổ phiếu có giá trị vốn hoá thị trường từ 0,3 tỷ USD (300 triệu USD) trở lên để loại trừ các cổ phiếu thanh khoản quá thấp (micro-cap/penny stocks).
- **Cắt tỉa lợi suất cực trị (Winsorization):** Tỷ suất sinh lời ngày của từng cổ phiếu được giới hạn trong khoảng `[-50%, +100%]` nhằm giảm thiểu biến dạng từ dữ liệu lỗi hoặc các sự kiện chia tách cổ phiếu chưa điều chỉnh.
- **Đặc thù thế hệ nguồn dữ liệu:** Bộ dữ liệu trong lakehouse là thế hệ nguồn dữ liệu lịch sử chuẩn hóa phục vụ phân tích nền tảng, không hoàn toàn đồng nhất với các pipeline dữ liệu định dạng dbt FMP ở các ứng dụng phát triển sau.
"""
    nb.cells.append(nbf.v4.new_markdown_cell(intro_md))

    # Cell 1: Setup code
    setup_code = """import warnings
warnings.filterwarnings('ignore')

import trino
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.io as pio
from IPython.display import display, Markdown

# Cấu hình renderer cho Plotly để xuất mime type plotly cho nbconvert
pio.renderers.default = "plotly_mimetype+notebook_connected"

# Kết nối Trino Lakehouse
conn = trino.dbapi.connect(
    host='localhost',
    port=8185,
    user='alpha',
    catalog='lakehouse',
    schema='mart'
)

def load_table(table_name):
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM lakehouse.mart.{table_name}")
    cols = [c[0] for c in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)

# Tải dữ liệu từ 6 bảng mart
df_macro = load_table("mart_macro_daily")
df_macro['date'] = pd.to_datetime(df_macro['date'])
for c in ['fed_funds_rate', 'yield_2y', 'yield_10y', 'curve_10y_2y', 'vix', 'credit_spread_hy', 'sp500_index']:
    df_macro[c] = pd.to_numeric(df_macro[c], errors='coerce')
df_macro = df_macro.sort_values('date').reset_index(drop=True)

df_conc = load_table("mart_concentration")
df_conc['date'] = pd.to_datetime(df_conc['date'])
for c in ['top10_share_pct', 'top50_share_pct', 'top500_share_pct']:
    df_conc[c] = pd.to_numeric(df_conc[c], errors='coerce')
df_conc = df_conc.sort_values('date').reset_index(drop=True)

df_size = load_table("mart_size_growth")
df_size['date'] = pd.to_datetime(df_size['date'])
df_size['growth_of_1'] = pd.to_numeric(df_size['growth_of_1'], errors='coerce')
df_size = df_size.sort_values(['bucket', 'date']).reset_index(drop=True)

df_style = load_table("mart_style_growth")
df_style['date'] = pd.to_datetime(df_style['date'])
df_style['growth_of_1'] = pd.to_numeric(df_style['growth_of_1'], errors='coerce')
df_style = df_style.sort_values(['bucket', 'date']).reset_index(drop=True)

df_sec_yr = load_table("mart_sector_year_return")
df_sec_yr['return_pct'] = pd.to_numeric(df_sec_yr['return_pct'], errors='coerce')
df_sec_yr = df_sec_yr.sort_values(['year', 'sector']).reset_index(drop=True)

df_sec_lat = load_table("mart_sector_latest")
for c in ['n_stocks', 'total_mcap_bn', 'median_pe', 'median_roe_pct', 'median_net_margin_pct']:
    df_sec_lat[c] = pd.to_numeric(df_sec_lat[c], errors='coerce')
"""
    nb.cells.append(nbf.v4.new_code_cell(setup_code))

    # Cell 2: Chart 1
    chart1_code = """fig1 = go.Figure()
fig1.add_trace(go.Scatter(
    x=df_macro['date'],
    y=df_macro['fed_funds_rate'],
    name='Lãi suất Fed (Fed Funds Rate)',
    line=dict(color='#1f77b4', width=2)
))
fig1.add_trace(go.Scatter(
    x=df_macro['date'],
    y=df_macro['curve_10y_2y'],
    name='Độ dốc đường cong 10Y-2Y (Yield Curve)',
    line=dict(color='#d62728', width=2)
))
fig1.add_hline(
    y=0,
    line_dash='dash',
    line_color='#7f7f7f',
    annotation_text='Ngưỡng 0 (Đảo chiều khi âm)',
    annotation_position='bottom right'
)
fig1.update_layout(
    title='Biểu đồ 1: Lãi suất Fed và Độ dốc Đường cong Lợi suất 10Y-2Y (2016–2026)',
    xaxis_title='Thời gian',
    yaxis_title='Lãi suất / Chênh lệch (%)',
    template='plotly_white',
    hovermode='x unified',
    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
)
fig1.show()
"""
    nb.cells.append(nbf.v4.new_code_cell(chart1_code))

    # Cell 3: Insight 1
    insight1_code = """min_spread_row = df_macro.loc[df_macro['curve_10y_2y'].idxmin()]
min_spread_date = min_spread_row['date'].strftime('%Y-%m-%d')
min_spread_val = float(min_spread_row['curve_10y_2y'])
neg_spread_days = int((df_macro['curve_10y_2y'] < 0).sum())
max_fed_val = float(df_macro['fed_funds_rate'].max())

display(Markdown(f\"\"\"### Nhận định: Lãi suất Fed và Đường cong Lợi suất 10Y-2Y
- **Hiện tượng đảo chiều đường cong:** Đường cong lợi suất 10Y-2Y chạm mức âm sâu nhất là **{min_spread_val:.2f}%** vào ngày **{min_spread_date}**.
- **Kéo dài giai đoạn nghịch đảo:** Tổng số ngày lịch ghi nhận đường cong lợi suất âm (spread < 0) lên tới **{neg_spread_days} ngày lịch**, cho thấy thị trường duy trì kỳ vọng kéo dài về rủi ro suy thoái kinh tế.
- **Sự đi cùng chính sách thắt chặt:** Chu kỳ tăng lãi suất của Fed lên đỉnh **{max_fed_val:.2f}%** đi cùng với sự thu hẹp biên độ lợi suất kỳ hạn dài so với kỳ hạn ngắn và hiện tượng đảo chiều đường cong lợi suất.\"\"\"))
"""
    nb.cells.append(nbf.v4.new_code_cell(insight1_code))

    # Cell 4: Chart 2
    chart2_code = """fig2 = make_subplots(specs=[[{"secondary_y": True}]])
fig2.add_trace(
    go.Scatter(
        x=df_macro['date'],
        y=df_macro['vix'],
        name='Chỉ số Biến động VIX',
        line=dict(color='#ff7f0e', width=1.8)
    ),
    secondary_y=False
)
df_hy_valid = df_macro.dropna(subset=['credit_spread_hy'])
fig2.add_trace(
    go.Scatter(
        x=df_hy_valid['date'],
        y=df_hy_valid['credit_spread_hy'],
        name='Chênh lệch Tín dụng HY (Credit Spread HY)',
        line=dict(color='#2ca02c', width=2)
    ),
    secondary_y=True
)
fig2.update_layout(
    title='Biểu đồ 2: Chỉ số Biến động VIX và Chênh lệch Tín dụng High Yield (HY Spread)',
    xaxis_title='Thời gian',
    template='plotly_white',
    hovermode='x unified',
    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
)
fig2.update_yaxes(title_text='Chỉ số VIX (điểm)', secondary_y=False)
fig2.update_yaxes(title_text='HY Spread (%)', secondary_y=True)
fig2.show()
"""
    nb.cells.append(nbf.v4.new_code_cell(chart2_code))

    # Cell 5: Insight 2
    insight2_code = """max_vix_row = df_macro.loc[df_macro['vix'].idxmax()]
max_vix_date = max_vix_row['date'].strftime('%Y-%m-%d')
max_vix_val = float(max_vix_row['vix'])

df_hy_nonnull = df_macro.dropna(subset=['credit_spread_hy'])
hy_start_date = df_hy_nonnull['date'].min().strftime('%Y-%m-%d')
hy_days_count = len(df_hy_nonnull)
max_hy_row = df_hy_nonnull.loc[df_hy_nonnull['credit_spread_hy'].idxmax()]
max_hy_date = max_hy_row['date'].strftime('%Y-%m-%d')
max_hy_val = float(max_hy_row['credit_spread_hy'])
min_hy_val = float(df_hy_nonnull['credit_spread_hy'].min())

display(Markdown(f\"\"\"### Nhận định: VIX và Chênh lệch Tín dụng High Yield
- **Đỉnh điểm biến động cổ phiếu:** Chỉ số VIX ghi nhận mức cao kỷ lục **{max_vix_val:.2f} điểm** vào ngày **{max_vix_date}** (ngày trong bảng macro có thể lệch ~1 ngày so với ngày quan sát (chưa kiểm chứng nguyên nhân; có thể là ngày dữ liệu khả dụng)).
- **Dữ liệu chênh lệch tín dụng High Yield:** Chuỗi dữ liệu credit spread HY bắt đầu từ ngày **{hy_start_date}** với tổng cộng **{hy_days_count} ngày lịch** có dữ liệu (trước thời điểm này là giá trị NULL).
- **Biên độ rủi ro tín dụng doanh nghiệp:** Trong giai đoạn có dữ liệu theo dõi, chênh lệch tín dụng High Yield dao động trong khoảng từ **{min_hy_val:.2f}%** đến mức đỉnh **{max_hy_val:.2f}%** vào ngày **{max_hy_date}**.\"\"\"))
"""
    nb.cells.append(nbf.v4.new_code_cell(insight2_code))

    # Cell 6: Chart 3
    chart3_code = """fig3 = go.Figure()
fig3.add_trace(go.Scatter(
    x=df_conc['date'],
    y=df_conc['top10_share_pct'],
    name='Top 10 Cổ phiếu',
    line=dict(color='#1f77b4', width=2.2)
))
fig3.add_trace(go.Scatter(
    x=df_conc['date'],
    y=df_conc['top50_share_pct'],
    name='Top 50 Cổ phiếu',
    line=dict(color='#ff7f0e', width=2.2)
))
fig3.add_trace(go.Scatter(
    x=df_conc['date'],
    y=df_conc['top500_share_pct'],
    name='Top 500 Cổ phiếu',
    line=dict(color='#2ca02c', width=2.2)
))
fig3.update_layout(
    title='Biểu đồ 3: Mức độ Tập trung Vốn hoá Thị trường (Top 10, Top 50, Top 500)',
    xaxis_title='Thời gian',
    yaxis_title='Tỷ trọng Vốn hoá (%)',
    yaxis=dict(range=[0, 100]),
    template='plotly_white',
    hovermode='x unified',
    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
)
fig3.show()
"""
    nb.cells.append(nbf.v4.new_code_cell(chart3_code))

    # Cell 7: Insight 3
    insight3_code = """start_date_c = df_conc.iloc[0]['date'].strftime('%Y-%m-%d')
end_date_c = df_conc.iloc[-1]['date'].strftime('%Y-%m-%d')
start_t10 = float(df_conc.iloc[0]['top10_share_pct'])
end_t10 = float(df_conc.iloc[-1]['top10_share_pct'])
start_t50 = float(df_conc.iloc[0]['top50_share_pct'])
end_t50 = float(df_conc.iloc[-1]['top50_share_pct'])
end_t500 = float(df_conc.iloc[-1]['top500_share_pct'])
t10_mult = end_t10 / start_t10

display(Markdown(f\"\"\"### Nhận định: Xu hướng Tập trung Vốn hoá Thị trường
- **Sự bùng nổ tỷ trọng Top 10:** Tỷ trọng vốn hoá của Top 10 cổ phiếu lớn nhất đã tăng từ **{start_t10:.2f}%** vào ngày **{start_date_c}** lên tới **{end_t10:.2f}%** vào ngày **{end_date_c}** (tăng gấp **{t10_mult:.2f} lần**).
- **Mức độ chi phối của nhóm dẫn đầu:** Top 50 mở rộng thị phần từ **{start_t50:.2f}%** lên **{end_t50:.2f}%**, trong khi Top 500 nắm giữ áp đảo **{end_t500:.2f}%** toàn bộ giá trị vốn hoá thị trường tại thời điểm kết thúc kỳ quan sát.
- **Góc nhìn cấu trúc:** Dữ liệu cho thấy tỷ trọng vốn hoá thị trường tập trung ngày càng dày đặc vào nhóm cổ phiếu quy mô lớn nhất (giả thuyết về sự ưu tiên của dòng vốn thụ động đối với các mã dẫn đầu cần kiểm chứng thêm qua dữ liệu chi tiết).\"\"\"))
"""
    nb.cells.append(nbf.v4.new_code_cell(insight3_code))

    # Cell 8: Chart 4
    chart4_code = """fig4 = go.Figure()
colors_size = {'1 Large (>10B)': '#1f77b4', '2 Mid (2-10B)': '#ff7f0e', '3 Small (0.3-2B)': '#2ca02c'}
for bucket in sorted(df_size['bucket'].unique()):
    df_b = df_size[df_size['bucket'] == bucket]
    fig4.add_trace(go.Scatter(
        x=df_b['date'],
        y=df_b['growth_of_1'],
        name=bucket,
        line=dict(color=colors_size.get(bucket, '#333333'), width=2)
    ))
fig4.update_layout(
    title='Biểu đồ 4: Tăng trưởng Lũy kế của 1 USD theo Quy mô Vốn hoá (Equal-Weight)',
    xaxis_title='Thời gian',
    yaxis_title='Giá trị Danh mục (USD)',
    template='plotly_white',
    hovermode='x unified',
    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
)
fig4.show()
"""
    nb.cells.append(nbf.v4.new_code_cell(chart4_code))

    # Cell 9: Insight 4
    insight4_code = """last_dt_size = df_size['date'].max()
last_size_snap = df_size[df_size['date'] == last_dt_size].sort_values('growth_of_1', ascending=False)
win_size_row = last_size_snap.iloc[0]
lose_size_row = last_size_snap.iloc[-1]
mid_size_row = last_size_snap.iloc[1]

win_size_name = win_size_row['bucket']
win_size_val = float(win_size_row['growth_of_1'])
mid_size_name = mid_size_row['bucket']
mid_size_val = float(mid_size_row['growth_of_1'])
lose_size_name = lose_size_row['bucket']
lose_size_val = float(lose_size_row['growth_of_1'])

display(Markdown(f\"\"\"### Nhận định: Hiệu suất Tăng trưởng theo Quy mô Vốn hoá
- **Nhóm dẫn đầu cuối kỳ:** Nhóm cổ phiếu quy mô vốn hoá lớn (**{win_size_name}**) ghi nhận giá trị tích luỹ cao nhất, đạt **{win_size_val:.4f} USD** từ 1 USD ban đầu.
- **Thứ hạng các nhóm quy mô:** Nhóm **{mid_size_name}** đạt vị trí thứ hai với **{mid_size_val:.4f} USD**, trong khi nhóm có giá trị tích luỹ thấp nhất là **{lose_size_name}** đạt **{lose_size_val:.4f} USD**.
- **Tương quan quy mô:** Kết quả cho thấy trong giai đoạn khảo sát, mức tăng trưởng giá trị danh mục đi cùng với thứ bậc quy mô vốn hoá (giả thuyết về sự khác biệt dòng vốn giữa các nhóm quy mô cần kiểm chứng thêm qua dữ liệu chi tiết).\"\"\"))
"""
    nb.cells.append(nbf.v4.new_code_cell(insight4_code))

    # Cell 10: Chart 5
    chart5_code = """fig5 = go.Figure()
colors_style = {
    '1 Value (P/E<12)': '#2ca02c',
    '2 Core (12-30)': '#1f77b4',
    '3 Growth (P/E>=30)': '#ff7f0e',
    '4 Loss-making (P/E n/a)': '#d62728'
}
for bucket in sorted(df_style['bucket'].unique()):
    df_b = df_style[df_style['bucket'] == bucket]
    fig5.add_trace(go.Scatter(
        x=df_b['date'],
        y=df_b['growth_of_1'],
        name=bucket,
        line=dict(color=colors_style.get(bucket, '#333333'), width=2)
    ))
fig5.update_layout(
    title='Biểu đồ 5: Tăng trưởng Lũy kế của 1 USD theo Phong cách Định giá (Value vs Growth)',
    xaxis_title='Thời gian',
    yaxis_title='Giá trị Danh mục (USD)',
    template='plotly_white',
    hovermode='x unified',
    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
)
fig5.show()
"""
    nb.cells.append(nbf.v4.new_code_cell(chart5_code))

    # Cell 11: Insight 5
    insight5_code = """last_dt_style = df_style['date'].max()
last_style_snap = df_style[df_style['date'] == last_dt_style].sort_values('growth_of_1', ascending=False)
win_style_row = last_style_snap.iloc[0]
lose_style_row = last_style_snap.iloc[-1]

win_style_name = win_style_row['bucket']
win_style_val = float(win_style_row['growth_of_1'])
lose_style_name = lose_style_row['bucket']
lose_style_val = float(lose_style_row['growth_of_1'])
core_val = float(last_style_snap[last_style_snap['bucket'].str.contains('Core')]['growth_of_1'].iloc[0])
growth_val = float(last_style_snap[last_style_snap['bucket'].str.contains('Growth')]['growth_of_1'].iloc[0])

display(Markdown(f\"\"\"### Nhận định: Phong cách Đầu tư (Value vs Growth)
- **Chiến lược giá trị dẫn đầu:** Nhóm **{win_style_name}** ghi nhận hiệu suất tích lũy cao nhất cuối chu kỳ, đạt **{win_style_val:.4f} USD** từ 1 USD đầu tư, xếp trên nhóm Core (**{core_val:.4f} USD**) và Growth (**{growth_val:.4f} USD**).
- **Hiệu suất nhóm doanh nghiệp thua lỗ:** Nhóm **{lose_style_name}** ghi nhận giá trị tích luỹ thấp nhất với **{lose_style_val:.4f} USD**, cho thấy sự phân hóa mạnh mẽ từ thị trường.
- **Đặc thù danh mục Equal-Weight:** Khi được phân bổ theo tỷ trọng đều và tái cân bằng định kỳ, danh mục cổ phiếu định giá thấp (Value) đi cùng với hiệu suất sinh lời cao hơn các nhóm còn lại trong khung thời gian khảo sát (giả thuyết về phần bù rủi ro giá trị cần kiểm chứng thêm).\"\"\"))
"""
    nb.cells.append(nbf.v4.new_code_cell(insight5_code))

    # Cell 12: Chart 6
    chart6_code = """pivot_return = df_sec_yr.pivot(index='sector', columns='year', values='return_pct')
fig6 = px.imshow(
    pivot_return,
    labels=dict(x="Năm", y="Ngành", color="Lợi suất (%)"),
    x=pivot_return.columns,
    y=pivot_return.index,
    color_continuous_scale='RdYlGn',
    text_auto='.1f',
    aspect='auto'
)
fig6.update_layout(
    title='Biểu đồ 6: Bản đồ Nhiệt (Heatmap) Lợi suất Bình quân Hàng năm theo Ngành (2017–2025)',
    xaxis_title='Năm',
    yaxis_title='Ngành Kinh tế',
    template='plotly_white'
)
fig6.show()
"""
    nb.cells.append(nbf.v4.new_code_cell(chart6_code))

    # Cell 13: Insight 6
    insight6_code = """best_per_yr = df_sec_yr.sort_values(['year', 'return_pct'], ascending=[True, False]).groupby('year').first().reset_index()
best_str = ", ".join([f"**{r['year']}**: {r['sector']} (+{float(r['return_pct']):.1f}%)" for _, r in best_per_yr.iterrows()])

max_ret_row = df_sec_yr.loc[df_sec_yr['return_pct'].idxmax()]
min_ret_row = df_sec_yr.loc[df_sec_yr['return_pct'].idxmin()]

display(Markdown(f\"\"\"### Nhận định: Luân chuyển Hiệu suất theo Ngành (2017–2025)
- **Ngành dẫn đầu qua các năm:** {best_str}.
- **Kỷ lục tăng trưởng đơn năm:** Mức sinh lời cao nhất ghi nhận được thuộc về ngành **{max_ret_row['sector']}** vào năm **{max_ret_row['year']}** với mức tăng **+{float(max_ret_row['return_pct']):.1f}%**.
- **Mức giảm sâu nhất trong một năm:** Thuộc về ngành **{min_ret_row['sector']}** vào năm **{min_ret_row['year']}** với mức giảm **{float(min_ret_row['return_pct']):.1f}%**.
- **Tính chu kỳ kinh tế:** Diễn biến lợi suất cho thấy sự biến động và phân hóa mạnh mẽ giữa các ngành qua từng năm, đi cùng với sự luân chuyển vị trí dẫn đầu giữa các nhóm ngành như công nghệ, năng lượng hay y tế.\"\"\"))
"""
    nb.cells.append(nbf.v4.new_code_cell(insight6_code))

    # Cell 14: Chart 7
    chart7_code = """df_sec_sorted_pe = df_sec_lat.sort_values('median_pe', ascending=True)
fig7 = go.Figure(go.Bar(
    x=df_sec_sorted_pe['median_pe'],
    y=df_sec_sorted_pe['sector'],
    orientation='h',
    text=df_sec_sorted_pe['median_pe'].apply(lambda x: f'{x:.1f}x'),
    textposition='auto',
    marker=dict(
        color=df_sec_sorted_pe['median_pe'],
        colorscale='Blues'
    )
))
fig7.update_layout(
    title='Biểu đồ 7: Định giá P/E Trung vị theo Ngành (Snapshot Mới nhất)',
    xaxis_title='P/E Trung vị (lần)',
    yaxis_title='Ngành Kinh tế',
    template='plotly_white'
)
fig7.show()
"""
    nb.cells.append(nbf.v4.new_code_cell(chart7_code))

    # Cell 15: Insight 7
    insight7_code = """max_pe_row = df_sec_lat.loc[df_sec_lat['median_pe'].idxmax()]
min_pe_row = df_sec_lat.loc[df_sec_lat['median_pe'].idxmin()]
max_pe_sector = max_pe_row['sector']
max_pe_val = float(max_pe_row['median_pe'])
min_pe_sector = min_pe_row['sector']
min_pe_val = float(min_pe_row['median_pe'])

display(Markdown(f\"\"\"### Nhận định: Định giá P/E Trung vị theo Ngành
- **Ngành có mức định giá cao nhất:** Ngành **{max_pe_sector}** ghi nhận mức định giá P/E trung vị cao nhất đạt **{max_pe_val:.1f}x**.
- **Ngành có định giá thấp nhất:** Ngành **{min_pe_sector}** có mức định giá thấp nhất thị trường với P/E trung vị chỉ **{min_pe_val:.1f}x**.
- **Biên độ phân hóa định giá:** Mức định giá giữa ngành cao nhất và thấp nhất chênh lệch hơn hai lần (giả thuyết về sự khác nhau trong kỳ vọng tăng trưởng đối với từng mô hình kinh doanh cần kiểm chứng thêm).\"\"\"))
"""
    nb.cells.append(nbf.v4.new_code_cell(insight7_code))

    # Cell 16: Chart 8
    chart8_code = """fig8 = px.scatter(
    df_sec_lat,
    x='median_pe',
    y='median_roe_pct',
    size='total_mcap_bn',
    color='sector',
    hover_name='sector',
    text='sector',
    size_max=50,
    labels=dict(
        median_pe='P/E Trung vị (lần)',
        median_roe_pct='ROE Trung vị (%)',
        total_mcap_bn='Tổng Vốn hoá (tỷ USD)',
        sector='Ngành'
    )
)
fig8.update_traces(textposition='top center')
fig8.update_layout(
    title='Biểu đồ 8: Tương quan ROE vs P/E theo Ngành (Kích thước Bubble = Vốn hoá)',
    xaxis_title='P/E Trung vị (lần)',
    yaxis_title='ROE Trung vị (%)',
    template='plotly_white',
    showlegend=False
)
fig8.show()
"""
    nb.cells.append(nbf.v4.new_code_cell(chart8_code))

    # Cell 17: Insight 8
    insight8_code = """max_roe_row = df_sec_lat.loc[df_sec_lat['median_roe_pct'].idxmax()]
max_mcap_row = df_sec_lat.loc[df_sec_lat['total_mcap_bn'].idxmax()]
mean_pe = float(df_sec_lat['median_pe'].mean())
high_roe_val = float(max_roe_row['median_roe_pct'])
high_roe_pe = float(max_roe_row['median_pe'])
high_roe_sector = max_roe_row['sector']
max_mcap_sector = max_mcap_row['sector']
max_mcap_val = float(max_mcap_row['total_mcap_bn'])
max_mcap_pe = float(max_mcap_row['median_pe'])

display(Markdown(f\"\"\"### Nhận định: Tương quan Hiệu quả Hoạt động (ROE) và Định giá (P/E)
- **Hiệu quả sinh lời ROE:** Ngành **{high_roe_sector}** ghi nhận tỷ suất sinh lời trên vốn chủ sở hữu ROE trung vị đạt **{high_roe_val:.1f}%**, đi cùng chỉ số định giá P/E ở mức **{high_roe_pe:.1f}x** (thấp hơn mức trung bình các ngành là **{mean_pe:.1f}x**).
- **Quy mô vốn hoá:** Ngành **{max_mcap_sector}** ghi nhận quy mô vốn hoá dẫn đầu đạt **{max_mcap_val:,.1f} tỷ USD**, với mức định giá P/E trung vị là **{max_mcap_pe:.1f}x**.
- **Đặc trưng phân bố:** Biểu đồ cho thấy sự phân hóa giữa các ngành, trong đó có ngành sinh lời ROE cao nhưng P/E thấp hơn mức trung bình các ngành, và có ngành quy mô vốn hoá lớn đi cùng với mức định giá P/E cao.\"\"\"))
"""
    nb.cells.append(nbf.v4.new_code_cell(insight8_code))

    # Cell 18: Markdown Concluding
    concluding_md = """## 4. Hạn chế và Bước tiếp theo

### Hạn chế (Limitations)
1. **Phạm vi chuỗi thời gian:** Một số chỉ số vĩ mô (cụ thể là `credit_spread_hy`) chỉ có dữ liệu từ ngày 2023-10-17 trong nguồn lakehouse, chưa bao quát toàn bộ chuỗi 10 năm như lãi suất trái phiếu và VIX.
2. **Mô phỏng Equal-Weight chưa trừ phí và trượt giá:** Các chỉ số tăng trưởng lũy kế của 1 USD theo chiến lược equal-weight được mô phỏng lý thuyết, chưa khấu trừ chi phí giao dịch, thuế và độ trượt giá thị trường thực tế.
3. **Chưa kiểm tra thiên lệch sống sót (Survivorship Bias):** Phân tích chưa đánh giá ảnh hưởng của thiên lệch sống sót đối với các mã cổ phiếu bị hủy niêm yết trong quá khứ.
4. **Đặc thù thế hệ nguồn dữ liệu cũ:** Dữ liệu là thế hệ nguồn cũ (Sharadar/Alpaca/FRED) không khớp với mô hình pipeline dbt FMP hiện tại.
5. **Tính chất thời điểm của ảnh chụp nhanh:** Bức tranh so sánh định giá và hiệu quả kinh doanh 11 ngành là ảnh chụp nhanh (snapshot) tại ngày dữ liệu mới nhất (2026-10-15); các chỉ số P/E hay ROE trung vị có thể dao động theo mùa vụ công bố báo cáo tài chính của các doanh nghiệp.

### Bước tiếp theo (Next Steps)
1. **Đồng bộ hóa dữ liệu dbt FMP:** Nâng cấp và mở rộng pipeline đồng bộ trực tiếp từ Financial Modeling Prep (FMP) qua mô hình dbt để cập nhật tự động các bảng mart theo lịch trình hàng ngày.
2. **Mô hình hóa chiến lược thích ứng vĩ mô:** Tận dụng tín hiệu đảo chiều đường cong lợi suất (10Y-2Y) và các mức ngưỡng biến động VIX để xây dựng thuật toán phân bổ tỷ trọng động giữa các lớp tài sản.
3. **Phân tích tài chính chuyên sâu:** Mở rộng bảng phân tích ngành sang mô hình phân rã DuPont (ROE = Biên lãi ròng × Vòng quay tài sản × Đòn bẩy tài chính) và bổ sung các chỉ số định giá đa chiều (P/B, EV/EBITDA, Free Cash Flow Yield).
"""
    nb.cells.append(nbf.v4.new_markdown_cell(concluding_md))

    return nb


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    nb_path = os.path.join(base_dir, "market_insights.ipynb")
    html_path = os.path.join(base_dir, "market_insights.html")

    print(f"Creating notebook at {nb_path}...")
    nb = create_notebook()
    with open(nb_path, "w", encoding="utf-8") as f:
        nbf.write(nb, f)

    print("Executing notebook inplace via nbconvert...")
    exec_cmd = [
        sys.executable,
        "-m",
        "jupyter",
        "nbconvert",
        "--to",
        "notebook",
        "--execute",
        "--inplace",
        nb_path,
    ]
    res = subprocess.run(exec_cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print("Subprocess nbconvert failed, trying ExecutePreprocessor directly...")
        print("STDERR:", res.stderr)
        ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
        with open(nb_path, "r", encoding="utf-8") as f:
            nb = nbf.read(f, as_version=4)
        ep.preprocess(nb, {"metadata": {"path": base_dir}})
        with open(nb_path, "w", encoding="utf-8") as f:
            nbf.write(nb, f)
        print("ExecutePreprocessor completed successfully.")
    else:
        print("Notebook executed successfully.")

    print(f"Exporting notebook to HTML at {html_path}...")
    html_cmd = [
        sys.executable,
        "-m",
        "jupyter",
        "nbconvert",
        "--to",
        "html",
        nb_path,
    ]
    res_html = subprocess.run(html_cmd, capture_output=True, text=True)
    if res_html.returncode != 0:
        print("Subprocess html export failed, trying HTMLExporter directly...")
        with open(nb_path, "r", encoding="utf-8") as f:
            nb = nbf.read(f, as_version=4)
        html_exporter = HTMLExporter()
        body, _ = html_exporter.from_notebook_node(nb)
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(body)
        print("HTMLExporter completed successfully.")
    else:
        print("HTML exported successfully.")

    print("Done building and exporting notebooks/market_insights.ipynb and market_insights.html.")


if __name__ == "__main__":
    main()
