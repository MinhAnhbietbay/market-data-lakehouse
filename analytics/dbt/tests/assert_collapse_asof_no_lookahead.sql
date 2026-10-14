-- Bất biến của mọi model dùng collapse_asof: không ô nào được mang giá trị từ
-- một phiên bản có known_from > date của ô đó. Test này chạy trên mọi model
-- curated nội bộ, nên nó bắt lỗi macro chứ không phải lỗi từng model.
{% set collapsed_models = ['__fam_fundamental', '__fam_analyst', '__fam_group'] %}

{% for m in collapsed_models %}
{% if not loop.first %}union all{% endif %}
select '{{ m }}' as model, date, symbol, known_from
from {{ ref(m) }}
where known_from > date
{% endfor %}
