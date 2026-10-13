{#
    Phân giải symbol + ngày -> share_class_figi qua phiên bản niêm yết CÓ HIỆU LỰC
    tại ngày đó. Spec 2026-08-19 Đ5: bảng sự kiện giữ symbol và KHÔNG mang figi,
    nên mọi truy vấn phải trả cái giá này -- một join theo thời gian.

    Vì sao không gắn figi thẳng vào 29 bảng sự kiện: nếu sau khi nâng gói phát hiện
    chọn sai khoá thì sửa MỘT bảng chứ không phải 29.

    Ranh giới khoảng là [valid_from, valid_to) -- đóng bên trái, MỞ bên phải. Ngày
    một niêm yết kết thúc chính là ngày niêm yết kế tiếp bắt đầu; đóng cả hai đầu
    thì một sự kiện đúng ngày đó khớp HAI phiên bản.

    Sự kiện rơi vào khe giữa hai lần niêm yết trả NULL. Đó là câu trả lời đúng, không
    phải lỗi cần vá: ta thật sự không biết nó thuộc chứng khoán nào. Người gọi tự
    quyết bỏ hay giữ, và phải quyết tường minh.
#}
{% macro resolve_security_at(event_alias, symbol_column, date_column) %}
left join {{ ref('std_ar_listing') }} as _sec
    on  _sec.symbol = {{ event_alias }}.{{ symbol_column }}
    and {{ event_alias }}.{{ date_column }} >= _sec.valid_from
    and ({{ _sec_valid_to_open() }} or {{ event_alias }}.{{ date_column }} < _sec.valid_to)
{% endmacro %}

{% macro _sec_valid_to_open() %}_sec.valid_to is null{% endmacro %}
