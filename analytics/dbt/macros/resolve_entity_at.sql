{#
    Song sinh của resolve_security_at, cho trục pháp nhân. PK của
    std_ip_legal_entity là (cik, valid_from, known_from) nhưng 7 FK trỏ vào
    riêng cik. Các FK đó vẫn hữu ích (kiểm tồn tại) nhưng KHÔNG đủ để join:
    join trần theo cik nhân bản dòng theo số phiên bản.

    Ranh giới [valid_from, valid_to) -- đóng trái, MỞ phải. Sự kiện rơi vào khe
    giữa hai phiên bản trả NULL; đó là câu trả lời đúng, không phải lỗi cần vá.
#}
{% macro resolve_entity_at(entity_alias, cik_column, date_column) %}
left join {{ ref('std_ip_legal_entity') }} as _ent
    on  _ent.cik = {{ entity_alias }}.{{ cik_column }}
    and {{ entity_alias }}.{{ date_column }} >= _ent.valid_from
    and (_ent.valid_to is null or {{ entity_alias }}.{{ date_column }} < _ent.valid_to)
{% endmacro %}
