{#
    Vân tay của tập thuộc tính nghiệp vụ, dùng để phát hiện "giá trị có đổi không"
    giữa hai run_date. Spec 2026-08-20 §3.5.

    KHÔNG dùng concat_ws('||', coalesce(col, '__NULL__'), ...): nó lẫn khi dữ liệu
    chứa chính chuỗi phân tách. to_json(struct(...)) giữ ranh giới trường bằng key
    JSON, thứ tự trường xác định theo thứ tự struct.

    map('ignoreNullFields', 'false') là BẮT BUỘC, không phải trang trí. Mặc định của
    Spark là 'true', lấy từ config phiên spark.sql.jsonGenerator.ignoreNullFields.
    Không ghim ở đây thì ai đó đổi nó ở tầng cluster sẽ làm MỌI hash đổi cùng lúc:
    mọi thực thể trông như vừa thay đổi, và ta sinh một phiên bản giả cho TOÀN BỘ
    bảng với known_from là ngày hôm đó. Im lặng, không lỗi, không log.

    Cột kỹ thuật (run_date, ingested_at, _source_file) KHÔNG được truyền vào đây --
    nếu vào thì mọi lần chạy đều "có thay đổi".
#}
{% macro attr_fingerprint(columns) %}
xxhash64(to_json(struct({{ columns | join(', ') }}), map('ignoreNullFields', 'false')))
{% endmacro %}
