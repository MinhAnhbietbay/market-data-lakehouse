import gzip
import json
import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from pipelines.landing.templating import render_mapping


@dataclass(frozen=True)
class PreparedRequest:
    url: str
    params: dict[str, Any]
    safe_params: dict[str, Any]


@dataclass(frozen=True)
class ApiResponse:
    status: int
    payload: Any
    error: str | None = None
    size_bytes: int = 0

def apply_query_param_auth(
    params: dict[str, Any],
    auth_config: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if auth_config.get("type") != "query_param":
        return params, params

    param_name = auth_config["param_name"]
    env_var = auth_config["env_var"]
    api_key = os.getenv(env_var)

    if not api_key:
        raise RuntimeError(f"Missing required environment variable: {env_var}")

    authed_params = {
        **params,
        param_name: api_key,
    }

    safe_params = {
        **params,
        param_name: "***",
    }

    return authed_params, safe_params

def apply_header_auth(auth_config: dict[str, Any]) -> dict[str, str]:
    """Auth qua HTTP header (OpenFIGI dùng X-OPENFIGI-APIKEY).

    Nổ ngay khi thiếu env var, cùng kiểu với apply_query_param_auth: một đợt pull
    chạy hàng trăm request không được phép phát hiện thiếu key ở request thứ 200.
    """
    if auth_config.get("type") != "header":
        return {}
    env_var = auth_config["env_var"]
    api_key = os.getenv(env_var)
    if not api_key:
        raise RuntimeError(f"Missing required environment variable: {env_var}")
    return {auth_config["header_name"]: api_key}

def prepare_request(
    source_config: dict[str, Any],
    endpoint_config: dict[str, Any],
    values: dict[str, Any],
) -> PreparedRequest:
    base_url = source_config["base_url"].rstrip("/")
    path = endpoint_config["path"]
    params = render_mapping(endpoint_config.get("params", {}), values)

    authed_params, safe_params = apply_query_param_auth(
        params=params,
        auth_config=source_config.get("auth", {}),
    )

    url = f"{base_url}{path}?{urlencode(authed_params)}"

    return PreparedRequest(
        url=url,
        params=authed_params,
        safe_params=safe_params,
    )


def _decode_wire_body(raw: bytes, headers: Any) -> bytes:
    """Giải nén thân response nếu server khai `Content-Encoding: gzip`.

    Chỉ giải nén khi CÓ khai báo: FMP trả một số response (đo thật: các lỗi 402)
    không nén, giải nén mù sẽ nổ trên chính những response đó. gzip hỏng thì cứ để
    ném ra -- `run_batch` bắt Exception theo từng endpoint, nên nó thành một endpoint
    fail nhìn thấy được chứ không âm thầm biến thành payload rỗng.
    """
    if (headers.get("Content-Encoding") or "").lower() == "gzip":
        return gzip.decompress(raw)
    return raw


def fetch_json(
    url: str,
    timeout_seconds: int,
    method: str = "GET",
    body: Any = None,
    headers: dict[str, str] | None = None,
) -> ApiResponse:
    # `Accept-Encoding: gzip` phải khai tay: urllib không tự thêm như requests. Không
    # khai thì mọi payload tải về dạng thô -- đo thật trên /historical-price-eod 10 năm
    # là 420KB thay vì 56KB (7,4x), và trần băng thông FMP Premium chỉ có 50GB/30 ngày.
    # Chỉ khai gzip (không deflate/br) để không bao giờ nhận về thứ ta không giải được.
    request_headers = {"Accept": "application/json", "Accept-Encoding": "gzip"}
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    request_headers.update(headers or {})
    request = Request(url, data=data, headers=request_headers, method=method)

    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read()
            body = _decode_wire_body(raw, response.headers)
            return ApiResponse(
                status=response.status,
                payload=json.loads(body.decode("utf-8")),
                # Byte QUA DÂY, không phải byte sau giải nén: `enforce_bandwidth_budget`
                # phải đếm đúng cái FMP tính, nếu không sẽ dừng run sớm oan ~7 lần.
                size_bytes=len(raw),
            )

    except HTTPError as error:
        raw = error.read()
        body = _decode_wire_body(raw, error.headers)
        try:
            payload = json.loads(body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            payload = {"raw_body": body.decode("utf-8", errors="replace")}
        return ApiResponse(status=error.code, payload=payload, error=str(error), size_bytes=len(raw))

    except URLError as error:
        # Lỗi tầng vận chuyển (DNS, timeout, TCP reset) trước đây ném thẳng ra ngoài và
        # abort cả endpoint, trong khi lỗi tầng HTTP lại được nuốt thành ApiResponse --
        # hai đường xử lý khác hẳn nhau cho cùng một loại sự cố. Chuẩn hoá về status=0 để
        # run_batch phân loại đồng nhất; --continue-on-error vẫn quyết định dừng hay chạy tiếp.
        return ApiResponse(status=0, payload={"error": str(error)}, error=str(error))


@dataclass(frozen=True)
class RetryDecision:
    retry: bool
    wait_seconds: float


def retry_decision(
    response: ApiResponse,
    attempt: int,
    backoff_factor: float,
    rate_limit_backoff_seconds: float,
) -> RetryDecision:
    """Quyết định có gọi lại một request vừa hỏng không, và chờ bao lâu.

    429: chờ nguyên một cửa sổ phút rồi thử lại. Đo 2026-09-30: hết quota NGÀY cũng trả 429
    kèm "Limit Reach" -- cùng dạng với vượt trần theo phút -- nên không phân biệt được bằng
    body; chờ rồi thử lại, hết lượt mới để classify_error coi là quota_exhausted.
    0 (lỗi vận chuyển) và 5xx: backoff luỹ thừa. Mọi status khác (402 khoá gói, 404) gọi lại
    cũng ra y hệt, chỉ tốn quota.
    """
    if response.status == 429:
        return RetryDecision(retry=True, wait_seconds=float(rate_limit_backoff_seconds))
    if response.status == 0 or 500 <= response.status < 600:
        return RetryDecision(retry=True, wait_seconds=float(backoff_factor) ** attempt)
    return RetryDecision(retry=False, wait_seconds=0.0)

