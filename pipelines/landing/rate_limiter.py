import time
from collections.abc import Callable


class RateLimiter:
    """Giữ khoảng cách tối thiểu giữa 2 request liên tiếp, đo bằng đồng hồ monotonic.

    Ngủ phần CÒN LẠI của cửa sổ chứ không ngủ trọn `delay` sau mỗi call: thời gian
    fetch + validate + ghi MinIO vốn đã trôi qua và phải được tính vào khoảng cách. Ngủ
    trọn delay làm tốc độ thật luôn thấp hơn cấu hình một lượng không đoán trước được --
    với ~277.000 call, chênh lệch đó là hàng giờ.

    `clock`/`sleeper` inject được để test không phải monkeypatch module `time` toàn cục.
    """

    def __init__(
        self,
        delay_seconds: float,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self._delay_seconds = delay_seconds
        self._clock = clock
        self._sleeper = sleeper
        self._next_allowed_at: float | None = None

    def wait(self) -> None:
        if self._delay_seconds <= 0:
            return
        now = self._clock()
        if self._next_allowed_at is not None and now < self._next_allowed_at:
            self._sleeper(self._next_allowed_at - now)
            now = self._next_allowed_at
        self._next_allowed_at = now + self._delay_seconds
