from datetime import datetime


from hdb_parking.client import fetch_availability
from hdb_parking.records import SGT


class StubResponse:
    def raise_for_status(self):
        pass

    def json(self):
        return {"items": [{"timestamp": "2026-10-01T08:00:00+08:00", "carpark_data": []}]}


class StubSession:
    def get(self, url, *, params, timeout):
        assert url.endswith("carpark-availability")
        assert params == {"date_time": "2026-10-01T08:00:00"}
        assert timeout > 0
        return StubResponse()


def test_historical_request_format():
    result = fetch_availability(StubSession(), datetime(2026, 10, 1, 8, tzinfo=SGT))
    assert result["items"]
