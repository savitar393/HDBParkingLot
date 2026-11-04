from datetime import datetime, timedelta, timezone

import pytest

from hdb_parking.pipeline_check import assess_gold_freshness, overall_status

SGT = timezone(timedelta(hours=8))
EXPECTED = datetime(2026, 11, 2, 22, tzinfo=SGT)


def test_gold_exact_hour_converts_sgt_to_utc():
    result = assess_gold_freshness(datetime(2026, 11, 2, 14), EXPECTED)
    assert result['status'] == 'healthy'
    assert result['lag_hours'] == 0
    assert result['latest_gold_utc'] == '2026-11-02T14:00:00Z'


def test_gold_one_hour_behind_is_tolerated():
    assert assess_gold_freshness(datetime(2026, 11, 2, 13), EXPECTED)['status'] == 'healthy'


def test_gold_two_hours_behind_is_degraded():
    assert assess_gold_freshness(datetime(2026, 11, 2, 12), EXPECTED)['status'] == 'degraded'


def test_gold_future_is_degraded():
    assert assess_gold_freshness(datetime(2026, 11, 2, 15), EXPECTED)['status'] == 'degraded'


def test_gold_missing_is_degraded():
    assert assess_gold_freshness(None, EXPECTED)['status'] == 'degraded'


def test_aware_utc_timestamp_is_supported():
    t = datetime(2026, 11, 2, 14, tzinfo=timezone.utc)
    assert assess_gold_freshness(t, EXPECTED)['status'] == 'healthy'


def test_invalid_parameters_rejected():
    with pytest.raises(ValueError):
        assess_gold_freshness(datetime(2026, 11, 2, 14), EXPECTED, max_lag_hours=-1)
    with pytest.raises(ValueError):
        assess_gold_freshness(datetime(2026, 11, 2, 14), datetime(2026, 11, 2, 22))


def test_overall_requires_all_guards():
    assert overall_status(collection_failed=0, bronze_status='healthy', gold_status='healthy',
                          export_status='match') == 'healthy'
    assert overall_status(collection_failed=1, bronze_status='healthy', gold_status='healthy',
                          export_status='match') == 'degraded'
    assert overall_status(collection_failed=0, bronze_status='degraded', gold_status='healthy',
                          export_status='match') == 'degraded'
    assert overall_status(collection_failed=0, bronze_status='healthy', gold_status='degraded',
                          export_status='match') == 'degraded'
    assert overall_status(collection_failed=0, bronze_status='healthy', gold_status='healthy',
                          export_status='mismatch') == 'degraded'
