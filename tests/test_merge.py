from saas_status.merge import merge_events
from saas_status.normalize import normalize_event


def test_status_updates_merge_to_latest_state():
    events = [normalize_event({"service":"S","component":"C","event_id":"1","started_at":"2026-01-01","updated_at":"2026-01-01T01:00","state":"investigating","impact":"major","summary":"first","source_ref":"a"}), normalize_event({"service":"s","component":"c","event_id":"1","started_at":"2026-01-01","updated_at":"2026-01-01T02:00","state":"resolved","impact":"major","summary":"done","source_ref":"b"})]
    merged = merge_events(events)
    assert len(merged) == 1
    assert merged[0].state == "resolved"
    assert merged[0].source_ref == "a,b"
