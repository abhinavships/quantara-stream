import json
from quantara_core.bus import MemBus
from stream.validate import Validator
from stream.enrich import Enricher, VSS


def tel(vid="V001", ts=1.0, temp=30.0, **sig):
    s = {"Vehicle.Speed": 60.0, "Battery.Temp": temp, "Coolant.Flow": 8.0, "Motor.Current": 50.0,
         "Battery.SoC": 0.8, "Battery.kmPerKwh": 6.0, "Pump.Duty": 0.5, "Power.kW": 20.0, **sig}
    return {"kind": "telemetry", "vehicle_id": vid, "ts": ts, "sim": True, "signals": s, "lat": 12.97, "lon": 77.59}


def topics_of(out): return [t for t, _, _ in out]


# ---------- validate ----------
def test_valid_passes_and_keyed():
    out = Validator().process("raw.telemetry", tel())
    assert topics_of(out) == ["valid.telemetry"] and out[0][2] == "V001"


def test_schema_error_goes_to_dlq_with_reason():
    out = Validator().process("raw.telemetry", {"vehicle_id": "V1", "ts": 1.0})
    assert topics_of(out) == ["dlq"] and "schema" in out[0][1]["reason"]


def test_undecodable_goes_to_dlq():
    assert topics_of(Validator().process("raw.telemetry", {"_undecodable": True})) == ["dlq"]


def test_out_of_range_dlq():
    out = Validator().process("raw.telemetry", tel(temp=900.0))
    assert topics_of(out) == ["dlq"] and "out of range" in out[0][1]["reason"]


def test_non_simulated_rejected():
    m = tel(); m["sim"] = False
    out = Validator().process("raw.telemetry", m)
    assert topics_of(out) == ["dlq"] and "SANDBOX" in out[0][1]["reason"]


def test_duplicates_dropped_once():
    v = Validator()
    assert topics_of(v.process("raw.telemetry", tel(ts=5.0))) == ["valid.telemetry"]
    assert v.process("raw.telemetry", tel(ts=5.0)) == [] and v.stats["dup"] == 1
    assert topics_of(v.process("raw.telemetry", tel(ts=6.0))) == ["valid.telemetry"]


def test_dedupe_cache_bounded():
    v = Validator(dedupe_size=10)
    for i in range(100): v.process("raw.telemetry", tel(ts=float(i)))
    assert len(v.seen) == 10


def test_physics_mismatch_quarantines_then_releases():
    v = Validator()
    v.process("raw.feeds", {"weather": {"ambient_c": 30}})
    kinds = []
    for i in range(3):                       # battery 5C while ambient 30C and delivering power -> impossible
        kinds += [(t, m.get("action")) for t, m, _ in v.process("raw.telemetry", tel(ts=float(i), temp=5.0))]
    assert ("quarantine", "enter") in kinds
    held = v.process("raw.telemetry", tel(ts=10.0, temp=5.0))
    assert "valid.telemetry" not in topics_of(held)              # held back, not forwarded
    rel = []
    for i in range(6): rel += v.process("raw.telemetry", tel(ts=20.0 + i, temp=35.0))
    assert ("quarantine", "release") in [(t, m.get("action")) for t, m, _ in rel]
    assert "valid.telemetry" in topics_of(v.process("raw.telemetry", tel(ts=40.0, temp=35.0)))


def test_security_event_quarantines_vehicle_and_forwards():
    v = Validator()
    out = v.process("raw.edge", {"kind": "security", "vehicle_id": "V7", "ts": 1.0, "incident": {"cause": "can_flood"}})
    assert topics_of(out) == ["quarantine", "valid.edge"]
    assert "valid.telemetry" not in topics_of(v.process("raw.telemetry", tel("V7", ts=2.0)))
    assert "valid.telemetry" in topics_of(v.process("raw.telemetry", tel("V8", ts=2.0)))   # others unaffected


def test_security_quarantine_expires_by_ttl():
    t = [1000.0]
    v = Validator(clock=lambda: t[0])
    v.process("raw.edge", {"kind": "security", "vehicle_id": "V7"})
    t[0] += 500
    out = v.process("raw.telemetry", tel("V7", ts=3.0))
    assert "valid.telemetry" in topics_of(out) and out[0][1]["action"] == "release"


def test_unknown_edge_kind_dlq_and_feeds_pass():
    v = Validator()
    assert topics_of(v.process("raw.edge", {"kind": "bogus"})) == ["dlq"]
    assert topics_of(v.process("raw.feeds", {"weather": {"ambient_c": 21}})) == ["valid.feeds"] and v.ambient == 21


# ---------- enrich ----------
def test_vss_mapping_and_windows():
    e = Enricher()
    for i in range(10):
        out = e.process("valid.telemetry", tel(ts=float(i), temp=30.0 + i))
    m = out[0][1]
    assert out[0][0] == "vss.enriched" and VSS["Battery.Temp"] in m["vss"]
    w = m["windows"]["Battery.Temp"]["5m"]
    assert w["n"] == 10 and w["min"] == 30.0 and w["max"] == 39.0 and abs(w["slope_per_s"] - 1.0) < 1e-6


def test_window_evicts_old_samples():
    e = Enricher()
    e.process("valid.telemetry", tel(ts=0.0))
    out = e.process("valid.telemetry", tel(ts=4000.0))
    assert out[0][1]["windows"]["Battery.Temp"]["1h"]["n"] == 1


def test_enrichment_with_weather_grid_nearest_station():
    e = Enricher()
    e.process("valid.feeds", {"weather": {"ambient_c": 28.0}, "grid": {"outage": True, "stations": [
        {"id": "S1", "lat": 12.97, "lon": 77.60, "online": False}, {"id": "S2", "lat": 13.5, "lon": 77.6, "online": True}]}})
    m = e.process("valid.telemetry", tel())[0][1]
    assert m["ambient_c"] == 28.0 and m["grid_outage"] is True
    assert m["nearest_station"]["id"] == "S1" and m["nearest_station"]["online"] is False


def test_escalation_routed_with_fleet_context():
    e = Enricher()
    esc = lambda vid: {"kind": "escalation", "vehicle_id": vid, "ts": 1.0, "incident": {"cause": "coolant_pump"}}
    e.process("valid.telemetry", tel("V1"))
    t, m, k = e.process("valid.edge", esc("V1"))[0]
    assert (t, k) == ("cloud.escalations", "V1") and m["context"]["peers_same_cause"] == 0
    m2 = e.process("valid.edge", esc("V2"))[0][1]
    assert m2["context"]["peers_same_cause"] == 1 and m2["context"]["fleet"]["escalations"] == 2


def test_security_summary_and_command_result_routing():
    e = Enricher()
    assert e.process("valid.edge", {"kind": "security", "vehicle_id": "V1", "incident": {"cause": "can_flood"}})[0][0] == "cloud.security"
    assert e.process("valid.edge", {"kind": "summary", "vehicle_id": "V1"})[0][0] == "cloud.fleet"
    assert e.process("valid.edge", {"kind": "command_result", "vehicle_id": "V1"})[0][0] == "cloud.ota"


# ---------- two-stage pipeline over the bus (what the two Condense deployments do) ----------
def test_end_to_end_over_membus(monkeypatch):
    monkeypatch.setenv("TOPIC_PREFIX", "quantara_")
    from quantara_core import config
    bus, T = MemBus(), config.topic
    v, e = Validator(), Enricher()
    bus.publish(T("raw.telemetry"), tel("V1", 1.0), key="V1")
    bus.publish(T("raw.telemetry"), {"garbage": 1}, key="V2")
    bus.publish(T("raw.edge"), {"kind": "escalation", "vehicle_id": "V1", "ts": 2.0, "incident": {"cause": "x"}}, key="V1")
    for t, _, m in bus.poll("g1", [T("raw.telemetry"), T("raw.edge")]):
        for ot, om, k in v.process(t[len(T("")):], m): bus.publish(T(ot), om, key=k)
    for t, _, m in bus.poll("g2", [T("valid.telemetry"), T("valid.edge")]):
        for ot, om, k in e.process(t[len(T("")):], m): bus.publish(T(ot), om, key=k)
    got = {t: len(bus.log[t]) for t in bus.log}
    assert got[T("dlq")] == 1 and got[T("vss.enriched")] == 1 and got[T("cloud.escalations")] == 1
    json.dumps(bus.log[T("vss.enriched")][0][1])              # serialisable
