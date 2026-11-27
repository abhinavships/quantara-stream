"""Stage 1 (Transform: validate). raw.* -> valid.telemetry | dlq | quarantine; edge events pass through.
Pure and testable: Validator.process(topic_short, msg) -> [(topic_short, msg, key)]."""
import time
from collections import OrderedDict, defaultdict

from quantara_core import schemas

RANGES = {"Vehicle.Speed": (-1, 250), "Battery.Temp": (-40, 120), "Coolant.Flow": (-0.5, 40),
          "Motor.Current": (-500, 800), "Battery.SoC": (0, 1.0001), "Battery.kmPerKwh": (0, 30),
          "Pump.Duty": (0, 1.0001), "Power.kW": (-200, 300)}
EDGE_KINDS = {"summary", "escalation", "security", "command_result"}
QUARANTINE_AFTER, RELEASE_AFTER, SECURITY_TTL_S = 3, 6, 120


class Validator:
    def __init__(self, dedupe_size=50_000, clock=time.time):
        self.seen, self.size, self.clock = OrderedDict(), dedupe_size, clock
        self.bad, self.good = defaultdict(int), defaultdict(int)     # consecutive counters
        self.quarantined = {}                                         # vid -> reason
        self.sec_until = {}                                           # vid -> epoch
        self.ambient = 25.0
        self.stats = defaultdict(int)

    def _dup(self, key):
        if key in self.seen:
            return True
        self.seen[key] = 1
        if len(self.seen) > self.size:
            self.seen.popitem(last=False)
        return False

    def _dlq(self, topic, msg, reason):
        self.stats["dlq"] += 1
        return [("dlq", {"from": topic, "reason": reason, "msg": msg}, msg.get("vehicle_id") if isinstance(msg, dict) else None)]

    def _physics_ok(self, s):
        """telemetry-vs-physics: pack cannot sit far below ambient while delivering power."""
        t = s.get("Battery.Temp")
        return t is None or t >= self.ambient - 8

    def process(self, topic, msg):
        if not isinstance(msg, dict) or msg.get("_undecodable"):
            return self._dlq(topic, {"raw": str(msg)[:200]}, "undecodable")
        if topic == "raw.feeds":
            a = (msg.get("weather") or {}).get("ambient_c")
            if isinstance(a, (int, float)): self.ambient = a
            return [("valid.feeds", msg, None)]
        if topic == "raw.edge":
            return self._edge(msg)
        return self._telemetry(msg)

    def _edge(self, m):
        if m.get("kind") not in EDGE_KINDS:
            return self._dlq("raw.edge", m, "unknown kind")
        vid = m.get("vehicle_id")
        if m["kind"] == "security" and vid:
            self.sec_until[vid] = self.clock() + SECURITY_TTL_S
            self.quarantined.setdefault(vid, "security_incident")
            self.stats["quarantine"] += 1
            return [("quarantine", {"vehicle_id": vid, "reason": "security_incident", "action": "enter"}, vid),
                    ("valid.edge", m, vid)]
        return [("valid.edge", m, vid)]

    def _telemetry(self, m):
        errs = schemas.validate_telemetry(m)
        if errs:
            return self._dlq("raw.telemetry", m, "schema: " + ",".join(errs))
        vid = m["vehicle_id"]
        if not m["sim"]:
            return self._dlq("raw.telemetry", m, "non-simulated vehicle (SANDBOX)")
        for k, v in m["signals"].items():
            lo, hi = RANGES.get(k, (-1e9, 1e9))
            if not lo <= v <= hi:
                return self._dlq("raw.telemetry", m, f"out of range {k}={v}")
        if self._dup((vid, m["ts"], m.get("kind"))):
            self.stats["dup"] += 1
            return []
        out, ok = [], self._physics_ok(m["signals"])
        if ok: self.good[vid] += 1; self.bad[vid] = 0
        else: self.bad[vid] += 1; self.good[vid] = 0
        was = vid in self.quarantined
        if not was and self.bad[vid] >= QUARANTINE_AFTER:
            self.quarantined[vid] = "telemetry_physics_mismatch"
            out.append(("quarantine", {"vehicle_id": vid, "reason": "telemetry_physics_mismatch", "action": "enter",
                                       "evidence": {"Battery.Temp": m["signals"]["Battery.Temp"], "ambient": self.ambient}}, vid))
            self.stats["quarantine"] += 1
            was = True
        elif was and self.quarantined[vid] == "telemetry_physics_mismatch" and self.good[vid] >= RELEASE_AFTER:
            del self.quarantined[vid]
            out.append(("quarantine", {"vehicle_id": vid, "reason": "recovered", "action": "release"}, vid))
            was = False
        if was and self.quarantined.get(vid) == "security_incident" and self.clock() > self.sec_until.get(vid, 0):
            del self.quarantined[vid]
            out.append(("quarantine", {"vehicle_id": vid, "reason": "ttl", "action": "release"}, vid))
            was = False
        if was:
            self.stats["held"] += 1
            return out + [("quarantine", {"vehicle_id": vid, "action": "held", "msg": m}, vid)]
        self.stats["valid"] += 1
        return out + [("valid.telemetry", m, vid)]
