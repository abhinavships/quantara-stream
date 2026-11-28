"""Stage 2 (Transform: enrich + route). valid.* -> vss.enriched, cloud.* fan-out.
Maps signals to VSS names, keeps sliding windows (5 min / 1 h), adds weather + grid + nearest-station
context, and routes edge events to the cloud topics with fleet context attached."""
import math
from collections import defaultdict, deque

VSS = {"Vehicle.Speed": "Vehicle.Speed",
       "Battery.Temp": "Vehicle.Powertrain.TractionBattery.Temperature.Average",
       "Coolant.Flow": "Vehicle.Powertrain.TractionBattery.Cooling.FlowRate",
       "Motor.Current": "Vehicle.Powertrain.ElectricMotor.Current",
       "Battery.SoC": "Vehicle.Powertrain.TractionBattery.StateOfCharge.Current",
       "Battery.kmPerKwh": "Vehicle.Powertrain.TractionBattery.Efficiency",
       "Pump.Duty": "Vehicle.Powertrain.TractionBattery.Cooling.PumpDuty",
       "Power.kW": "Vehicle.Powertrain.ElectricMotor.Power",
       "Chassis.Accel.Long": "Vehicle.Acceleration.Longitudinal", "Chassis.Accel.Lat": "Vehicle.Acceleration.Lateral",
       "Chassis.Accel.Vert": "Vehicle.Acceleration.Vertical", "Steering.Angle": "Vehicle.Chassis.SteeringWheel.Angle",
       "Suspension.Travel.FL": "Vehicle.Chassis.Axle.Row1.Wheel.Left.Suspension.Travel",
       "Suspension.Travel.FR": "Vehicle.Chassis.Axle.Row1.Wheel.Right.Suspension.Travel",
       "Suspension.Travel.RL": "Vehicle.Chassis.Axle.Row2.Wheel.Left.Suspension.Travel",
       "Suspension.Travel.RR": "Vehicle.Chassis.Axle.Row2.Wheel.Right.Suspension.Travel",
       "Wheel.Speed.FL": "Vehicle.Chassis.Axle.Row1.Wheel.Left.Speed", "Wheel.Speed.FR": "Vehicle.Chassis.Axle.Row1.Wheel.Right.Speed",
       "Wheel.Speed.RL": "Vehicle.Chassis.Axle.Row2.Wheel.Left.Speed", "Wheel.Speed.RR": "Vehicle.Chassis.Axle.Row2.Wheel.Right.Speed",
       "Lamp.Brake": "Vehicle.Body.Lights.Brake.IsActive", "Lamp.LowBeam": "Vehicle.Body.Lights.Beam.Low.IsOn",
       "Lamp.Hazard": "Vehicle.Body.Lights.Hazard.IsSignaling", "Gear": "Vehicle.Powertrain.Transmission.CurrentGear"}
WINDOWS = {"5m": 300, "1h": 3600}
WSIGNALS = ("Battery.Temp", "Coolant.Flow", "Battery.kmPerKwh", "Power.kW")


def _hav(a, b, c, d):
    p = math.pi / 180
    x = math.sin((c - a) * p / 2) ** 2 + math.cos(a * p) * math.cos(c * p) * math.sin((d - b) * p / 2) ** 2
    return 12742 * math.asin(math.sqrt(x))


def _stats(pts):
    vals = [v for _, v in pts]
    n = len(vals)
    if n == 0: return None
    mean = sum(vals) / n
    slope = 0.0
    if n >= 3:
        t0 = pts[0][0]
        xs = [t - t0 for t, _ in pts]; xm = sum(xs) / n
        den = sum((x - xm) ** 2 for x in xs)
        slope = sum((x - xm) * (v - mean) for x, v in zip(xs, vals)) / den if den else 0.0
    return {"n": n, "mean": round(mean, 3), "min": round(min(vals), 3), "max": round(max(vals), 3),
            "slope_per_s": round(slope, 5)}


class Enricher:
    def __init__(self):
        self.win = defaultdict(lambda: {s: deque() for s in WSIGNALS})
        self.weather, self.grid, self.last = {}, {}, {}
        self.fleet = {"summaries": 0, "escalations": 0, "security": 0}
        self.recent_esc = deque(maxlen=500)          # (ts, vid, cause) for fleet context

    def _windows(self, vid, ts):
        out = {}
        for sig in WSIGNALS:
            dq = self.win[vid][sig]
            while dq and ts - dq[0][0] > WINDOWS["1h"]: dq.popleft()
            out[sig] = {w: _stats([p for p in dq if ts - p[0] <= secs]) for w, secs in WINDOWS.items()}
        return out

    def _nearest(self, lat, lon):
        best = None
        for s in self.grid.get("stations", []):
            d = _hav(lat, lon, s["lat"], s["lon"])
            if best is None or d < best["distance_km"]:
                best = {"id": s["id"], "online": s["online"], "distance_km": round(d, 2)}
        return best

    def context(self, vid=None):
        c = {"weather": self.weather, "grid_outage": self.grid.get("outage", False),
             "stations_offline": sum(1 for s in self.grid.get("stations", []) if not s["online"]),
             "fleet": dict(self.fleet)}
        if vid and vid in self.last:
            c["position"] = self.last[vid]
            c["nearest_station"] = self._nearest(*self.last[vid]) if self.last[vid] else None
        return c

    def process(self, topic, m):
        if topic == "valid.feeds":
            self.weather, self.grid = m.get("weather", {}), m.get("grid", {})
            return [("cloud.fleet", {"kind": "feeds", "weather": self.weather, "grid": self.grid,
                                     "edge_counters": m.get("counters"), "ts": m.get("ts")}, None)]
        if topic == "valid.telemetry":
            return self._telemetry(m)
        return self._edge(m)

    def _telemetry(self, m):
        vid, ts, s = m["vehicle_id"], m["ts"], m["signals"]
        for sig in WSIGNALS:
            if sig in s: self.win[vid][sig].append((ts, s[sig]))
        pos = (m["lat"], m["lon"]) if "lat" in m else None
        if pos: self.last[vid] = pos
        e = {"kind": "vss", "vehicle_id": vid, "ts": ts, "position": pos, "edge": m.get("edge"),
             "vss": {VSS[k]: v for k, v in s.items() if k in VSS},
             "windows": self._windows(vid, ts), "ambient_c": self.weather.get("ambient_c"),
             "grid_outage": self.grid.get("outage", False), "nearest_station": self._nearest(*pos) if pos else None}
        return [("vss.enriched", e, vid)]

    def _edge(self, m):
        k, vid = m.get("kind"), m.get("vehicle_id")
        if k == "escalation" or k == "security":
            self.fleet["security" if k == "security" else "escalations"] += 1
            self.recent_esc.append((m.get("ts", 0), vid, m.get("incident", {}).get("cause")))
            ctx = self.context(vid)
            same = sum(1 for _, v, c in self.recent_esc if c == m.get("incident", {}).get("cause") and v != vid)
            ctx["peers_same_cause"] = same
            out = {**m, "context": ctx}
            return [("cloud.security" if k == "security" else "cloud.escalations", out, vid)]
        if k == "summary":
            self.fleet["summaries"] += 1
            return [("cloud.fleet", {"kind": "summary", **m}, vid)]
        if k == "command_result":
            return [("cloud.ota", {"kind": "command_result", **m}, vid)]
        return []
