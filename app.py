"""Condense transform entrypoint. ROLE=validate|enrich selects the stage (same image, two deployments)."""
import logging
import sys
import time

from quantara_core import config, topics
from quantara_core.bus import get_bus
from stream.enrich import Enricher
from stream.validate import Validator

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("stream")
ROLES = {"validate": (Validator, ["raw.telemetry", "raw.edge", "raw.feeds"], {
             "valid.telemetry": "valid.telemetry", "valid.edge": "valid.edge", "valid.feeds": "valid.feeds",
             "dlq": "dlq", "quarantine": "quarantine"}),
         "enrich": (Enricher, ["valid.telemetry", "valid.edge", "valid.feeds"], {
             "vss.enriched": "vss.enriched", "cloud.escalations": "cloud.escalations", "cloud.security": "cloud.security",
             "cloud.fleet": "cloud.fleet", "cloud.ota": "cloud.ota"})}


def main():
    role = config.env("ROLE", required=True)
    cls, ins, outs = ROLES[role]
    stage, bus = cls(), get_bus()
    grp, T = config.group(f"stream-{role}"), config.topic
    log.info("topics: %s", {k: v for k, v in topics.ensure_topics().items() if v != "exists"} or "all exist")
    log.info("starting role=%s prefix=%s", role, config.prefix())
    n, last = 0, time.time()
    while True:
        try:
            for topic, _, m in bus.poll(grp, [T(i) for i in ins], max_n=500, timeout=0.5):
                short = topic[len(T("")):]
                for out_t, out_m, key in stage.process(short, m):
                    bus.publish(T(outs[out_t]), out_m, key=key); n += 1
            if time.time() - last >= 30:
                last = time.time()
                log.info("out=%d stats=%s bus_ok=%s bus_failed=%s", n, getattr(stage, "stats", getattr(stage, "fleet", {})),
                         getattr(bus, "ok", "-"), getattr(bus, "failed", "-"))
        except Exception:
            log.exception("loop error (continuing)"); time.sleep(1)


if __name__ == "__main__":
    main()
