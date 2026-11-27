# quantara-stream

Repo 2 of 4 (ingest · **stream** · services · ops) for Configure 2026. Everything is synthetic.

One image, two Condense **Transform** deployments selected by `ROLE`:

| ROLE | consumes | produces |
|---|---|---|
| `validate` | `raw.telemetry`, `raw.edge`, `raw.feeds` | `valid.telemetry`, `valid.edge`, `valid.feeds`, `dlq`, `quarantine` |
| `enrich` | `valid.*` | `vss.enriched`, `cloud.escalations`, `cloud.security`, `cloud.fleet`, `cloud.ota` |

**validate**: schema + range checks, SANDBOX (`sim:true`) enforcement, bounded dedupe, dead-letter queue with reason,
quarantine of vehicles whose telemetry contradicts physics (battery far below ambient while delivering power) or that
raised a security incident (TTL-released). **enrich**: VSS signal naming, 5 min / 1 h sliding windows (n/mean/min/max/slope),
weather + grid + nearest-station context, fleet context (peers with the same cause) on escalations, routing to cloud topics.

Env: `TOPIC_PREFIX`, `KAFKA_BOOTSTRAP`, `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME`,
`KAFKA_SASL_PASSWORD`, `ROLE`. Consumer groups are `<prefix>cg-stream-<role>`. Test: `pip install -r requirements.txt pytest && pytest`.
MIT licensed. Built with AI assistance (Claude Code); see the ops repo for the full disclosure.
