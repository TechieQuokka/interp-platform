# Test plan

The build is already covered by unit and integration tests, and by one happy-path load test
(6,000 listeners, see [load-test-results.md](load-test-results.md)). This campaign checks
behavior under faults and at the limits. **No application code is changed**; findings are
recorded in [test-report.md](test-report.md).

## Pass criteria (all tests)
- Seen by clients: 0 missing, 0 duplicates, 0 out-of-order.
- Fan-out latency (translated → client): p99 < 200 ms.
- Speaker ingest (`POST /utterances`): p95 < 100 ms.
- Under fault: normal service resumes within 30 s after the fault ends.

## T1 — Fault injection
Runs during a load run of about 1,000 listeners. Listeners resume with `lastSeq`, and switch to
the other gateway when theirs fails.

| # | Fault | Expected |
|---|---|---|
| 1-1 | `docker kill` worker-ko mid-stream, then start it again | Redelivery shows up as duplicates that the gateway drops; clients see 0 missing / 0 duplicates |
| 1-2 | `docker kill` gateway-1 while it has listeners | Listeners fail over to gateway-2 with `lastSeq` and get replay; no loss |
| 1-3 | Restart the Kafka broker | Ingest fails during the outage and the seq rolls back, so there are no gaps; the flow recovers |
| 1-4 | Restart Postgres | Ingest fails during the outage; fan-out of in-flight messages continues; the flow recovers |
| 1-5 | Stop the persister, then start it again | The live path is unaffected; the backlog is persisted after restart |

## T2 — Slow listener over a real socket
A raw client that stops reading. Expected: it is closed with code 4000, it resumes with `lastSeq`
and gets no gaps, and the latency of the other listeners is unaffected.

## T3 — Limits
- Listener ramp (2k → 4k → 8k → 12k): find where fan-out p99 crosses 200 ms. Record CPU and heap.
- Session count (3 → 30 → 100): find where per-partition sequential translation saturates.
- One session, both gateways ingesting concurrently: seq stays dense and unique.

## T4 — Soak
3,000 listeners for 60 min. Watch heap, threads and latency drift; in-memory channels are never
evicted.

## Environment
One machine, an i3-14100F (4 cores / 8 threads) with 23 GiB RAM, shared by k6 and the whole
compose stack. The numbers are relative to this box.
