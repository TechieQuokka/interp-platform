# Load test results

Run on 2026-10-02 with `loadtest/fanout.js` against the full `docker/compose.yaml` stack
(2 gateways, 3 workers, persister, Kafka, Postgres). The stack and k6 shared one machine:
Intel i3-14100F (4 cores / 8 threads), 23 GiB RAM.

## Scenario
- 3 sessions; each speaker sends 1 utterance every 2 s for 120 s (180 utterances in total).
- N listeners, spread evenly over both gateways, all 3 languages and all sessions. They connect
  during a 30 s ramp with `lastSeq=0`, so a late joiner first gets a replay from the start.
- Every 5th listener disconnects every 5–20 s and resumes with `lastSeq`, which exercises replay
  under load.
- Each listener checks every message: `seq == last + 1`, otherwise it counts a gap or a duplicate.
  At the end it compares its last seq with the session's `lastSeq`, and any difference counts as missing.

## Results

| Listeners | Messages delivered | Reconnects | Gaps | Duplicates | Missing at end | Unexpected closes |
|---:|---:|---:|---:|---:|---:|---:|
| 3,000 | 180,060 | 6,872 | 0 | 0 | 0 | 0 |
| 6,000 | 360,060 | 13,828 | 0 | 0 | 0 | 0 |

Latency, measured on the client:

| Listeners | Metric | p50 | p95 | p99 | max |
|---:|---|---:|---:|---:|---:|
| 3,000 | end-to-end (speaker ingest → client) | 576 ms | 1.24 s | 1.44 s | 1.45 s |
| 3,000 | fan-out (translated → client) | 19 ms | 29 ms | 44 ms | 75 ms |
| 6,000 | end-to-end | 627 ms | 1.32 s | 1.49 s | 1.56 s |
| 6,000 | fan-out | 24 ms | 44 ms | 120 ms | 342 ms |
| 6,000 | speaker `POST /utterances` | 12.5 ms | 17.9 ms | 20.3 ms | 20.5 ms |

End-to-end latency is dominated by the mock translation delay (uniform 100–800 ms, processed
sequentially per partition). The fan-out row isolates what the gateway adds.

## Findings
- **Ordering and loss hold under churn.** About 14k reconnect-with-replay cycles produced no gap,
  duplicate or reordering.
- **The DB pool, not threads, is the bottleneck.** At the end of the run, all 6,000 listeners call
  `GET /sessions/{id}` at once, and that request took about 0.8 s (p50). Prometheus showed
  `hikaricp_connections_pending` peaking at 533 on one gateway, with all 10 connections active.
  With virtual threads every request gets a thread immediately, so the bounded resource just moves to
  the 10-connection Hikari pool. The speaker path does not hit this because it runs at about 1.5 req/s.
- **Per-partition sequential translation caps throughput per session** at about 1 / mean delay ≈ 2.2 msg/s.
  Sessions that hash to the same partition share that budget. This is why the test uses 3 sessions
  at 0.5 msg/s each.
