# Test report

This report covers the campaign in [test-plan.md](test-plan.md), run on 2026-10-02 against the
`docker/compose.yaml` stack. **No application code was changed.** The ad-hoc test clients (a
fault-tolerant k6 variant, a raw-socket slow listener and a Kafka blaster) were kept outside the
repository.

Clients check correctness on every message: a gap, a duplicate or reordering counts as a failure.
At the end, each listener's last seq is compared with the session's `lastSeq` from the gateway.

## Summary

| Test | Correctness (gaps / dups / loss) | Verdict | Key observation |
|---|---|---|---|
| T1-1 worker SIGKILL | 0 / 0 / 0 | ⚠️ recovery too slow | One language stalls about 45 s (consumer session timeout), then about 40 s of backlog |
| T1-2 gateway SIGKILL | 0 / 0 / 0 | ✅ | 500 listeners failed over to the other gateway with replay |
| T1-3 Kafka restart | 0 / 0 / 0 | ⚠️ | Ingest blocks for up to 10 s or more; requests the client saw as failed were committed on the server |
| T1-4 Postgres restart | 0 / 0 / 0 | ⚠️ | Same ambiguous ingest outcome as T1-3 |
| T1-5 persister stop | 0 / 0 / 0 | ✅ | Live path unaffected; backlog persisted on restart |
| T2 slow listener | 0 / 0 / 0 | ✅ with notes | Only the stalled listener was cut; it resumed from Postgres in 0.6 s |
| T3a listener ramp | 0 / 0 / 0 up to 12k | knee at about 12k | Fan-out p99 crosses 200 ms at 12,000 listeners |
| T3b session count | 0 loss, after drain | ❌ throughput | 30+ sessions saturate the per-partition sequential worker |
| T3c concurrent ingest, one session | dense and unique | ✅ | 2,000 concurrent posts via both gateways → seq 1..2000 |
| T4 soak 60 min | see below | see below | |

## Correction to earlier results
`loadtest/fanout.js` maps speakers to sessions with `exec.vu.idInTest`. k6 does not assign VU ids
per scenario, so two speakers shared one session and the third session never received an
utterance: the database shows sessions with 0 / 60 / 120 utterances instead of 60 / 60 / 60.

As a result, in the runs reported in [load-test-results.md](load-test-results.md), about a third
of the listeners received no messages. The zero-loss result still holds for the listeners that
did receive traffic, but the effective listener counts were about 2/3 of the stated ones. The
repository script was left unchanged per the campaign's no-code rule. The campaign's own k6 variant
rotates sessions by iteration, and every run below checks `listeners_with_data == listeners`.

## T1 — Fault injection
Each run used 1,000 listeners (3 sessions × 3 languages, spread over both gateways) and 3
speakers at 0.5 utterances/s each. The fault was injected at t = 45 s and lifted 10 s later.
The baseline without a fault was 0 / 0 / 0 with fan-out p99 15 ms.

### T1-1 — worker-ko SIGKILL
- Clients saw 45,000 messages and 0 gaps, duplicates or missing.
- ko e2e latency p95 was 35 s and max 44.7 s. en and ja were unaffected.
- Timeline:

  | Time | Event |
  |---|---|
  | 20:38:11 | SIGKILL |
  | 20:38:21 | Restart command issued |
  | 20:38:25 | App started |
  | 20:38:55 | Partitions assigned, 44 s after the kill: the group waited for the dead members' `session.timeout.ms` (45 s, the default) to expire |
  | about 40 s later | Backlog drained at about 2.2 msg/s per partition |

- The kill happened during the mock delay, before the send, so there was no redelivery and the
  gateway's duplicate counter stayed at 0. Duplicates need a kill between the produce ack and the
  offset commit, a window of a few milliseconds.
- **Finding:** the time to recover from a worker crash is bounded by the consumer session timeout,
  not by restart speed. Config-level remedies exist (static membership via `group.instance.id`, a
  lower session timeout, a standby replica) but were not applied.

### T1-2 — gateway-1 SIGKILL
- 500 unexpected closes, which is every listener on gateway-1. They reconnected to gateway-2 with
  `lastSeq` and were served from its ring buffer.
- 0 gaps, duplicates or missing. Fan-out max 703 ms (the replayed messages); p99 17 ms.
- 12 speaker attempts hit the dead gateway and were retried on the other one, so all 135
  utterances were accepted.

### T1-3 — Kafka broker stop/start (10 s)
- 0 gaps, duplicates or missing; e2e max 17.9 s while the broker was down.
- Speaker `POST /utterances` p95 was 730 ms and p99 hit the 10 s client timeout.
- **Finding: ambiguous ingest outcome.** k6 recorded 114 accepted and 3 timed-out requests, yet
  the database shows `last_seq` = 39 × 3 = 117. All 3 "failed" requests were committed and
  delivered after the client gave up.
  - Cause: `KafkaTemplate.send()` blocks on metadata for up to `max.block.ms` (60 s by default)
    *before* the bounded `.get(5 s)` starts. Meanwhile the transaction holds the session row lock
    and a DB connection.
  - A real speaker would retry, and the same text would get a second seq. Ingest has no idempotency
    key.

### T1-4 — Postgres stop/start (10 s)
- 0 gaps, duplicates or missing. Fan-out of messages already in flight continued, with p99 13 ms.
- The same ambiguity as T1-3: 120 accepted + 3 timed out in k6 against `last_seq` = 41 × 3 = 123.
  The requests waited on Hikari's `connectionTimeout` (30 s by default) instead of failing fast,
  and completed after Postgres came back.

### T1-5 — persister stop/start
- Live delivery was unaffected: 0 / 0 / 0, fan-out p99 24 ms.
- 405 rows were expected (135 utterances × 3 languages) and 405 were present after restart.

## T2 — Slow listener over a real socket
Setup:
- `translated.ko` was fed directly at 200 msg/s with 1 KB messages, 12,000 in total. That is
  about 100× the per-channel rate the worker can produce.
- Two normal listeners, one on each gateway.
- One raw-socket listener with a 4 KB receive buffer that stopped reading for 45 s.

| Listener | Result |
|---|---|
| normal, gateway-1 | 12,000 / 12,000 in order; fan-out p50 38 ms, p99 372 ms |
| normal, gateway-2 | 12,000 / 12,000 in order; fan-out p50 38 ms, p99 375 ms |
| stalled | 1..2,234 in order, then cut (`slow consumer` counter +1) |
| stalled, resumed with `lastSeq=2234` | 2,235..12,000 (9,766 messages) in order, in 565 ms, from Postgres |

Notes:
- **The close frame never reached the stalled client.** The socket's send path was full, so the
  client saw a plain EOF without code 4000 and cannot tell a slow-consumer cut from a network drop.
  It still recovers correctly by resuming with `lastSeq`.
- **The replay metric double-counts.** When replay comes from Postgres,
  `interp_replay_messages_total{source="buffer"}` still adds the ring snapshot size (+1,000 here),
  although those rows are merged into the DB result.
- **A burst beyond one connection's send rate also cuts healthy clients.** At an unthrottled
  burst of about 4,600 msg/s × 1 KB, even the two normal Node clients were cut with 4000 after
  2,964 and 6,717 messages. One sender thread per connection writes roughly 1.5–3k msgs/s here. This
  is far beyond the design rate (about 2 msg/s per channel), but it is a hard limit.

## T3 — Limits

### T3a — Listener ramp
3 sessions with 3 speakers; listeners ramp in over 30 s, then 60 s of speech.

| Listeners | Msgs delivered | Gaps / dups / missing | Fan-out p50 / p95 / p99 / max | Ingest p95 |
|---:|---:|---|---|---:|
| 4,000 | 120,000 | 0 / 0 / 0 | 16 / 20 / 23 / 50 ms | 14.8 ms |
| 8,000 | 240,000 | 0 / 0 / 0 | 26 / 55 / 163 / 233 ms | 13.3 ms |
| 12,000 | 360,000 | 0 / 0 / 0 | 38 / 83 / **217** / 379 ms | 14.0 ms |

- The SLO (fan-out p99 < 200 ms) is crossed at about **12,000 listeners**, which is about 6,000
  per gateway.
- Gateway heap at 12k was 1.4–1.7 GiB. k6 itself used up to 3.9 GiB and shared the 4 cores, so the
  knee is a lower bound for the gateways.
- Each utterance fans out to about 1,333 sockets per (session, lang) at 12k, all in one burst.

### T3b — Session count (worker saturation)
1,000 listeners; every session gets 1 utterance every 2 s.

| Sessions | Offered per language | e2e p50 / p99 / max | Undelivered at test end |
|---:|---:|---|---:|
| 3 | 1.5 msg/s | 0.5 s / 1.5 s / 1.6 s | 0 |
| 30 | 15 msg/s | 4.7 s / 24.8 s / 26.6 s | 5,040 (backlog) |
| 100 | 50 msg/s | 46 s / 92 s / 97 s | 16,935 (backlog) |

- Capacity per language is 6 partitions × 1 / (mean 450 ms) ≈ **13 msg/s**. Above that, latency
  grows without bound.
- **Nothing was lost.** After the speakers stopped, the backlog drained in about 6 minutes. The
  database then held exactly the expected 11,700 rows for the 130 sessions involved, with 0
  incomplete (session, lang) channels.
- The limit comes from the design choice of processing each partition sequentially to keep order.
  More partitions or per-key concurrency would raise it.

### T3c — Concurrent ingest into one session
2,000 `POST /utterances` to one session, 100 in parallel, alternating between the two gateways:
- All returned 202; the seqs were exactly 1..2000, unique and dense; `last_seq` = 2000.
- Throughput was about 126 ingests/s per session. Row-lock serialization made latency p50 786 ms
  and p99 1.37 s at this contention level. A single speaker per session never sees this.

## T4 — Soak (60 min)
Setup, 21:17–22:19:
- 3,000 listeners on 3 sessions with 3 speakers at 0.5 msg/s each.
- In parallel, a churn loop created 1 new session per second (3,494 in total), each with 3
  utterances. That left about 10.5k extra (session, lang) channels in each gateway's memory.

| Metric | Result |
|---|---|
| Messages delivered | 5,373,000 (3,000 listeners × 1,791 utterances) |
| Gaps / duplicates / missing | 0 / 0 / 0; all 3,000 listeners received data |
| Fan-out latency | p50 14 ms, p95 20 ms, p99 28 ms, max 124 ms |
| e2e latency | p50 567 ms, p99 2.38 s (higher than at baseline because churn sessions share worker partitions) |
| Speaker ingest | p95 13.4 ms, p99 15.7 ms |
| e2e p99 per 10-minute window | 2.1 → 2.2 → 2.3 → 2.1 → 2.4 → 2.4 s (no upward trend beyond noise) |
| Live data after GC, per gateway | 225–378 MiB at start, settling at 288–306 MiB; flat |
| Platform threads | 37–38 throughout (listener senders are virtual threads) |
| Gateway CPU | about 0.5 % average |

Conclusion:
- No leak or drift is visible at this scale.
- The unbounded channel map is real but costs little here: churn channels hold 3 messages each,
  so 10.5k of them do not show above GC noise.
- The risk grows with sessions × ring size. A long session can hold up to 1,000 messages × about
  200 B, roughly 200 KB per channel, and channels are never evicted.

## Findings, ranked
1. **Ambiguous ingest outcome under broker or DB outage (T1-3, T1-4).** Server-side waits
   (`max.block.ms` 60 s, Hikari 30 s) exceed client timeouts, so a request the client believes
   failed can still commit. With no idempotency key, retries would duplicate utterances under new
   seqs.
2. **Worker crash recovery is bound to the consumer session timeout (T1-1).** One language stalls
   about 45 s, plus backlog.
3. **Worker throughput is about 13 msg/s per language (T3b).** Sequential per-partition
   processing caps capacity at about 26 sessions speaking every 2 s. Above that, latency grows
   without bound; there is no loss.
4. **The load-test script shares sessions between speakers.** Earlier published listener counts
   were effectively about 2/3 of the stated numbers (see *Correction*).
5. **Slow-consumer close code is not observable by the client** when its socket is full (T2).
6. **The replay metric double-counts** ring entries on the DB path (T2).
7. **The fan-out knee is at about 12k listeners** on this 4-core box with k6 co-located (T3a).
   No correctness degradation was seen up to that point.

Correctness held in every scenario: across about 6.5M client-checked messages, there was no
gap, no duplicate, no reorder, and no loss after drain.

## Fixes applied after the campaign
Two findings were fixed in code afterwards; [decisions.md](decisions.md) has the design.

| Finding | Fix | Verified by |
|---|---|---|
| 1. Ambiguous ingest outcome | Idempotency key `utteranceId` (Flyway V2 `utterance` table); bounded producer/Hikari waits; 503 on Kafka/DB unavailability | `IngestIdempotencyIT`, `SessionControllerTest`, T1-3/T1-4 re-run below |
| 3. Worker cap of about 13 msg/s per language | Batches split by session; sessions translated in parallel, each in order | `ParallelTranslationIT`, T3b re-run below |
| 4. k6 speaker→session mapping | Rotate sessions by iteration; threshold `listeners_with_data == LISTENERS`; retries reuse `utteranceId` | Used in the re-runs below |
| 6. Replay metric double-counts | On the DB path, `source="buffer"` counts only ring entries the DB result did not have | `ChannelHubTest` |
| New (code review): listener leak when the replay read fails | The listener is unsubscribed again if building its replay throws | `ChannelHubTest` |

### T1-3 / T1-4 re-run with idempotent retries
Speakers retry a failed post with the **same** `utteranceId` (up to 8 attempts, 1 s apart).

| Run | Accepted utterances | Sum of `last_seq` | `utterance` rows | 503s | Ingest max | Gaps / dups / missing |
|---|---:|---:|---:|---:|---:|---|
| T1-3 before (Kafka stop) | 114 (+3 "failed") | 117 | — | 0 | 10 s (client timeout) | 0 / 0 / 0 |
| T1-3 after | 114 | **114** | 114 | 9 | 4.0 s | 0 / 0 / 0 |
| T1-4 before (Postgres stop) | 120 (+3 "failed") | 123 | — | 0 | 10 s (client timeout) | 0 / 0 / 0 |
| T1-4 after | 120 | **120** | 120 | 6 | 3.0 s | 0 / 0 / 0 |

- After the fix, the count of seqs issued equals the count of accepted utterances, so no stray
  seqs remain. Outages surface as fast 503s, which the client retries safely.
- Neither re-run hit an attempt that had committed before the client gave up
  (`retry_was_committed` = 0). That path is covered by `IngestIdempotencyIT` instead.
- **Ingest p95 still breaks its 100 ms SLO while an outage lasts** (p95 2–3 s), as expected: the
  503s take up to the 3 s bounds.
- New observation in the T1-3 re-run: fan-out p99 was 2.1 s (max 2.6 s), against 16 ms in the earlier
  run. Translations produced while the broker was coming back carry a `translatedAtMs` from before
  their delayed send, so the metric includes the broker outage. The code confirms this:
  `TranslationWorker` stamps `translatedAtMs` before `kafka.send()`, so the wait in the worker's
  producer is counted as fan-out. It is a property of the metric, not a gateway slowdown; the
  field's KDoc now says so.

### T3b re-run after the worker fix (2026-10-03)
Same shape as T3b: 1,000 listeners, every session gets 1 utterance every 2 s for 120 s, using the
repository's `loadtest/fanout.js` (`SESSIONS=3/30/100`, 15 s drain).

| Sessions | Offered per language | e2e p50 / p99 / max before | e2e p50 / p99 / max after | Undelivered at test end | Fan-out p99 |
|---:|---:|---|---|---:|---:|
| 3 | 1.5 msg/s | 0.5 s / 1.5 s / 1.6 s | 0.50 s / 1.35 s / 1.51 s | 0 | 20 ms |
| 30 | 15 msg/s | 4.7 s / 24.8 s / 26.6 s | 0.97 s / 1.97 s / 2.26 s | 0 (was 5,040) | 36 ms |
| 100 | 50 msg/s | 46 s / 92 s / 97 s | 1.20 s / 2.09 s / 2.35 s | 0 (was 16,935) | 10 ms |

- Every run passed all thresholds: 0 gaps, duplicates, missing or unexpected closes, and all 1,000
  listeners received data (60,000 messages per run). Speaker ingest p95 stayed at 15–27 ms.
- The database held exactly 7,980 utterances × 3 languages = 23,940 rows for the 133 sessions,
  with 0 incomplete (session, lang) channels.
- Worker throughput now follows the offered load (15 and then 50 translations/s per language on the
  dashboard) instead of flattening at about 13 msg/s.
- The median rises with load (0.5 s → 1.2 s), most likely because the next poll waits until the
  slowest session of the current batch finishes, so newly arrived records queue behind it. With more
  sessions per batch, that slowest delay is more often near the 800 ms maximum. The latency stays
  bounded, which the old design did not achieve above about 26 sessions.
- Not measured: the new ceiling. Latency was still flat at 100 sessions, so it lies higher, but
  this run did not look for it.
