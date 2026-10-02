# Decisions

## Stack
- **Spring MVC + virtual threads, not WebFlux.** Blocking code that reads top to bottom, while
  still handling thousands of connections. The load test bears this out: 6,000 WebSocket listeners
  run on 2 gateways.
- **JPA where it fits, JDBC where it does not.** JPA is used for session creation and replay reads.
  The seq counter is a single `UPDATE ... RETURNING` through `JdbcClient`, because JPA would need
  read-lock-write (2 round trips) for the same thing. `SessionEntity` implements `Persistable` so
  that `save()` with an assigned UUID does a plain INSERT instead of SELECT + INSERT.
- **No Redis.** Kafka carries the stream, Postgres holds history, and the replay window lives in
  gateway memory.

## Pipeline
- **Topics** are `source-text` → `translated.{ko,en,ja}`, keyed by sessionId, with 6 partitions.
  A key always maps to one partition, so per-session order comes from Kafka and needs no extra
  machinery.
- **Ingest is idempotent on a client-supplied `utteranceId`** (added after the fault tests).
  - The fault tests showed that a request the client gave up on could still commit, and a retry
    would then store the same utterance twice under a new seq.
  - Now the seq is allocated first, which takes the session row lock and serializes concurrent
    retries. If `(session_id, utterance_id)` already exists, the transaction rolls back (returning
    the new seq) and the original seq comes back with `duplicate: true`. Nothing is published again.
  - Server waits are bounded so clients get a fast 503 instead of hanging: Kafka `max.block.ms`
    3 s, `delivery.timeout.ms` 4 s, Hikari `connection-timeout` 3 s.
  - Residual risk, not addressed: if the DB commit fails *after* Kafka acked, a published seq is
    reused by the next utterance. Closing that needs a transactional outbox, which was judged out of
    scope.
- **Seq comes from a Postgres counter.** It is allocated and published in one transaction. If the
  Kafka send fails, the increment rolls back, so a failed ingest leaves no gap. The row lock
  serializes concurrent ingests for the same session, and any gateway can accept the speaker.
  - Remaining risk: the commit could fail after Kafka has acked. The next utterance would then reuse
    that seq, and the gateway would drop it as a duplicate. This was accepted as very unlikely and
    is documented rather than engineered away.
- **Delivery is at-least-once, with dedupe by seq.** The worker commits a batch's offsets only
  after all of its output is acked (`ack-mode: batch`). The gateway keeps a per-(session, lang) watermark, drops
  `seq <= last`, and counts `seq > last + 1` as a gap. The persister uses `ON CONFLICT DO NOTHING`.
- **Each gateway uses its own consumer group** (`gateway-{instanceId}`) that starts at `latest`.
  Every instance must see every message, because any listener can be connected anywhere. History
  older than the instance's start is served by the replay path.
- **Topics are declared by every app, and broker auto-create is disabled.** See mistake 5 below.
- **Flyway runs in both gateway and persister.** The plan had only the gateway running it. Flyway's
  lock makes concurrent runs safe, and this removes a startup-order dependency.

## Worker parallelism
- **Batches are split by session, and sessions run in parallel; each session stays sequential.**
  The fault tests showed that processing each partition one record at a time capped a language
  at about 13 msg/s (6 partitions / 450 ms).
  - Now a poll's batch is grouped by key, each session gets a virtual thread, and the records of
    one session are still translated and sent in offset order.
  - The batch commits only after every send is acked. Any failure redelivers the whole batch;
    the resulting duplicates are dropped by the gateway.
  - Throughput now scales with the number of concurrently speaking sessions instead of the
    partition count. The slowest session in a batch bounds that batch's time.
  - Chosen over raising the partition count, which does not fix the root cause and reshuffles
    key→partition on a live topic. Also chosen over the Confluent Parallel Consumer, an extra
    non-Spring dependency.
  - Verified by `ParallelTranslationIT`: 4 sessions × 5 records forced into one partition finish
    well under the 6 s that sequential processing needs, each session in order. Not yet re-measured
    under load (T3b).

## Fan-out
- **Raw WebSocket, no STOMP.** The protocol is one-way and tiny:
  `/ws/listen?session&lang&lastSeq` → JSON `{seq, lang, text, ingestedAtMs, translatedAtMs}`.
- **Each listener has a bounded queue (default 256) and one virtual-thread sender.** The Kafka
  consumer thread never blocks on a socket. When a listener's queue is full, that listener is
  closed with code 4000 ("slow consumer"). The single sender per connection also satisfies
  `WebSocketSession`'s single-writer rule.
- **Replay on reconnect works like this:**
  1. Subscribe under the channel lock and take a snapshot of the ring buffer (1,000 messages per
     session+lang).
  2. If the snapshot does not reach back to `lastSeq + 1`, read the rest from Postgres.
  3. The sender skips any `seq <= lastSent`, so where replay and live traffic overlap, nothing is
     sent twice.
- **Known limits, accepted for the scope:**
  - Channels are never evicted from memory.
  - When a fresh gateway replays from the DB, the persister's lag can leave a short gap. The client
    sees it and reconnects.

## Mistakes made by the LLM (Claude) during the build
1. **Scaffolded into the category folder `prototype/`** instead of the project folder, and started
   work without explicit approval. Led to the working rules: project-folder boundary and an explicit
   승인 per step.
2. **Put the Gradle user home inside the project** (`.gradle-home`, 1.1 GB per project). Reverted
   to the global `~/.gradle` on review.
3. **Deleted an unrelated `.gitignore` line** (`.kotlin`) with a line-number `sed`. Noticed in the
   output and restored.
4. **Relied on stale versions from memory** (Kotlin 2.3.21, Gradle 9.7.1). Re-checking the
   registries found Kotlin 2.4.20 and Gradle 9.8.0.
5. **Topic auto-creation race.** On the first stack start, the workers subscribed before the
   gateway created the topics. The broker auto-created a 1-partition `source-text`, the gateway
   then expanded it to 6 partitions, and the workers kept consuming only partition 0 until a
   metadata refresh. The smoke test's session hashed to partition 1, so nothing was delivered. Fixed
   by declaring the topics in every app and disabling auto-create.
6. **Test consumer pointed at the wrong Kafka.** It was built from `KafkaProperties`, which ignores
   Testcontainers `@ServiceConnection` and kept `localhost:9094`, so it read the dev compose stack.
   Fixed by using the app's `ConsumerFactory`.
7. **The slow-consumer test also overflowed the healthy listener.** It published 10 messages into
   2-slot queues at once. The test was fixed to publish at the healthy listener's pace.
8. **`k6 --summary-export` wrote nothing.** The k6 image runs as a non-root user that cannot write
   to the mounted directory. The numbers were recorded from the console summary instead.
9. **The load-test script mapped speakers to sessions with `exec.vu.idInTest`.** k6 does not
   number VUs per scenario, so one session got no utterances and another got double. The first
   published listener counts were therefore overstated by about a third. Caught during the test
   campaign by checking `last_seq` per session in the DB, then fixed and documented.
10. **Stated the wrong finish time for the soak test** (21:20 instead of about 22:20). Corrected
   when asked.
