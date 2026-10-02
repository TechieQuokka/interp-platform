# interp — real-time interpretation fan-out pipeline

Speaker text → translation workers → thousands of multilingual listeners, built to verify
**latency, ordering and no-loss**. Translation itself is mocked; the pipeline is the point.

```
speaker ──POST──▶ gateway ──▶ Kafka source-text ──▶ worker-{ko,en,ja} ──▶ Kafka translated.{lang}
          (seq from                                   (mock, 100–800 ms)          │
          Postgres)                                                               ├──▶ persister ──▶ Postgres
                                                                                  │
listeners ◀──WebSocket── gateway-1 / gateway-2 ◀── own consumer group each ◀──────┘
                         (dedupe by seq, bounded queue per listener,
                          ring buffer + Postgres replay on reconnect)
```

## Results (6,000 listeners, 2 gateways, one 4-core machine)
- 360,060 messages and 13,828 reconnect-with-replay cycles: **0 gaps, 0 duplicates, 0 missing**.
- Gateway fan-out latency (translated → client): p50 24 ms / p95 44 ms / p99 120 ms.
- Details and findings are in [docs/load-test-results.md](docs/load-test-results.md).

## Modules
| Module | Role |
|---|---|
| `common` | Event contracts, JSON codec, topic declarations, Flyway schema, shared Testcontainers fixtures |
| `gateway` | Speaker REST, seq allocation, Kafka produce, fan-out consumer, WebSocket listeners, replay |
| `worker` | Mock translator, one consumer group per language |
| `persister` | Idempotent translation history writer |

Stack: Kotlin 2.4.20, Spring Boot 4.1.1 (MVC + virtual threads), Java 25, Kafka 4.3.1,
Postgres 18.6, Prometheus, Grafana, k6. Design decisions and the build's mistake log are in
[docs/decisions.md](docs/decisions.md).

## Run
```bash
./gradlew build                      # unit + Testcontainers integration tests (needs Docker)
./gradlew bootJar
docker compose -f docker/compose.yaml --profile apps up -d --build
```
- Gateways: http://localhost:8081, http://localhost:8082
- Grafana: http://localhost:3000 (dashboard "Interp pipeline"), Prometheus: http://localhost:9090
- Infra only (to run the apps from the IDE): `docker compose -f docker/compose.yaml up -d`

### API
```bash
curl -X POST localhost:8081/sessions                                   # {"sessionId": "..."}
curl -X POST localhost:8081/sessions/$SID/utterances \
     -H 'Content-Type: application/json' -d '{"text":"hello"}'         # {"seq": 1, ...}
# Listener (any gateway): ws://localhost:8082/ws/listen?session=$SID&lang=ko[&lastSeq=N]
```

### Load test
```bash
docker run --rm --network host -v "$PWD/loadtest:/scripts" grafana/k6:2.3.0 run \
  -e LISTENERS=3000 /scripts/fanout.js
```
