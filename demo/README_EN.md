[한국어](README.md) / [English](README_EN.md)

# Fault Injection Demo

Scripts that actually verify the relay loses no events under failure. They run
five scenarios and each time check that "rows inserted == unique `eventId`
values that arrived in Kafka".

## Running

```bash
docker compose -f demo/docker-compose.yml up -d
docker compose -f demo/docker-compose.yml exec -T postgres \
  psql -U docrelay -d docrelay < demo/seed.sql

./gradlew bootRun --args='--spring.profiles.active=demo'   # separate terminal

chmod +x demo/run.sh demo/verify.sh
./demo/run.sh
```

While it runs, metrics are at `http://127.0.0.1:9090/actuator/prometheus` and the
`DEAD` list at `http://127.0.0.1:9090/actuator/outbox/dead`.

Clean up with `docker compose -f demo/docker-compose.yml down`. This is a
standalone stack, not Testcontainers, so it does not disappear on its own when
the run finishes.

## Scenarios

| # | Scenario | What it shows | Automated |
|:-:|---|---|:-:|
| 1 | Normal upload | Arrival in Kafka within a second through the LISTEN path | Yes |
| 2 | `kill -9` the relay mid-publish | Zombie reclaim | No |
| 3 | Stop Kafka and keep uploading | A PENDING backlog recovering in batches after restart | Yes |
| 4 | Extended Kafka outage | Reaching `DEAD` through backoff exhaustion, then automatic recovery | Yes |
| 5 | Sever the LISTEN connection | Continued operation on the polling safety net alone | Yes |

Scenario 2 includes an interactive step (`read -r`) where a person must restart
the relay process. It assumes a presenter is driving it and is not a target for
unattended automation.

Scenario 4 also prints `EXPLAIN ANALYZE` for the `DEAD` query. There is no
partial index matching `DEAD`, so a Seq Scan appears; measuring its cost is the
evidence used to request an index from the partner repository.

## How verification works

Each scenario is verified with `demo/verify.sh <expected count>`.

```bash
./demo/verify.sh 1
# → expected 1 / unique arrived 1, lost 0
```

It extracts `eventId` from the messages that arrived in Kafka and counts the
**unique** values. Duplicate arrivals are normal under the at-least-once
contract, so only a unique count — not a total count — can tell you whether
anything was lost.

## Running scenario 1 unattended

```bash
docker compose -f demo/docker-compose.yml up -d
docker compose -f demo/docker-compose.yml exec -T postgres \
  psql -U docrelay -d docrelay < demo/seed.sql
./gradlew bootRun --args='--spring.profiles.active=demo' &
sleep 20
docker compose -f demo/docker-compose.yml exec -T postgres psql -U docrelay -d docrelay -c \
  "INSERT INTO document_version (document_id, version_no, source_object_key, original_filename, mime_type, file_size, content_hash, created_by_principal_id)
   VALUES (1, 1, 'demo/v1.pdf', '\x00'::bytea, 'application/pdf', 1024, 'sha256:demo', 'USER:1');"
sleep 3
./demo/verify.sh 1
```

## Verification status

| Scenario | Status |
|---|---|
| 1 | Verified end-to-end against the real `docker compose` stack |
| 2 | Written as a script but not verified by execution, because it is interactive |
| 3 · 4 · 5 | Verified end-to-end against the real `docker compose` stack |

Scenarios 3, 4, and 5 were reproduced by skipping 1 and 2 and renumbering the
document versions (3 = v1–5, 4 = v6, 5 = v7) rather than running `run.sh`
straight through, using the same logic and the same timings
(`application-demo.yaml`). In all three, `verify.sh` confirmed "expected ==
unique arrived", reconfirming zero loss.

## Things to know

- `demo/seed.sql` creates the `outbox_event` schema on behalf of this stack,
  which has no API server. **Do not carry it into the integrated stack.** The
  production schema is owned by the API server's migrations.
- The demo profile (`application-demo.yaml`) sets `spring.datasource.*` and
  `spring.kafka.bootstrap-servers` in addition to the `relay.*` timings. Unlike
  production and tests, this stack has neither a deployment environment nor
  Testcontainers' `@ServiceConnection` to supply those values.
- Scenario 2's `kill -9` finds the process with `pgrep -f 'doc-relay.*\.jar'`.
  `./gradlew bootRun` builds no separate jar, but the classpath holds enough
  `.jar` entries that the regex matches by coincidence. In practice both the
  Gradle wrapper client and the application JVM matched, and killing the wrapper
  picked by `head -1` made the Gradle daemon detect the closed connection and
  bring the application JVM down with it. The desired effect happened, but the
  path depends on coincidence — where process identification must be certain,
  build a jar with `./gradlew bootJar` and run
  `java -jar build/libs/*.jar --spring.profiles.active=demo` instead.
