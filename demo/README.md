[한국어](README.md) / [English](README_EN.md)

# 장애 주입 데모

릴레이가 장애 상황에서도 이벤트를 잃지 않는지 실제로 확인하는 스크립트입니다. 다섯 가지 시나리오를 돌리고 매번 "INSERT 건수 == 카프카에 도착한 고유 `eventId` 건수" 를 검증합니다.

## 실행

```bash
docker compose -f demo/docker-compose.yml up -d
docker compose -f demo/docker-compose.yml exec -T postgres \
  psql -U docrelay -d docrelay < demo/seed.sql

./gradlew bootRun --args='--spring.profiles.active=demo'   # 별도 터미널

chmod +x demo/run.sh demo/verify.sh
./demo/run.sh
```

돌아가는 동안 메트릭은 `http://127.0.0.1:9090/actuator/prometheus`, `DEAD` 목록은 `http://127.0.0.1:9090/actuator/outbox/dead` 에서 봅니다.

정리는 `docker compose -f demo/docker-compose.yml down` 으로 합니다. 이 스택은 Testcontainers 가 아니라 독립 실행 스택이라 테스트가 끝나도 저절로 사라지지 않습니다.

## 시나리오

| # | 시나리오 | 보여주는 것 | 자동 |
|:-:|---|---|:-:|
| 1 | 정상 업로드 | LISTEN 경로로 1초 안에 카프카 도착 | O |
| 2 | 발행 도중 릴레이 `kill -9` | 좀비 회수 | X |
| 3 | 카프카 중지 후 계속 업로드 | PENDING 적체가 재기동 후 배치로 회복 | O |
| 4 | 카프카 장기 중지 | 백오프 소진으로 `DEAD` 도달 후 자동 복구 | O |
| 5 | LISTEN 커넥션 강제 종료 | 알림이 끊겨도 폴링 안전망만으로 계속 동작 | O |

시나리오 2 는 릴레이 프로세스를 사람이 직접 다시 띄워야 하는 대화형 단계(`read -r`)를 포함합니다. 발표자가 실행하는 것을 전제로 하며 무인 자동화 대상이 아닙니다.

시나리오 4 는 `DEAD` 조회의 `EXPLAIN ANALYZE` 를 함께 출력합니다. `DEAD` 에 맞는 부분 인덱스가 없어 Seq Scan 이 걸리는데, 그 비용을 실측해 파트너에게 인덱스를 요청할 근거로 씁니다.

## 검증 방식

각 시나리오는 `demo/verify.sh <기대 건수>` 로 검증합니다.

```bash
./demo/verify.sh 1
# → 기대 1 / 고유 도착 1, 유실 0
```

카프카에 도착한 메시지에서 `eventId` 를 뽑아 **고유 개수**를 셉니다. 중복 도착은 at-least-once 계약상 정상이므로 총 건수가 아니라 고유 개수로 세야 유실 여부를 가릴 수 있습니다.

## 시나리오 1만 무인으로 돌리기

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

## 검증 현황

| 시나리오 | 상태 |
|---|---|
| 1 | 실제 `docker compose` 스택 대상으로 end-to-end 검증 완료 |
| 2 | 스크립트로 존재하나 대화형이라 실행 검증되지 않음 |
| 3 · 4 · 5 | 실제 `docker compose` 스택 대상으로 end-to-end 검증 완료 |

시나리오 3·4·5 는 `run.sh` 를 그대로 돌리는 대신 1·2 를 건너뛰고 문서 버전 번호를 다시 매겨(3 = v1~5, 4 = v6, 5 = v7) 같은 로직·같은 타이밍(`application-demo.yaml`)으로 재현했습니다. 세 시나리오 모두 `verify.sh` 가 "기대 == 고유 도착" 을 확인해 유실 0 을 재확인했습니다.

## 알아둘 것

- `demo/seed.sql` 은 API 서버가 없는 이 스택을 위해 `outbox_event` 스키마를 대신 만듭니다. **통합 스택에 가져가면 안 됩니다.** 운영 스키마는 API 서버의 마이그레이션이 소유합니다.
- 데모 프로파일(`application-demo.yaml`)은 `relay.*` 타이밍 외에 `spring.datasource.*` 와 `spring.kafka.bootstrap-servers` 도 함께 지정합니다. 운영·테스트와 달리 이 스택에는 그 값을 대신 채워 줄 배포 환경도 Testcontainers 의 `@ServiceConnection` 도 없기 때문입니다.
- 시나리오 2 의 `kill -9` 는 `pgrep -f 'doc-relay.*\.jar'` 로 프로세스를 찾습니다. `./gradlew bootRun` 은 별도 jar 를 만들지 않지만 클래스패스에 `.jar` 항목이 많아 정규식이 우연히 매치됩니다. 실측에서는 Gradle 래퍼 클라이언트와 애플리케이션 JVM 두 개가 모두 매치됐고, `head -1` 로 고른 래퍼를 죽이자 Gradle 데몬이 연결 종료를 감지해 애플리케이션 JVM 도 함께 내려갔습니다. 원하는 효과는 났지만 경로가 우연에 의존하므로, 프로세스 식별이 확실해야 하는 환경이라면 `./gradlew bootJar` 로 jar 를 만들어 `java -jar build/libs/*.jar --spring.profiles.active=demo` 로 띄우는 편이 안전합니다.
