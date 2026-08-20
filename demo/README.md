# 장애 주입 데모

```bash
docker compose -f demo/docker-compose.yml up -d
docker compose -f demo/docker-compose.yml exec -T postgres \
  psql -U docrelay -d docrelay < demo/seed.sql

./gradlew bootRun --args='--spring.profiles.active=demo'   # 별도 터미널
chmod +x demo/run.sh demo/verify.sh
./demo/run.sh
```

메트릭은 `http://127.0.0.1:9090/actuator/prometheus`,
DEAD 목록은 `http://127.0.0.1:9090/actuator/outbox/dead` 에서 본다.

시나리오 4 의 `EXPLAIN ANALYZE` 출력은 spec 미해결 의존성 #5(`DEAD` 부분 인덱스)의
판단 근거다. Seq Scan 비용이 눈에 띄면 파트너에게 인덱스를 요청한다.

## 시나리오 1만 확인하고 싶을 때 (자동)

`run.sh` 의 시나리오 1만 떼어 무인으로 돌릴 수 있다:

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

Expected: `기대 1 / 고유 도착 1` + `유실 0`

## 검증 현황 (최종 전체 브랜치 리뷰 수정 웨이브, 2026-08-17)

Task 16 은 시나리오 1만 자동 실행해 검증했고 원장은 M5 를 무조건부로 "완료"로 기록했다
(시나리오 2는 대화형이라 원래도 자동화 대상이 아니다). 최종 전체 브랜치 리뷰가 이 격차를
지적해, 이 수정 웨이브에서 시나리오 3·4·5 를 실제 `docker compose` 스택(Testcontainers 아님)
대상으로 처음부터 다시 실행해 검증했다:

- **시나리오 1** — Task 16 에서 end-to-end 로 검증됨(이번 웨이브에서는 재실행하지 않음).
- **시나리오 2** — 대화형(사람이 릴레이를 직접 재기동)이라 여전히 무인 자동화 대상이 아니다.
  스크립트로는 작성돼 있으나 이번 웨이브에서도 실행 검증되지 않았다.
- **시나리오 3·4·5** — 이번 웨이브에서 실제로 실행해 검증됨. `run.sh` 를 그대로 돌리는 대신
  시나리오 1·2 를 건너뛰고 문서 버전 번호를 1부터 다시 매겨(시나리오 3 = v1~5, 시나리오 4 = v6,
  시나리오 5 = v7) 같은 로직·같은 타이밍(`application-demo.yaml`)으로 재현했다. 세 시나리오
  모두 `verify.sh` 가 "기대 == 고유 도착" 을 확인해 유실 0 을 재확인했고, 시나리오 4 는
  DEAD 도달 후 자동 복구까지, 시나리오 5 는 `EXPLAIN ANALYZE` 로 spec 미해결 의존성 #5(`DEAD`
  스캔이 Seq Scan) 도 함께 실측했다. 실행 후 `docker compose -f demo/docker-compose.yml down`
  으로 컨테이너를 정리하고 릴레이 프로세스도 종료했다 — 남은 컨테이너/프로세스 없음.

결론: 5개 시나리오 중 1·3·4·5 는 실제 end-to-end 실행으로 검증됐고, 2는 대화형 특성상
스크립트로만 존재하며 실행 검증되지 않았다 — "완료"를 무조건부로 선언하는 대신 이 상태
그대로를 기록으로 남긴다.

## run.sh 의 5개 시나리오

1. **정상 업로드** — LISTEN 경로로 ~1초 안에 카프카 도착을 확인한다. 완전 자동.
2. **발행 도중 릴레이 kill -9** — 좀비 회수를 보여준다. 릴레이 프로세스를 사람이
   직접 다시 띄워야 하는 대화형 단계(`read -r`)가 있다. 발표자가 직접 실행하는 것을
   전제로 한다 — 무인 자동화 대상이 아니다.
3. **카프카 중지 후 계속 업로드** — PENDING 적체가 카프카 재기동 후 배치로 회복됨을 보여준다.
4. **카프카 장기 중지** — 백오프 소진으로 DEAD 도달 후 자동 복구까지 보여준다. `DEAD` 조회의
   `EXPLAIN ANALYZE` 를 함께 출력해 부분 인덱스 필요 여부(spec 미해결 의존성 #5)를 실측한다.
5. **LISTEN 커넥션 강제 종료** — 알림 경로가 끊겨도 폴링 안전망만으로 계속 동작함을 보여준다.

각 시나리오는 `demo/verify.sh <기대 건수>` 로 "INSERT 건수 == 카프카에 도착한 고유 eventId
건수" 를 검증한다. at-least-once 계약상 중복 도착은 정상이므로 고유 개수로 센다.

## 알아둘 것

- `demo/docker-compose.yml` 은 Testcontainers 가 아니라 독립 실행 스택이다. 정리는
  `docker compose -f demo/docker-compose.yml down` 으로 한다.
- 데모 프로파일(`application-demo.yaml`)은 `relay.*` 타이밍 외에 `spring.datasource.*` /
  `spring.kafka.bootstrap-servers` 도 함께 지정한다 — 운영/테스트와 달리 이 스택에는
  이 값들을 대신 채워줄 배포 환경도 Testcontainers 의 `@ServiceConnection` 도 없기 때문이다.
- 시나리오 2의 `kill -9`는 `pgrep -f 'doc-relay.*\.jar'` 로 프로세스를 찾는다. `./gradlew
  bootRun` 은 별도 jar 를 만들지 않지만, 이 프로젝트 경로 자체가 `doc-relay.worktrees/...` 를
  포함하고 클래스패스 뒷부분에 `.jar` 항목들이 많아 정규식이 우연히 매치된다(경로에 있는
  `doc-relay` 뒤로 어딘가에 `.jar` 문자열만 있으면 매치되므로, "실제 doc-relay 애플리케이션
  jar" 를 찾는 것과는 다른 이야기다). 실측 결과 `pgrep -f 'doc-relay.*\.jar'` 는 Gradle
  래퍼 클라이언트 프로세스와 실제 애플리케이션 JVM 두 개 모두를 매치했고, `head -1` 로 고른
  래퍼 클라이언트 프로세스를 `kill -9` 하자 (이 환경에서는) Gradle 데몬이 클라이언트 연결
  종료를 감지해 `bootRun` 이 띄운 애플리케이션 JVM 도 함께 종료됐다 — 결과적으로 원하는
  효과(릴레이 프로세스 종료)는 났지만, 매치/종료 경로가 우연에 의존한다. 프로세스 식별이
  더 확실해야 하는 환경이라면 `./gradlew bootJar` 로 jar 를 만들어 `java -jar
  build/libs/*.jar --spring.profiles.active=demo` 로 띄우거나, `run.sh` 의 pgrep 패턴을
  그 세션에서 실제로 뜨는 프로세스 명령행에 맞게 바꾸는 편이 안전하다.
