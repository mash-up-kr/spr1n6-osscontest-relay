#!/usr/bin/env bash
# spec §10 장애 주입 데모 5종. 각 단계마다 유실 0 을 검증한다.
#
# 시나리오 2는 릴레이 프로세스를 사람이 직접 다시 띄우는 대화형 단계(read -r)를 포함한다.
# 그래서 이 스크립트는 무인 자동화가 아니라 발표 중 사람이 직접 실행하는 것을 전제로 한다.
set -euo pipefail

PSQL="docker compose -f demo/docker-compose.yml exec -T postgres psql -U docrelay -d docrelay -tAq"
COMPOSE="docker compose -f demo/docker-compose.yml"

upload() {   # upload <version_no>
  $PSQL -c "INSERT INTO document_version (
      document_id, version_no, source_object_key, original_filename,
      mime_type, file_size, content_hash, created_by_principal_id
    ) VALUES (1, $1, 'demo/v$1.pdf', '\\x00'::bytea, 'application/pdf', 1024, 'sha256:demo', 'USER:1');"
}

echo "=== 1. 정상 업로드: LISTEN 경로 즉시성 ==="
upload 1
sleep 2
demo/verify.sh 1

echo "=== 2. 발행 도중 릴레이 kill -9: 좀비 회수 ==="
upload 2
kill -9 "$(pgrep -f 'doc-relay.*\.jar' | head -1)" || true
echo "릴레이를 다시 띄운 뒤 Enter"; read -r
sleep 40    # zombie.lock-timeout 30s + scan-interval 5s
demo/verify.sh 2

echo "=== 3. 카프카 중지 후 계속 업로드: PENDING 적체 -> 배치 회복 ==="
$COMPOSE stop kafka
for v in 3 4 5 6 7; do upload $v; done
$PSQL -c "SELECT status, count(*) FROM outbox_event GROUP BY status;"
$COMPOSE start kafka
sleep 30
demo/verify.sh 7

echo "=== 4. 카프카 장기 중지: DEAD 도달 -> 자동 복구 ==="
$COMPOSE stop kafka
upload 8
sleep 60    # backoff 2s*2^n 5회 소진
$PSQL -c "SELECT status, publish_attempt_count FROM outbox_event WHERE document_version_id = 8;"
echo "--- DEAD 조회 쿼리 실측 (spec 미해결 의존성 #5) ---"
$PSQL -c "EXPLAIN ANALYZE SELECT count(*) FROM outbox_event WHERE status = 'DEAD' AND next_attempt_at <= now();"
$COMPOSE start kafka
sleep 60    # dead.recovery-delay 30s + scan-interval 10s
demo/verify.sh 8

echo "=== 5. LISTEN 커넥션 강제 종료: 폴링만으로 계속 동작 ==="
$PSQL -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
          WHERE application_name = 'doc-relay-listener';"
upload 9
sleep 15    # 폴링 3s 안에 잡힌다
demo/verify.sh 9

echo "모든 시나리오 통과"
