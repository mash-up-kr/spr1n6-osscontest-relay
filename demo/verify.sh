#!/usr/bin/env bash
# INSERT 한 건수와 카프카에 도착한 고유 eventId 건수가 같은지 확인한다.
# 중복 도착은 at-least-once 계약상 정상이므로 고유 개수로 센다.
set -euo pipefail

EXPECTED="${1:?사용법: verify.sh <기대 건수>}"
TOPIC="${TOPIC:-doc.events.v1}"
BROKER="${BROKER:-localhost:9092}"

# kafka-console-consumer.sh 는 --timeout-ms 로 정상 종료할 때도 0이 아닌 코드로 끝난다
# (ConsumeTimeoutException). grep 도 매치가 하나도 없으면 1을 낸다. 둘 다 "정상"인 경우이지
# 에러가 아니므로, set -e/pipefail 이 이 파이프라인 때문에 스크립트를 죽이지 않도록
# `|| true` 로 감싸고 빈 값은 0으로 취급한다.
ARRIVED=$(
  docker compose -f demo/docker-compose.yml exec -T kafka \
    /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server "$BROKER" \
      --topic "$TOPIC" \
      --from-beginning \
      --timeout-ms 15000 2>/dev/null \
  | grep -o '"eventId":"[^"]*"' \
  | sort -u | wc -l | tr -d ' '
) || true
ARRIVED="${ARRIVED:-0}"

echo "기대 $EXPECTED / 고유 도착 $ARRIVED"
if [ "$ARRIVED" -lt "$EXPECTED" ]; then
  echo "유실 발생: $((EXPECTED - ARRIVED))건" >&2
  exit 1
fi
echo "유실 0"
