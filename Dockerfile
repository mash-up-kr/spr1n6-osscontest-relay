# doc-relay 실행 이미지.
#
# 이 이미지는 접속 정보를 하나도 들고 있지 않다. DB/카프카 주소는 띄우는 쪽이 환경변수로 넣어
# 준다(아래 "필요한 환경변수" 참고). 다른 팀 컴포즈에 그대로 얹혀도 동작하게 하려면 이래야 한다 —
# 이미지 안에 localhost 를 박아 두면 컨테이너 안에서의 localhost 는 자기 자신이라 아무 데도 못 붙는다.

FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace

# 래퍼를 먼저 복사해 gradle 배포판 다운로드를 별도 레이어로 남긴다. 소스만 고친 재빌드에서
# 이 레이어가 캐시에 걸려 빌드 시간의 상당 부분을 건너뛴다.
COPY gradlew ./
COPY gradle gradle
RUN chmod +x gradlew && ./gradlew --no-daemon --version

COPY build.gradle.kts settings.gradle.kts ./
COPY src src

# 테스트는 Testcontainers 로 도커를 띄운다. 이미지 빌드 안에서는 도커를 쓸 수 없으므로 여기서는
# jar 만 만든다. 테스트는 CI/로컬에서 ./gradlew test 로 따로 돌린다.
#
# bootJar 산출물 이름에 버전이 들어가고 설정에 따라 -plain.jar 이 같이 생길 수 있다. 런타임
# 단계가 glob 으로 집으면 파일이 둘일 때 실패하므로, 여기서 app.jar 하나로 정규화한다.
RUN ./gradlew --no-daemon bootJar -x test \
 && find build/libs -name '*.jar' ! -name '*-plain.jar' -exec cp {} /workspace/app.jar \;


FROM eclipse-temurin:21-jre AS runtime

# GHCR 이 패키지를 이 저장소에 연결하는 근거로 삼는 라벨. 없으면 패키지가 저장소와 따로 놀아서
# 공개 여부와 접근 권한을 저장소 설정으로 관리하지 못하고 패키지 쪽에서 따로 만져야 한다.
LABEL org.opencontainers.image.source="https://github.com/mash-up-kr/spr1n6-osscontest-relay"

# 헬스체크가 액추에이터를 두드리는 데 쓴다. JRE 이미지에는 HTTP 를 쏠 도구가 없다.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

RUN useradd --system --create-home --uid 10001 relay
WORKDIR /app
COPY --from=build --chown=relay:relay /workspace/app.jar app.jar
USER relay

# docker 프로파일은 컨테이너 안에서만 필요한 것(액추에이터 바인드 주소)만 담는다.
# 접속 정보는 여기 없다 — 환경변수가 프로파일 yaml 보다 우선순위가 높으므로, 띄우는 쪽이
# SPRING_PROFILES_ACTIVE 를 "docker,something" 으로 덮어써도 이 파일의 값은 살아 있다.
ENV SPRING_PROFILES_ACTIVE=docker

# 관리 포트. 이 서버는 HTTP 요청을 받지 않으므로(server.port=-1) 열리는 포트는 이거 하나다.
EXPOSE 9090

HEALTHCHECK --interval=10s --timeout=3s --start-period=60s --retries=6 \
  CMD curl -fsS http://127.0.0.1:9090/actuator/health/readiness || exit 1

# exec 형식이라 SIGTERM 이 java 에 그대로 간다. 정상 종료(진행 중인 드레인을 기다리는 경로)가
# 동작하려면 이게 중요하다. JVM 옵션이 필요하면 JAVA_TOOL_OPTIONS 환경변수로 넣는다.
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

# 필요한 환경변수 (띄우는 쪽에서 반드시 넣어야 하는 것):
#   SPRING_DATASOURCE_URL       jdbc:postgresql://<db-host>:5432/<db>
#   SPRING_DATASOURCE_USERNAME
#   SPRING_DATASOURCE_PASSWORD
#   SPRING_KAFKA_BOOTSTRAP_SERVERS  <kafka-host>:<port>  (컨테이너 간 통신용 리스너 주소)
#
# 선택:
#   RELAY_KAFKA_TOPIC           기본 doc.events.v1
#   RELAY_KAFKA_PARTITIONS      기본 3
#   RELAY_ADMIN_TOKEN           비어 있으면 어드민 쓰기 액션 인증이 꺼진다
#   RELAY_INSTANCE_ID           기본은 컨테이너 호스트명
