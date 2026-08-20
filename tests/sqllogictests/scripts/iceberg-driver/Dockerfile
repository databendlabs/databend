FROM maven:3.9.5-eclipse-temurin-11 AS builder

WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN set -eux; \
    for attempt in 1 2 3 4 5; do \
      if mvn -B -ntp clean package -DskipTests; then \
        ls -lh target; \
        exit 0; \
      fi; \
      echo "WARN: mvn package failed (attempt ${attempt}/5), retrying..."; \
      sleep $((attempt * 5)); \
    done; \
    echo "FAIL: mvn package failed after 5 attempts"; \
    exit 1

FROM eclipse-temurin:11-jdk

WORKDIR /app
COPY --from=builder /app/target/*.jar app.jar
CMD ["java", "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED", "-jar", "app.jar"]