FROM maven:3.9.5-eclipse-temurin-11 AS builder

WORKDIR /app
COPY pom.xml .
COPY src ./src
# Maven Central rate-limits GitHub runners (HTTP 429). Retry the package
# step instead of failing the Iceberg fixture image on the first rejection.
RUN set -eux; \
    attempt=1; \
    while :; do \
      if mvn -B -ntp clean package -DskipTests \
        -Daether.connector.http.retryHandler.count=8 \
        -Daether.connector.http.retryHandler.interval=2000; then \
        break; \
      fi; \
      if [ "$attempt" -ge 8 ]; then exit 1; fi; \
      echo "Maven package failed on attempt ${attempt}; retrying..."; \
      sleep $((attempt * 5)); \
      attempt=$((attempt + 1)); \
    done; \
    ls -lh target

FROM eclipse-temurin:11-jdk

WORKDIR /app
COPY --from=builder /app/target/*.jar app.jar
CMD ["java", "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED", "-jar", "app.jar"]
