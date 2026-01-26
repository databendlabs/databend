FROM maven:3.9.5-eclipse-temurin-11 AS builder

WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests && ls -lh target

FROM eclipse-temurin:11-jdk

WORKDIR /app
COPY --from=builder /app/target/*.jar app.jar
CMD ["java", "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED", "-jar", "app.jar"]