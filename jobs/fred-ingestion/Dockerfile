# Build stage: compile and run the tests with Maven
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /build

# Download dependencies first so they are cached between code changes
COPY pom.xml .
RUN mvn -B -q dependency:go-offline

COPY src ./src
RUN mvn -B -q package

# Runtime stage: just a JRE and the jar
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/target/fred-ingestion-*.jar app.jar

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
