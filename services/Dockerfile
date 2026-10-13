# syntax=docker/dockerfile:1
# Shared by all services. Build context = the service folder (see docker-compose.yml).

# Stage 1: build the jar with the service's own Maven wrapper.
FROM eclipse-temurin:21.0.12_8-jdk AS build
WORKDIR /src
COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY src src
# The cache mount keeps ~/.m2 between builds, so dependencies download only once.
RUN --mount=type=cache,target=/root/.m2 ./mvnw -q package -DskipTests && cp target/*.jar app.jar

# Stage 2: only a JRE and the jar (no JDK, no Maven, no sources).
FROM eclipse-temurin:21.0.12_8-jre
RUN useradd --system app
USER app
WORKDIR /app
COPY --from=build /src/app.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
