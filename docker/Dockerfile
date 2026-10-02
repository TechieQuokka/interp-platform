# Packages a pre-built Spring Boot jar: run `./gradlew bootJar` first.
FROM eclipse-temurin:25.0.4.1_1-jre
ARG MODULE
WORKDIR /app
COPY ${MODULE}/build/libs/${MODULE}-0.0.1-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
