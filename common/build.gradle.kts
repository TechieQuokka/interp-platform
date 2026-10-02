import org.springframework.boot.gradle.plugin.SpringBootPlugin

plugins {
    kotlin("jvm")
    id("io.spring.dependency-management")
    `java-test-fixtures`
}

dependencyManagement {
    imports {
        mavenBom(SpringBootPlugin.BOM_COORDINATES)
    }
}

dependencies {
    api("tools.jackson.module:jackson-module-kotlin")
    api("org.jetbrains.kotlin:kotlin-reflect")
    compileOnly("org.springframework.kafka:spring-kafka")

    testFixturesApi("org.springframework.boot:spring-boot-test")
    testFixturesApi("org.springframework.boot:spring-boot-testcontainers")
    testFixturesApi("org.testcontainers:testcontainers-junit-jupiter")
    testFixturesApi("org.testcontainers:testcontainers-kafka")
    testFixturesApi("org.testcontainers:testcontainers-postgresql")

    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
