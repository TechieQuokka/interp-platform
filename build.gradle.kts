import org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension

plugins {
    kotlin("jvm") version "2.4.20" apply false
    kotlin("plugin.spring") version "2.4.20" apply false
    kotlin("plugin.jpa") version "2.4.20" apply false
    id("org.springframework.boot") version "4.1.1" apply false
    id("io.spring.dependency-management") version "1.1.7" apply false
}

subprojects {
    group = "dev.interp"
    version = "0.0.1-SNAPSHOT"

    repositories {
        mavenCentral()
    }

    plugins.withId("org.jetbrains.kotlin.jvm") {
        configure<JavaPluginExtension> {
            toolchain {
                languageVersion = JavaLanguageVersion.of(25)
            }
        }
        configure<KotlinJvmProjectExtension> {
            compilerOptions {
                freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
            }
        }
    }

    // Spring Boot's BOM would otherwise pin kotlin-stdlib to an older version than the compiler.
    extra["kotlin.version"] = "2.4.20"

    tasks.withType<Test> {
        useJUnitPlatform()
        jvmArgs("--enable-native-access=ALL-UNNAMED")
    }
}
