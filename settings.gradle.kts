plugins {
    // Auto-provisions the JDK 25 toolchain (only JDK 21 is installed on the host).
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "interp"

include("common", "gateway", "worker", "persister")
