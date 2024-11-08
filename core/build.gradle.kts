plugins {
    kotlin("jvm") version plugins.versions.kotlin
}

dependencies {
    implementation(gcp.pubsub)
    implementation(avro.avro)
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(17)
}
