plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.8.0"
}
rootProject.name = "gcp-pub-sub-plugin"

include(":core")

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
    versionCatalogs {
        create("plugins") {
            version("kotlin", "2.0.20")
        }

        create("gcp") {
            library("pubsub", "com.google.cloud:google-cloud-pubsub:1.133.1")
        }

        create("avro"){
            library("avro", "org.apache.avro:avro:1.12.0")
        }
    }
}

