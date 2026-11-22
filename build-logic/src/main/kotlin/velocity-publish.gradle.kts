plugins {
    java
    `maven-publish`
}

// Spaceport publishes under its own coordinates (dev.cosmoworks.spaceport:spaceport-api), so it
// can never be mistaken for upstream's com.velocitypowered:velocity-api.
extensions.configure<PublishingExtension> {
    repositories {
        maven {
            name = "cosmoworks"
            setUrl("https://repo.cosmoworks.dev/")
            credentials {
                username = "publish"
                password = providers.environmentVariable("MAVEN_PUBLISH_TOKEN").orNull
            }
        }
    }
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            groupId = "dev.cosmoworks.spaceport"
            artifactId = project.name.replace("velocity", "spaceport")
            pom {
                name.set("Spaceport")
                description.set("Spaceport, a Velocity fork that runs several proxies as one network")
                url.set("https://cosmoworks.dev/spaceport")
                scm {
                    url.set("https://github.com/Cosmo-Works/Velocity-Spaceport")
                    connection.set("scm:git:https://github.com/Cosmo-Works/Velocity-Spaceport.git")
                    developerConnection.set("scm:git:https://github.com/Cosmo-Works/Velocity-Spaceport.git")
                }
            }
        }
    }
}
