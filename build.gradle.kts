plugins {
    id("com.hayden.git")
    id("com.hayden.jpa-persistence")
    id("com.hayden.observable-app")
    id("com.hayden.docker")
    id("com.hayden.docker-compose")
    id("com.hayden.log")
    id("com.hayden.ai")
    id("com.hayden.spring-app")
}

tasks.register("prepareKotlinBuildScriptModel") {}

dependencies {
    implementation(project(":proto"))
    implementation(project(":tracing"))
    implementation(project(":utilitymodule"))
    implementation(project(":jpa-persistence"))
    implementation(project(":commit-diff-model"))
    implementation(project(":commit-diff-context"))
    agent("io.opentelemetry.javaagent:opentelemetry-javaagent:2.0.0")
    implementation("io.github.java-diff-utils:java-diff-utils:4.9")

}

tasks.bootJar {
    archiveFileName = "cdc-agents-data-stream.jar"
    enabled = true
}
