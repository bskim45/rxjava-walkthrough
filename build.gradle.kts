import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

version = "1.0-SNAPSHOT"
val kotlin_version = "1.2.10"

plugins {
    kotlin("jvm") version "1.2.10"
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib-jdk8", kotlin_version))

    // rx
    implementation("io.reactivex.rxjava2:rxjava:2.1.8")
    implementation("io.reactivex.rxjava2:rxkotlin:2.2.0")

    implementation("org.slf4j:slf4j-api:1.7.25")
    implementation("org.slf4j:slf4j-simple:1.7.25")

    testImplementation("junit:junit:4.12")
    testImplementation(kotlin("test-junit", kotlin_version))
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

