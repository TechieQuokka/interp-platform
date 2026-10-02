package dev.interp.common

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

// Same image versions as docker/compose.yaml, so tests run against what we deploy.

@TestConfiguration(proxyBeanMethods = false)
class KafkaContainerConfiguration {

    @Bean
    @ServiceConnection
    fun kafkaContainer(): KafkaContainer = KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"))
}

@TestConfiguration(proxyBeanMethods = false)
class PostgresContainerConfiguration {

    @Bean
    @ServiceConnection
    fun postgresContainer(): PostgreSQLContainer = PostgreSQLContainer(DockerImageName.parse("postgres:18.6"))
}

@TestConfiguration(proxyBeanMethods = false)
@Import(KafkaContainerConfiguration::class, PostgresContainerConfiguration::class)
class TestcontainersConfiguration
