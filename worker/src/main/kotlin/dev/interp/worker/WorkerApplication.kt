package dev.interp.worker

import dev.interp.common.KafkaTopicsConfig
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Import

@SpringBootApplication
@ConfigurationPropertiesScan
@Import(KafkaTopicsConfig::class)
class WorkerApplication

fun main(args: Array<String>) {
    runApplication<WorkerApplication>(*args)
}
