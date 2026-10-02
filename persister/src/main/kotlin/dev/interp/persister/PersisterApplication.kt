package dev.interp.persister

import dev.interp.common.KafkaTopicsConfig
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Import

@SpringBootApplication
@Import(KafkaTopicsConfig::class)
class PersisterApplication

fun main(args: Array<String>) {
    runApplication<PersisterApplication>(*args)
}
