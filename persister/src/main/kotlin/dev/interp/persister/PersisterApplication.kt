package dev.interp.persister

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class PersisterApplication

fun main(args: Array<String>) {
    runApplication<PersisterApplication>(*args)
}
