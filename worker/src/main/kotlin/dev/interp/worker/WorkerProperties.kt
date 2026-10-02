package dev.interp.worker

import dev.interp.common.Lang
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("interp.worker")
data class WorkerProperties(
    /** Target language; one consumer group (and usually one deployment) per language. */
    val lang: Lang,
    val minDelayMs: Long = 100,
    val maxDelayMs: Long = 800,
)
