package dev.interp.gateway

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("interp.gateway")
data class GatewayProperties(
    /** Unique per running instance; becomes this instance's Kafka consumer group. */
    val instanceId: String,
    /** Max messages buffered per listener before it is disconnected as a slow consumer. */
    val listenerQueueCapacity: Int = 256,
    /** Recent messages kept in memory per (session, lang) for reconnect replay. */
    val ringBufferCapacity: Int = 1000,
    /** Upper bound on rows read from Postgres for a single replay. */
    val maxDbReplay: Int = 10_000,
)
