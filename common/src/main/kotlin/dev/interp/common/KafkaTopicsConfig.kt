package dev.interp.common

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaAdmin

/**
 * Every app declares all topics, so whichever starts first creates them with the right partition
 * count. Broker-side auto-creation is disabled: a consumer subscribing first would otherwise create
 * a 1-partition topic and keep consuming only that partition after it is expanded.
 */
@Configuration(proxyBeanMethods = false)
class KafkaTopicsConfig {

    @Bean
    fun interpTopics(): KafkaAdmin.NewTopics =
        KafkaAdmin.NewTopics(
            *(listOf(Topics.SOURCE_TEXT) + Topics.allTranslated)
                .map { TopicBuilder.name(it).partitions(PARTITIONS).replicas(1).build() }
                .toTypedArray(),
        )

    companion object {
        const val PARTITIONS = 6
    }
}
