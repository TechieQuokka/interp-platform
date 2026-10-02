package dev.interp.gateway.fanout

import dev.interp.common.EventCodec
import dev.interp.common.Topics
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

/**
 * Every gateway instance consumes every translation (its own consumer group), because any
 * listener may be connected to any instance. A (session, lang) always maps to one partition,
 * so its messages are handled by one thread, in order.
 */
@Component
class FanOutConsumer(private val hub: ChannelHub) {

    @KafkaListener(
        id = "fan-out",
        topics = [Topics.TRANSLATED_KO, Topics.TRANSLATED_EN, Topics.TRANSLATED_JA],
        groupId = "gateway-\${interp.gateway.instance-id}",
        concurrency = "3",
    )
    fun onTranslated(value: String) {
        hub.publish(EventCodec.decodeTranslated(value))
    }
}
