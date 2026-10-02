package dev.interp.worker

import dev.interp.common.EventCodec
import dev.interp.common.KafkaContainerConfiguration
import dev.interp.common.Lang
import dev.interp.common.SourceText
import dev.interp.common.Topics
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import java.time.Duration
import java.util.Properties
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Import(KafkaContainerConfiguration::class)
@SpringBootTest(properties = ["interp.worker.lang=en", "interp.worker.min-delay-ms=300", "interp.worker.max-delay-ms=300"])
class ParallelTranslationIT {

    @Autowired
    lateinit var kafka: KafkaTemplate<String, String>

    @Autowired
    lateinit var consumerFactory: ConsumerFactory<String, String>

    @Test
    fun `sessions sharing a partition are translated in parallel, each in order`() {
        val sessions = List(4) { UUID.randomUUID() }
        val perSession = 5
        // Force everything into one partition: before, these 20 records took 20 x 300 ms = 6 s.
        val overrides = Properties().apply { put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") }
        consumerFactory.createConsumer("test-${UUID.randomUUID()}", null, null, overrides).use { consumer ->
            consumer.subscribe(listOf(Topics.translated(Lang.EN)))
            consumer.poll(Duration.ofSeconds(2)) // join the group before producing

            val start = System.currentTimeMillis()
            for (seq in 1L..perSession) {
                sessions.forEach { kafka.send(Topics.SOURCE_TEXT, 0, it.toString(), EventCodec.encode(SourceText(it, seq, "s$seq", start))) }
            }
            kafka.flush()

            val received = mutableListOf<Pair<UUID, Long>>()
            while (received.size < sessions.size * perSession && System.currentTimeMillis() - start < 20_000) {
                consumer.poll(Duration.ofMillis(100)).forEach {
                    val event = EventCodec.decodeTranslated(it.value())
                    if (event.sessionId in sessions) received += event.sessionId to event.seq
                }
            }
            val elapsed = System.currentTimeMillis() - start

            sessions.forEach { s -> assertEquals((1L..perSession).toList(), received.filter { it.first == s }.map { it.second }) }
            // Sequential would be >= 6 s; parallel across sessions is about 5 x 300 ms plus batching.
            assertTrue(elapsed < 4_000, "took $elapsed ms")
        }
    }
}
