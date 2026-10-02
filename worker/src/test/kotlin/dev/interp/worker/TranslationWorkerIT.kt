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
@SpringBootTest(properties = ["interp.worker.lang=ja", "interp.worker.min-delay-ms=1", "interp.worker.max-delay-ms=5"])
class TranslationWorkerIT {

    @Autowired
    lateinit var kafka: KafkaTemplate<String, String>

    // Use the app's factory: it carries the Testcontainers bootstrap servers, KafkaProperties does not.
    @Autowired
    lateinit var consumerFactory: ConsumerFactory<String, String>

    @Test
    fun `translates into the configured language keeping key, seq, order and ingest time`() {
        val sessionId = UUID.randomUUID()
        val sources = (1L..20L).map { SourceText(sessionId, it, "hello $it", 1_000L + it) }
        sources.forEach { kafka.send(Topics.SOURCE_TEXT, sessionId.toString(), EventCodec.encode(it)).get() }

        val overrides = Properties().apply { put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") }
        val received = consumerFactory.createConsumer("test-${UUID.randomUUID()}", null, null, overrides).use { consumer ->
            consumer.subscribe(listOf(Topics.translated(Lang.JA)))
            val out = mutableListOf<Pair<String, String>>()
            val deadline = System.currentTimeMillis() + 30_000
            while (out.size < sources.size && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach { out += it.key() to it.value() }
            }
            out
        }

        assertTrue(received.all { it.first == sessionId.toString() })
        val events = received.map { EventCodec.decodeTranslated(it.second) }
        assertEquals((1L..20L).toList(), events.map { it.seq })
        assertTrue(events.all { it.lang == Lang.JA && it.text == "[ja] hello ${it.seq}" && it.ingestedAtMs == 1_000L + it.seq })
    }
}
