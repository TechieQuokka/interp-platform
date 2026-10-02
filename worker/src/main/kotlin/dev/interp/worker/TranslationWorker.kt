package dev.interp.worker

import dev.interp.common.EventCodec
import dev.interp.common.Topics
import dev.interp.common.TranslatedText
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

/**
 * Mock translator. Records of one partition are processed one at a time, so per-session order is
 * kept. The offset is committed only after the translated record is acked by Kafka (ack-mode:
 * record), so a crash causes redelivery (duplicates), never loss — the gateway dedupes by seq.
 */
@Component
class TranslationWorker(
    private val properties: WorkerProperties,
    private val kafka: KafkaTemplate<String, String>,
    registry: MeterRegistry,
) {
    private val translated = registry.counter("interp.worker.translated", "lang", properties.lang.code)

    @KafkaListener(
        id = "translator",
        topics = [Topics.SOURCE_TEXT],
        groupId = "worker-\${interp.worker.lang}",
        concurrency = "6",
    )
    fun translate(value: String) {
        val source = EventCodec.decodeSource(value)
        Thread.sleep(ThreadLocalRandom.current().nextLong(properties.minDelayMs, properties.maxDelayMs + 1))
        val lang = properties.lang
        val result = TranslatedText(
            sessionId = source.sessionId,
            lang = lang,
            seq = source.seq,
            text = "[${lang.code}] ${source.text}",
            ingestedAtMs = source.ingestedAtMs,
            translatedAtMs = System.currentTimeMillis(),
        )
        kafka.send(Topics.translated(lang), source.sessionId.toString(), EventCodec.encode(result))
            .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        translated.increment()
    }

    companion object {
        private const val SEND_TIMEOUT_SECONDS = 10L
    }
}
