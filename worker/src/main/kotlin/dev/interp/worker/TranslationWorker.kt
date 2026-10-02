package dev.interp.worker

import dev.interp.common.EventCodec
import dev.interp.common.SourceText
import dev.interp.common.Topics
import dev.interp.common.TranslatedText
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.util.concurrent.Executors
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

/**
 * Mock translator. Each poll's batch is split by session: different sessions are translated in
 * parallel (one virtual thread each), while the records of one session are processed one at a
 * time in offset order, so per-session order is kept. The batch's offsets are committed only after
 * every translated record has been acked by Kafka, so a crash causes redelivery (duplicates),
 * never loss — the gateway dedupes by seq.
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
        batch = "true",
    )
    fun translate(records: List<ConsumerRecord<String, String>>) {
        // A key always lives in one partition, and a batch lists each partition's records in
        // offset order, so grouping by key keeps every session's records in order.
        val bySession = records.groupBy { it.key() }.values
        Executors.newVirtualThreadPerTaskExecutor().use { executor ->
            bySession
                .map { sessionRecords -> executor.submit { sessionRecords.forEach { translate(EventCodec.decodeSource(it.value())) } } }
                .forEach { it.get() } // rethrows the first failure; the whole batch is then redelivered
        }
    }

    private fun translate(source: SourceText) {
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
