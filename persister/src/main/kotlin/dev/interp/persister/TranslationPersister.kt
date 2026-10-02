package dev.interp.persister

import dev.interp.common.EventCodec
import dev.interp.common.Topics
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.sql.Timestamp
import java.time.Instant

/**
 * Writes translations to Postgres off the hot path. Inserts are idempotent on
 * (session_id, lang, seq), so Kafka redeliveries are harmless.
 */
@Component
class TranslationPersister(
    private val jdbc: JdbcTemplate,
    registry: MeterRegistry,
) {
    private val inserted = registry.counter("interp.persister.inserted")
    private val skipped = registry.counter("interp.persister.duplicates")

    @KafkaListener(
        id = "persister",
        topics = [Topics.TRANSLATED_KO, Topics.TRANSLATED_EN, Topics.TRANSLATED_JA],
        groupId = "persister",
        concurrency = "3",
    )
    fun persist(values: List<String>) {
        val events = values.map(EventCodec::decodeTranslated)
        val counts = jdbc.batchUpdate(
            """
            INSERT INTO translation (session_id, lang, seq, text, ingested_at, translated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (session_id, lang, seq) DO NOTHING
            """.trimIndent(),
            events.map {
                arrayOf<Any>(
                    it.sessionId,
                    it.lang.code,
                    it.seq,
                    it.text,
                    Timestamp.from(Instant.ofEpochMilli(it.ingestedAtMs)),
                    Timestamp.from(Instant.ofEpochMilli(it.translatedAtMs)),
                )
            },
        )
        val written = counts.count { it > 0 }
        inserted.increment(written.toDouble())
        skipped.increment((events.size - written).toDouble())
    }
}
