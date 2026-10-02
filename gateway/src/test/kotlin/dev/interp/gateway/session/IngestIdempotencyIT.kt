package dev.interp.gateway.session

import dev.interp.common.TestcontainersConfiguration
import dev.interp.common.Topics
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.kafka.core.ConsumerFactory
import java.time.Duration
import java.util.Properties
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@Import(TestcontainersConfiguration::class)
@SpringBootTest
class IngestIdempotencyIT {

    @Autowired
    lateinit var ingest: IngestService

    @Autowired
    lateinit var jdbc: JdbcClient

    @Autowired
    lateinit var consumerFactory: ConsumerFactory<String, String>

    private fun lastSeq(sessionId: UUID) =
        jdbc.sql("SELECT last_seq FROM session WHERE id = ?").param(sessionId).query(Long::class.java).single()

    private fun publishedCount(sessionId: UUID): Int {
        val overrides = Properties().apply { put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") }
        return consumerFactory.createConsumer("test-${UUID.randomUUID()}", null, null, overrides).use { consumer ->
            consumer.subscribe(listOf(Topics.SOURCE_TEXT))
            var count = 0
            var idlePolls = 0
            while (idlePolls < 5) {
                val records = consumer.poll(Duration.ofMillis(500))
                if (records.isEmpty) idlePolls++ else idlePolls = 0
                count += records.count { it.key() == sessionId.toString() }
            }
            count
        }
    }

    @Test
    fun `a retried utterance gets its original seq and is published once`() {
        val sessionId = ingest.createSession()
        val utteranceId = UUID.randomUUID()

        val first = ingest.ingest(sessionId, utteranceId, "hello")
        val retry = ingest.ingest(sessionId, utteranceId, "hello")
        val next = ingest.ingest(sessionId, UUID.randomUUID(), "world")

        assertFalse(first.duplicate)
        assertTrue(retry.duplicate)
        assertEquals(first.seq, retry.seq)
        assertEquals(first.ingestedAtMs, retry.ingestedAtMs)
        assertEquals(first.seq + 1, next.seq) // the retry did not burn a seq
        assertEquals(2L, lastSeq(sessionId))
        assertEquals(2, publishedCount(sessionId))
    }

    @Test
    fun `concurrent retries of one utterance all get the same seq`() {
        val sessionId = ingest.createSession()
        val utteranceId = UUID.randomUUID()
        val tasks = List(20) { Callable { ingest.ingest(sessionId, utteranceId, "hello") } }

        val results = Executors.newFixedThreadPool(20).use { pool -> pool.invokeAll(tasks).map { it.get() } }

        assertEquals(setOf(1L), results.map { it.seq }.toSet())
        assertEquals(1, results.count { !it.duplicate })
        assertEquals(1L, lastSeq(sessionId))
        assertEquals(1, publishedCount(sessionId))
    }
}
