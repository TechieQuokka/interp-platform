package dev.interp.gateway.session

import dev.interp.common.EventCodec
import dev.interp.common.SourceText
import dev.interp.common.Topics
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.kafka.KafkaException
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.transaction.interceptor.TransactionAspectSupport
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class SessionNotFoundException(sessionId: UUID) : RuntimeException("Session not found: $sessionId")

/** Kafka could not take the utterance in time; nothing was committed, so the client may retry. */
class IngestUnavailableException(cause: Throwable) : RuntimeException("Ingest temporarily unavailable", cause)

data class IngestResult(val seq: Long, val ingestedAtMs: Long, val duplicate: Boolean)

@Service
class IngestService(
    private val sessions: SessionRepository,
    private val seqAllocator: SeqAllocator,
    private val jdbc: JdbcClient,
    private val kafka: KafkaTemplate<String, String>,
) {

    @Transactional
    fun createSession(): UUID = sessions.save(SessionEntity(UUID.randomUUID())).id

    @Transactional(readOnly = true)
    fun find(sessionId: UUID): SessionResponse =
        sessions.findById(sessionId)
            .map { SessionResponse(it.id, it.lastSeq) }
            .orElseThrow { SessionNotFoundException(sessionId) }

    /**
     * Idempotent on [utteranceId]: a retry of an utterance that was already accepted returns the
     * original seq and is not published again.
     *
     * Allocating the seq first takes the session row lock, so concurrent retries of the same
     * utterance are serialized and the lookup below sees any committed earlier attempt. On a
     * duplicate the transaction is rolled back, which also returns the just-allocated seq.
     * Allocation, the utterance row and the Kafka publish commit or fail together.
     */
    @Transactional
    fun ingest(sessionId: UUID, utteranceId: UUID, text: String): IngestResult {
        val seq = seqAllocator.next(sessionId) ?: throw SessionNotFoundException(sessionId)

        findAccepted(sessionId, utteranceId)?.let {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly()
            return it
        }

        val event = SourceText(sessionId, seq, text, System.currentTimeMillis())
        jdbc.sql("INSERT INTO utterance (session_id, utterance_id, seq, ingested_at) VALUES (?, ?, ?, ?)")
            .params(sessionId, utteranceId, seq, Timestamp.from(Instant.ofEpochMilli(event.ingestedAtMs)))
            .update()
        publish(event)
        return IngestResult(seq, event.ingestedAtMs, duplicate = false)
    }

    private fun findAccepted(sessionId: UUID, utteranceId: UUID): IngestResult? =
        jdbc.sql("SELECT seq, ingested_at FROM utterance WHERE session_id = ? AND utterance_id = ?")
            .params(sessionId, utteranceId)
            .query { rs, _ -> IngestResult(rs.getLong("seq"), rs.getTimestamp("ingested_at").time, duplicate = true) }
            .optional()
            .orElse(null)

    private fun publish(event: SourceText) {
        try {
            kafka.send(Topics.SOURCE_TEXT, event.sessionId.toString(), EventCodec.encode(event))
                .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        } catch (e: ExecutionException) {
            throw IngestUnavailableException(e.cause ?: e)
        } catch (e: TimeoutException) {
            throw IngestUnavailableException(e)
        } catch (e: KafkaException) {
            throw IngestUnavailableException(e)
        }
    }

    companion object {
        // Above the producer's delivery.timeout.ms, so a timed-out send has really been given up on.
        private const val SEND_TIMEOUT_SECONDS = 5L
    }
}
