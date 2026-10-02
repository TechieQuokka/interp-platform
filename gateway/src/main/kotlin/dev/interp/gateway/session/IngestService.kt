package dev.interp.gateway.session

import dev.interp.common.EventCodec
import dev.interp.common.SourceText
import dev.interp.common.Topics
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.util.UUID
import java.util.concurrent.TimeUnit

class SessionNotFoundException(sessionId: UUID) : RuntimeException("Session not found: $sessionId")

@Service
class IngestService(
    private val sessions: SessionRepository,
    private val seqAllocator: SeqAllocator,
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
     * Allocates the seq and publishes inside one transaction: if the Kafka send fails, the seq
     * increment is rolled back, so a failed ingest never leaves a permanent gap.
     */
    @Transactional
    fun ingest(sessionId: UUID, text: String): SourceText {
        val seq = seqAllocator.next(sessionId) ?: throw SessionNotFoundException(sessionId)
        val event = SourceText(sessionId, seq, text, System.currentTimeMillis())
        kafka.send(Topics.SOURCE_TEXT, sessionId.toString(), EventCodec.encode(event))
            .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        return event
    }

    companion object {
        private const val SEND_TIMEOUT_SECONDS = 5L
    }
}
