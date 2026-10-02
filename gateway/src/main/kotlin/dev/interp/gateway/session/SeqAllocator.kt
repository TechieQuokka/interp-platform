package dev.interp.gateway.session

import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Component
import java.util.UUID

/**
 * Allocates the next per-session seq with a single atomic UPDATE. The row lock is held until the
 * surrounding transaction ends, so concurrent ingests for one session are serialized, and any
 * gateway instance can serve the speaker.
 */
@Component
class SeqAllocator(private val jdbc: JdbcClient) {

    /** Returns the new seq, or null if the session does not exist. Must run inside a transaction. */
    fun next(sessionId: UUID): Long? =
        jdbc.sql("UPDATE session SET last_seq = last_seq + 1 WHERE id = :id RETURNING last_seq")
            .param("id", sessionId)
            .query(Long::class.java)
            .optional()
            .orElse(null)
}
