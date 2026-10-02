package dev.interp.gateway.history

import org.springframework.data.domain.Limit
import org.springframework.data.jpa.repository.JpaRepository
import java.util.UUID

interface TranslationRepository : JpaRepository<TranslationEntity, TranslationId> {

    fun findBySessionIdAndLangAndSeqGreaterThanOrderBySeqAsc(
        sessionId: UUID,
        lang: String,
        seq: Long,
        limit: Limit,
    ): List<TranslationEntity>
}
