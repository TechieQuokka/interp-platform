package dev.interp.gateway.history

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.IdClass
import jakarta.persistence.Table
import java.io.Serializable
import java.time.Instant
import java.util.UUID

/** Read model for replay; rows are written by the persister. */
@Entity
@Table(name = "translation")
@IdClass(TranslationId::class)
class TranslationEntity(
    @Id val sessionId: UUID,
    @Id val lang: String,
    @Id val seq: Long,
    val text: String,
    val ingestedAt: Instant,
    val translatedAt: Instant,
)

data class TranslationId(
    val sessionId: UUID? = null,
    val lang: String? = null,
    val seq: Long? = null,
) : Serializable
