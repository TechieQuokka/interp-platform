package dev.interp.gateway.fanout

import dev.interp.common.Lang
import dev.interp.common.TranslatedText
import java.util.UUID

/** Durable history, used for replay when the in-memory ring buffer no longer covers the gap. */
fun interface TranslationHistory {
    fun after(sessionId: UUID, lang: Lang, afterSeq: Long, limit: Int): List<TranslatedText>
}
