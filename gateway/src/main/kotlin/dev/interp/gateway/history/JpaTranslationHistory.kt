package dev.interp.gateway.history

import dev.interp.common.Lang
import dev.interp.common.TranslatedText
import dev.interp.gateway.fanout.TranslationHistory
import org.springframework.data.domain.Limit
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.util.UUID

@Component
class JpaTranslationHistory(private val translations: TranslationRepository) : TranslationHistory {

    @Transactional(readOnly = true)
    override fun after(sessionId: UUID, lang: Lang, afterSeq: Long, limit: Int): List<TranslatedText> =
        translations.findBySessionIdAndLangAndSeqGreaterThanOrderBySeqAsc(sessionId, lang.code, afterSeq, Limit.of(limit))
            .map {
                TranslatedText(
                    sessionId = it.sessionId,
                    lang = lang,
                    seq = it.seq,
                    text = it.text,
                    ingestedAtMs = it.ingestedAt.toEpochMilli(),
                    translatedAtMs = it.translatedAt.toEpochMilli(),
                )
            }
}
