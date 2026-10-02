package dev.interp.gateway.fanout

import dev.interp.common.TranslatedText

/** Wire format sent to listeners. */
data class ListenerMessage(
    val seq: Long,
    val lang: String,
    val text: String,
    val ingestedAtMs: Long,
    val translatedAtMs: Long,
) {
    companion object {
        fun of(event: TranslatedText) =
            ListenerMessage(event.seq, event.lang.code, event.text, event.ingestedAtMs, event.translatedAtMs)
    }
}
