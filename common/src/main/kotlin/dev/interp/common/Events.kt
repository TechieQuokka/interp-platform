package dev.interp.common

import java.util.UUID

/** One speaker utterance. [seq] is dense per session (1, 2, 3, ...), allocated by the gateway. */
data class SourceText(
    val sessionId: UUID,
    val seq: Long,
    val text: String,
    val ingestedAtMs: Long,
)

/**
 * A [SourceText] translated into [lang]. Carries the original seq and ingest time end to end.
 *
 * [translatedAtMs] is stamped when translation finishes, before the record is produced, so
 * "fan-out" latency measured from it also includes any wait in the worker's producer (for example
 * while a broker recovers).
 */
data class TranslatedText(
    val sessionId: UUID,
    val lang: Lang,
    val seq: Long,
    val text: String,
    val ingestedAtMs: Long,
    val translatedAtMs: Long,
)
