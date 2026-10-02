package dev.interp.common

import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.KotlinModule
import tools.jackson.module.kotlin.readValue

/**
 * JSON codec for Kafka record values. Records are plain JSON strings (no type headers),
 * so any consumer — including non-JVM tools — can read them.
 */
object EventCodec {
    val mapper: JsonMapper = JsonMapper.builder()
        .addModule(KotlinModule.Builder().build())
        .build()

    fun encode(event: Any): String = mapper.writeValueAsString(event)

    fun decodeSource(json: String): SourceText = mapper.readValue(json)

    fun decodeTranslated(json: String): TranslatedText = mapper.readValue(json)
}
