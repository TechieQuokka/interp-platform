package dev.interp.common

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals

class EventCodecTest {

    @Test
    fun `source text round-trips`() {
        val event = SourceText(UUID.randomUUID(), 7, "hello", 1_000L)
        assertEquals(event, EventCodec.decodeSource(EventCodec.encode(event)))
    }

    @Test
    fun `translated text round-trips and keeps lang as enum`() {
        val event = TranslatedText(UUID.randomUUID(), Lang.JA, 3, "[ja] hello", 1_000L, 1_200L)
        val json = EventCodec.encode(event)
        assertEquals(event, EventCodec.decodeTranslated(json))
        assert(json.contains("\"lang\":\"JA\"")) { json }
    }
}
