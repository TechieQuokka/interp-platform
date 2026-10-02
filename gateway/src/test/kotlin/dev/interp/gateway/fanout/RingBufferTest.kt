package dev.interp.gateway.fanout

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class RingBufferTest {

    @Test
    fun `keeps only the most recent items in insertion order`() {
        val ring = RingBuffer<Int>(3)
        assertNull(ring.firstOrNull())
        (1..5).forEach(ring::add)
        assertEquals(listOf(3, 4, 5), ring.snapshot())
        assertEquals(3, ring.firstOrNull())
        assertEquals(3, ring.size)
    }
}
