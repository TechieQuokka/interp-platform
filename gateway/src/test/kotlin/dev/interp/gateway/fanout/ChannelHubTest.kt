package dev.interp.gateway.fanout

import dev.interp.common.EventCodec
import dev.interp.common.Lang
import dev.interp.common.TranslatedText
import dev.interp.common.eventually
import dev.interp.gateway.GatewayProperties
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import kotlin.test.Test
import kotlin.test.assertEquals

class ChannelHubTest {

    private val sessionId = UUID.randomUUID()
    private val registry = SimpleMeterRegistry()
    private val stored = CopyOnWriteArrayList<TranslatedText>()

    private fun hub(queueCapacity: Int = 256, ringCapacity: Int = 1000) = ChannelHub(
        GatewayProperties(instanceId = "test", listenerQueueCapacity = queueCapacity, ringBufferCapacity = ringCapacity),
        { sid, lang, after, limit -> stored.filter { it.sessionId == sid && it.lang == lang && it.seq > after }.take(limit) },
        GatewayMetrics(registry),
    )

    private fun event(seq: Long) = TranslatedText(sessionId, Lang.KO, seq, "t$seq", System.currentTimeMillis(), System.currentTimeMillis())

    private fun counter(name: String) = registry.find(name).counters().sumOf { it.count() }

    @Test
    fun `live listener receives messages in order and duplicates are dropped`() {
        val hub = hub()
        val out = RecordingOutbound()
        hub.connect(sessionId, Lang.KO, null, out)

        listOf(1L, 2L, 2L, 1L, 3L).forEach { hub.publish(event(it)) }

        eventually { assertEquals(listOf(1L, 2L, 3L), out.seqs()) }
        assertEquals(2.0, counter("interp.fanout.duplicates"))
        assertEquals(0.0, counter("interp.fanout.gaps"))
    }

    @Test
    fun `a jump in seq is counted as a gap`() {
        val hub = hub()
        listOf(1L, 2L, 5L).forEach { hub.publish(event(it)) }
        assertEquals(1.0, counter("interp.fanout.gaps"))
    }

    @Test
    fun `reconnect replays from the ring buffer and then continues live without overlap`() {
        val hub = hub()
        (1L..5L).forEach { hub.publish(event(it)) }

        val out = RecordingOutbound()
        hub.connect(sessionId, Lang.KO, 2, out)
        hub.publish(event(6))

        eventually { assertEquals(listOf(3L, 4L, 5L, 6L), out.seqs()) }
        assertEquals(3.0, counter("interp.replay.messages"))
    }

    @Test
    fun `reconnect older than the ring buffer replays from history`() {
        val hub = hub(ringCapacity = 3)
        (1L..6L).forEach {
            stored += event(it)
            hub.publish(event(it))
        }

        val out = RecordingOutbound()
        hub.connect(sessionId, Lang.KO, 1, out)

        eventually { assertEquals((2L..6L).toList(), out.seqs()) }
        assertEquals(5.0, registry.find("interp.replay.messages").tag("source", "db").counter()!!.count())
    }

    @Test
    fun `listener that cannot keep up is disconnected as a slow consumer`() {
        val hub = hub(queueCapacity = 2)
        val stuck = RecordingOutbound(blockSends = true)
        val healthy = RecordingOutbound()
        hub.connect(sessionId, Lang.KO, null, stuck)
        hub.connect(sessionId, Lang.KO, null, healthy)

        // Publish at a pace the healthy listener keeps up with; the stuck one overflows after 1 in flight + 2 queued.
        (1L..10L).forEach {
            hub.publish(event(it))
            eventually { assertEquals((1L..it).toList(), healthy.seqs()) }
        }

        eventually { assertEquals(ListenerConnection.SLOW_CONSUMER_CODE, stuck.closeCode) }
        assertEquals(null, healthy.closeCode)
        stuck.unblock()
    }

    private class RecordingOutbound(blockSends: Boolean = false) : Outbound {
        override val id: String = UUID.randomUUID().toString()
        private val sent = CopyOnWriteArrayList<String>()
        private val gate = CountDownLatch(if (blockSends) 1 else 0)

        @Volatile
        var closeCode: Int? = null

        override fun send(text: String) {
            gate.await()
            sent += text
        }

        override fun close(code: Int, reason: String) {
            closeCode = code
        }

        fun unblock() = gate.countDown()

        fun seqs(): List<Long> = sent.map { EventCodec.mapper.readTree(it)["seq"].asLong() }
    }
}
