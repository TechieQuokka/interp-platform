package dev.interp.gateway.fanout

import dev.interp.common.Lang
import dev.interp.common.TranslatedText
import dev.interp.gateway.GatewayProperties
import org.springframework.stereotype.Component
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

@Component
class ChannelHub(
    private val properties: GatewayProperties,
    private val history: TranslationHistory,
    private val metrics: GatewayMetrics,
) {
    private data class Key(val sessionId: UUID, val lang: Lang)

    private val channels = ConcurrentHashMap<Key, Channel>()

    private fun channel(sessionId: UUID, lang: Lang) =
        channels.computeIfAbsent(Key(sessionId, lang)) { Channel(properties.ringBufferCapacity) }

    fun publish(event: TranslatedText) {
        metrics.received(event.lang)
        when (channel(event.sessionId, event.lang).publish(event)) {
            Channel.PublishResult.DUPLICATE -> metrics.duplicate(event.lang)
            Channel.PublishResult.GAP -> metrics.gap(event.lang)
            Channel.PublishResult.DELIVERED -> Unit
        }
    }

    /**
     * Connects a listener. With [lastSeq] == null the listener only gets live traffic; otherwise
     * everything after [lastSeq] is replayed first — from the ring buffer when it still covers the
     * range, from Postgres when it does not.
     */
    fun connect(sessionId: UUID, lang: Lang, lastSeq: Long?, outbound: Outbound): ListenerConnection {
        val channel = channel(sessionId, lang)
        val listener = ListenerConnection(outbound, properties.listenerQueueCapacity, metrics) { channel.unsubscribe(it) }
        val ring = channel.subscribe(listener)
        if (lastSeq == null) {
            listener.start(emptyList(), 0)
            return listener
        }
        val fromRing = ring.filter { it.seq > lastSeq }
        val ringCovers = ring.isNotEmpty() && ring.first().seq <= lastSeq + 1
        val replay = if (ringCovers) {
            fromRing
        } else {
            val fromDb = history.after(sessionId, lang, lastSeq, properties.maxDbReplay)
            metrics.replayed("db", fromDb.size)
            (fromDb + fromRing).distinctBy { it.seq }.sortedBy { it.seq }
        }
        metrics.replayed("buffer", fromRing.size)
        listener.start(replay, lastSeq)
        return listener
    }
}
