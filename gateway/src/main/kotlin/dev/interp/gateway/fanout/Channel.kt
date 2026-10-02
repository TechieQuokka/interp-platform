package dev.interp.gateway.fanout

import dev.interp.common.TranslatedText

/**
 * All state for one (session, lang): the dedupe watermark, the replay ring buffer and the
 * connected listeners. Every mutation happens under the channel's lock, so a listener that
 * subscribes sees a ring snapshot and then exactly the live messages published after it.
 */
class Channel(ringCapacity: Int) {

    enum class PublishResult { DELIVERED, DUPLICATE, GAP }

    private val ring = RingBuffer<TranslatedText>(ringCapacity)
    private val listeners = LinkedHashSet<ListenerConnection>()
    private var lastSeq = 0L

    @Synchronized
    fun publish(event: TranslatedText): PublishResult {
        // Kafka delivery is at-least-once, so redelivered records show up with an old seq.
        if (event.seq <= lastSeq) return PublishResult.DUPLICATE
        val gap = lastSeq > 0 && event.seq > lastSeq + 1
        lastSeq = event.seq
        ring.add(event)
        listeners.toList().forEach { it.offer(event) }
        return if (gap) PublishResult.GAP else PublishResult.DELIVERED
    }

    /** Registers [listener] for live messages and returns the ring contents at that instant. */
    @Synchronized
    fun subscribe(listener: ListenerConnection): List<TranslatedText> {
        listeners += listener
        return ring.snapshot()
    }

    @Synchronized
    fun unsubscribe(listener: ListenerConnection) {
        listeners -= listener
    }

    @Synchronized
    fun listenerCount(): Int = listeners.size
}
