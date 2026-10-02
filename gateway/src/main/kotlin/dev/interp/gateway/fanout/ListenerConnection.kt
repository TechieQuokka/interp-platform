package dev.interp.gateway.fanout

import dev.interp.common.EventCodec
import dev.interp.common.TranslatedText
import org.slf4j.LoggerFactory
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * One listener: a bounded queue filled by the fan-out thread and drained by a dedicated virtual
 * thread, which is also the only thread that writes to the socket.
 *
 * The fan-out side never blocks: when the queue is full the listener is disconnected as a slow
 * consumer and is expected to reconnect with its last received seq.
 */
class ListenerConnection(
    private val outbound: Outbound,
    capacity: Int,
    private val metrics: GatewayMetrics,
    private val onClosed: (ListenerConnection) -> Unit,
) {
    private val queue = ArrayBlockingQueue<TranslatedText>(capacity)
    private val closed = AtomicBoolean(false)

    // Only touched by the sender thread.
    private var lastSentSeq = 0L

    val id: String get() = outbound.id

    /** Called by the fan-out thread. Returns false if the message was not accepted. */
    fun offer(event: TranslatedText): Boolean {
        if (closed.get()) return false
        if (queue.offer(event)) return true
        disconnect(SLOW_CONSUMER_CODE, "slow consumer")
        return false
    }

    /**
     * Starts the sender. [replay] is sent first; afterwards the queue is drained. Any message with
     * seq <= the last one sent is skipped, which removes the overlap between replay and live traffic.
     */
    fun start(replay: List<TranslatedText>, startAfterSeq: Long) {
        lastSentSeq = startAfterSeq
        metrics.listenerOpened()
        Thread.ofVirtual().name("listener-$id").start { run(replay) }
    }

    fun disconnect(code: Int, reason: String) {
        if (!closed.compareAndSet(false, true)) return
        metrics.disconnected(reason)
        // Closing can block on a stuck client, so never do it on the caller's (fan-out) thread.
        Thread.ofVirtual().start {
            try {
                outbound.close(code, reason)
            } catch (e: Exception) {
                log.debug("close failed for {}", id, e)
            }
        }
    }

    /** The peer already closed the socket. */
    fun stop() {
        closed.set(true)
    }

    private fun run(replay: List<TranslatedText>) {
        try {
            replay.forEach(::deliver)
            while (!closed.get()) {
                val event = queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS) ?: continue
                deliver(event)
            }
        } catch (e: Exception) {
            log.debug("listener {} failed", id, e)
            disconnect(1011, "send failed")
        } finally {
            closed.set(true)
            metrics.listenerClosed()
            onClosed(this)
        }
    }

    private fun deliver(event: TranslatedText) {
        if (event.seq <= lastSentSeq || closed.get()) return
        outbound.send(EventCodec.encode(ListenerMessage.of(event)))
        lastSentSeq = event.seq
        metrics.delivered(event.lang, event.ingestedAtMs)
    }

    companion object {
        const val SLOW_CONSUMER_CODE = 4000
        private const val POLL_TIMEOUT_MS = 500L
        private val log = LoggerFactory.getLogger(ListenerConnection::class.java)
    }
}
