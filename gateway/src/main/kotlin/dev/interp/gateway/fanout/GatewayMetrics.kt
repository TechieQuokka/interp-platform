package dev.interp.gateway.fanout

import dev.interp.common.Lang
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

@Component
class GatewayMetrics(private val registry: MeterRegistry) {

    private val activeListeners = AtomicInteger().also {
        registry.gauge("interp.listener.active", it)
    }

    private fun perLang(name: String) = Lang.entries.associateWith { Counter.builder(name).tag("lang", it.code).register(registry) }

    private val received = perLang("interp.fanout.received")
    private val duplicates = perLang("interp.fanout.duplicates")
    private val gaps = perLang("interp.fanout.gaps")
    private val delivered = perLang("interp.listener.delivered")
    private val latency = Lang.entries.associateWith {
        Timer.builder("interp.e2e.latency")
            .description("Speaker ingest to listener send")
            .tag("lang", it.code)
            .register(registry)
    }

    fun received(lang: Lang) = received.getValue(lang).increment()

    fun duplicate(lang: Lang) = duplicates.getValue(lang).increment()

    fun gap(lang: Lang) = gaps.getValue(lang).increment()

    fun delivered(lang: Lang, ingestedAtMs: Long) {
        delivered.getValue(lang).increment()
        latency.getValue(lang).record(Duration.ofMillis(System.currentTimeMillis() - ingestedAtMs))
    }

    fun listenerOpened() = activeListeners.incrementAndGet()

    fun listenerClosed() = activeListeners.decrementAndGet()

    fun disconnected(reason: String) = registry.counter("interp.listener.disconnects", "reason", reason).increment()

    fun replayed(source: String, count: Int) = registry.counter("interp.replay.messages", "source", source).increment(count.toDouble())
}
