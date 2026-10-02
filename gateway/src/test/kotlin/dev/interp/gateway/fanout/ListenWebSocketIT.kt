package dev.interp.gateway.fanout

import dev.interp.common.EventCodec
import dev.interp.common.Lang
import dev.interp.common.TestcontainersConfiguration
import dev.interp.common.Topics
import dev.interp.common.TranslatedText
import dev.interp.common.eventually
import dev.interp.gateway.session.IngestService
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.utils.ContainerTestUtils
import java.net.URI
import java.net.http.HttpClient
import java.net.http.WebSocket
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletionStage
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals

@Import(TestcontainersConfiguration::class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ListenWebSocketIT {

    @LocalServerPort
    var port: Int = 0

    @Autowired
    lateinit var kafka: KafkaTemplate<String, String>

    @Autowired
    lateinit var ingest: IngestService

    @Autowired
    lateinit var jdbc: JdbcClient

    @Autowired
    lateinit var registry: KafkaListenerEndpointRegistry

    @BeforeEach
    fun waitForFanOutConsumer() {
        // The fan-out group starts at the latest offset, so records sent before assignment are not seen.
        ContainerTestUtils.waitForAssignment(registry.getListenerContainer("fan-out")!!, 3 * 6)
    }

    private fun event(sessionId: UUID, seq: Long) =
        TranslatedText(sessionId, Lang.KO, seq, "[ko] $seq", System.currentTimeMillis(), System.currentTimeMillis())

    private fun publish(event: TranslatedText) {
        kafka.send(Topics.translated(event.lang), event.sessionId.toString(), EventCodec.encode(event)).get()
    }

    @Test
    fun `listener receives translations in order, exactly once`() {
        val sessionId = ingest.createSession()
        val client = Listener("ws://localhost:$port/ws/listen?session=$sessionId&lang=ko")

        listOf(1L, 2L, 2L, 3L, 1L, 4L).forEach { publish(event(sessionId, it)) }

        eventually { assertEquals(listOf(1L, 2L, 3L, 4L), client.seqs()) }
        client.close()
    }

    @Test
    fun `reconnect on a fresh gateway replays persisted history`() {
        val sessionId = ingest.createSession()
        (1L..5L).forEach { seq ->
            jdbc.sql(
                "INSERT INTO translation (session_id, lang, seq, text, ingested_at, translated_at) VALUES (?, 'ko', ?, ?, ?, ?)",
            ).params(sessionId, seq, "[ko] $seq", Timestamp.from(Instant.now()), Timestamp.from(Instant.now())).update()
        }

        val client = Listener("ws://localhost:$port/ws/listen?session=$sessionId&lang=ko&lastSeq=2")
        eventually { assertEquals(listOf(3L, 4L, 5L), client.seqs()) }

        publish(event(sessionId, 6))
        eventually { assertEquals(listOf(3L, 4L, 5L, 6L), client.seqs()) }
        client.close()
    }

    @Test
    fun `invalid query is rejected with a policy close`() {
        val client = Listener("ws://localhost:$port/ws/listen?session=nope&lang=ko")
        eventually { assertEquals(1007, client.closeCode) }
    }

    private class Listener(uri: String) : WebSocket.Listener {
        private val received = CopyOnWriteArrayList<Long>()
        private val socket: WebSocket = HttpClient.newHttpClient().newWebSocketBuilder().buildAsync(URI(uri), this).join()

        @Volatile
        var closeCode: Int? = null

        override fun onText(webSocket: WebSocket, data: CharSequence, last: Boolean): CompletionStage<*>? {
            received += EventCodec.mapper.readTree(data.toString())["seq"].asLong()
            webSocket.request(1)
            return null
        }

        override fun onClose(webSocket: WebSocket, statusCode: Int, reason: String): CompletionStage<*>? {
            closeCode = statusCode
            return null
        }

        fun seqs(): List<Long> = received.toList()

        fun close() {
            socket.sendClose(WebSocket.NORMAL_CLOSURE, "bye").join()
        }
    }
}
