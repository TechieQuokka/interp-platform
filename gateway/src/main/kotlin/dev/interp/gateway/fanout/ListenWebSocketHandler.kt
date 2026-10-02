package dev.interp.gateway.fanout

import dev.interp.common.Lang
import org.springframework.stereotype.Component
import org.springframework.web.socket.CloseStatus
import org.springframework.web.socket.TextMessage
import org.springframework.web.socket.WebSocketSession
import org.springframework.web.socket.handler.TextWebSocketHandler
import org.springframework.web.util.UriComponentsBuilder
import java.util.UUID

/** `/ws/listen?session={uuid}&lang={ko|en|ja}[&lastSeq={n}]` — server-to-client only. */
@Component
class ListenWebSocketHandler(private val hub: ChannelHub) : TextWebSocketHandler() {

    override fun afterConnectionEstablished(session: WebSocketSession) {
        val params = UriComponentsBuilder.fromUri(requireNotNull(session.uri)).build().queryParams
        val request = runCatching {
            Triple(
                UUID.fromString(params.getFirst("session")),
                Lang.of(params.getFirst("lang") ?: ""),
                params.getFirst("lastSeq")?.toLong(),
            )
        }.getOrElse {
            session.close(CloseStatus.BAD_DATA.withReason("expected session, lang and optional lastSeq"))
            return
        }
        val (sessionId, lang, lastSeq) = request
        session.attributes[LISTENER] = hub.connect(sessionId, lang, lastSeq, WebSocketOutbound(session))
    }

    override fun handleTextMessage(session: WebSocketSession, message: TextMessage) {
        // Listeners are receive-only.
    }

    override fun afterConnectionClosed(session: WebSocketSession, status: CloseStatus) {
        (session.attributes[LISTENER] as? ListenerConnection)?.stop()
    }

    private class WebSocketOutbound(private val session: WebSocketSession) : Outbound {
        override val id: String = session.id

        override fun send(text: String) = session.sendMessage(TextMessage(text))

        override fun close(code: Int, reason: String) = session.close(CloseStatus(code, reason))
    }

    companion object {
        private const val LISTENER = "listener"
    }
}
