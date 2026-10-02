package dev.interp.gateway.fanout

/** The sending side of one listener connection; a thin seam over WebSocketSession for testing. */
interface Outbound {
    val id: String

    /** May block while the client is slow to read. */
    fun send(text: String)

    fun close(code: Int, reason: String)
}
