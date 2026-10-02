package dev.interp.gateway.fanout

import org.springframework.context.annotation.Configuration
import org.springframework.web.socket.config.annotation.EnableWebSocket
import org.springframework.web.socket.config.annotation.WebSocketConfigurer
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry

@Configuration(proxyBeanMethods = false)
@EnableWebSocket
class WebSocketConfig(private val handler: ListenWebSocketHandler) : WebSocketConfigurer {

    override fun registerWebSocketHandlers(registry: WebSocketHandlerRegistry) {
        registry.addHandler(handler, "/ws/listen").setAllowedOrigins("*")
    }
}
