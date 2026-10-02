package dev.interp.gateway.session

import jakarta.validation.Valid
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Size
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController
import java.util.UUID

data class CreateSessionResponse(val sessionId: UUID)

data class UtteranceRequest(
    @field:NotBlank
    @field:Size(max = 2000)
    val text: String,
)

data class UtteranceResponse(val seq: Long, val ingestedAtMs: Long)

@RestController
@RequestMapping("/sessions")
class SessionController(private val ingest: IngestService) {

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    fun create(): CreateSessionResponse = CreateSessionResponse(ingest.createSession())

    @PostMapping("/{sessionId}/utterances")
    @ResponseStatus(HttpStatus.ACCEPTED)
    fun utter(@PathVariable sessionId: UUID, @Valid @RequestBody request: UtteranceRequest): UtteranceResponse {
        val event = ingest.ingest(sessionId, request.text)
        return UtteranceResponse(event.seq, event.ingestedAtMs)
    }

    @ExceptionHandler(SessionNotFoundException::class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    fun notFound(e: SessionNotFoundException): Map<String, String?> = mapOf("error" to e.message)
}
