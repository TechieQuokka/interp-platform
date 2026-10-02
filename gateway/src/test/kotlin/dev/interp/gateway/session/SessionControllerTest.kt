package dev.interp.gateway.session

import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.BDDMockito.given
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.post
import org.springframework.transaction.CannotCreateTransactionException
import java.util.UUID

@WebMvcTest(SessionController::class)
class SessionControllerTest {

    @Autowired
    lateinit var mvc: MockMvc

    @MockitoBean
    lateinit var ingest: IngestService

    private val sessionId = UUID.randomUUID()

    private fun utter(body: String) = mvc.post("/sessions/$sessionId/utterances") {
        contentType = MediaType.APPLICATION_JSON
        content = body
    }

    private fun validBody() = """{"utteranceId":"${UUID.randomUUID()}","text":"hello"}"""

    private fun givenIngestThrows(e: Throwable) {
        given(ingest.ingest(any() ?: sessionId, any() ?: sessionId, anyString())).willThrow(e)
    }

    @Test
    fun `kafka unavailable maps to 503`() {
        givenIngestThrows(IngestUnavailableException(RuntimeException("broker down")))
        utter(validBody()).andExpect { status { isServiceUnavailable() } }
    }

    @Test
    fun `database unavailable maps to 503`() {
        givenIngestThrows(CannotCreateTransactionException("no connection"))
        utter(validBody()).andExpect { status { isServiceUnavailable() } }
    }

    @Test
    fun `missing utteranceId is rejected`() {
        utter("""{"text":"hello"}""").andExpect { status { isBadRequest() } }
    }
}
