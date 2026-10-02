package dev.interp.persister

import dev.interp.common.EventCodec
import dev.interp.common.Lang
import dev.interp.common.TestcontainersConfiguration
import dev.interp.common.Topics
import dev.interp.common.TranslatedText
import dev.interp.common.eventually
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.kafka.core.KafkaTemplate
import java.util.UUID
import kotlin.test.assertEquals

@Import(TestcontainersConfiguration::class)
@SpringBootTest(
    properties = [
        "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
        "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer",
    ],
)
class TranslationPersisterIT {

    @Autowired
    lateinit var kafka: KafkaTemplate<String, String>

    @Autowired
    lateinit var jdbc: JdbcClient

    @Test
    fun `redelivered translations are stored once`() {
        val sessionId = UUID.randomUUID()
        jdbc.sql("INSERT INTO session (id) VALUES (?)").param(sessionId).update()
        val first = TranslatedText(sessionId, Lang.EN, 1, "[en] a", 1_000, 1_100)
        val second = first.copy(seq = 2, text = "[en] b")

        listOf(first, second, first, second, first).forEach {
            kafka.send(Topics.translated(Lang.EN), sessionId.toString(), EventCodec.encode(it)).get()
        }

        eventually(timeoutMs = 20_000) {
            val rows = jdbc.sql("SELECT seq, text FROM translation WHERE session_id = ? ORDER BY seq")
                .param(sessionId)
                .query { rs, _ -> rs.getLong("seq") to rs.getString("text") }
                .list()
            assertEquals(listOf(1L to "[en] a", 2L to "[en] b"), rows)
        }
    }
}
