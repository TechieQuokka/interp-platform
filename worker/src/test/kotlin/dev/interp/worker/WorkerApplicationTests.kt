package dev.interp.worker

import dev.interp.common.KafkaContainerConfiguration
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import

@Import(KafkaContainerConfiguration::class)
@SpringBootTest
class WorkerApplicationTests {

    @Test
    fun contextLoads() {
    }
}
