package dev.interp.gateway.session

import dev.interp.common.TestcontainersConfiguration
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.transaction.support.TransactionTemplate
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import kotlin.test.assertEquals
import kotlin.test.assertNull

@Import(TestcontainersConfiguration::class)
@SpringBootTest
class SeqAllocatorIT {

    @Autowired
    lateinit var ingest: IngestService

    @Autowired
    lateinit var seqAllocator: SeqAllocator

    @Autowired
    lateinit var tx: TransactionTemplate

    @Test
    fun `concurrent allocations for one session are dense and unique`() {
        val sessionId = ingest.createSession()
        val tasks = List(500) { Callable { tx.execute { seqAllocator.next(sessionId) }!! } }

        val seqs = Executors.newFixedThreadPool(20).use { pool -> pool.invokeAll(tasks).map { it.get() } }

        assertEquals((1L..500L).toList(), seqs.sorted())
    }

    @Test
    fun `unknown session yields null`() {
        assertNull(tx.execute { seqAllocator.next(UUID.randomUUID()) })
    }
}
