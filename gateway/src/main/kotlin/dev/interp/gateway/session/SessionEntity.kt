package dev.interp.gateway.session

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.PostLoad
import jakarta.persistence.PostPersist
import jakarta.persistence.Table
import jakarta.persistence.Transient
import org.springframework.data.domain.Persistable
import java.time.Instant
import java.util.UUID

@Entity
@Table(name = "session")
class SessionEntity(
    @Id
    private val id: UUID,
    val lastSeq: Long = 0,
    val createdAt: Instant = Instant.now(),
) : Persistable<UUID> {

    // The id is assigned by us, so without this save() would merge() and issue a needless SELECT first.
    @Transient
    private var newEntity = true

    override fun getId(): UUID = id

    override fun isNew(): Boolean = newEntity

    @PostPersist
    @PostLoad
    fun markNotNew() {
        newEntity = false
    }
}
