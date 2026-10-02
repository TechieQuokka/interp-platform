package dev.interp.gateway.session

import org.springframework.data.jpa.repository.JpaRepository
import java.util.UUID

interface SessionRepository : JpaRepository<SessionEntity, UUID>
