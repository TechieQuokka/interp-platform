package dev.interp.common

/** Polls [assertion] until it stops throwing or [timeoutMs] elapses (then rethrows the last failure). */
fun eventually(timeoutMs: Long = 10_000, intervalMs: Long = 50, assertion: () -> Unit) {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (true) {
        try {
            assertion()
            return
        } catch (e: AssertionError) {
            if (System.currentTimeMillis() > deadline) throw e
            Thread.sleep(intervalMs)
        }
    }
}
