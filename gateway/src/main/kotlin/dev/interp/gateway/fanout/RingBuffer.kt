package dev.interp.gateway.fanout

/** Fixed-capacity FIFO that drops the oldest element when full. Not thread-safe. */
class RingBuffer<T>(private val capacity: Int) {
    init {
        require(capacity > 0) { "capacity must be positive" }
    }

    private val items = ArrayDeque<T>(capacity)

    val size: Int get() = items.size

    fun add(item: T) {
        if (items.size == capacity) items.removeFirst()
        items.addLast(item)
    }

    fun firstOrNull(): T? = items.firstOrNull()

    fun snapshot(): List<T> = items.toList()
}
