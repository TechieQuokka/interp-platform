package dev.interp.common

enum class Lang(val code: String) {
    KO("ko"),
    EN("en"),
    JA("ja");

    companion object {
        fun of(code: String): Lang =
            entries.firstOrNull { it.code == code } ?: throw IllegalArgumentException("Unsupported lang: $code")
    }
}
