package dev.interp.common

object Topics {
    const val SOURCE_TEXT = "source-text"
    const val TRANSLATED_KO = "translated.ko"
    const val TRANSLATED_EN = "translated.en"
    const val TRANSLATED_JA = "translated.ja"

    fun translated(lang: Lang): String = "translated.${lang.code}"

    val allTranslated: List<String> = Lang.entries.map(::translated)
}
