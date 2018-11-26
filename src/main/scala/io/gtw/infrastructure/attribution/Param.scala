package io.gtw.infrastructure.attribution

import io.gtw.infrastructure.attribution.utils.EnvUtils

case class Param (
                   inputWikipedia: Option[String] = EnvUtils.inputWikipediaFromEnv,
                   inputWikidata: Option[String] = EnvUtils.inputWikidataFromEnv,
                   inputBase: Option[String] = EnvUtils.inputBaseFromEnv,
                   outputItem: Option[String] = EnvUtils.outputItemFromEnv,
                   outputProperty: Option[String] = EnvUtils.outputPropertyFromEnv,
                   outputBase: Option[String] = EnvUtils.outputBaseFromEnv,
                   tolerance: Double = EnvUtils.toleranceFromEnv,
                   language: Option[String] = EnvUtils.languageFromEnv
                 )
