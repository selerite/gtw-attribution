package io.gtw.infrastructure.attribution.utils

import scala.util.Properties

object EnvUtils {
  def inputBaseFromEnv: Option[String] = Properties.envOrNone("ATTRITION_INPUT_BASE")
  def inputWikipediaFromEnv: Option[String] = Properties.envOrSome("ATTRITION_INPUT_WIKIPEDIA", Some("D:\\self\\gtw\\data\\wikipedia_head_less_json_lines.json"))
  def inputWikidataFromEnv: Option[String] = Properties.envOrSome("ATTRIBUTION_INPUT_WIKIDATA", Some("D:\\self\\gtw\\data\\wikidata_head200_with_1_modified.json"))
  def outputBaseFromEnv: Option[String] = Properties.envOrNone("ATTRITION_OUTPUT_BASE")
  def outputItemFromEnv: Option[String] = Properties.envOrSome("ATTRIBUTION_OUTPUT_ITEM", Some("xxx"))
  def outputRelationshipFromEnv: Option[String] = Properties.envOrSome("ATTRIBUTION_OUTPUT_RELATIONSHIP", Some("xxx"))
  def outputPropertyFromEnv: Option[String] = Properties.envOrSome("ATTRIBUTION_OUTPUT_PROPERTY", Some("xxx"))
  def toleranceFromEnv: Double = Properties.envOrElse("ATTRIBUTION_TOLERANCE", "0.01").toDouble
  def languageFromEnv: Option[String] = Properties.envOrSome("ATTRIBUTION_LANGUAGE", Some("en|zh"))
  def kafkaBrokersFromEnv: Option[String] = Properties.envOrSome("ATTRIBUTION_KAFKA_BROKERS", Some("10.131.40.191:9092"))
  def kafkaTopicItemFromEnv: Option[String] = Properties.envOrSome("ATTRIBUTION_KAFKA_TOPIC_ITEM", Some("items"))
  def kafkaTopicRelationshipFromEnv: Option[String] = Properties.envOrSome("ATTRIBUTION_KAFKA_TOPIC_RELATIONSHIP", Some("relationships"))
  def kafkaTopicPropertyFromEnv: Option[String] = Properties.envOrSome("ATTRIBUTION_KAFKA_TOPIC_PROPERTY", Some("properties"))
}
