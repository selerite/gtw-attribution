package io.gtw.infrastructure.attribution

import com.typesafe.scalalogging.LazyLogging
import io.gtw.infrastructure.attribution.output.KafkaProducerManager
import io.gtw.infrastructure.attribution.utils.JsonUtils
import io.gtw.infrastructure.attribution.weight.PageRank
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object Attribution  extends LazyLogging {
  def main(args: Array[String]) {
    val parser = new OptionParser[Param] ("scopt") {
      head("Attribution Module")
      opt[String]('p', "inputWikipedia").action((x, c) => c.copy(inputWikipedia = Some(x))).text("input wikipedia path of the file that need extract attributes from.")
      opt[String]('d', "inputWikidata").action((x, c) => c.copy(inputWikidata = Some(x))).text("input wikidata path.")
      opt[String]('i', "inputBase").action((x, c) => c.copy(inputBase = Some(x))).text("input base. [HDFS address].")
      opt[String]('t', "outputItem").action((x, c) => c.copy(outputItem = Some(x))).text("output item path of the result that need put to.")
      opt[String]('r', "outputRelationship").action((x, c) => c.copy(outputRelationship = Some(x))).text("output relationship path of the result that need put to.")
      opt[String]('y', "outputProperty").action((x, c) => c.copy(outputProperty = Some(x))).text("output property path of the result that need put to.")
      opt[String]('o', "outputBase").action((x, c) => c.copy(outputBase = Some(x))).text("output base. [HDFS address].")
      opt[Double]('r', "tolerance").action((x, c) => c.copy(tolerance = x)).text("tolerance of PageRank.")
      opt[String]('l', "language").action((x, c) => c.copy(language = Some(x))).text("language supported.")
      opt[String]('b', "kafkaBrokers").action((x, c) => c.copy(kafkaBrokers = Some(x))).text("kafka brokers to output")
      opt[String]('e', "kafkaTopicItem").action((x, c) => c.copy(kafkaTopicItem = Some(x))).text("kafka topic item to output")
      opt[String]('h', "kafkaTopicRelationship").action((x, c) => c.copy(kafkaTopicRelationship = Some(x))).text("kafka topic relationship to output")
      opt[String]('s', "kafkaTopicProperty").action((x, c) => c.copy(kafkaTopicProperty = Some(x))).text("kafka topic properties to output")
    }
    parser.parse(args, Param()) match {
      case Some(param) =>
        run(param)
      case None =>
        parser.showUsageAsError()
        logger.error("parser is error.")
    }
  }

  def run(param: Param): Unit = {
    val inputBase: Option[String] = param.inputBase
    val inputWikipedia: Option[String] = param.inputWikipedia
    val inputWikidata: Option[String] = param.inputWikidata
    val outputBase: Option[String] = param.outputBase
    val outputItem: Option[String] = param.outputItem
    val outputRelationship: Option[String] = param.outputRelationship
    val outputProperty: Option[String] = param.outputProperty
    val tolerance: Double = param.tolerance
    val language: Option[String] = param.language
    val kafkaBrokers: Option[String] = param.kafkaBrokers
    val kafkaTopicItem: Option[String] = param.kafkaTopicItem
    val kafkaTopicRelationship: Option[String] = param.kafkaTopicRelationship
    val kafkaTopicProperty: Option[String] = param.kafkaTopicProperty

    val inputSource: String = inputBase match {
      case Some(x) => x
      case None => ""
    }

    val inputWikipediaPath: String = inputWikipedia match {
      case Some(x) => x
      case None => throw new IllegalArgumentException(s"set input wikipedia file in command line or set Env ATTRIBUTION_INPUT_WIKIPEDIA")
    }

    val inputWikidataPath: String = inputWikidata match {
      case Some(x) => x
      case None => throw new IllegalArgumentException(s"set input wikidata file in command line or set Env ATTRIBUTION_INPUT_WIKIDATA")
    }

    val outputSource: String = outputBase match {
      case Some(x) => x
      case None => ""
    }

    val outputItemPath: String = outputItem match {
      case Some(x) => x
      case None => throw new IllegalArgumentException(s"set output item file [-o] in command line or set Env ATTRIBUTION_OUTPUT_ITEM")
    }

    val outputRelationshipPath: String = outputRelationship match {
      case Some(x) => x
      case None => throw new IllegalArgumentException(s"set output relations file [-o] in command line or set Env ATTRIBUTION_OUTPUT_RELATIONSHIP")
    }

    val outputPropertyPath: String = outputProperty match {
      case Some(x) => x
      case None => throw new IllegalArgumentException(s"set output property file in command line or set Env ATTRIBUTION_OUTPUT_PROPERTY")
    }

    val (languageList, languageWikiList): (Array[String], Array[String]) = language match {
      case Some(x) => (x.split(','), x.split(',').map(x => x + "wiki"))
    }

    val kafkaBrokersStr = kafkaBrokers match {
      case Some(x) => x
      case None => throw new IllegalArgumentException(s"set kafka brokers in command line or set Env ATTRIBUTION_KAFKA_BROKERS")
    }

    val kafkaTopicItemStr = kafkaTopicItem match {
      case Some(x) => x
      case None => throw new IllegalArgumentException(s"set kafka output topic item in command line or set Env ATTRIBUTION_KAFKA_TOPIC_ITEM")
    }

    val kafkaTopicRelationshipStr = kafkaTopicRelationship match {
      case Some(x) => x
      case None => throw new IllegalArgumentException(s"set kafka output topic relationship in command line or set Env ATTRIBUTION_KAFKA_TOPIC_RELATIONSHIP")
    }

    val kafkaTopicPropertyStr = kafkaTopicProperty match {
      case Some(x) => x
      case None => throw new IllegalArgumentException(s"set kafka output topic property in command line or set Env ATTRIBUTION_KAFKA_TOPIC_PROPERTIES")
    }

    val inputWikipediaAddress: String = inputSource + inputWikipediaPath
    val inputWikidataAddress: String = inputSource + inputWikidataPath
    val outputItemAddress: String = outputSource + outputItemPath
    val outputRelationshipAddress: String = outputSource + outputRelationshipPath
    val outputPropertyAddress: String = outputSource + outputPropertyPath
    val sparkConf = new SparkConf().setAppName("Attribution")
    // val sparkConf = new SparkConf().setAppName("Attribution").setMaster("local[1]")
    val sparkContext = new SparkContext(sparkConf)

    /* -- load wikipedia data and convert every single document from json to format: Map[key, value] -- */
    val wikipediaSourceMapRDD: RDD[Map[String, Any]] = sparkContext.textFile(inputWikipediaAddress).map(jsonToMap)

    /* -- calculating <Weight>: PageRank for every document to generate weight with format: Map[`wid`: `weight`] -- */
    val weightRDD: RDD[(Long, Double)] = PageRank.pageRank(sparkContext, wikipediaSourceMapRDD, tolerance)
    val widWikiTitleRDD: RDD[(Long, String)] = wikipediaSourceMapRDD.map(widWikiTitleMapping)
    val WeightedWidWikiTitleRDD = widWikiTitleRDD.leftOuterJoin(weightRDD)
    val formattedWidWeightKeyedByTitle = WeightedWidWikiTitleRDD.map(formatToByIDWeightKeyedByTitle)
    // formattedWidWeightKeyedByTitle.foreach(println)

    /* -- load wikidata -- */
    val wikidataSourceMapRDD = sparkContext.textFile(inputWikidataAddress).map(preprocess).filter(line => line != "").map(jsonToMap).map(cleanWikidata(_, languageList, languageWikiList))
    // wikidataSourceMapRDD.foreach(println)
    val wikiDataItemRDD = wikidataSourceMapRDD.filter(wikiData => wikiData.getOrElse("type", "") == "item")
    val wikiDataRelationshipRDD = wikiDataItemRDD.flatMap(generateRelationships)
    val wikiDataPropertyRDD = wikidataSourceMapRDD.filter(wikiData => wikiData.getOrElse("type", "") == "property")

    /* -- output relationships and properties -- */
    // wikiDataPropertyRDD.map(JsonUtils.toJson).foreach(println)
    wikiDataPropertyRDD.map(JsonUtils.toJson).saveAsTextFile(outputPropertyAddress)
    wikiDataPropertyRDD.map(JsonUtils.toJson).foreachPartition(sendRecordByPartitions(kafkaBrokersStr, kafkaTopicPropertyStr, _))

    // wikiDataRelationshipRDD.map(JsonUtils.toJson).foreach(println)
    wikiDataRelationshipRDD.map(JsonUtils.toJson).saveAsTextFile(outputRelationshipAddress)
    wikiDataRelationshipRDD.map(JsonUtils.toJson).foreachPartition(sendRecordByPartitions(kafkaBrokersStr, kafkaTopicRelationshipStr, _))

    /* join wikipedia with wikidata */
    val wikiDataItemSourceKeyedByWikiTitleRDD = wikiDataItemRDD.map(wikidataKeyedByWikiTitle)
    // wikiDataItemSourceKeyedByWikiTitleRDD.foreach(println)
    val joinedRDD = formattedWidWeightKeyedByTitle.join(wikiDataItemSourceKeyedByWikiTitleRDD)
    val formattedJoinedRDD = joinedRDD.map {
      case (_, (attributedMap, wikidataMap)) => attributedMap ++ wikidataMap
    }

    /*-- output items --*/
    // formattedJoinedRDD.map(JsonUtils.toJson).foreach(println)
    formattedJoinedRDD.map(JsonUtils.toJson).saveAsTextFile(outputItemAddress)
    formattedJoinedRDD.map(JsonUtils.toJson).foreachPartition(sendRecordByPartitions(kafkaBrokersStr, kafkaTopicItemStr, _))


    /*-- stop spark context --*/
    sparkContext.stop()
  }

  def sendRecordByPartitions(kafkaBrokers: String, topic: String, partitionOfRecords: Iterator[String]): Unit = {
    val producer: Producer[String, String] = KafkaProducerManager.getProducer(kafkaBrokers)
    partitionOfRecords.foreach(record => producer.send(new ProducerRecord[String, String](topic, record)))
    KafkaProducerManager.close(producer)
  }

  private def jsonToMap(line: String): Map[String, Any] = {
    val sourceMap: Map[String, Any] = JsonUtils.parseJson(line)
    sourceMap
  }

  private def widWikiTitleMapping(sourceMap: Map[String, Any]): (Long, String) = {
    val wikiTitle = sourceMap.getOrElse("wikiTitle", "xxx").toString
    val wid = sourceMap.getOrElse("wid", "0").toString.toLong
    (wid, wikiTitle)
  }

  private def formatToByIDWeightKeyedByTitle(result: (Long, (String, Option[Double]))): (String, Map[String, Any]) = {
    val formattedResultMap = new scala.collection.mutable.HashMap[String, Any]()
    result match {
      case (wid, (wikiTitle, weight)) =>
        formattedResultMap += ("wid" -> wid)
        formattedResultMap += ("wikiTitle" -> wikiTitle)
        formattedResultMap += ("weight" -> weight.getOrElse(0.15))
        (wikiTitle, formattedResultMap.toMap)
    }
  }

  private def preprocess(line: String): String = {
    if (line != "[" && line != "]") {
      if (line.charAt(line.length() - 1) == ',') {
        line.substring(0, line.length - 1)
      } else {
        line
      }
    } else {
      ""
    }
  }

  def wikidataKeyedByWikiTitle(wikidataMap: Map[String, Any]): (String, Map[String, Any]) = {
    val siteLinks = wikidataMap.get("sitelinks")
    siteLinks match {
      case Some(siteLinksInfo) =>
        val siteLinksInfoMap: Map[String, Any] = siteLinksInfo match {
          case x: Map[String, Any] => x
          case x: java.util.LinkedHashMap[String, Any] => x.asScala.toMap
          case _ => Map[String, Any]()
        }
        siteLinksInfoMap.get("enwiki") match {
          case Some(enwikiInfo) =>
            val wikiTitle = enwikiInfo match {
              case enwikiInfoX: Map[String, String] =>
                enwikiInfoX.get("title") match {
                  case Some(title) => title
                }
              case _ => ""
            }
            (wikiTitle, wikidataMap)
          case None => ("", Map[String, Any]())
        }
      case None => ("", Map[String, Any]())
    }
  }

  private def generateRelationships(itemInfo: Map[String, Any]): List[Map[String, Any]] = {
    val srcVertex: String = itemInfo.getOrElse("id", "00").asInstanceOf[String]
    val vertexList = ListBuffer[(String, String)]()
    if (!itemInfo.isDefinedAt("type") || itemInfo.getOrElse("type", "") != "item") {
      return Nil
    }
    val claimInfoMap = itemInfo.get("claims")
    if (claimInfoMap.isDefined) {
      claimInfoMap.get.asInstanceOf[Map[String, Any]].values
        .foreach((propertyList: Any) => propertyList.asInstanceOf[List[Any]]
          .foreach((property: Any)=> {
            val mainsnakInfoMap = property.asInstanceOf[Map[String, Any]].get("mainsnak")
            if (mainsnakInfoMap.isDefined) {
              val property = mainsnakInfoMap.get.asInstanceOf[Map[String, Any]].getOrElse("property", "").toString
              val dataValueInfoMap = mainsnakInfoMap.get.asInstanceOf[Map[String, Any]].get("datavalue")
              if (dataValueInfoMap.isDefined) {
                val dataValueMap = dataValueInfoMap.get.asInstanceOf[Map[String, Any]].get("value")
                if (dataValueMap.isDefined && dataValueMap.get.isInstanceOf[Map[String, Any]]) {
                  val dataValueEntityMap = dataValueMap.get.asInstanceOf[Map[String, Any]]
                  val entity_type = dataValueEntityMap.get("entity-type")
                  if (entity_type.isDefined && entity_type.get == "item") {
                    vertexList.append(("Q" + dataValueEntityMap.getOrElse("numeric-id", 0).toString, property))
                  }
                }
              }
            }
          })
        )
    }
    vertexList.toList.map { vertex =>
      val formattedResultMap = new scala.collection.mutable.HashMap[String, Any]()
      formattedResultMap += ("src" -> srcVertex)
      formattedResultMap += ("dest" -> vertex._1)
      formattedResultMap += ("property" -> vertex._2)
      formattedResultMap.toMap
    }
  }

  def cleanWikidata(wikidataSourceMap: Map[String, Any], languageList: Array[String], LanguageWikiList: Array[String]): Map[String, Any] = {
    val labels = wikidataSourceMap.getOrElse("labels", Map())
    val descriptions = wikidataSourceMap.getOrElse("descriptions", Map())
    val aliases = wikidataSourceMap.getOrElse("aliases", Map())
    val sitelinks = wikidataSourceMap.getOrElse("sitelinks", Map())

    val filteredLabels = labels match {
      case sMap: Map[String, Any] => sMap.filter(key => languageList.contains(key._1))
      case _ => Map[String, Any]()
    }
    val filteredDescriptions = descriptions match {
      case sMap: Map[String, Any] => sMap.filter(key => languageList.contains(key._1))
      case _ => Map[String, Any]()
    }
    val filteredAliases = aliases match {
      case sMap: Map[String, Any] => sMap.filter(key => languageList.contains(key._1))
      case _ => Map[String, Any]()
    }
    val filteredSitelinks = sitelinks match {
      case sMap: Map[String, Any] => sMap.filter(key => LanguageWikiList.contains(key._1))
      case _ => Map[String, Any]()
    }
    wikidataSourceMap + ("labels" -> filteredLabels, "descriptions" -> filteredDescriptions, "aliases" -> filteredAliases, "sitelinks" -> filteredSitelinks)
  }
}
