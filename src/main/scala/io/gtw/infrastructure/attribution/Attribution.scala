package io.gtw.infrastructure.attribution

import com.typesafe.scalalogging.slf4j.LazyLogging
import io.gtw.infrastructure.attribution.utils.JsonUtils
import io.gtw.infrastructure.attribution.weight.PageRank
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.collection.JavaConverters._

object Attribution  extends LazyLogging {
  def main(args: Array[String]) {
    val parser = new OptionParser[Param] ("scopt") {
      head("Attribution Module")
      opt[String]('p', "inputWikipedia").action((x, c) => c.copy(inputWikipedia = Some(x))).text("input wikipedia path of the file that need extract attributes from.")
      opt[String]('d', "inputWikidata").action((x, c) => c.copy(inputWikidata = Some(x))).text("input wikidata path.")
      opt[String]('i', "inputBase").action((x, c) => c.copy(inputBase = Some(x))).text("input base. [HDFS address].")
      opt[String]('t', "outputItem").action((x, c) => c.copy(outputItem = Some(x))).text("output item path of the result that need put to.")
      opt[String]('y', "outputProperty").action((x, c) => c.copy(outputItem = Some(x))).text("output property path of the result that need put to.")
      opt[String]('o', "outputBase").action((x, c) => c.copy(outputBase = Some(x))).text("output base. [HDFS address].")
      opt[Double]('r', "tolerance").action((x, c) => c.copy(tolerance = x)).text("tolerance of PageRank.")
      opt[String]('l', "language").action((x, c) => c.copy(language = Some(x))).text("language supported.")
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
    val outputProperty: Option[String] = param.outputProperty
    val tolerance: Double = param.tolerance
    val language: Option[String] = param.language

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

    val outputPropertyPath: String = outputProperty match {
      case Some(x) => x
      case None => throw new IllegalArgumentException(s"set output property file in command line or set Env ATTRIBUTION_OUTPUT_PROPERTY")
    }

    val (languageList, languageWikiList): (Array[String], Array[String]) = language match {
      case Some(x) => (x.split('|'), x.split('|').map(x => x + "wiki"))
    }
    languageList.foreach(println)
    languageWikiList.foreach(println)

    val inputWikipediaAddress: String = inputSource + inputWikipediaPath
    val inputWikidataAddress: String = inputSource + inputWikidataPath
    val outputItemAddress: String = outputSource + outputItemPath
    val outputPropertyAddress: String = outputSource + outputPropertyPath

    val sparkConf = new SparkConf().setAppName("Attribution").setMaster("local[1]")
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
    val wikiDataPropertyRDD = wikidataSourceMapRDD.filter(wikiData => wikiData.getOrElse("type", "") == "property")

    /* join wikipedia with wikidata */
    val wikiDataItemSourceKeyedByWikiTitleRDD = wikiDataItemRDD.map(wikidataKeyedByWikiTitle)
    // wikiDataItemSourceKeyedByWikiTitleRDD.foreach(println)
    val joinedRDD = formattedWidWeightKeyedByTitle.join(wikiDataItemSourceKeyedByWikiTitleRDD)
    val formattedJoinedRDD = joinedRDD.map {
      case (_, (attributedMap, wikidataMap)) => attributedMap ++ wikidataMap
    }

    /*-- output to file --*/
    formattedJoinedRDD.map(JsonUtils.toJson).foreach(println)

    /*-- stop spark context --*/
    sparkContext.stop()
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

  def cleanWikidata(wikidataSourceMap: Map[String, Any], languageList: Array[String], LanguageWikiList: Array[String]): Map[String, Any] = {
    val labels = wikidataSourceMap.getOrElse("labels", Map())
    val descriptions = wikidataSourceMap.getOrElse("descriptions", Map())
    val aliases = wikidataSourceMap.getOrElse("aliases", Map())
    val sitelinks = wikidataSourceMap.getOrElse("sitelinks", Map())

    val filteredLabels = labels match {
      case sMap: Map[String, Any] => sMap.filter(key => languageList.contains(key._1))
    }
    val filteredDescriptions = descriptions match {
      case sMap: Map[String, Any] => sMap.filter(key => languageList.contains(key._1))
    }
    val filteredAliases = aliases match {
      case sMap: Map[String, Any] => sMap.filter(key => languageList.contains(key._1))
    }
    val filteredSitelinks = sitelinks match {
      case sMap: Map[String, Any] => sMap.filter(key => LanguageWikiList.contains(key._1))
    }
    wikidataSourceMap + ("labels" -> filteredLabels, "descriptions" -> filteredDescriptions, "aliases" -> filteredAliases, "sitelinks" -> filteredSitelinks)
  }
}
