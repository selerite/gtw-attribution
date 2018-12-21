package io.gtw.infrastructure.attribution.weight

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

object PageRank {
  def pageRank(sc: SparkContext, sourceMapRDD: RDD[Map[String, Any]], tolerance: Double): RDD[(Long, Double)] = {
    /* -- convert to RDD[Map[`wid`, `widTitle`] and broadcast as dictionary -- */
    // cache like redis may be another good way regardless of networking cost.
    val wikiTitleWidRDD = sourceMapRDD.map(wikiTitleWidMapping)
    val wikiTitleWidMap = wikiTitleWidRDD.collectAsMap().toMap
    val wikiTitleWidMapBroadcast = sc.broadcast(wikiTitleWidMap)

    /* -- page rank to generate weight -- */
    val pageRankEdgesRDD = sourceMapRDD.flatMap(generatePageRankEdges(_, wikiTitleWidMapBroadcast))
    // wikiTitleWidMapBroadcast.destroy()
    val graph = Graph.fromEdgeTuples(pageRankEdgesRDD, 0.15)
    val weightRDD = graph.pageRank(tolerance).vertices
    weightRDD
  }

  private def wikiTitleWidMapping(sourceMap: Map[String, Any]): (String, Long) = {
    val wikiTitle = sourceMap.getOrElse("title", "xxx").toString
    val wid = sourceMap.getOrElse("wid", "0").toString.toLong
    (wikiTitle, wid)
  }

  private def generatePageRankEdges(sourceMap: Map[String, Any], wikiTitleWidMapBroadcast: Broadcast[Map[String, Long]]): List[(Long, Long)] = {
    val wid = sourceMap.getOrElse("wid", "0").toString.toLong
    val linkList: List[Long] = sourceMap.get("links") match {
      case Some(theList) =>
        val xList = theList match {
          case x: List[Any] => x
          case x: java.util.ArrayList[Any] => x.asScala.toList
        }
        xList.map { theMap =>
          val xMap = theMap match {
            case x: Map[String, String] => x
            case x: java.util.LinkedHashMap[String, String] => x.asScala.toMap
          }
          val wikiTitle = xMap.getOrElse("id", "x")
          wikiTitleWidMapBroadcast.value.getOrElse(wikiTitle, "0").toString.toLong
        }
      case None => List()
    }
    linkList.map(linkId => (wid, linkId))
  }
}
