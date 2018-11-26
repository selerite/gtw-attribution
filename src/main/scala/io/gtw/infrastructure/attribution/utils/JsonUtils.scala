package io.gtw.infrastructure.attribution.utils

import com.fasterxml.jackson.databind.{JsonMappingException, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
  * Created by lvjin on 2017/1/22-14:05.
  * Description: 
  */
object JsonUtils {
  private var objectMapperRead: ObjectMapper = _
  private var objectMapperWrite: ObjectMapper = _

  /**
    * parse json to map
    *
    * @param line: json line
    * @return parsed map
    */
  def parseJson(line: String): Map[String, Any] = {
    if (objectMapperRead == null) {
      objectMapperRead = new ObjectMapper()
      objectMapperRead.registerModule(DefaultScalaModule)
    }
    try {
      val resultMap = objectMapperRead.readValue(line, classOf[Map[String, Any]])
      resultMap
    } catch {
      case ex: JsonMappingException => Map[String, Any]()
    }
  }

  /**
    * dumps map to json
    *
    * @param resMap: map
    * @return json
    */
  def toJson(resMap: Map[String, Any]): String = {
    if (objectMapperWrite == null) {
      objectMapperWrite = new ObjectMapper()
      objectMapperWrite.registerModule(DefaultScalaModule)
    }
    objectMapperWrite.writeValueAsString(resMap)
  }
}
