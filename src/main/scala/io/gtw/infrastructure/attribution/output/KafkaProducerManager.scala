package io.gtw.infrastructure.attribution.output

import org.apache.kafka.clients.producer.{KafkaProducer, Producer}
import java.util.Properties

object KafkaProducerManager {
  def getProducer(kafkaBrokers: String): Producer[String, String] = {
    val properties: Properties = new Properties()
    properties.setProperty("retries", "3")
    properties.setProperty("batch.size", "16384")
    properties.setProperty("linger.ms", "1")
    properties.setProperty("buffer.memory", "33554432")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("bootstrap.servers", kafkaBrokers)
    new KafkaProducer[String, String](properties)
  }

  def close(producer: Producer[String, String]): Unit = {
    if (producer != null) {
      producer.close()
    }
  }
}
