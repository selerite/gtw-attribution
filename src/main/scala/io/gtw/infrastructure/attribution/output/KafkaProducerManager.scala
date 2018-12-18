package io.gtw.infrastructure.attribution.output

import org.apache.kafka.clients.producer.{KafkaProducer, Producer}
import java.util.Properties
import java.io.FileInputStream

object KafkaProducerManager {
  def getProducer(kafkaBrokers: String): Producer[String, String] = {
    val properties: Properties = new Properties()
    val path = getClass.getClassLoader.getResource("kafkaConfiguration.properties").getPath.replaceAll("%20", " ")
    properties.load(new FileInputStream(path))
    properties.setProperty("bootstrap.servers", kafkaBrokers)
    new KafkaProducer[String, String](properties)
  }

  def close(producer: Producer[String, String]): Unit = {
    if (producer != null) {
      producer.close()
    }
  }
}
