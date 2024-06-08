package put.poznan.pl.klaudiak
package producers

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.io.File
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.io.Source

object FlightsProducer extends App {

  if (args.length != 5) {
    println("Należy podać pięć parametrów: " +
      "inputDir sleepTime topicName headerLength bootstrapServers")
    System.exit(0)
  }

  val inputDir = args(0)
  val sleepTime = args(1).toInt
  val topicName = args(2)
  val headerLength = args(3).toInt
  val bootstrapServers = args(4)

  val props = new Properties
  props.put("bootstrap.servers", bootstrapServers)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "all")
  props.put("retries", 0)
  props.put("batch.size", 16384)
  props.put("linger.ms", 1)
  props.put("buffer.memory", 33554432)

  val producer = new KafkaProducer[String, String](props)

  val folder = new File(inputDir)
  val listOfFiles = folder.listFiles()
  val listOfPaths = listOfFiles.map(_.getAbsolutePath).sorted

  for (fileName <- listOfPaths) {
    try {
      val source = Source.fromFile(fileName)
      val lines = source.getLines().drop(headerLength)
      lines.foreach(line => producer.send(new ProducerRecord(topicName, line.hashCode.toString, line)))
      TimeUnit.SECONDS.sleep(sleepTime)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  producer.close()
}