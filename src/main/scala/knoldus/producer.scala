package knoldus
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Scanner
import io.circe.syntax.EncoderOps
import io.circe.generic.auto._
import java.util.{Properties, Scanner}

object producer extends App {
  val scanner = new Scanner(System.in)
  val props = new Properties()

  props.put("bootstrap.servers", "localhost:9092")
  props.put("client.id", "ScalaProducerExample")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "1")
  props.put("retries", "0")
  props.put("batch.size", "16384")
  props.put("linger.ms", "1")
  props.put("buffer.memory", "33554432")
  val producer: KafkaProducer[Nothing, String] = new KafkaProducer[Nothing, String](props)
  val topic = "kafka-topic"

  println(s"Sending Records in Kafka Topic [$topic]")

  for (i <- 1 to 80) {


    val record: ProducerRecord[Nothing, String] =
      new ProducerRecord(topic, createUserMessage(i).asJson.toString)
    println(record.value)

    producer.send(record)
  }

  producer.close()
  def createUserMessage(id: Int): Student ={
    var name = ""
    var course = ""
    var age = 0
    println("Enter the user name ")
    name = scanner.nextLine()

    println("Enter the course")
    course = scanner.nextLine()

    println("Enter the user age")
    age = scanner.nextInt()
    scanner.nextLine()

    val student = Student( id,name,
      course,
      age.toInt)
    student
  }


}

