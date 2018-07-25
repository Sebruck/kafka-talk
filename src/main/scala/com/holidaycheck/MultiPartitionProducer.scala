package com.holidaycheck

import java.util.{Properties, UUID}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object MultiPartitionProducer extends App {

  val NumberOfPartitions = 3
  TopicCreator.createTopic(Topic.multiPartition, NumberOfPartitions)

  val producerProps = new Properties()
  producerProps.put("bootstrap.servers", "localhost:9092")
  producerProps.put("acks", "all") // Producer waits for all replicas to Ack
  producerProps.put("key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  producerProps.put("value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](producerProps)

  while (true) {
    val key =  UUID.randomUUID().toString
    val value =  System.nanoTime().toString
    val partition = randomPartition()
    println(s"producing record into partition $partition")
    val record = new ProducerRecord[String, String](Topic.multiPartition, partition, key, value)
    producer
      .send(record)
      .get() // This is blocking! Should be wrapped in a Future or something similar

    Thread.sleep(1000)
  }

  def randomPartition(): Integer = scala.util.Random.nextInt(NumberOfPartitions)
}
