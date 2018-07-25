package com.holidaycheck

import java.util.{Properties, UUID}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Producer extends App {

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
    val record = new ProducerRecord[String, String](Topic.singlePartition, key, value)
    producer
      .send(record)
      .get() // This is blocking! Should be wrapped in a Future or something similar

    Thread.sleep(1000)
  }
}
