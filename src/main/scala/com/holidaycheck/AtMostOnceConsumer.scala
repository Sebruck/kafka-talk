package com.holidaycheck

import java.util.{Properties, UUID}

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}

import scala.collection.JavaConverters._

object AtMostOnceConsumer extends App {
  val PollTimeoutMs = 100
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("group.id", "at-most-once-consumer-group")
  props.put("enable.auto.commit", "false")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("auto.offset.reset", "earliest")

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(List(Topic.multiPartition).asJava)

  while (true) {
    val records = consumer.poll(PollTimeoutMs).asScala
    consumer.commitSync()

    // If something fails here the offset is already commited
    records.foreach(operationWhichCouldFail)
  }

  def operationWhichCouldFail(record: ConsumerRecord[String, String]): Unit = {
    println(s"Got record with key: ${record.key()} and value: ${record.value()}")
  }
}
