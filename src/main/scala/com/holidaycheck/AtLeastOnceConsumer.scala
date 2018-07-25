package com.holidaycheck

import java.util.{Properties, UUID}

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}

import scala.collection.JavaConverters._

object AtLeastOnceConsumer extends App {
  val PollTimeoutMs = 100
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("group.id", "at-least-once-consumer-group")
  props.put("enable.auto.commit", "false")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("auto.offset.reset", "earliest")

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(List(Topic.singlePartition).asJava)

  while (true) {
    val records = consumer.poll(PollTimeoutMs).asScala

    // If something fails here the offset is not commited,
    // therefore the consumer group will continue at the last commit offset
    records.foreach(operationWhichCouldFail)

    consumer.commitSync()
  }

  def operationWhichCouldFail(record: ConsumerRecord[String, String]): Unit = {
    println(s"Got record with key: ${record.key()} and value: ${record.value()}")
  }
}
