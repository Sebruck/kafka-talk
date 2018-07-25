package com.holidaycheck

import java.util.Properties

import org.apache.kafka.clients.admin.{AdminClient, NewTopic}

import scala.collection.JavaConverters._

object TopicCreator {

  def createTopic(name: String, partitions: Int): Unit = {
    val adminProps = new Properties()
    adminProps.setProperty("bootstrap.servers", "localhost:9092")
    val adminClient = AdminClient.create(adminProps)

    // Create topic
    val topic = new NewTopic(name, partitions, 1)
    adminClient.createTopics(List(topic).asJavaCollection)
  }
}
