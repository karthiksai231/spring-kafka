package com.kafka.demo;

import java.util.Properties;

public class TestApplication {
  public static void main(String[] args) {
    Producer producer = new Producer();
    Properties properties = producer.configureProducer();
    producer.producer(properties);
    Consumer consumer = new Consumer();
    Properties consumerProperties = consumer.configureConsumer();
    consumer.consumeMessages(consumerProperties);

  }
}
