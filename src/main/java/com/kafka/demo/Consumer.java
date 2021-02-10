package com.kafka.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class Consumer {
  public  Properties configureConsumer() {
    Properties properties = new Properties();

    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-first-application");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    return properties;
  }

  public void consumeMessages(Properties properties) {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    ConsumerThread consumerThread = new ConsumerThread(countDownLatch, properties);
    Thread thread = new Thread(consumerThread);
    thread.start();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Caught shutdown hook");
      consumerThread.stop();
      try {
        countDownLatch.await();
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
    }));
    try {
      countDownLatch.await();
    } catch (Exception ex) {
      log.error("Consumer Flow Interrupted");
    } finally {
      log.info("Consumer closing!!!");
    }
  }
}
