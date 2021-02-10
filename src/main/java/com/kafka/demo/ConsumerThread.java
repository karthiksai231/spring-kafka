package com.kafka.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class ConsumerThread implements Runnable {
  private CountDownLatch countDownLatch;
  private KafkaConsumer<String, String> consumer;

  public ConsumerThread(CountDownLatch countDownLatch,
      Properties properties) {
    this.countDownLatch = countDownLatch;
    consumer = createConsumer(properties);
    consumer.subscribe(Collections.singleton("first_topic"));
  }


  public KafkaConsumer<String, String> createConsumer(Properties properties) {
    return new KafkaConsumer<String, String>(properties);
  }

  @Override
  public void run() {
    try {
      while(true) {
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

        for (var rec:
            consumerRecords) {
          log.info("Received Message \n" +
              "Topic: " + rec.topic() + "\n" +
              "Partition: " + rec.partition() + "\n" +
              "Offset: " + rec.offset() + "\n" +
              "Timestamp: " + rec.timestamp() + "\n" +
              "Message: " + rec.value());
        }
      }
    } catch (WakeupException ex) {
      log.info("Received WakeUp Exception!!!");
    } finally {
      consumer.close();
      countDownLatch.countDown();
    }
  }

  public void stop() {
    consumer.wakeup();
  }
}
