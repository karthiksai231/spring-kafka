package com.kafka.demo.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

@Slf4j
@RequiredArgsConstructor
@Service
public class ProducerService {
  private final KafkaTemplate<String, String> kafkaConsumer;

  public ListenableFuture<SendResult<String, String>> publishEvent(String message) {
    ListenableFuture<SendResult<String, String>> future = kafkaConsumer.send("first_topic",
        message);
    future.addCallback(new ListenableFutureCallback<>() {

      @Override
      public void onSuccess(SendResult<String, String> result) {
        log.info("Message successfully sent!!!!",
            keyValue("partition", result.getRecordMetadata().partition()),
            keyValue("offset", result.getRecordMetadata().offset()),
            keyValue("topic", result.getRecordMetadata().topic()));
      }

      @Override
      public void onFailure(Throwable throwable) {
        log.error("Failed to send the message");
      }
    });
    return null;
  }
}
