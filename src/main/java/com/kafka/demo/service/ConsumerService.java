package com.kafka.demo.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

@Slf4j
@Service
public class ConsumerService {
  @KafkaListener(topics = "first_topic", groupId = "my-first-application")
  public void consumerMessage(String message) {
    log.info("Received message!!!", keyValue("message", message));
  }
}
