package com.kafka.demo.controller;

import com.kafka.demo.service.ProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("api/v1/message")
public class MessageController {
  private final ProducerService producerService;

  @PostMapping
  public ResponseEntity postMessage() {
    producerService.publishEvent();
    return ResponseEntity.ok("Success");
  }
}
