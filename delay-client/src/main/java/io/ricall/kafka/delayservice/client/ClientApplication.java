/*
 * Copyright (c) 2021 Richard Allwood
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package io.ricall.kafka.delayservice.client;

import io.ricall.kafka.delayservice.client.config.ClientProperties;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@EnableKafka
@SpringBootApplication
@RequiredArgsConstructor
@ConfigurationPropertiesScan
public class ClientApplication {

    private final ClientProperties properties;
    private final KafkaTemplate<String, String> template;

    public static void main(String[] args) {
        SpringApplication.run(ClientApplication.class, args);
    }

    @Bean
    public CommandLineRunner startup() {
        return args -> {
            log.info("Application starting");
            Flux.range(1, 1_000)
                    .delayElements(Duration.ofMillis(20))
                    .subscribe(i -> sendMessage(i, i % 20));
        };
    }

    private void sendMessage(int index, int delay) {
        String payload = String.format("Test Message %d (with %d delay)", index, delay);
        String key = String.format("%04d", index);

        var message = MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.TOPIC, properties.getDelayTopic())
                .setHeader(KafkaHeaders.MESSAGE_KEY, key)
                .setHeader(DelayHeaders.PERIOD, "PT" + delay + "S")
                .setHeader(DelayHeaders.TOPIC, properties.getProcessingTopic())
                .build();
        template.send(message);
    }

    private AtomicInteger counter = new AtomicInteger();

    @SneakyThrows
    @KafkaListener(topics = "#{deliveryTopic.name}")
    public void onMessageDelayed(Message<String> message) {
        var jitter = Optional.ofNullable(message.getHeaders().get(DelayHeaders.DELIVERY_TIME, Long.class))
                .map(t -> System.currentTimeMillis() - t);
        log.info("PROCESS: {} {} - {}", counter.incrementAndGet(), message.getPayload(), jitter);
    }

}
