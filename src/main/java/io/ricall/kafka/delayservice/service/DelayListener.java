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
package io.ricall.kafka.delayservice.service;

import io.ricall.kafka.delayservice.config.DelayProperties.DelayTopic;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Comparator.comparingLong;

@Slf4j
@Getter
@Builder
@RequiredArgsConstructor
public class DelayListener {

    private final DelayTopic topic;
    private final MessageRouter messageRouter;
    private final KafkaTemplate<String, String> template;

    @SneakyThrows
    @KafkaListener(topics = "#{__listener.topic.name}",
            clientIdPrefix = "#{__listener.topic.name}-client",
            containerFactory = "batchListenerFactory")
    public void onDelayMessage(List<Message<String>> messages) {
        final CountDownLatch latch = new CountDownLatch(messages.size());
        Flux.fromIterable(messages)
                .map(messageRouter::routeMessage)
                .sort(comparingLong(this::deliveryTime))
                .flatMap(this::delayMessage)
                .flatMap(this::sendMessage)
                .doOnEach(m -> latch.countDown())
                .doOnError(m -> latch.countDown())
                .subscribe();

        boolean processed = latch.await(120_000, TimeUnit.MILLISECONDS);
        if (!processed) {
            throw new IllegalStateException("Timed out waiting for message");
        }

    }

    private long deliveryTime(Message<String> message) {
        return Optional.ofNullable(message.getHeaders().get(DelayHeaders.DELIVERY_TIME, Long.class))
                .orElse(0L);
    }

    @SneakyThrows
    private Mono<Message<String>> delayMessage(Message<String> message) {
        long deliveryTime = DelayHeaders.getDeliveryTimeForMessage(message.getHeaders());

        long wait = Math.min(deliveryTime - System.currentTimeMillis(), topic.getDelay().toMillis()) - 100;
        if (wait <= 0) {
            return Mono.just(message);
        }
        return Mono.just(message)
                .delaySubscription(Duration.ofMillis(wait));
    }

    private Mono<Message<String>> sendMessage(Message<String> message) {
        return Mono.create(sink -> template.send(message).addCallback(r -> sink.success(message), sink::error));
    }

}
