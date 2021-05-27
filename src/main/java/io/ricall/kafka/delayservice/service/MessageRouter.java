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

import io.ricall.kafka.delayservice.config.DelayProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.concurrent.ListenableFuture;
import reactor.core.publisher.Mono;

import java.util.stream.Stream;

import static io.ricall.kafka.delayservice.service.DelayHeaders.getDeliveryTimeForMessage;

@Slf4j
@Component
@RequiredArgsConstructor
public class MessageRouter {

    private final DelayProperties properties;

    public Message<String> routeMessage(Message<String> message) {
        MessageHeaders headers = message.getHeaders();
        String topic = determineTopicForMessage(getDeliveryTimeForMessage(headers), headers);
        long time = getDeliveryTimeForMessage(headers);

        log.info("Sending message {} on {}", message.getPayload(), topic);
        return MessageBuilder.fromMessage(message)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(DelayHeaders.DELIVERY_TIME, time)
                .build();
    }

    public String determineTopicForMessage(long deliveryTime, MessageHeaders headers) {
        long delay = deliveryTime - System.currentTimeMillis() - 100;
        if (delay < 0) {
            return headers.get(DelayHeaders.TOPIC, String.class);
        }

        return Stream.of(properties.getInternal4(),
                properties.getInternal3(),
                properties.getInternal2(),
                properties.getInternal1())
                .filter(t -> t.getDelay().toMillis() <= delay)
                .findFirst()
                .orElse(properties.getInternal1())
                .getName();
    }

}
