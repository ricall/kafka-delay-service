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
package io.ricall.kafka.delayservice;

import io.ricall.kafka.delayservice.config.DelayProperties;
import io.ricall.kafka.delayservice.service.DelayHeaders;
import io.ricall.kafka.delayservice.service.MessageRouter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.List;

import static io.ricall.kafka.delayservice.service.DelayHeaders.DELIVERY_TIME;
import static io.ricall.kafka.delayservice.service.DelayHeaders.RETIRES;

@Component
@RequiredArgsConstructor
public class DelayService {

    private final DelayProperties properties;
    private final MessageRouter messageRouter;
    private final KafkaTemplate<String, String> template;

    @SneakyThrows
    @KafkaListener(topics = "#{@delayTopic.name}", clientIdPrefix = "delay-client", containerFactory = "batchListenerFactory")
    public void onDelayMessage(List<Message<String>> messages) {
        messages.stream()
                .map(this::createDelayMessage)
                .forEach(template::send);
    }

    private Message<String> createDelayMessage(Message<String> message) {
        DelayHeaders headers = DelayHeaders.from(message.getHeaders());
        if (!validateHeaders(headers)) {
            return MessageBuilder.fromMessage(message)
                    .setHeader(KafkaHeaders.TOPIC, properties.getRequestDlq())
                    .build();
        }

        long deliveryTime = calculateDeliveryTimeFor(headers);
        return MessageBuilder.fromMessage(message)
                .setHeader(DELIVERY_TIME, deliveryTime)
                .setHeader(RETIRES, headers.getRetries() + 1)
                .setHeader(KafkaHeaders.TOPIC, messageRouter.determineTopicForMessage(deliveryTime, message.getHeaders()))
                .build();
    }

    private boolean validateHeaders(DelayHeaders headers) {
        return headers.hasTopic();
    }

    private long calculateDeliveryTimeFor(DelayHeaders headers) {
        long messageTimestamp = headers.getMessageReceivedTimestamp();
        Duration period = headers.getPeriod();
        int multiplier = headers.isExponentialBackoff() ? headers.getRetries() + 1 : 1;

        return messageTimestamp + multiplier * period.toMillis();
    }

}
