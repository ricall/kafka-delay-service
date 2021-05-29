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

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.Optional;

import static org.springframework.kafka.support.KafkaHeaders.RECEIVED_TIMESTAMP;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class DelayHeaders {

    public static final String PREFIX = "delay_";
    public static final String PERIOD = PREFIX + "period";
    public static final String RETIRES = PREFIX + "retries";
    public static final String BACKOFF = PREFIX + "backoff";
    public static final String TOPIC = PREFIX + "topic";
    public static final String DELIVERY_TIME = "delivery_time";

    public static final String LINEAR_BACKOFF = "linear";
    public static final String EXPONENTIAL_BACKOFF = "exponential";

    public static final String DEFAULT_DELIVERY_DELAY = "PT17S";

    public static DelayHeaders from(MessageHeaders headers) {
        return new DelayHeaders(headers);
    }

    private final MessageHeaders headers;

    public boolean hasTopic() {
        String topic = headers.get(TOPIC, String.class);

        return StringUtils.hasText(topic);
    }

    public int getRetries() {
        return Optional.ofNullable(headers.get(RETIRES, Integer.class))
                .orElse(0);
    }

    public Duration getPeriod() {
        return Duration.parse(Optional.ofNullable(headers.get(PERIOD, String.class))
                .orElse(DEFAULT_DELIVERY_DELAY));
    }

    public long getMessageReceivedTimestamp() {
        return Optional.ofNullable(headers.get(RECEIVED_TIMESTAMP, Long.class))
                .orElseThrow(IllegalStateException::new);
    }

    public boolean isExponentialBackoff() {
        return Optional.ofNullable(headers.get(BACKOFF, String.class))
                .orElse(LINEAR_BACKOFF)
                .equalsIgnoreCase(EXPONENTIAL_BACKOFF);
    }

    public static long getDeliveryTimeForMessage(MessageHeaders headers) {
        return Optional.ofNullable(headers.get(DELIVERY_TIME, Long.class))
                .orElseThrow(IllegalAccessError::new);
    }

}
