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
package io.ricall.kafka.delayservice.config;

import io.ricall.kafka.delayservice.config.DelayProperties.DelayTopic;
import io.ricall.kafka.delayservice.service.DelayListener;
import io.ricall.kafka.delayservice.service.MessageRouter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.StringUtils;

import java.util.Collection;
import java.util.stream.Collectors;

@Configuration
@RequiredArgsConstructor
public class DelayConfiguration {

    private final DelayProperties properties;
    private final MessageRouter handler;
    private final KafkaTemplate<String, String> template;
    private final ConfigurableListableBeanFactory beanFactory;

    @Bean
    Collection<NewTopic> topics() {
        return properties.topicNames().stream()
                .map(this::createTopic)
                .collect(Collectors.toList());
    }

    NewTopic createTopic(@NonNull String name) {
        return registerBean("topic", name, TopicBuilder.name(name)
                .partitions(1)
                .build());
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> batchListenerFactory(KafkaProperties properties) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(properties.buildConsumerProperties()));
        factory.setBatchListener(true);
        return factory;
    }

    @Bean
    Collection<DelayListener> internalTopicListeners() {
        return properties.internalTopics().stream()
                .map(this::createTopicListener)
                .collect(Collectors.toList());
    }

    DelayListener createTopicListener(@NonNull DelayTopic topic) {
        return registerBean("listener", topic.getName(), DelayListener.builder()
                .topic(topic)
                .messageRouter(handler)
                .template(template)
                .build());
    }

    private <T> T registerBean(String prefix, String name, T bean) {
        String beanName = prefix + StringUtils.capitalize(name);
        beanFactory.applyBeanPostProcessorsBeforeInitialization(bean, beanName);
        beanFactory.registerSingleton(beanName, bean);
        beanFactory.applyBeanPostProcessorsAfterInitialization(bean, beanName);

        return bean;
    }

}
