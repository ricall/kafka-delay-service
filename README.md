# kafka-delay-service

Simple project using Spring Boot + Kafka to implement retries. This is a rewrite of kafka-retry as Spring Cloud Streams
didn't support batch processing properly.

The Retry logic in this project is similar, using Kafka Message Headers to control retry actions.

## Retry Headers
- delay_period: String (message delay in Duration format, eg. PT1H, PT2S, PT1.400S)
- delay_retries: Integer (number of message retries)
- delay_backoff: String (linear or exponential)
- delay_topic: String (topic to deliver message to once it has been delayed)
- delivery_time: Long (internally used to determine when the message needs to be delivered)

## Getting Started
In a terminal start the docker containers:
```bash
$ make start-dev
```

Run the `DelayServiceApplication` to start the delay service.
```bash
$ ./gradlew bootRun
```

Run the `ClientApplication` to start the delay service client
```bash
$ ./gradlew delay-client:bootRun
```

The client will schedule 1000 messages with varying delay intervals (in seconds)

TODO:
- Uses Reactor Kafka to process messages (see. 
  https://projectreactor.io/docs/kafka/release/reference/#_what_s_new_in_reactor_kafka_1_2_0_release 7.10. Exactly-Once delivery)

