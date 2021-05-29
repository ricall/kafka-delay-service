package io.ricall.kafka.delayservice;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Slf4j
@EnableKafka
@SpringBootApplication
@EnableTransactionManagement
@ConfigurationPropertiesScan
public class DelayServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(DelayServiceApplication.class, args);
	}

}
