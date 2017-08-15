package com.example;

import org.axonframework.config.EventHandlingConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AxoniqExampleApplication {

	public static void main(String[] args) {
		SpringApplication.run(AxoniqExampleApplication.class, args);
	}

}
