#!/bin/sh

java -cp ./bin:./lib/kafka-clients-1.0.0.jar:./lib/slf4j-api-1.7.25.jar:./lib/slf4j-log4j12-1.7.25.jar:./lib/log4j-1.2.17.jar: com.example.kafka.consumer.ConsumerMain $1 -Dlogback.configurationFile=./logback.xml
