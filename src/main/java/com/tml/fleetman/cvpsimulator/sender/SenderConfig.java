package com.tml.fleetman.cvpsimulator.sender;

import java.util.Map;
import org.slf4j.Logger;
import java.util.HashMap;
import java.util.Properties;
import org.slf4j.LoggerFactory;
import javax.annotation.Resource;
import kafka.javaapi.producer.Producer;
import org.springframework.core.env.Environment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.EnvironmentAware;
import com.tml.fleetman.cvpsimulator.vo.VehicleData;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.apache.commons.lang3.RandomStringUtils;
//import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;



/**
 * @author Pallavi Shetty
 * @since May 2020
 */

@Configuration
@PropertySource(ignoreResourceNotFound = true, value = "classpath:application.properties")
public class SenderConfig implements EnvironmentAware {

	@Resource
	private Environment env;

	public Environment getEnv() {
		return env;
	}

	@Override
	public void setEnvironment(final Environment environment) {
		this.env = environment;
		bootstrapAddress = env.getProperty("kafka.bootstrapServers");
		zookeeper = env.getProperty("kafka.zookeeper");
		topic = env.getProperty("kafka.consumer.topics.telemetryTopic");

		logger.info("Using Zookeeper=" + zookeeper + " ,Broker-list=" + bootstrapAddress + " and topic " + topic);
	}

	@Value("${kafka.bootstrapServers}")
	private String bootstrapAddress;

	@Value("${kafka.zookeeper}")
	String zookeeper;

    @Value("${kafka.producer.clientId}")
    private String producerClientId;
    
	private String clientIdPrefix = RandomStringUtils.randomAlphabetic(4);
	
	//private String clientIdPrefix = "ABCK";

	@Value("${kafka.consumer.topics.telemetryTopic}")
	String topic;

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	private static final Logger logger = LoggerFactory.getLogger(SenderConfig.class);

	SenderConfig() {

		logger.info("Using Zookeeper=" + zookeeper + " ,Broker-list=" + bootstrapAddress + " and topic " + topic);

	}


	  
	@Bean
	public ProducerFactory<String, String> producerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
		configProps.put(ProducerConfig.CLIENT_ID_CONFIG, producerClientId + "-" + clientIdPrefix);
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		return new DefaultKafkaProducerFactory<>(configProps);
	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	@Bean
	public ProducerFactory<String, VehicleData> vehicleDataProducerFactory() {
		Map<String, Object> config = new HashMap<>();

		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
		config.put(ProducerConfig.CLIENT_ID_CONFIG, producerClientId + "-" + clientIdPrefix);
		config.put(ProducerConfig.ACKS_CONFIG, "all");
		config.put(ProducerConfig.RETRIES_CONFIG, 0);
		config.put(ProducerConfig.BATCH_SIZE_CONFIG, 1000);
		config.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

		return new DefaultKafkaProducerFactory<>(config);
	}

	@Bean
	public KafkaTemplate<String, VehicleData> vehicleDataKafkaTemplate() {
		return new KafkaTemplate<>(vehicleDataProducerFactory());
	}

	@Bean
	public Producer<String, VehicleData> vehicleDataProducer() {
		// set producer properties
		Properties properties = new Properties();
		properties.put("zookeeper.connect", zookeeper);
		properties.put("metadata.broker.list", bootstrapAddress);
		properties.put("request.required.acks", "1");
		properties.put("serializer.class", "com.tml.fleetman.cvpsimulator.vo.VehicleDataEncoder");
		// generate event
		kafka.javaapi.producer.Producer<String, VehicleData> producer = new kafka.javaapi.producer.Producer<String, VehicleData>(
				new kafka.producer.ProducerConfig(properties));
		return producer;
	}
}