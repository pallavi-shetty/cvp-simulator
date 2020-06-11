package com.tml.fleetman.cvpsimulator.sender;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

import com.tml.fleetman.cvpsimulator.vo.VehicleData;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import kafka.javaapi.producer.Producer;



import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Resource;


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
		bootstrapAddress = env.getProperty("kafka.bootstrapAddress");
		zookeeper = env.getProperty("com.cvp.kafka.zookeeper");
		brokerList= env.getProperty("com.cvp.kafka.brokerlist");
		topic = env.getProperty("com.cvp.kafka.topic");
		
		logger.info("Using Zookeeper=" + zookeeper + " ,Broker-list=" + brokerList + " and topic " + topic);
    }

	@Value("${kafka.bootstrapAddress}")
	private String bootstrapAddress;
	   

	@Value("${com.cvp.kafka.zookeeper}")
	String zookeeper;


	@Value("${com.cvp.kafka.brokerlist}")
	String brokerList;


	@Value("${com.cvp.kafka.topic}")
	String topic;



	public String getTopic() {
		return topic;
	}


	public void setTopic(String topic) {
		this.topic = topic;
	}

	private static final Logger logger = LoggerFactory.getLogger(SenderConfig.class);
	
	SenderConfig() {
		
		logger.info("Using Zookeeper=" + zookeeper + " ,Broker-list=" + brokerList + " and topic " + topic);
		
	}

	@Bean
	public ProducerFactory<String, String> producerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
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
	public Producer<String, VehicleData> vehicleDataProducer() 
	{
	// set producer properties
	Properties properties = new Properties();
	properties.put("zookeeper.connect", zookeeper);
	properties.put("metadata.broker.list", brokerList);
	properties.put("request.required.acks", "1");
	properties.put("serializer.class", "com.tml.fleetman.cvpsimulator.vo.VehicleDataEncoder");
	//generate event
	kafka.javaapi.producer.Producer<String, VehicleData> producer = new kafka.javaapi.producer.Producer<String, VehicleData>(new kafka.producer.ProducerConfig(properties));
	return producer;
	}
}