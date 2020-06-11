package com.tml.fleetman.cvpsimulator;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.tml.fleetman.cvpsimulator.sender.SenderConfig;
import com.tml.fleetman.cvpsimulator.vo.VehicleData;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@SpringBootApplication(exclude = { DataSourceAutoConfiguration.class })
public class CvpSimulatorApplication {

	private static final Logger logger = LoggerFactory.getLogger(CvpSimulatorApplication.class);

	public static void main(String[] args) throws Exception {

		ConfigurableApplicationContext context = SpringApplication.run(CvpSimulatorApplication.class, args);

		MessageProducer producer = context.getBean(MessageProducer.class);

		/*
		 * Sending a Hello World message to topic 'topic1'. Must be received by both
		 * listeners with group foo and bar with containerFactory
		 * fooKafkaListenerContainerFactory and barKafkaListenerContainerFactory
		 * respectively. It will also be received by the listener with
		 * headersKafkaListenerContainerFactory as container factory
		 */
		producer.sendMessage("Hello, World!");

		/*
		 * Sending message to a topic with 5 partition, each message to a different
		 * partition. But as per listener configuration, only the messages from
		 * partition 0 and 3 will be consumed.
		 */
		for (int i = 0; i < 5; i++) {
			producer.sendMessageToPartion("Hello To Partioned Topic!", i);
		}

		/*
		 * Sending message to 'filtered' topic. As per listener configuration, all
		 * messages with char sequence 'World' will be discarded.
		 */
		producer.sendMessageToFiltered("Hello Pallavi!");
		producer.sendMessageToFiltered("Hello World!");

		/*
		 * Sending message to 'VehicleData' topic. This will send and received a java
		 * object with the help of VehicleDataKafkaListenerContainerFactory.
		 */

		String vehicleId = UUID.randomUUID().toString();
		String vehicleType = "Large Truck";
		String routeId = "Route-37";
		double speed = 80;
		double fuelLevel = 10;
		int latitude = 33;
		int longitutde = -96;

		VehicleData vehicleEvent = new VehicleData(vehicleId, vehicleType, routeId, latitude + "", longitutde + "",
				null, speed, fuelLevel);
		producer.sendVehicleDataMessage(vehicleEvent);

		Producer<String, VehicleData> vehicleDataProducer = (Producer<String, VehicleData>) context
				.getBean("vehicleDataProducer");
		String topic = ((SenderConfig) context.getBean("senderConfig")).getTopic();

		// generate event
		CvpSimulatorApplication iotProducer = new CvpSimulatorApplication();
		iotProducer.generateIoTEvent(vehicleDataProducer, topic);

		context.close();
	}

	/**
	 * Method runs in while loop and generates random IoT data in JSON with below
	 * format.
	 * 
	 * {"vehicleId":"52f08f03-cd14-411a-8aef-ba87c9a99997","vehicleType":"Public
	 * Transport","routeId":"route-43","latitude":",-85.583435","longitude":"38.892395","timestamp":1465471124373,"speed":80.0,"fuelLevel":28.0}
	 * 
	 * @throws InterruptedException
	 * 
	 * 
	 */
	private void generateIoTEvent(Producer<String, VehicleData> producer, String topic) throws InterruptedException {

		List<String> vehicleTypeList = Arrays
				.asList(new String[] { "CV", "PV", "EV" });

		List<String> routeList = Arrays.asList(new String[] { "Route-1", "Route-5", "Route-10", "Route-15", "Route-20",
				"Route-25", "Route-30", "Route-35", "Route-40", "Route-45", "Route-50", "Route-55", "Route-60",
				"Route-65", "Route-70", "Route-75", "Route-80", "Route-85", "Route-90", "Route-95", "Route-100" });

		List<String> eventList = Arrays.asList(new String[] { "3", "6", "8", "12", "15", "16", "20", "29", "30", "34",
				"38", "64", "89", "98", "114" });

		Random rand = new Random();
		logger.info("Sending events");
		// generate event in loop
		while (true) {
			List<VehicleData> vehicleDataList = new ArrayList<VehicleData>();

			String vehicleId = "0352984084330025";

			// Use this if different type of vehicle
			// String vehicleType = vehicleTypeList.get(rand.nextInt(3));

			String vehicleType = vehicleTypeList.get(0);

			// Vehicle route from Thane to CST
			for (int j = 0; j < 21; j++) {
				String eventCode = eventList.get(rand.nextInt(15));
				double gpsSpeedInPoint1Kph = rand.nextInt(100 - 20) + 20;// random speed between 20 to 100
				double vehicleFuelInMillilitres = rand.nextInt(40 - 10) + 10;
				String routeId = routeList.get(j);
				String coords = getCoordinates(routeId);
				String latitude = coords.substring(0, coords.indexOf(","));
				String longitude = coords.substring(coords.indexOf(",") + 1, coords.length());

				VehicleData event = new VehicleData(vehicleId, vehicleType, eventCode, latitude, longitude, null,
						gpsSpeedInPoint1Kph, vehicleFuelInMillilitres);
				vehicleDataList.add(event);
			}

			for (VehicleData event : vehicleDataList) {
				event.setEventDateTime(new Date());
				KeyedMessage<String, VehicleData> data = new KeyedMessage<String, VehicleData>(topic, event);
				producer.send(data);
				Thread.sleep(rand.nextInt(1000 - 500) + 500);// random delay of 0.5 to 1 second
			}

			Thread.sleep(10000);// delay of 10 second
			vehicleDataList = new ArrayList<VehicleData>();

			// Vehicle route from CST to Thane
			for (int j = 20; j >= 0; j--) {
				String eventCode = eventList.get(rand.nextInt(15));
				double gpsSpeedInPoint1Kph = rand.nextInt(100 - 20) + 20;// random speed between 20 to 100
				double vehicleFuelInMillilitres = rand.nextInt(40 - 10) + 10;

				String routeId = routeList.get(j);
				String coords = getCoordinates(routeId);
				String latitude = coords.substring(0, coords.indexOf(","));
				String longitude = coords.substring(coords.indexOf(",") + 1, coords.length());

				VehicleData event = new VehicleData(vehicleId, vehicleType, eventCode, latitude, longitude, null,
						gpsSpeedInPoint1Kph, vehicleFuelInMillilitres);
				vehicleDataList.add(event);
			}

			for (VehicleData event : vehicleDataList) {
				event.setEventDateTime(new Date());
				KeyedMessage<String, VehicleData> data = new KeyedMessage<String, VehicleData>(topic, event);
				producer.send(data);
				Thread.sleep(rand.nextInt(1000 - 500) + 500);// random delay of 0.5 to 1 second
			}

			Thread.sleep(10000);// delay of 10 second

		}
	}

	// Method to generate latitude and longitude for route that starts from Tata
	// Motors, Teen Haath Naka to CST Local train station and return
	private String getCoordinates(String routeId) {

		double lattitude = 0.0;
		double longitude = -0.0;

		if (routeId.equals("Route-1")) // Tata Motors office
		{
			lattitude = 19.185170;
			longitude = 72.964810;

		} else if (routeId.equals("Route-5")) { // Agra National Hwy, Kopri, Thane East
			lattitude = 19.177346;
			longitude = 72.967836;
		} else if (routeId.equals("Route-10")) { // Agra National Hwy, Durgawadi, Mulund East
			lattitude = 19.171495;
			longitude = 72.968223;
		} else if (routeId.equals("Route-15")) { // Service Rd, MHADA Colony, Mulund East
			lattitude = 19.162229;
			longitude = 72.962203;
		} else if (routeId.equals("Route-20")) { // Nahur East, Bhandup East
			lattitude = 19.151946;
			longitude = 72.955093;
		} else if (routeId.equals("Route-25")) { // Agra National Hwy, Kannamwar Nagar II, Bhandup East
			lattitude = 19.123973;
			longitude = 72.938563;
		} else if (routeId.equals("Route-30")) { // Godrej Creekside Colony, Vikhroli
			lattitude = 19.100676;
			longitude = 72.929248;
		} else if (routeId.equals("Route-35")) { // Ghatkopar Traffic Police Chowky
			lattitude = 19.084028;
			longitude = 72.920917;
		} else if (routeId.equals("Route-40")) { // Virat Bus Stop, Ghatkopar East
			lattitude = 19.070606;
			longitude = 72.907495;
		} else if (routeId.equals("Route-45")) { // Nimoni Nagar, Govandi East
			lattitude = 19.056643;
			longitude = 72.911897;
		} else if (routeId.equals("Route-50")) { // Chatrapati Shivaji Maharaj Chowk Ghatla, Chembur
			lattitude = 19.045890;
			longitude = 72.909251;
		} else if (routeId.equals("Route-55")) { // Shivaji Nagar, MMRDA Colony, Chembur
			lattitude = 19.033302;
			longitude = 72.901703;
		} else if (routeId.equals("Route-60")) { // Adarsha Vidyalaya, Sangam Nagar
			lattitude = 19.021802;
			longitude = 72.876090;
		} else if (routeId.equals("Route-65")) { // Wadala, Mumbai
			lattitude = 19.012869;
			longitude = 72.871342;
		} else if (routeId.equals("Route-70")) { // MPT, Sewri
			lattitude = 18.997681;
			longitude = 72.854567;
		} else if (routeId.equals("Route-75")) { // Bombay Port Trust Rd, Sewri
			lattitude = 18.984802;
			longitude = 72.848164;
		} else if (routeId.equals("Route-80")) { // Dockyard Rd, Dockyard, Wadi Bandar, Mazgaon
			lattitude = 18.967687;
			longitude = 72.845461;
		} else if (routeId.equals("Route-85")) { // Bombay Launch Service Pvt Ltd, Orient House, Adi Marzaban Path, Ballard Estate
			lattitude = 18.954164;
			longitude = 72.841889;
		} else if (routeId.equals("Route-90")) { // Hotel Elphinstone Annexe, Indira Docks, Caranak, Masjid Bandar
			lattitude = 18.948763;
			longitude = 72.840722;
		} else if (routeId.equals("Route-95")) { // JJ Flyover, Koliwada, Chippi Chawl, Masjid Bandar
			lattitude = 18.947853;
			longitude = 72.835522;
		} else if (routeId.equals("Route-100")) { // Chhatrapati Shivaji Terminus Area, Fort
			lattitude = 18.941474;
			longitude = 72.835319;
		}

		return lattitude + "," + longitude;
	}

	@Bean
	public MessageProducer messageProducer() {
		return new MessageProducer();
	}

	public static class MessageProducer {

		@Autowired
		private KafkaTemplate<String, String> kafkaTemplate;

		@Autowired
		private KafkaTemplate<String, VehicleData> vehicleDataKafkaTemplate;

		@Value(value = "${message.topic.name}")
		private String topicName;

		@Value(value = "${partitioned.topic.name}")
		private String partionedTopicName;

		@Value(value = "${filtered.topic.name}")
		private String filteredTopicName;

		@Value(value = "${vehicleData.topic.name}")
		private String vehicleDataTopicName;

		public void sendMessage(String message) {

			ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);

			future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

				@Override
				public void onSuccess(SendResult<String, String> result) {
					System.out.println(
							"Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
				}

				@Override
				public void onFailure(Throwable ex) {
					System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
				}
			});
		}

		public void sendMessageToPartion(String message, int partition) {
			kafkaTemplate.send(partionedTopicName, partition, null, message);
		}

		public void sendMessageToFiltered(String message) {
			kafkaTemplate.send(filteredTopicName, message);
		}

		public void sendVehicleDataMessage(VehicleData VehicleData) {
			vehicleDataKafkaTemplate.send(vehicleDataTopicName, VehicleData);
		}
	}

}
