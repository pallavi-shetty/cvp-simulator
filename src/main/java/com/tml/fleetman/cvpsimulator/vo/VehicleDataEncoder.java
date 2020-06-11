package com.tml.fleetman.cvpsimulator.vo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tml.fleetman.cvpsimulator.vo.VehicleData;
import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * Class to convert VehicleData java object to JSON String
 * 
 * @author Pallavi Shetty 
 * @since May 2020
 */

public class VehicleDataEncoder implements Encoder<VehicleData> {

	private static final Logger logger = LoggerFactory.getLogger(VehicleDataEncoder.class);

	private static ObjectMapper objectMapper = new ObjectMapper();

	public VehicleDataEncoder(VerifiableProperties verifiableProperties) {

	}

	public byte[] toBytes(VehicleData iotEvent) {
		try {
			String msg = objectMapper.writeValueAsString(iotEvent);
			logger.info(msg);
			return msg.getBytes();
		} catch (JsonProcessingException e) {
			logger.error("Error in Serialization", e);
		}
		return null;
	}
}
