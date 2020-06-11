package com.tml.fleetman.cvpsimulator.vo;

import java.util.Date;
import com.fasterxml.jackson.annotation.JsonFormat;

/**
 * Class that represents IoT vehicle data
 * 
 * @author Pallavi Shetty
 * @since May 2020
 */

public class VehicleData {

	private String vehicleId;
	private String vehicleType;
	private String eventCode;
	private String latitude;
	private String longitude;

	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss", timezone = "IST")
	private Date eventDateTime;

	// "gpsDistanceInMetres":56783295,
	// "externalVoltageInPointOneOfAVolt":234,
	// "internalBatteryVoltageInPointOneOfAVolt":383,
	// "odometer":51821600

	private double gpsSpeedInPoint1Kph;
	private double vehicleFuelInMillilitres;

	public VehicleData() {

	}

	public VehicleData(String vehicleId, String vehicleType, String eventCode, String latitude, String longitude,
			Date eventDateTime, double gpsSpeedInPoint1Kph, double vehicleFuelInMillilitres) {
		super();
		this.vehicleId = vehicleId;
		this.vehicleType = vehicleType;
		this.eventCode = eventCode;
		this.latitude = latitude;
		this.longitude = longitude;
		this.eventDateTime = eventDateTime;
		this.gpsSpeedInPoint1Kph = gpsSpeedInPoint1Kph;
		this.vehicleFuelInMillilitres = vehicleFuelInMillilitres;
	}

	public String getVehicleId() {
		return vehicleId;
	}

	public void setVehicleId(String vehicleId) {
		this.vehicleId = vehicleId;
	}

	public String getVehicleType() {
		return vehicleType;
	}

	public void setVehicleType(String vehicleType) {
		this.vehicleType = vehicleType;
	}

	public String getEventCode() {
		return eventCode;
	}

	public void setEventCode(String eventCode) {
		this.eventCode = eventCode;
	}

	public String getLatitude() {
		return latitude;
	}

	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}

	public String getLongitude() {
		return longitude;
	}

	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}

	public Date getEventDateTime() {
		return eventDateTime;
	}

	public void setEventDateTime(Date eventDateTime) {
		this.eventDateTime = eventDateTime;
	}

	public double getGpsSpeedInPoint1Kph() {
		return gpsSpeedInPoint1Kph;
	}

	public void setGpsSpeedInPoint1Kph(double gpsSpeedInPoint1Kph) {
		this.gpsSpeedInPoint1Kph = gpsSpeedInPoint1Kph;
	}

	public double getVehicleFuelInMillilitres() {
		return vehicleFuelInMillilitres;
	}

	public void setVehicleFuelInMillilitres(double vehicleFuelInMillilitres) {
		this.vehicleFuelInMillilitres = vehicleFuelInMillilitres;
	}

	@Override
	public String toString() {
		return "VehicleData [vehicleId=" + vehicleId + ", vehicleType=" + vehicleType + ", eventCode=" + eventCode
				+ ", latitude=" + latitude + ", longitude=" + longitude + ", eventDateTime=" + eventDateTime
				+ ", gpsSpeedInPoint1Kph=" + gpsSpeedInPoint1Kph + ", vehicleFuelInMillilitres="
				+ vehicleFuelInMillilitres + "]";
	}

}
