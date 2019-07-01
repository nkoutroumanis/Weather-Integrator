package com.github.nkoutroumanis.parsers.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.Serializable;
import java.util.ArrayList;

public class VfiMapPoint {

    private String id;
    private String company;
    private String vehicle;
    private long timestamp;
    private String engineStatus;
    private String driver;
    private String driverEvent;
    private double longitude;
    private double latitude;
    private String altitude;
    private double angle;
    private double speed;
    private String odometer;
    private String satellites;
    private String fuelLevelLt;
    private String countryCode;
    private String rpm;
    private String levelType;
    private String fuelTankSize;
    private String vehicleOdometer;
    private String fuelConsumed;
    private String engineHours;
    private String closeToGasStation;
    private String deviceType;
    private String VehicleType;
    private String fuelRawValue;
    private boolean valid;

    private long roadID;
    private long osm_id;
    private double road_lon;
    private double road_lat;
    private double distance;
    private double probability;
    private double myspeed;
    private boolean slow_motion;
    private boolean gap;
    private long traj_id;
    private String path;
    private long execution_time;
    private long sampling;
    private double entrance_time;
    private double exit_time;
    private boolean start_stop;
    private boolean end_stop;
    private float road_speed;
    private String road_type;
    private double fraction;
    //  private String kstate;

    public VfiMapPoint(String id, String company, String vehicle, long timestamp, String engineStatus, String driver, String driverEvent, double longitude, double latitude, String altitude, double angle, double speed, String odometer, String satellites, String fuelLevelLt, String countryCode, String rpm, String levelType, String fuelTankSize, String vehicleOdometer, String fuelConsumed, String engineHours, String closeToGasStation, String deviceType, String vehicleType, String fuelRawValue, boolean valid, long roadID, long osm_id, double road_lon, double road_lat, double distance, double probability, double myspeed, boolean slow_motion, boolean gap, long traj_id, String path, long execution_time, long sampling, double entrance_time, double exit_time, boolean start_stop, boolean end_stop, float road_speed, String road_type, double fraction) {
        this.id = id;
        this.company = company;
        this.vehicle = vehicle;
        this.timestamp = timestamp;
        this.engineStatus = engineStatus;
        this.driver = driver;
        this.driverEvent = driverEvent;
        this.longitude = longitude;
        this.latitude = latitude;
        this.altitude = altitude;
        this.angle = angle;
        this.speed = speed;
        this.odometer = odometer;
        this.satellites = satellites;
        this.fuelLevelLt = fuelLevelLt;
        this.countryCode = countryCode;
        this.rpm = rpm;
        this.levelType = levelType;
        this.fuelTankSize = fuelTankSize;
        this.vehicleOdometer = vehicleOdometer;
        this.fuelConsumed = fuelConsumed;
        this.engineHours = engineHours;
        this.closeToGasStation = closeToGasStation;
        this.deviceType = deviceType;
        VehicleType = vehicleType;
        this.fuelRawValue = fuelRawValue;
        this.valid = valid;
        this.roadID = roadID;
        this.osm_id = osm_id;
        this.road_lon = road_lon;
        this.road_lat = road_lat;
        this.distance = distance;
        this.probability = probability;
        this.myspeed = myspeed;
        this.slow_motion = slow_motion;
        this.gap = gap;
        this.traj_id = traj_id;
        this.path = path;
        this.execution_time = execution_time;
        this.sampling = sampling;
        // this.sizestate = sizestate;
        this.entrance_time = entrance_time;
        this.exit_time = exit_time;
        // this.state2 = state2;
        this.start_stop = start_stop;
        this.end_stop = end_stop;
        this.road_speed=road_speed;
        this.road_type=road_type;
        this.fraction=fraction;
        //  this.kstate=kstate;
    }

    public VfiMapPoint() {

    }



    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getVehicle() {
        return vehicle;
    }

    public void setVehicle(String vehicle) {
        this.vehicle = vehicle;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getEngineStatus() {
        return engineStatus;
    }

    public void setEngineStatus(String engineStatus) {
        this.engineStatus = engineStatus;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getDriverEvent() {
        return driverEvent;
    }

    public void setDriverEvent(String driverEvent) {
        this.driverEvent = driverEvent;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public String getAltitude() {
        return altitude;
    }

    public void setAltitude(String altitude) {
        this.altitude = altitude;
    }

    public double getAngle() {
        return angle;
    }

    public void setAngle(double angle) {
        this.angle = angle;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public String getOdometer() {
        return odometer;
    }

    public void setOdometer(String odometer) {
        this.odometer = odometer;
    }

    public String getSatellites() {
        return satellites;
    }

    public void setSatellites(String satellites) {
        this.satellites = satellites;
    }

    public String getFuelLevelLt() {
        return fuelLevelLt;
    }

    public void setFuelLevelLt(String fuelLevelLt) {
        this.fuelLevelLt = fuelLevelLt;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public String getRpm() {
        return rpm;
    }

    public void setRpm(String rpm) {
        this.rpm = rpm;
    }

    public String getLevelType() {
        return levelType;
    }

    public void setLevelType(String levelType) {
        this.levelType = levelType;
    }

    public String getFuelTankSize() {
        return fuelTankSize;
    }

    public void setFuelTankSize(String fuelTankSize) {
        this.fuelTankSize = fuelTankSize;
    }

    public String getVehicleOdometer() {
        return vehicleOdometer;
    }

    public void setVehicleOdometer(String vehicleOdometer) {
        this.vehicleOdometer = vehicleOdometer;
    }

    public String getFuelConsumed() {
        return fuelConsumed;
    }

    public void setFuelConsumed(String fuelConsumed) {
        this.fuelConsumed = fuelConsumed;
    }

    public String getEngineHours() {
        return engineHours;
    }

    public void setEngineHours(String engineHours) {
        this.engineHours = engineHours;
    }

    public String getCloseToGasStation() {
        return closeToGasStation;
    }

    public void setCloseToGasStation(String closeToGasStation) {
        this.closeToGasStation = closeToGasStation;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public String getVehicleType() {
        return VehicleType;
    }

    public void setVehicleType(String vehicleType) {
        VehicleType = vehicleType;
    }

    public String getFuelRawValue() {
        return fuelRawValue;
    }

    public void setFuelRawValue(String fuelRawValue) {
        this.fuelRawValue = fuelRawValue;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public long getRoadID() {
        return roadID;
    }

    public void setRoadID(long roadID) {
        this.roadID = roadID;
    }

    public long getOsm_id() {
        return osm_id;
    }

    public void setOsm_id(long osm_id) {
        this.osm_id = osm_id;
    }

    public double getRoad_lon() {
        return road_lon;
    }

    public void setRoad_lon(double road_lon) {
        this.road_lon = road_lon;
    }

    public double getRoad_lat() {
        return road_lat;
    }

    public void setRoad_lat(double road_lat) {
        this.road_lat = road_lat;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public double getProbability() {
        return probability;
    }

    public void setProbability(double probability) {
        this.probability = probability;
    }

    public double getMyspeed() {
        return myspeed;
    }

    public void setMyspeed(double myspeed) {
        this.myspeed = myspeed;
    }

    public boolean isSlow_motion() {
        return slow_motion;
    }

    public void setSlow_motion(boolean slow_motion) {
        this.slow_motion = slow_motion;
    }

    public boolean isGap() {
        return gap;
    }

    public void setGap(boolean gap) {
        this.gap = gap;
    }

    public long getTraj_id() {
        return traj_id;
    }

    public void setTraj_id(long traj_id) {
        this.traj_id = traj_id;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getExecution_time() {
        return execution_time;
    }

    public void setExecution_time(long execution_time) {
        this.execution_time = execution_time;
    }

    public long getSampling() {
        return sampling;
    }

    public void setSampling(long sampling) {
        this.sampling = sampling;
    }

    /*
        public int getSizestate() {
            return sizestate;
        }

        public void setSizestate(int sizestate) {
            this.sizestate = sizestate;
        }
    */
    public double getEntrance_time() {
        return entrance_time;
    }

    public void setEntrance_time(double entrance_time) {
        this.entrance_time = entrance_time;
    }

    public double getExit_time() {
        return exit_time;
    }

    public void setExit_time(double exit_time) {
        this.exit_time = exit_time;
    }

    /*
        public int getState2() {
            return state2;
        }

        public void setState2(int state2) {
            this.state2 = state2;
        }
    */
    public boolean isStart_stop() {
        return start_stop;
    }

    public void setStart_stop(boolean start_stop) {
        this.start_stop = start_stop;
    }

    public boolean isEnd_stop() {
        return end_stop;
    }

    public void setEnd_stop(boolean end_stop) {
        this.end_stop = end_stop;
    }

    public float getRoad_speed() {
        return road_speed;
    }

    public void setRoad_speed(float road_speed) {
        this.road_speed = road_speed;
    }

    public String getRoad_type() {
        return road_type;
    }

    public void setRoad_type(String road_type) {
        this.road_type = road_type;
    }

    public double getFraction() {
        return fraction;
    }

    public void setFraction(double fraction) {
        this.fraction = fraction;
    }

/*
    public String getKstate() {
        return kstate;
    }

    public void setKstate(String kstate) {
        this.kstate = kstate;
    }
*/

    @Override
    public String toString() {
        Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();
        return gson.toJson(this);
    }

    public String toString2() {
        return String.format("%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s",
                id, company, vehicle, timestamp, engineStatus, driver, driverEvent, longitude, latitude, altitude, angle, speed, odometer, satellites, fuelLevelLt, countryCode, rpm, levelType, fuelTankSize, vehicleOdometer, fuelConsumed, engineHours, closeToGasStation, deviceType, VehicleType, fuelRawValue, valid, roadID, osm_id, road_lon, road_lat, distance, probability, myspeed, slow_motion, gap, traj_id, path, execution_time, sampling, entrance_time, exit_time, start_stop, end_stop, road_speed, road_type,fraction);
    }

    /******
     *
     * Panagiotis changes from here
     *
     */

    public static final String header = "company;vehicle;localDate;engineStatus;driver;driverEvent;longitude;" +
            "latitude;altitude;angle;speed;odometer;satellites;fuelLevelLt;countryCode;rpm;levelType;fuelTankSize;" +
            "vehicleOdometer;fuelConsumed;engineHours;closeToGasStation;deviceType;VehicleType;fuelRawValue;" +
            "roadID;osm_id;road_lon;road_lat;distance;probability;myspeed;slow_motion;gap;traj_id;path;" +
            "execution_time;sampling;entrance_time;exit_time;start_stop;end_stop;road_speed;road_type;fraction";

    public String[] getValuesInCsvOrder() {
        ArrayList<String> lstValues = new ArrayList<>();
        lstValues.add(company);
        lstValues.add(vehicle);
        //TODO: add more fields here...
        return lstValues.toArray(new String[0]);
    }

    public static final int longitudeFieldId = 6;

    public static final int latitudeFieldId = 7;

    public static final int dateFieldId = 2;

}