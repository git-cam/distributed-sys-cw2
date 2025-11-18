package com.function;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.microsoft.azure.functions.annotation.TimerTrigger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Random;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;

/**
 * SQL db username is azureuser
 * SQL db password is tzbt0672@leeds.ac.uk
 * Azure Functions with HTTP Trigger.
 */

public class Function {

    public class DataSourceFactory {
        private static HikariDataSource dataSource;
            
            public static synchronized DataSource getDataSource() {
                if (dataSource == null) {
                    HikariConfig config = new HikariConfig();
                    config.setJdbcUrl(System.getenv("SqlConnectionString"));
                    config.setMaximumPoolSize(20);
                    config.setMinimumIdle(5);
                    config.setConnectionTimeout(10000); // 10 seconds
                    config.setIdleTimeout(300000); // 5 minutes
                    dataSource = new HikariDataSource(config);
                }
                return dataSource;
            }
        }
    
    //TASK 1 - HTTP Trigger to generate sensor data on demand
    @FunctionName("GenerateSensorData")
    public HttpResponseMessage run(
        
            @HttpTrigger(
                name = "req",
                methods = {HttpMethod.GET, HttpMethod.POST},
                authLevel = AuthorizationLevel.ANONYMOUS)
                HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed a request.");

        // Random weather generator
        // Generate global weather values (temp, wind speed, relative humidity, co2 level)
        
        Random rand = new Random();
        int baseTemp = rand.nextInt(5,18);
        int baseWind = rand.nextInt(12,24);
        int baseHumidity = rand.nextInt(30,60);
        int baseCO2 = rand.nextInt(400,1600);
        // For each sensor, generate local variations based on global values. 
        // Populate the data structure and return as JSON response.
        
        // JSON builder
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode sensorsArray = mapper.createArrayNode();

        for(int i=0; i<20; i++) {
            int localTemp = baseTemp + rand.nextInt(-2,2);
            if (localTemp < 5) localTemp = 5;
            if (localTemp > 18) localTemp = 18;

            int localWind = baseWind + rand.nextInt(-1,1);
            if (localWind < 12) localWind = 12;
            if (localWind > 24) localWind = 24;

            int localHumidity = baseHumidity + rand.nextInt(-7,7);
            if (localHumidity < 30) localHumidity = 30;
            if (localHumidity > 60) localHumidity = 60;

            int localCO2 = baseCO2 + rand.nextInt(-125,125);
            if (localCO2 < 400) localCO2 = 400;
            if (localCO2 > 1600) localCO2 = 1600;

            // Create JSON object for this sensor
            ObjectNode sensor = mapper.createObjectNode();
            sensor.put("sensorID", i+1);
            sensor.put("temperature", localTemp);
            sensor.put("windSpeed", localWind);
            sensor.put("humidity", localHumidity);
            sensor.put("co2Level", localCO2);

            sensorsArray.add(sensor);
            context.getLogger().info(String.format(
            "Sensor %d: Temp=%d, Wind=%d, Humidity=%d, CO2=%d",
            i+1, localTemp, localWind, localHumidity, localCO2
        ));
        }
        // Use connection pool
        try (Connection conn = DataSourceFactory.getDataSource().getConnection()) {
            // Your existing batch insert logic here
            conn.setAutoCommit(false);
            String sql = "INSERT INTO SensorReadings (sensorID, temp, wind, humidity, co2) VALUES (?, ?, ?, ?, ?)";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                for (int i = 0; i < sensorsArray.size(); i++) {
                ObjectNode sensor = (ObjectNode) sensorsArray.get(i);
                stmt.setInt(1, sensor.get("sensorID").asInt());
                stmt.setInt(2, sensor.get("temperature").asInt());
                stmt.setInt(3, sensor.get("windSpeed").asInt());
                stmt.setInt(4, sensor.get("humidity").asInt());
                stmt.setInt(5, sensor.get("co2Level").asInt());
                stmt.addBatch();
            }
            stmt.executeBatch();
            conn.commit(); // commit transaction
            } catch (SQLException e) {
                conn.rollback(); // rollback all inserts if any fail
                throw e;
            }
        } catch (SQLException e) {
            context.getLogger().severe("SQL error: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("SQL Error: " + e.getMessage())
                    .build();
        }
        
        // Wrap the array in a root object
        ObjectNode responseJson = mapper.createObjectNode();
        responseJson.set("sensors", sensorsArray);

        try {
            String jsonResponse = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(responseJson);
            return request.createResponseBuilder(HttpStatus.OK)
                    .header("Content-Type", "application/json")
                    .body(jsonResponse)
                    .build();
        } catch (Exception e) {
            context.getLogger().severe("Error creating JSON: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error creating JSON response")
                    .build();
        }
    }
    
    //TASK 3 - Automated data generation every minute
    @FunctionName("GenerateSensorDataTimer")
    public void GenerateSensorDataTimer(
        
        @TimerTrigger(name = "GenerateSensorDataTrigger", schedule = "0 */1 * * * *") String timerInfo,
            ExecutionContext context
        ) {
        // Random weather generator
        // Generate global weather values (temp, wind speed, relative humidity, co2 level)
        
        Random rand = new Random();
        int baseTemp = rand.nextInt(5,18);
        int baseWind = rand.nextInt(12,24);
        int baseHumidity = rand.nextInt(30,60);
        int baseCO2 = rand.nextInt(400,1600);
        // For each sensor, generate local variations based on global values. 
        // Populate the data structure and return as JSON response.
        
        // JSON builder
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode sensorsArray = mapper.createArrayNode();

        for(int i=0; i<20; i++) {
            int localTemp = baseTemp + rand.nextInt(-2,2);
            if (localTemp < 5) localTemp = 5;
            if (localTemp > 18) localTemp = 18;

            int localWind = baseWind + rand.nextInt(-1,1);
            if (localWind < 12) localWind = 12;
            if (localWind > 24) localWind = 24;

            int localHumidity = baseHumidity + rand.nextInt(-7,7);
            if (localHumidity < 30) localHumidity = 30;
            if (localHumidity > 60) localHumidity = 60;

            int localCO2 = baseCO2 + rand.nextInt(-125,125);
            if (localCO2 < 400) localCO2 = 400;
            if (localCO2 > 1600) localCO2 = 1600;

            // Create JSON object for this sensor
            ObjectNode sensor = mapper.createObjectNode();
            sensor.put("sensorID", i+1);
            sensor.put("temperature", localTemp);
            sensor.put("windSpeed", localWind);
            sensor.put("humidity", localHumidity);
            sensor.put("co2Level", localCO2);

            sensorsArray.add(sensor);
            context.getLogger().info(String.format(
            "Sensor %d: Temp=%d, Wind=%d, Humidity=%d, CO2=%d",
            i+1, localTemp, localWind, localHumidity, localCO2
        ));
        }
        // Use connection pool
        try (Connection conn = DataSourceFactory.getDataSource().getConnection()) {
            // Your existing batch insert logic here
            conn.setAutoCommit(false);
            String sql = "INSERT INTO SensorReadings (sensorID, temp, wind, humidity, co2) VALUES (?, ?, ?, ?, ?)";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                for (int i = 0; i < sensorsArray.size(); i++) {
                ObjectNode sensor = (ObjectNode) sensorsArray.get(i);
                stmt.setInt(1, sensor.get("sensorID").asInt());
                stmt.setInt(2, sensor.get("temperature").asInt());
                stmt.setInt(3, sensor.get("windSpeed").asInt());
                stmt.setInt(4, sensor.get("humidity").asInt());
                stmt.setInt(5, sensor.get("co2Level").asInt());
                stmt.addBatch();
            }
            stmt.executeBatch();
            conn.commit(); // commit transaction
            } catch (SQLException e) {
                conn.rollback(); // rollback all inserts if any fail
                throw e;
            }
        } catch (SQLException e) {
            context.getLogger().severe("SQL error: " + e.getMessage());
            
        }
    }

    // For development/testing
    @FunctionName("GetAllSensorReadings")
    public HttpResponseMessage getAllSensorReadings(
            @HttpTrigger(
                    name = "req",
                    methods = {HttpMethod.GET},
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {

        context.getLogger().info("Java SQL test endpoint triggered.");

        String connString = System.getenv("SqlConnectionString");
        ObjectMapper mapper2 = new ObjectMapper();
        ArrayNode sensorsArray2 = mapper2.createArrayNode();

        try (Connection conn = DriverManager.getConnection(connString);
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM SensorReadings");
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                ObjectNode sensor = mapper2.createObjectNode();
                sensor.put("readingID", rs.getInt("readingID"));
                sensor.put("sensorID", rs.getInt("sensorID"));
                sensor.put("temp", rs.getInt("temp"));
                sensor.put("wind", rs.getInt("wind"));
                sensor.put("humidity", rs.getInt("humidity"));
                sensor.put("co2", rs.getInt("co2"));
                sensorsArray2.add(sensor);
            }

            ObjectNode responseJson = mapper2.createObjectNode();
            responseJson.set("sensorReadings", sensorsArray2);

            String jsonResponse = mapper2.writerWithDefaultPrettyPrinter().writeValueAsString(responseJson);

            return request.createResponseBuilder(HttpStatus.OK)
                    .header("Content-Type", "application/json")
                    .body(jsonResponse)
                    .build();

        } catch (SQLException e) {
            context.getLogger().severe("SQL error: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("SQL Error: " + e.getMessage())
                    .build();
        } catch (Exception e) {
            context.getLogger().severe("JSON error: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error creating JSON response")
                    .build();
        }
    }

    // For development/testing
    @FunctionName("ClearSensorReadings")
    public HttpResponseMessage clearSensorReadings(
            @HttpTrigger(
                    name = "req",
                    methods = {HttpMethod.GET},
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {

        context.getLogger().info("Clearing all sensor readings...");

        String connString = System.getenv("SqlConnectionString");

        try (Connection conn = DriverManager.getConnection(connString);
            PreparedStatement stmt = conn.prepareStatement("DELETE FROM SensorReadings")) {

            int rowsDeleted = stmt.executeUpdate(); 
            context.getLogger().info("Deleted " + rowsDeleted + " rows from SensorReadings.");

            ObjectMapper mapper = new ObjectMapper();
            ObjectNode responseJson = mapper.createObjectNode();
            responseJson.put("deletedRows", rowsDeleted);

            String jsonResponse = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(responseJson);

            return request.createResponseBuilder(HttpStatus.OK)
                    .header("Content-Type", "application/json")
                    .body(jsonResponse)
                    .build();

        } catch (SQLException e) {
            context.getLogger().severe("SQL error: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("SQL Error: " + e.getMessage())
                    .build();
        } catch (Exception e) {
            context.getLogger().severe("Error creating JSON: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error creating JSON response")
                    .build();
        }
    }

}
