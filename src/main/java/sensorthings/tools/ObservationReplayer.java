package sensorthings.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.sql.*;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

public class ObservationReplayer {

    private ObjectMapper mapper = new ObjectMapper();
    private String BROKER_URI = System.getenv("BROKER_URI");
    private String BROKER_USERNAME = System.getenv("BROKER_USERNAME");
    private String BROKER_PASSWORD = System.getenv("BROKER_PASSWORD");
    private String GOST_MQTT_PREFIX = System.getenv("GOST_MQTT_PREFIX");

    private String GOST_DB_HOST = System.getenv("GOST_DB_HOST");
    private String GOST_DB_DATABASE = System.getenv("GOST_DB_DATABASE");
    private String GOST_DB_USER = System.getenv("GOST_DB_USER");
    private String GOST_DB_PASSWORD = System.getenv("GOST_DB_PASSWORD");

    private String DATASTREAM_ID = System.getenv("DATASTREAM_ID");
    private int OBSERVATION_ID_START = Integer.parseInt(System.getenv("OBSERVATION_ID_START"));
    private int OBSERVATION_ID_END = Integer.parseInt(System.getenv("OBSERVATION_ID_END"));
    private int OBSERVATION_INTERVAL_MS = Integer.parseInt(System.getenv("OBSERVATION_INTERVAL_MS"));

    private int qos = 0;
    private MqttClient sampleClient = null;
    Connection c = null;

    private void connectPostgres() {
        String url = "jdbc:postgresql://" + GOST_DB_HOST + ":5432/" + GOST_DB_DATABASE;
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager.getConnection(url, GOST_DB_USER, GOST_DB_PASSWORD);
            c.setAutoCommit(false);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        System.out.println("Opened database at " + url + " successfully");
    }

    private void disconnectPostgres() {
        try {
            c.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void connectMqtt() {
        String clientId = "ObservationReplayer_" + DATASTREAM_ID + "_" + OBSERVATION_ID_START + "_" + OBSERVATION_ID_END;
        MemoryPersistence persistence = new MemoryPersistence();

        try {
            sampleClient = new MqttClient(BROKER_URI, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            if (BROKER_USERNAME != null) {
                connOpts.setUserName(BROKER_USERNAME);
            }

            if (BROKER_PASSWORD != null) {
                connOpts.setPassword(BROKER_PASSWORD.toCharArray());
            }
            sampleClient.setTimeToWait(30000);
            sampleClient.connect(connOpts);
        } catch (MqttException me) {
            System.out.println("reason " + me.getReasonCode());
            System.out.println("msg " + me.getMessage());
            System.out.println("loc " + me.getLocalizedMessage());
            System.out.println("cause " + me.getCause());
            System.out.println("excep " + me);
            me.printStackTrace();
            System.exit(0);
        }
        System.out.println("Connected to MQTT broker at " + BROKER_URI + " successfully");
    }

    private void disconnectMqtt() {
        try {
            sampleClient.disconnect();
            sampleClient.close();
        } catch (MqttException me) {
            System.out.println("reason " + me.getReasonCode());
            System.out.println("msg " + me.getMessage());
            System.out.println("loc " + me.getLocalizedMessage());
            System.out.println("cause " + me.getCause());
            System.out.println("excep " + me);
            me.printStackTrace();
        }
    }

    private void loopOverObservations() {
        Statement stmt = null;
        try {
            stmt = c.createStatement();
            String query = "SELECT id, data, stream_id FROM v1.observation where stream_id in (" + DATASTREAM_ID + ") and id between " + OBSERVATION_ID_START + " and " + OBSERVATION_ID_END + " order by id asc;";
            System.out.println("query = " + query);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                int id = rs.getInt("id");
                int stream_id = rs.getInt("stream_id");
                String data = rs.getString("data");
                System.out.println("Read observation with id " + id + ", stream_id "+stream_id+":");
                System.out.println(data);
                try {
                    JsonNode observationNode = mapper.readTree(data);
                    JsonNode resultNode = observationNode.get("result");
                    JsonNode phenomenonTimeNode = observationNode.get("phenomenonTime");
                    double result = resultNode.asDouble();
                    String phenomenonTime = phenomenonTimeNode.asText();
                    DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SS'Z'");
                    Date phenomenonTimeAsDate = format.parse(phenomenonTime);

                    ObjectNode newObservation = mapper.createObjectNode();
                    if (observationNode != null) {
                        Iterator<Map.Entry<String, JsonNode>> iter = observationNode.fields();
                        while (iter.hasNext()) {
                            Map.Entry<String, JsonNode> observationField = iter.next();
                            newObservation.replace(observationField.getKey(), observationField.getValue());
                            //System.out.println("Set key " + observationField.getKey() + " with value " + observationField.getValue());
                        }
                    }
                    Date date = new Date();
                    newObservation.put("phenomenonTime", format.format(date));
                    //System.out.println("Set key phenomenonTime with value " + format.format(date));
                    String topic = GOST_MQTT_PREFIX + "/Datastreams(" + stream_id + ")/Observations";
                    publish(newObservation.toString(), topic);
                    System.out.println("Write observation to " + topic + ":");
                    System.out.println(newObservation);
                    Thread.sleep(OBSERVATION_INTERVAL_MS);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.err.println(e.getClass().getName() + ": " + e.getMessage());

                } catch (ParseException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            rs.close();
            stmt.close();


        } catch (SQLException e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    private void publish(String content, String topic) {
        MqttMessage message = null;
        message = new MqttMessage(content.getBytes());
        message.setQos(qos);
        try {
            sampleClient.publish(topic, message);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    private void checkMandatoryEnvVariable(String name) {
        String value = System.getenv(name);

        if (value == null) {
            System.out.println("Set environment variable " + name);
            System.exit(0);
        } else {
            System.out.println(name + " = " + value);
        }
    }

    private void checkOptionalEnvVariable(String name) {
        String value = System.getenv(name);

        if (value == null) {
            System.out.println("Environment variable " + name + " is not set.");
        } else {
            System.out.println(name + " = " + value);
        }
    }

    public ObservationReplayer() {

        checkMandatoryEnvVariable("BROKER_URI");
        checkOptionalEnvVariable("BROKER_USERNAME");
        checkOptionalEnvVariable("BROKER_PASSWORD");
        checkMandatoryEnvVariable("DATASTREAM_ID");
        checkMandatoryEnvVariable("OBSERVATION_ID_START");
        checkMandatoryEnvVariable("OBSERVATION_ID_END");
        checkMandatoryEnvVariable("OBSERVATION_INTERVAL_MS");
        checkMandatoryEnvVariable("GOST_MQTT_PREFIX");
        checkMandatoryEnvVariable("GOST_DB_HOST");
        checkMandatoryEnvVariable("GOST_DB_DATABASE");
        checkMandatoryEnvVariable("GOST_DB_USER");
        checkMandatoryEnvVariable("GOST_DB_PASSWORD");

        connectPostgres();
        connectMqtt();
        loopOverObservations();
        disconnectMqtt();
        disconnectPostgres();
    }

    public static void main(String args[]) {
        ObservationReplayer replayer = new ObservationReplayer();
    }
}
