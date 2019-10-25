package sensorthings.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.swing.*;
import java.io.IOException;
import java.sql.*;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

public class ObservationReplayer {

    private ObjectMapper mapper = new ObjectMapper();
    private String BROKER_URI = null;
    private String BROKER_USERNAME = null;
    private String BROKER_PASSWORD = null;
    private String GOST_MQTT_PREFIX = null;

    private String GOST_DB_HOST = null;
    private String GOST_DB_DATABASE = null;
    private String GOST_DB_USER = null;
    private String GOST_DB_PASSWORD = null;

    private String DATASTREAM_IDS = null;

    private int OBSERVATION_ID_START = 0;
    private int OBSERVATION_ID_END = Integer.MAX_VALUE;
    private int OBSERVATION_INTERVAL_MS = 1000;

    public void setBROKER_URI(String value) {
        if (value != null && !value.isEmpty()) {
            BROKER_URI = value;
            System.out.println("BROKER_URI is " + BROKER_URI);
        } else {
            System.out.println("BROKER_URI cannot be null, exiting...");
            System.exit(0);
        }
    }

    public void setBROKER_USERNAME(String value) {
        if (value != null && !value.isEmpty()) {
            BROKER_USERNAME = value;
            System.out.println("BROKER_USERNAME is " + BROKER_USERNAME);
        } else {
            System.out.println("BROKER_USERNAME not set");
        }
    }

    public void setBROKER_PASSWORD(String value) {
        if (value != null && !value.isEmpty()) {
            BROKER_PASSWORD = value;
            System.out.println("BROKER_PASSWORD is " + BROKER_PASSWORD);
        } else {
            System.out.println("BROKER_PASSWORD not set");
        }
    }

    public void setGOST_MQTT_PREFIX(String value) {
        if (value != null && !value.isEmpty()) {
            GOST_MQTT_PREFIX = value;
            System.out.println("GOST_MQTT_PREFIX is " + GOST_MQTT_PREFIX);
        } else {
            System.out.println("GOST_MQTT_PREFIX cannot be null, exiting...");
            System.exit(0);
        }
    }


    public void setGOST_DB_HOST(String value) {
        if (value != null && !value.isEmpty()) {
            GOST_DB_HOST = value;
            System.out.println("GOST_DB_HOST is " + GOST_DB_HOST);
        } else {
            System.out.println("GOST_DB_HOST cannot be null, exiting...");
            System.exit(0);
        }
    }

    public void setGOST_DB_DATABASE(String value) {
        if (value != null && !value.isEmpty()) {
            GOST_DB_DATABASE = value;
            System.out.println("GOST_DB_DATABASE is " + GOST_DB_DATABASE);
        } else {
            System.out.println("GOST_DB_DATABASE cannot be null, exiting...");
            System.exit(0);
        }
    }

    public void setGOST_DB_USER(String value) {
        if (value != null && !value.isEmpty()) {
            GOST_DB_USER = value;
            System.out.println("GOST_DB_USER is " + GOST_DB_USER);
        } else {
            System.out.println("GOST_DB_USER cannot be null, exiting...");
            System.exit(0);
        }
    }

    public void setGOST_DB_PASSWORD(String value) {
        if (value != null && !value.isEmpty()) {
            GOST_DB_PASSWORD = value;
            System.out.println("GOST_DB_PASSWORD is " + GOST_DB_PASSWORD);
        } else {
            System.out.println("GOST_DB_PASSWORD cannot be null, exiting...");
            System.exit(0);
        }
    }

    public void setDATASTREAM_IDS(String value) {
        if (value != null && !value.isEmpty()) {
            DATASTREAM_IDS = value;
            DATASTREAM_IDS = DATASTREAM_IDS.startsWith(",") ? DATASTREAM_IDS.substring(1) : DATASTREAM_IDS;
            DATASTREAM_IDS = DATASTREAM_IDS.replaceAll(",$", "");
            StringTokenizer tok = new StringTokenizer(DATASTREAM_IDS,",");
            while(tok.hasMoreTokens())
            {
                String current = tok.nextToken();
                try {
                    int id = Integer.parseInt(current);
                }
                catch(NumberFormatException e)
                {
                    System.out.println("Datastream id "+current+" is not a parsable Integer");
                    System.exit(0);
                }

            }
            System.out.println("DATASTREAM_IDS is " + DATASTREAM_IDS);
        } else {
            System.out.println("DATASTREAM_IDS not set, default is all");
        }
    }

    public void setOBSERVATION_ID_START(String value) {
        if (value != null && !value.isEmpty()) {
            OBSERVATION_ID_START = Integer.parseInt(value);
            System.out.println("OBSERVATION_ID_START is " + OBSERVATION_ID_START);
        } else {
            System.out.println("OBSERVATION_ID_START not set, default is "+OBSERVATION_ID_START);
        }
    }

    public void setOBSERVATION_ID_END(String value) {
        if (value != null && !value.isEmpty()) {
            OBSERVATION_ID_END = Integer.parseInt(value);
            System.out.println("OBSERVATION_ID_END is " + OBSERVATION_ID_END);
        } else {
            System.out.println("OBSERVATION_ID_END not set, default is "+OBSERVATION_ID_END);
        }
    }

    public void setOBSERVATION_INTERVAL_MS(String value) {
        if (value != null && !value.isEmpty()) {
            OBSERVATION_INTERVAL_MS = Integer.parseInt(value);
            System.out.println("OBSERVATION_INTERVAL_MS is " + OBSERVATION_INTERVAL_MS);
        } else {
            System.out.println("OBSERVATION_INTERVAL_MS not set, default is "+OBSERVATION_INTERVAL_MS);
        }
    }

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
        String clientId = "ObservationReplayer_" + DATASTREAM_IDS + "_" + OBSERVATION_ID_START + "_" + OBSERVATION_ID_END;
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
            String query = null;
            if(DATASTREAM_IDS != null) {
                query = "SELECT id, data, stream_id FROM v1.observation where stream_id in (" + DATASTREAM_IDS + ") and id between " + OBSERVATION_ID_START + " and " + OBSERVATION_ID_END + " order by id asc;";
            }
            else
            {
                query = "SELECT id, data, stream_id FROM v1.observation where id between " + OBSERVATION_ID_START + " and " + OBSERVATION_ID_END + " order by id asc;";
            }
            System.out.println("query = " + query);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                int id = rs.getInt("id");
                int stream_id = rs.getInt("stream_id");
                String data = rs.getString("data");
                System.out.println("Read observation with id " + id + ", stream_id " + stream_id + ":");
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

    public ObservationReplayer() {

        setBROKER_URI(System.getenv("BROKER_URI"));
        setBROKER_USERNAME(System.getenv("BROKER_USERNAME"));
        setBROKER_PASSWORD(System.getenv("BROKER_PASSWORD"));
        setGOST_MQTT_PREFIX(System.getenv("GOST_MQTT_PREFIX"));
        setGOST_DB_HOST(System.getenv("GOST_DB_HOST"));
        setGOST_DB_DATABASE(System.getenv("GOST_DB_DATABASE"));
        setGOST_DB_USER(System.getenv("GOST_DB_USER"));
        setGOST_DB_PASSWORD(System.getenv("GOST_DB_PASSWORD"));
        setDATASTREAM_IDS(System.getenv("DATASTREAM_IDS"));
        setOBSERVATION_ID_START(System.getenv("OBSERVATION_ID_START"));
        setOBSERVATION_ID_END(System.getenv("OBSERVATION_ID_END"));
        setOBSERVATION_INTERVAL_MS(System.getenv("OBSERVATION_INTERVAL_MS"));

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
