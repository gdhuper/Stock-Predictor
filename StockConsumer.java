package Lab2;

import org.apache.kafka.clients.consumer.*;


import java.io.IOException;
import java.util.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class StockConsumer {

    // Declare a new consumer
    public static KafkaConsumer<String, JsonNode> consumer;

    public static void main(String[] args) throws IOException {
        // check command-line arguments
        if(args.length != 4) {
            System.err.println("usage: StockConsumer <broker-socket> <input-topic> <stock-symbol> <group-id> <threshold-%>");
            System.err.println("e.g.: StockConsumer localhost:9092 stats orcl mycg 0.5");
            System.exit(1);
        }
        
        // initialize varaibles
        String brokerSocket = args[0];
        String inputTopic = args[1];
        String stockSymbol = args[2];
        String groupId = args[3];
        double thresholdPercentage = Double.parseDouble(args[4]);
        
        long pollTimeOut = 1000;

        
        // configure consumer
        configureConsumer(brokerSocket, groupId);
        
        // TODO subscribe to the topic

        // TODO loop infinitely -- pulling messages out every pollTimeOut ms
        
        // TODO iterate through message batch
        
        // TODO create a ConsumerRecord from message
        
        // TODO pull out statistics from message
        
        // TODO calculate batch statistics meanHigh, meanLow, meanOpen, meanClose, meanVolume
        
        // TODO calculate currentAggregatedStatistic and compare to previousAggregatedStatistic
        
        // TODO determine if delta percentage is greater than threshold 
        
        // TODO print output to screen
        
        
    }

    public static void configureConsumer(String brokerSocket, String groupId) {
        Properties props = new Properties();
        props.put("value.deserializer", "org.apache.kafka.connect.json.JsonDeserializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("bootstrap.servers", brokerSocket);
        props.put("group.id", groupId);
        props.put("auto.commit.enable", true);

        consumer = new KafkaConsumer<String, JsonNode>(props);
    }
}

