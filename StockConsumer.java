package Lab2;

import org.apache.kafka.clients.consumer.*;


import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class StockConsumer {

    // Declare a new consumer
    public static KafkaConsumer<String, JsonNode> consumer;

    public static void main(String[] args) throws IOException {
        // check command-line arguments
        if(args.length != 5) {
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
        List<String> topics = new ArrayList<>();
        topics.add(inputTopic);
        consumer.subscribe(topics);
        
        // TODO loop infinitely -- pulling messages out every pollTimeOut ms
       
        double meanMeanHigh = 0, meanMeanLow = 0, meanMeanOpen = 0, meanMeanClose = 0, meanMeanVolume= 0;
         
        while(true) {
            // Request unread messages from the topic.
            ConsumerRecords<String, JsonNode> consumerRecords = consumer.poll(pollTimeOut);
            
            Iterator<ConsumerRecord<String, JsonNode>> iterator = consumerRecords.iterator();
            while (iterator.hasNext()) {
            	// TODO create a ConsumerRecord from message
                    ConsumerRecord<String, JsonNode> record = iterator.next();
                                                
                 // TODO iterate through message batch
                    ObjectNode obj = (ObjectNode) record.value();
                                       
                                        
                    // TODO pull out statistics from message
                    meanMeanHigh += obj.get("meanHigh").asDouble();
                    meanMeanLow += obj.get("meanLow").asDouble();
                 	meanMeanOpen += obj.get("meanOpen").asDouble();
                  	meanMeanClose += obj.get("meanClose").asDouble();
                    meanMeanVolume += obj.get("meanVolume").asDouble();
                    
                    // TODO calculate batch statistics meanHigh, meanLow, meanOpen, meanClose, meanVolume
                    
                    // TODO calculate currentAggregatedStatistic and compare to previousAggregatedStatistic
                    
                    // TODO determine if delta percentage is greater than threshold 
                    
                    // TODO print output to screen
                   // set previos to next
                    
             } 
            System.out.println(meanMeanClose/consumerRecords.count());
            System.out.println(meanMeanOpen/consumerRecords.count());
            
            System.out.println(meanMeanLow/consumerRecords.count());
            System.out.println(meanMeanHigh/consumerRecords.count());
            System.out.println(meanMeanVolume/consumerRecords.count());
            
        }

        
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

