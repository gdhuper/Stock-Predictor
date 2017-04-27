package Lab2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
        
public class StockProducer {
    // Set the stream and topic to publish to.
    public static String topic;
    
        
    // Declare a new producer
    public static KafkaProducer<String, JsonNode> producer;
        
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        // check command-line args
        if(args.length != 5) {
            System.err.println("usage: StockProducer <broker-socket> <input-file> <stock-symbol> <output-topic> <sleep-time>");
            System.err.println("eg: StockProducer localhost:9092 /user/user01/LAB2/orcl.csv orcl prices 1000");
            System.exit(1);
        }
        
        // initialize variables
        String brokerSocket = args[0];
        String inputFile = args[1];
        String stockSymbol = args[2];
        String outputTopic = args[3];
        topic = outputTopic;
        long sleepTime = Long.parseLong(args[4]);
        
        
        // configure the producer
        configureProducer(brokerSocket);
        
        // TODO create a buffered file reader for the input file
 
        BufferedReader br = new BufferedReader(new FileReader(inputFile));
        String temp1 = null;
        String temp2 = br.readLine();
        String temp3 = br.readLine();

       
        
     // TODO loop through all lines in input file
        while(true)
        {
        	temp1 = temp2;
        	temp2 = temp3;
        	temp3 = br.readLine();
        	if(temp3 == null)
        	{
        		break; //skips last two records
        	}
        	else
        	{
                // TODO create an ObjectNode to store data in
        		ObjectNode obj = JsonNodeFactory.instance.objectNode();
 
        		// TODO parse out the fields from the line and create key-value pairs in ObjectNode               
        		String[] arr = temp1.split(",");
        		
        		// TODO filter out "bad" records
        		if(arr.length !=0 && arr[0] != null && arr[1] != null && arr[2] != null && arr[3] != null && arr[4] != null && arr[5]!= null){
        		obj.put("timestamp", arr[0]);
        		obj.put("open", arr[1]);
        		obj.put("high", arr[2]);
        		obj.put("low", arr[3]);
        		obj.put("close", arr[4]);
        		obj.put("volume", arr[5]);
        		obj.put("stockSymbol", stockSymbol);
        		
               ProducerRecord<String, JsonNode> rec = new ProducerRecord<String, JsonNode>(topic, obj);
               System.out.println(stockSymbol + " : " + obj.toString()); //debugging
               try{
               producer.send(rec);
               }
               catch (Exception e)
               {
            	   System.out.println(e.getMessage());
               }       
               // TODO sleep the thread
               Thread.sleep(sleepTime);
        		}
        	
        		
        	}
        }
        
        // TODO close buffered reader
        br.close();
        
        // TODO close producer
        producer.close();
        
              
        
    }

    public static void configureProducer(String brokerSocket) {
        Properties props = new Properties();
        props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("bootstrap.servers", brokerSocket);
        producer = new KafkaProducer<String, JsonNode>(props);
    }
}
