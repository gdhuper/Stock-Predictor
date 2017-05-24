s
/*
*Gurpreet Singh CS185 Lab2
*/

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
            
         // TODO loop through all lines in input file
        while(true)
        {
        	//temp1 = temp2;
        	//temp2 = temp3;
        	String temp = br.readLine();
        	if(temp == null)
        	{
        		break; //skips last two records
        	}
        	else
        	{
        		
                // TODO create an ObjectNode to store data in
        		ObjectNode obj = JsonNodeFactory.instance.objectNode();
 
        		// TODO parse out the fields from the line and create key-value pairs in ObjectNode               
        		String[] arr = temp.split(",");
        		if(isNumber(arr[0]) == true && isNumber(arr[1]) == true && isNumber(arr[2]) == true && isNumber(arr[3]) == true && isNumber(arr[4]) == true && isNumber(arr[5]) == true){
        		// TODO filter out "bad" records
        		if(arr.length !=0 && arr[0] != null && arr[1] != null && arr[2] != null && arr[3] != null && arr[4] != null && arr[5]!= null){
        		obj.put("timestamp", arr[0]);
        		obj.put("open", arr[1]);
        		obj.put("high", arr[2]);
        		obj.put("low", arr[3]);
        		obj.put("close", arr[4]);
        		obj.put("volume", arr[5]);
        		//obj.put("stockSymbol", stockSymbol); //double check if this is required
               ProducerRecord<String, JsonNode> rec = new ProducerRecord<String, JsonNode>(topic, stockSymbol, obj); //stock symbol
              // System.out.println(stockSymbol + " : " + obj.toString()); //debugging
               try{
               producer.send(rec);
               }
               catch (Exception e)
               {
            	   System.out.println("Record could not be sent!");
               }       
               // TODO sleep the thread
               Thread.sleep(sleepTime);
        		}
        	}
        		else
        		{
        			System.out.println("Discarding bad Record!");
        			continue;
        		}
        	    		
        	}
        }
        
        // TODO close buffered reader
        br.close();
        
        // TODO close producer
        producer.close();
                   
        
    }
    
    /*
     * Helper method to check if string is numeric or not 
     */
    public static boolean isNumber(String s)
    {
    	char[] ca = s.toCharArray();
    	for(char c: ca)
    	{
    		if(Character.isAlphabetic(c))
    		{
    			return false;
    		}
    		else{
    			continue;
    		}
    	}
    	return true;
    }

    public static void configureProducer(String brokerSocket) {
        Properties props = new Properties();
        props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("bootstrap.servers", brokerSocket);
        producer = new KafkaProducer<String, JsonNode>(props);
    }
}
