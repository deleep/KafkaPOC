package main.java.com.kafkaservice;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ConsumerService {

public static Properties properties = new Properties();
    
	static {
		properties.put("bootstrap.servers", "54.84.108.48:9092");
		properties.put("group.id", "test");
		properties.put("enable.auto.commit", true);
		properties.put("auto.commit.interval.ms", 1000);
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("session.timeout.ms", 10000);
		properties.put("fetch.min.bytes", 50000);
		properties.put("receive.buffer.bytes", 262144);
		properties.put("max.partition.fetch.bytes", 2097152);
		//properties.put("message.max.bytes", 2000000);
		properties.put("max.partition.fetch.bytes", 2000000);
		properties.put("max.partition.fetch.bytes", 2000000);
		
		
		
		  
	}
	
	public ConsumerService(String[] args) {
		try
		{
	  /*  ObjectMapper mapper = new ObjectMapper();
        Histogram stats = new Histogram(1, 10000000, 2);
        Histogram global = new Histogram(1, 10000000, 2);

*/       KafkaConsumer<String, String> consumer;
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("fast-messages", "summary-markers"));
        int timeouts = 0;
        
        //getRecords(consumer);
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
 	    
         while (true) {
        	 ConsumerRecords<String, String> records = consumer.poll(100);
	         for (ConsumerRecord<String, String> record : records) {
	        	 
	             buffer.add(record);
	             System.out.println("record key.."+record.key()+"   value.."+record.value());
	         }
	         System.out.println("List size:  "+buffer.size());
	         buffer.clear();
	     }
     
        /*
        while (true) {
            // read records with a short timeout. If we time out, we don't really care.
        	System.out.println("trying to pull");
            ConsumerRecords<String, String> records = consumer.poll(100);
            if (records.count() == 0) {
                timeouts++;
            } else {
                System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
                timeouts = 0;
            }
       
            
            for (ConsumerRecord<String, String> record : records) {
                switch (record.topic()) {
                    case "fast-messages":
                        // the send time is encoded inside the message
                        JsonNode msg = mapper.readTree(record.value());
                        switch (msg.get("type").asText()) {
                            case "test":
                                long latency = (long) ((System.nanoTime() * 1e-9 - msg.get("t").asDouble()) * 1000);
                                stats.recordValue(latency);
                                global.recordValue(latency);
                                break;
                            case "marker":
                                // whenever we get a marker message, we should dump out the stats
                                // note that the number of fast messages won't necessarily be quite constant
                                System.out.printf("%d messages received in period, latency(min, max, avg, 99%%) = %d, %d, %.1f, %d (ms)\n",
                                        stats.getTotalCount(),
                                        stats.getValueAtPercentile(0), stats.getValueAtPercentile(100),
                                        stats.getMean(), stats.getValueAtPercentile(99));
                                System.out.printf("%d messages received overall, latency(min, max, avg, 99%%) = %d, %d, %.1f, %d (ms)\n",
                                        global.getTotalCount(),
                                        global.getValueAtPercentile(0), global.getValueAtPercentile(100),
                                        global.getMean(), global.getValueAtPercentile(99));

                                stats.reset();
                                break;
                            default:
                                throw new IllegalArgumentException("Illegal message type: " + msg.get("type"));
                        }
                        break;
                    case "summary-markers":
                        break;
                    default:
                        throw new IllegalStateException("Shouldn't be possible to get message on topic " + record.topic());
                }
            } 
        } */
	    
		}  
		catch(Exception e){
        	e.printStackTrace();
        }
}
   public void getRecords(KafkaConsumer<String, String> consumer){
	  try{
	   //List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
	     while (true) {
	    	 List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
	   	  
	         ConsumerRecords<String, String> records = consumer.poll(200);
	         for (ConsumerRecord<String, String> record : records) {
	        	 
	             buffer.add(record);
	         }
	        // System.out.println("record key.."+record.key()+"   value.."+record.value());
	         System.out.println("List size:"+buffer.size());
	         buffer.clear();
	     }
	  }  
		catch(Exception e){
      	e.printStackTrace();
      }
   }
	
	public static void main(String[] args)  {
        // set up house-keeping
        }
}
