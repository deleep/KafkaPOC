package main.java.com.kafkaservice;

import java.io.InputStream;
import java.util.Properties;
import java.io.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerService {

public static Properties properties = new Properties();
    
	static {
		properties.put("bootstrap.servers", "54.84.108.48:9092");
		properties.put("acks", "all");
		properties.put("retries", 0);
		properties.put("batch.size", 16384);
		properties.put("auto.commit.interval.ms", 1000);
		properties.put("linger.ms", 0);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("block.on.buffer.full", true);
		properties.put("message.max.bytes", 200000000);
		
		
		  
	}
	 
	public String readfile(String filename){
	
	    try {
	    	
		BufferedReader reader = new BufferedReader(new FileReader ("d:/"+filename));
	    String         line = null;
	    StringBuilder  stringBuilder = new StringBuilder();
	    String         ls = System.getProperty("line.separator");

	        while((line = reader.readLine()) != null) {
	            stringBuilder.append(line);
	            
	            stringBuilder.append(ls);
	        }
  
	       // System.out.println("string ..."+ stringBuilder.toString());
	        
	        return stringBuilder.toString();
	    }catch(Exception e){
	    	e.printStackTrace();
	    }
	    
	    return null;
	}
	public ProducerService(String[] args) {
	
		KafkaProducer<String, String> producer;
        producer = new KafkaProducer<>(properties);
       
	   try {
           // for (int i = 0; i < 50; i++) {
               // producer.send(new ProducerRecord<String, String>( "fast-messages",properties.toString()));
                producer.send(new ProducerRecord<String, String>( "fast-messages",readfile(args[1])));
                System.out.printf("Send ...");
              //  producer.wait(1000);
            //}
        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
        }
	}
  
	 public static void main(String[] args)  {
	        // set up the producer
		 new ProducerService(args);
	 }
	 
}
