package main.java.com.kafkaservice;

import java.io.IOException;

public class KafkaService {

	public KafkaService() {
	}

	
	public static void main(String[] args)  {
        if (args.length < 1) {
            throw new IllegalArgumentException("Must have either 'producer' or 'consumer' as argument");
        }
        switch (args[0]) {
            case "producer":
                new ProducerService(args);
                break;
            case "consumer":
             new ConsumerService(args);
                break;
            default:
                throw new IllegalArgumentException("Don't know how to do " + args[0]);
        }
    }
	
}
