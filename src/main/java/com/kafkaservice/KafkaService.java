package main.java.com.kafkaservice;

import java.io.IOException;

public class KafkaService {

	public KafkaService() {
		// TODO Auto-generated constructor stub
	}

	
	public static void main(String[] args)  {
        if (args.length < 1) {
            throw new IllegalArgumentException("Must have either 'producer' or 'consumer' as argument");
        }
        switch (args[0]) {
            case "producer":
                ProducerService.main(args);
                break;
            case "consumer":
             new ConsumerService(args);
                break;
            default:
                throw new IllegalArgumentException("Don't know how to do " + args[0]);
        }
    }
	
}
