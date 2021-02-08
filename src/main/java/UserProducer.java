
import java.util.*;
import java.lang.Math;

import org.apache.kafka.clients.producer.*;

public class UserProducer {

    public static void main(String[] args) throws Exception {

        String topicName = "user";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "UserSerializer");

        Producer<String, User> producer = new KafkaProducer<>(props);


         int max=24;
         int min=16;
         int range=max-min+1;

        for(int i=1;i<=50;i++) {
            int rand= (int)(Math.random()*range)+min;
            User user = new User(i, "Akash Priyadarshi ", rand, "B.Tech");
            producer.send(new ProducerRecord<String, User>(topicName, Integer.toString(i), user)).get();

        }

        System.out.println("UserProducer Completed.");
        producer.close();

    }
}