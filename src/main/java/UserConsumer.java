import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class UserConsumer {

    public static void main(String[] args) throws Exception {

        String topicName = "user";
        String groupName = "test-group";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", groupName);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "UserDeserializer");

        KafkaConsumer<String, User> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));

        while (true) {
            ConsumerRecords<String, User> records = consumer.poll(100);
            for (ConsumerRecord<String, User> record : records) {
                System.out.println("id= " + String.valueOf(record.value().getID()) + " Name = " + record.value().getName() +" age = "+ record.value().getAge()+ " course = "+ String.valueOf(record.value().getCourse()));
                File file =new File("C:\\Users\\Aakash\\IdeaProjects\\kafka-test\\output.txt");
                FileWriter fw = new FileWriter(file,true);
                BufferedWriter bw = new BufferedWriter(fw);
                PrintWriter pw = new PrintWriter(bw);
                pw.println("id= " + String.valueOf(record.value().getID()) + " Name = " + record.value().getName() +" age = "+ record.value().getAge()+ " course = "+ String.valueOf(record.value().getCourse()));

                pw.close();
            }
        }

    }
}




