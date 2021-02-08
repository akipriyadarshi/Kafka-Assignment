import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.errors.SerializationException;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.nio.ByteBuffer;

public class UserSerializer implements Serializer<User> {
    private String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // nothing to configure
    }

    @Override
    public byte[] serialize(String topic, User data) {
        int sizeOfName;
        int sizeOfCourse;
        byte[] serializedName;
        byte[] serializedCourse;

        try {
            if (data == null)
                return null;

            serializedName = data.getName().getBytes(encoding);
            sizeOfName = serializedName.length;
           serializedCourse = data.getCourse().getBytes(encoding);
            sizeOfCourse = serializedCourse.length;

            ByteBuffer buf = ByteBuffer.allocate( 4 + sizeOfName + 4 + 4 + 4 + sizeOfCourse );
            buf.putInt(data.getID());
            buf.putInt(sizeOfName);
            buf.put(serializedName);
            buf.putInt(sizeOfCourse);
            buf.put(serializedCourse);
            buf.putInt(data.getAge());

            return buf.array();

        } catch (Exception e) {
            throw new SerializationException("Error when serializing User to byte[]");
        }
    }

    @Override
    public void close() {
        // nothing to do
    }
}


