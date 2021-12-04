import java.nio.ByteBuffer;
import java.util.Collections;

import jiffy.JiffyClient;
import jiffy.storage.HashTableClient;
import jiffy.notification.HashTableListener;
import jiffy.directory.directory_service.Client;
import jiffy.notification.event.Notification;
import jiffy.util.ByteBufferUtils;


public class JiffyExample {

    public static void main(String... args) throws Exception {
        System.out.println("hello world");

        JiffyClient client = new JiffyClient("127.0.0.1", 9090, 9091);
        String op1 = "put";
        ByteBuffer key = ByteBufferUtils.fromString("key1");
        ByteBuffer value = ByteBufferUtils.fromString("value1");
        client.createHashTable("/a/file.txt", "local://tmp", 1, 1);
        // Create listener for specific file
        HashTableListener n1 = client.listenOnHashTable("/a/file.txt");
        // Subscribe for a operation
        n1.subscribe(Collections.singletonList(op1));

        HashTableClient kv = client.openHashTable("/a/file.txt");
        kv.put(key, value);
        kv.remove(key);

        // Receive notification
        Notification N1 = n1.getNotification();
        System.out.println(N1);

        // Unsubscribe operation
        n1.unsubscribe(Collections.singletonList(op1));

        client.close("/a/file.txt");
        client.remove("/a/file.txt");
        client.close();
    }
    
}
