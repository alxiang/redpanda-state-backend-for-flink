import java.nio.ByteBuffer;
import java.util.Collections;

import jiffy.JiffyClient;
import jiffy.storage.FileWriter;
import jiffy.storage.FileReader;
import jiffy.storage.HashTableClient;
import jiffy.notification.HashTableListener;
import jiffy.directory.directory_service.Client;
import jiffy.notification.event.Notification;
import jiffy.util.ByteBufferUtils;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.File;
import java.io.IOException;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class JiffyExample {

    private static ChronicleMap<String, Long> createChronicleMap(JiffyClient client) throws IOException {
        String[] filePrefixes = {
            "namespaceKeyStateNameToValue",
        };
        File[] files = createPersistedFiles(filePrefixes, client);

        ChronicleMapBuilder<String, Long> cmapBuilder =
                ChronicleMapBuilder.of(String.class, Long.class)
                        .name("key-and-namespace-to-values")
                        .entries(20_000_000L);
        cmapBuilder.averageKeySize(10);
        // https://github.com/OpenHFT/Chronicle-Map/issues/130
        //https://www.javadoc.io/doc/net.openhft/chronicle-map/3.13.0/net/openhft/chronicle/hash/ChronicleHashBuilder.html#maxBloatFactor-double-

        return cmapBuilder.createPersistedTo(files[0]);
    }

    private static File[] createPersistedFiles(String[] filePrefixes, JiffyClient client) throws IOException {
        File[] files = new File[filePrefixes.length];
        for (int i = 0; i < filePrefixes.length; i++) {

            try {
                FileWriter writer = client.createFile(
                "/BackendChronicleMaps/JiffyExample/"
                + filePrefixes[i] 
                + ".txt", 
                "local://tmp",
                "172.27.77.254"
            );
            } catch (Exception e) {
                System.out.println(e);
            }
            
            files[i] = new File(
                "/tmp/BackendChronicleMaps/JiffyExample/"
                + filePrefixes[i] 
                + ".txt"
            );
            files[i].getParentFile().mkdirs();
        }
        return files;
    }

    public static void main(String... args) throws Exception {

        InetAddress ip;
        String hostname;
        try {
            ip = InetAddress.getLocalHost();
            hostname = ip.getHostName();
            System.out.println("Your current IP address : " + ip);
            System.out.println("Addr: " + ip.getHostAddress());
            System.out.println("Your current Hostname : " + hostname);
 
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        JiffyClient client = new JiffyClient("127.0.0.1", 9090, 9091);
        ChronicleMap kvStore = createChronicleMap(client);

        for(int i=1; i<=10_000_000; i++){

            if(i % 100_000 == 0){
                System.out.println(i);
            }

            try{
                kvStore.put(String.valueOf(i), 1L*i);
            }
            catch (Exception e) {
                System.out.printf("failed at record %d\n", i);
                e.printStackTrace();
                client.remove("/BackendChronicleMaps/JiffyExample/namespaceKeyStateNameToValue.txt");
                client.close();
                System.exit(-1);
                
            }
            // System.out.println("temp");
           
            // System.out.println(kvStore.get(String.valueOf(i)));
        }

        client.remove("/BackendChronicleMaps/JiffyExample/namespaceKeyStateNameToValue.txt");
        client.close();
    }
    
}
