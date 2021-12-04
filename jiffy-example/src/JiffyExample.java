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

public class JiffyExample {

    private static ChronicleMap<String, Long> createChronicleMap(JiffyClient client) throws IOException {
        String[] filePrefixes = {
            "namespaceKeyStateNameToValue",
        };
        File[] files = createPersistedFiles(filePrefixes, client);

        ChronicleMapBuilder<String, Long> cmapBuilder =
                ChronicleMapBuilder.of(String.class, Long.class)
                        .name("key-and-namespace-to-values")
                        .entries(1_000_000);
        cmapBuilder.averageKeySize(64);

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
                "local://tmp"
            );
            } catch (Exception e) {
                System.out.println(e);
            }
            

            files[i] = new File(
                "/tmp/BackendChronicleMaps/JiffyExample/"
                + filePrefixes[i] 
                + ".txt"
            );
            // new File(
            //                 OS.getTarget()
            //                         + "/BackendChronicleMaps/"
            //                         + this.className
            //                         + "/"
            //                         + filePrefixes[i]
            //                         + "_"
            //                         + Integer.toString(this.numKeyedStatesBuilt)
            //                         + ".dat");

            files[i].getParentFile().mkdirs();
            // files[i].delete();
            // files[i].createNewFile();
        }
        return files;
    }

    public static void main(String... args) throws Exception {

        JiffyClient client = new JiffyClient("127.0.0.1", 9090, 9091);
        ChronicleMap kvStore = createChronicleMap(client);

        for(int i=1; i<=100; i++){
            kvStore.put(String.valueOf(i), 1L*i);
            System.out.println(kvStore.get(String.valueOf(i)));
        }
        

        // FileWriter writer = client.createFile("/a/file.txt", "local://tmp");
        // FileReader reader = client.openFile("/a/file.txt");

        // ByteBuffer buf = ByteBufferUtils.fromString("abcdefghijklmnopqrstuvwxy");
        // writer.write(buf);

        // reader.seek(0);
        // buf = reader.read(3);
        // buf.rewind();
        // byte[] arr = new byte[buf.remaining()];
        // System.out.println(buf.get(arr));

        client.remove("/BackendChronicleMaps/JiffyExample/namespaceKeyStateNameToValue.txt");

        client.close();
    }
    
}
