package History;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import static us.bpsm.edn.Keyword.newKeyword;

import com.sun.javafx.UnmodifiableArrayList;
import us.bpsm.edn.Keyword;
import us.bpsm.edn.parser.CollectionBuilder;
import us.bpsm.edn.parser.Parseable;
import us.bpsm.edn.parser.Parser; // For edn file
import us.bpsm.edn.parser.Parsers;

public class HistoryReader {

    public static Keyword type = newKeyword("type");
    public static Keyword ok = newKeyword("ok");
    public static Keyword process = newKeyword("process");
    public static Keyword value = newKeyword("value");
    public static Keyword startTimestamp = newKeyword("start-timestamp");
    public static Keyword commitTimestamp = newKeyword("commit-timestamp");
    public static Keyword w = newKeyword("w");
    public static Keyword r = newKeyword("r");


    public static void main(String[] args) throws IOException {
        String URLHistory = "E:\\Programs\\Java-Programs\\\\Snapshot-Isolation-Checker-Java\\src\\main\\resources\\example\\history.edn";
        String URLWTLog = "E:\\Programs\\Java-Programs\\\\Snapshot-Isolation-Checker-Java\\src\\main\\resources\\example\\wiredtiger.log";

        // read history.edn
        try {
            BufferedReader in = new BufferedReader(new FileReader(URLHistory));
            String line;
            while ((line = in.readLine()) != null) {
//                System.out.println(line);
                Parseable pbr = Parsers.newParseable(line);
                Parser p = Parsers.newParser(Parsers.defaultConfiguration());
                Map<?, ?> m = (Map<?, ?>) p.nextValue(pbr);

                if (m.get(type) != ok) {
                    continue;
                }

                Long start = (Long) m.get(startTimestamp);
                Long commit = (Long) m.get(commitTimestamp);
                Long session = (Long) m.get(process);

                List<?> values = (List<?>) m.get(value);
                for (Object o : values) {
                    List<?> ops = (List<?>) o;
                    if (ops.get(0) == w) {
                        long key = (Long) ops.get(1);
                        long value = (Long) ops.get(2);
                        System.out.println(":w "+key+" " + value);
                    } else {
                        long key = (Long) ops.get(1);
                        long value = (Long) ops.get(2);
                        System.out.println(":r "+key+" " + value);
                    }
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
