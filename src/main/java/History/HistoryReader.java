package History;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static us.bpsm.edn.Keyword.newKeyword;

import us.bpsm.edn.Keyword;
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

    public static ArrayList<Transaction> readHistory(String urlHistory, String urlWTLog) {
        ArrayList<Transaction> histories = new ArrayList<Transaction>();

        HashMap<List<Long>, Integer> KVTxnMap = new HashMap<>();

        // 1. Reading history.edn
        try {
            BufferedReader in = new BufferedReader(new FileReader(urlHistory));
            String line;
            int idx = 0;
            while ((line = in.readLine()) != null) {
                Parseable pbr = Parsers.newParseable(line);
                Parser p = Parsers.newParser(Parsers.defaultConfiguration());
                Map<?, ?> m = (Map<?, ?>) p.nextValue(pbr);

                if (m.get(type) != ok) {
                    continue;
                }

                Long start = (Long) m.get(startTimestamp);
                Long commit = (Long) m.get(commitTimestamp);
                Long session = (Long) m.get(process);

                // Now we have not tid, it will be assigned until reading wiredtiger.log
                Transaction txn = new Transaction(-1, session, start, commit);

                List<?> values = (List<?>) m.get(value);
                for (Object o : values) {
                    List<?> ops = (List<?>) o;
                    long key = (Long) ops.get(1);
                    long val;
                    try {
                        val = (Long) ops.get(2);
                    } catch (NullPointerException e) {
                        val = 0;
                    }
                    OPType type = null;
                    if (ops.get(0) == w) {
                        type = OPType.write;
                        KVTxnMap.put(Arrays.asList(key, val), idx);
                    } else {
                        type = OPType.read;
                    }
                    txn.addOperation(new Operation(type, key, val));
                }
                idx++;

                histories.add(txn);
            }
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 2. Reading wiredtiger.log
        try {
            BufferedReader in = new BufferedReader(new FileReader(urlWTLog));
            String line;
            while ((line = in.readLine()) != null) {
//                System.out.println(line);
                String[] infos = line.split(", ");
                Long tid = Long.valueOf(infos[0].split(":")[1]);
                Long key = Long.valueOf(infos[1].split(":")[1]);
                Long value = Long.valueOf(infos[2].split(":")[1]);
                int idx = KVTxnMap.get(Arrays.asList(key, value));
                if(histories.get(idx).tid == -1){
                    histories.get(idx).tid = tid;
                }else{
                    if(histories.get(idx).tid != tid){
                        System.exit(-1);
                    }
                }
            }
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        return histories;
    }


    public static void main(String[] args) throws IOException {
        String URLHistory = "E:\\Programs\\Java-Programs\\Snapshot-Isolation-Checker-Java\\src\\main\\resources\\example\\history.edn";
        String URLWTLog = "E:\\Programs\\Java-Programs\\Snapshot-Isolation-Checker-Java\\src\\main\\resources\\example\\wiredtiger.log";

        ArrayList<Transaction> transactions = HistoryReader.readHistory(URLHistory, URLWTLog);
        for (Transaction txn : transactions) {
            System.out.println(txn);
        }

    }
}
