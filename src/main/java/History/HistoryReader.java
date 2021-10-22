package History;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import static us.bpsm.edn.Keyword.newKeyword;

import Exceptions.HistoryInvalidException;
import History.WiredTiger.WtLog;
import History.WiredTiger.WtOp;
import History.WiredTiger.WtTxn;
import TestUtil.Finals;
import Const.Const;
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

    public static History readHistory(String urlHistory, String urlWTLog) throws HistoryInvalidException {
        ArrayList<Transaction> transactions = new ArrayList<>();
        WtLog wtLog = new WtLog();
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
                Transaction txn = new Transaction(Const.INIT_TID, session, start, commit);

                List<?> values = (List<?>) m.get(value);
                for (Object o : values) {
                    List<?> ops = (List<?>) o;
                    long key = (Long) ops.get(1);
                    long val;
                    try {
                        val = (Long) ops.get(2);
                    } catch (NullPointerException e) {
                        val = Const.INIT_READ;
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

                transactions.add(txn);
            }
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 2. Reading wiredtiger.log
        try {
            BufferedReader in = new BufferedReader(new FileReader(urlWTLog));
            String line;
            HashMap<Long, WtTxn> tidTxnMap = new HashMap<>();
            while ((line = in.readLine()) != null) {
//                System.out.println(line);
                String[] infos = line.split(", ");
                long tid = Long.parseLong(infos[0].split(":")[1]);
                long key = Long.parseLong(infos[1].split(":")[1]);
                long value = Long.parseLong(infos[2].split(":")[1]);
                int idx = KVTxnMap.get(Arrays.asList(key, value));

                WtTxn txn;
                if(tidTxnMap.containsKey(tid)){
                    txn = tidTxnMap.get(tid);
                }else{
                    txn = new WtTxn(tid);
                    tidTxnMap.put(tid, txn);
                }

                txn.add(new WtOp(tid, key, value));

                if(transactions.get(idx).tid == Const.INIT_TID){
                    transactions.get(idx).tid = tid;
                }else{
                    if(transactions.get(idx).tid != tid){
                        System.exit(-1);
                    }
                }
            }
            for(Map.Entry<Long, WtTxn> entry : tidTxnMap.entrySet()){
                wtLog.add(entry.getValue());
            }
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new History(transactions, wtLog);
    }

    public static void main(String[] args) throws IOException, HistoryInvalidException {
        String URLHistory = Finals.URLHistory;
        String URLWTLog = Finals.URLWTLog;

        History history = HistoryReader.readHistory(URLHistory, URLWTLog);
        ArrayList<Transaction> transactions = history.transactions;
        for (Transaction txn : transactions) {
            System.out.println(txn);
        }

    }
}
