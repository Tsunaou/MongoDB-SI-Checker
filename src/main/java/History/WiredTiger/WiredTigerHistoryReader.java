package History.WiredTiger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import static us.bpsm.edn.Keyword.newKeyword;

import Exceptions.HistoryInvalidException;
import History.HistoryReader;
import History.MongoDB.LogicalClock;
import History.OPType;
import History.Operation;
import History.WiredTiger.LSN.WtLog;
import History.WiredTiger.LSN.WtOp;
import History.WiredTiger.LSN.WtTxn;
import TestUtil.Finals;
import Const.Const;
import us.bpsm.edn.Keyword;
import us.bpsm.edn.parser.Parseable;
import us.bpsm.edn.parser.Parser; // For edn file
import us.bpsm.edn.parser.Parsers;

public class WiredTigerHistoryReader extends HistoryReader {

    public static WiredTigerHistory readHistory(String urlHistory, String urlWTLog) throws HistoryInvalidException {
        ArrayList<WiredTigerTransaction> transactions = new ArrayList<>();
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

                LogicalClock start = new LogicalClock((Long) m.get(startTimestamp), 0);
                LogicalClock commit = new LogicalClock((Long) m.get(commitTimestamp), 0);
                Long session = (Long) m.get(process);

                // Now we have not tid, it will be assigned until reading wiredtiger.log
                WiredTigerTransaction txn = new WiredTigerTransaction(Const.INIT_TID, session, start, commit);

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

        return new WiredTigerHistory(transactions, wtLog);
    }

    public static void main(String[] args) throws IOException, HistoryInvalidException {
        String URLHistory = Finals.URLHistory;
        String URLWTLog = Finals.URLWTLog;

        WiredTigerHistory history = WiredTigerHistoryReader.readHistory(URLHistory, URLWTLog);
        ArrayList<WiredTigerTransaction> transactions = history.transactions;
        for (WiredTigerTransaction txn : transactions) {
            System.out.println(txn);
        }

    }
}
