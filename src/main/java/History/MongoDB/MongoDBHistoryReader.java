package History.MongoDB;

import Const.Const;
import Exceptions.HistoryInvalidException;
import History.HistoryReader;
import History.MongoDB.Oplog.OplogHistory;
import History.MongoDB.Oplog.OplogTxn;
import History.MongoDB.Oplog.TxnType;
import History.OPType;
import History.Operation;
import History.WiredTiger.WiredTigerTransaction;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONReader;
import us.bpsm.edn.parser.Parseable;
import us.bpsm.edn.parser.Parser;
import us.bpsm.edn.parser.Parsers;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class MongoDBHistoryReader extends HistoryReader {
    public static MongoDBHistory readHistory(String urlHistory, String urlOplog) throws HistoryInvalidException {
        ArrayList<MongoDBTransaction> transactions = new ArrayList<>();
        OplogHistory oplogHistory = new OplogHistory();
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

                Long session = (Long) m.get(process);

                // Now we have not tid, it will be assigned until reading wiredtiger.log
                MongoDBTransaction txn = new MongoDBTransaction(session);

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

        // 2. Reading txns.json
        JSONReader reader = null;
        try {
            reader = new JSONReader(new FileReader(urlOplog));
            reader.startArray();
            while (reader.hasNext()){
                JSONObject obj = (JSONObject) reader.readObject();

                JSONArray ops = obj.getJSONArray("ops");
                JSONArray ts = obj.getJSONArray("commitTs");
                String type = obj.getString("type");
                JSONArray part = obj.getJSONArray("participants");

                LogicalClock clock = new LogicalClock(ts.getInteger(0), ts.getInteger(1));
                TxnType txnType = null;
                if(type.equals("replica")){
                    txnType = TxnType.REPLICA_SET_TXN;
                }else{
                    txnType = TxnType.SHARDED_CLUSTER_TXN;
                }
                ArrayList<String> participants = new ArrayList<>();
                for(int i=0; i<part.size(); i++){
                    participants.add(part.getString(i));
                }

                OplogTxn txn = new OplogTxn(clock, txnType,participants);

                for(Object op: ops){
                    JSONArray write = (JSONArray) op;
                    txn.add(new Operation(OPType.write, write.getLong(1), write.getLong(2)));
                }

                oplogHistory.add(txn);

            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        // 3. Merge information
        MongoDBTransaction txn = null;
        for(OplogTxn oplogTxn: oplogHistory.txns){
            Operation op = oplogTxn.ops.get(0);
            int idx = KVTxnMap.get(Arrays.asList(op.key, op.value));
            txn = transactions.get(idx);
            txn.txnType = oplogTxn.type;
            txn.participants = oplogTxn.participants;
            txn.setCommitClusterTime(oplogTxn.commitTimestamp);
        }


        return new MongoDBHistory(transactions, oplogHistory);
    }

    public static void main(String[] args) throws HistoryInvalidException {
        String URLHistory = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/latest/history.edn";
        String URLOplog = "/home/young/Programs/Jepsen-Mongo-Txn/logs/txns.json";

        MongoDBHistory history = MongoDBHistoryReader.readHistory(URLHistory, URLOplog);
        ArrayList<MongoDBTransaction> transactions = history.transactions;

        for (MongoDBTransaction txn : transactions) {
            System.out.println(txn);
        }
    }
}
