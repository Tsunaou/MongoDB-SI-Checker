package History.MongoDB;

import Const.Const;
import Exceptions.HistoryInvalidException;
import History.HistoryReader;
import History.MongoDB.Oplog.OplogHistory;
import History.MongoDB.Oplog.OplogTxn;
import History.MongoDB.Oplog.TxnType;
import History.OPType;
import History.Operation;
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

    private static final String URL_HISTORY = "src/main/resources/data-20220726/history.edn";
    private static final String URL_OPLOG = "src/main/resources/data-20220726/txns.json";
    private static final String URL_MONGOD_LOG = "src/main/resources/data-20220726/mongod.json";

    private static final String URL_RO_TXNS = "src/main/resources/data-20220726/ro_txns.json";

    public static MongoDBHistory readHistory(String urlHistory, String urlOplog, String urlMongodLog) throws HistoryInvalidException {
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
                Map<?, ?> mongod = (Map<?, ?>) m.get(sessionInfo);
                txn.uuid = (String) mongod.get(uuid);
                txn.txnNumber = (Long) mongod.get(txnNumber);

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
            while (reader.hasNext()) {
                JSONObject obj = (JSONObject) reader.readObject();

                JSONArray ops = obj.getJSONArray("ops");
                JSONArray ts = obj.getJSONArray("commitTs");
                String type = obj.getString("type");
                JSONArray part = obj.getJSONArray("participants");

                LogicalClock clock = new LogicalClock(ts.getInteger(0), ts.getInteger(1));
                TxnType txnType = null;
                if (type.equals("replica")) {
                    txnType = TxnType.REPLICA_SET_TXN;
                } else {
                    txnType = TxnType.SHARDED_CLUSTER_TXN;
                }
                ArrayList<String> participants = new ArrayList<>();
                for (int i = 0; i < part.size(); i++) {
                    participants.add(part.getString(i));
                }

                OplogTxn txn = new OplogTxn(clock, txnType, participants);

                for (Object op : ops) {
                    JSONArray write = (JSONArray) op;
                    txn.add(new Operation(OPType.write, write.getLong(1), write.getLong(2)));
                }

                oplogHistory.add(txn);

            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        // 3. Reading mongod.json
        HashMap<String, HashMap<Long, MongoDBTransaction>> uuidToTxnNumberToTransaction = new HashMap<>();
        for (MongoDBTransaction txn : transactions) {
            HashMap<Long, MongoDBTransaction> txnNumberToTransaction = null;
            if (uuidToTxnNumberToTransaction.containsKey(txn.uuid)) {
                txnNumberToTransaction = uuidToTxnNumberToTransaction.get(txn.uuid);
            } else {
                txnNumberToTransaction = new HashMap<>();
                uuidToTxnNumberToTransaction.put(txn.uuid, txnNumberToTransaction);
            }
            txnNumberToTransaction.put(txn.txnNumber, txn);
        }

        try {
            reader = new JSONReader(new FileReader(urlMongodLog));
            JSONObject obj = (JSONObject) reader.readObject();
            for (String uuid : obj.keySet()) {
                JSONObject txnNumber2timestamp = obj.getJSONObject(uuid);
                for (String txnNumber : txnNumber2timestamp.keySet()) {
                    JSONArray ts = txnNumber2timestamp.getJSONArray(txnNumber);
                    MongoDBTransaction txn;
                    try {
                        txn = uuidToTxnNumberToTransaction.get(uuid).get(Long.parseLong(txnNumber));
                    } catch (NullPointerException e) {
                        System.out.println("Timeout actually executed");
                        System.out.println(uuid);
                        System.out.println(txnNumber);
                        continue;
                    }
                    if (txn == null) {
                        throw new HistoryInvalidException("Some timeout write actually executed");
                    }

                    txn.txnNumber = Long.parseLong(txnNumber);
                    long time = ts.getLong(0);
                    long inc = ts.getLong(1);
                    txn.startTimestamp = new LogicalClock(time, inc);
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        // 4. Merge information
        MongoDBTransaction txn = null;
        for (OplogTxn oplogTxn : oplogHistory.txns) {
            if (oplogTxn.ops.isEmpty()) {
                // TODO: Figure out why there are transactions committed but no operations(may be read only transactions)
                continue;
            }
            Operation op = oplogTxn.ops.get(0);
            int idx;
            try {
                idx = KVTxnMap.get(Arrays.asList(op.key, op.value));
            } catch (NullPointerException e) {
                System.out.println(op);
                throw new HistoryInvalidException("May be timeout but actually executed");
            }

            txn = transactions.get(idx);
            txn.txnType = oplogTxn.type;
            txn.participants = oplogTxn.participants;
            txn.setCommitClusterTime(oplogTxn.commitTimestamp);
        }
        ArrayList<MongoDBTransaction> readOnlyTxns = new ArrayList<>();
        for (MongoDBTransaction transaction : transactions) {
            if (transaction.writeKeySet.isEmpty()) {
                readOnlyTxns.add(transaction);
            }
        }
        fillTsOfReadOnlyTxns(readOnlyTxns);

        return new MongoDBHistory(transactions, oplogHistory);
    }

    private static void fillTsOfReadOnlyTxns(ArrayList<MongoDBTransaction> readOnlyTxns) {
        System.out.println(readOnlyTxns.size());
        List<ArrayList<MongoDBTransaction>> uuidClassifications = new ArrayList<>();
        for (MongoDBTransaction txn : readOnlyTxns) {
            addToUuidClassifications(uuidClassifications, txn);
        }
        for (ArrayList<MongoDBTransaction> txnList : uuidClassifications) {
            txnList.sort((o1, o2) -> (int) (o1.txnNumber - o2.txnNumber));
        }
        uuidClassifications.sort(Comparator.comparing(o -> o.get(0).uuid));
        JSONReader reader;
        List<ArrayList<JSONObject>> uuidClassificationsJson = new ArrayList<>();
        try {
            reader = new JSONReader(new FileReader(URL_RO_TXNS));
            reader.startArray();
            while (reader.hasNext()) {
                JSONObject obj = (JSONObject) reader.readObject();
                addToUuidClassificationsJson(uuidClassificationsJson, obj);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        for (ArrayList<JSONObject> jsonObjectList : uuidClassificationsJson) {
            jsonObjectList.sort((o1, o2) -> {
                JSONArray ts1 = o1.getJSONArray("commitTs");
                JSONArray ts2 = o2.getJSONArray("commitTs");
                long t1 = ts1.getLong(0) * 1000 + ts1.getLong(1);
                long t2 = ts2.getLong(0) * 1000 + ts2.getLong(1);
                return (int) (t1 - t2);
            });
        }
        uuidClassificationsJson.sort(Comparator.comparing(o -> o.get(0).getString("UUID")));
        assert uuidClassifications.size() == uuidClassificationsJson.size();
        for (int i = 0; i < uuidClassifications.size(); i++) {
            ArrayList<MongoDBTransaction> txnList = uuidClassifications.get(i);
            ArrayList<JSONObject> jsonObjectList = uuidClassificationsJson.get(i);
            assert txnList.size() == jsonObjectList.size();
            assert txnList.get(0).uuid.equals(jsonObjectList.get(0).getString("UUID"));
            for (int j = 0; j < txnList.size(); j++) {
                MongoDBTransaction txn = txnList.get(j);
                JSONObject jsonObject = jsonObjectList.get(j);
                JSONArray ts = jsonObject.getJSONArray("commitTs");
                txn.commitTimestamp = new LogicalClock(ts.getLong(0), ts.getLong(1));
            }
        }
        /*
        for (ArrayList<JSONObject> jsonObjectList : uuidClassificationsJson) {
            System.err.println(jsonObjectList.size());
            for (JSONObject jsonObject : jsonObjectList) {
                System.err.println(jsonObject);
            }
        }
        for (ArrayList<MongoDBTransaction> txnList : uuidClassifications) {
            System.err.println(txnList.size());
            for (MongoDBTransaction txn : txnList) {
                System.err.println(txn);
            }
        }
         */
    }

    private static void addToUuidClassifications(List<ArrayList<MongoDBTransaction>> uuidClassifications,
                                                 MongoDBTransaction transaction) {
        for (ArrayList<MongoDBTransaction> txnList : uuidClassifications) {
            if (txnList.get(0).uuid.equals(transaction.uuid)) {
                txnList.add(transaction);
                return;
            }
        }
        ArrayList<MongoDBTransaction> txnList = new ArrayList<>();
        txnList.add(transaction);
        uuidClassifications.add(txnList);
    }

    private static void addToUuidClassificationsJson(List<ArrayList<JSONObject>> uuidClassificationsJson,
                                                     JSONObject jsonObject) {
        for (ArrayList<JSONObject> jsonObjectList : uuidClassificationsJson) {
            if (jsonObjectList.get(0).getString("UUID").equals(jsonObject.getString("UUID"))) {
                jsonObjectList.add(jsonObject);
                return;
            }
        }
        ArrayList<JSONObject> jsonObjectList = new ArrayList<>();
        jsonObjectList.add(jsonObject);
        uuidClassificationsJson.add(jsonObjectList);
    }

    public static void main(String[] args) throws HistoryInvalidException {
//        String URLHistory = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/latest/history.edn";
//        String URLOplog = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/latest/txns.json";
//        String URLMongodLog = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/latest/mongod.json";

        MongoDBHistory history = MongoDBHistoryReader.readHistory(URL_HISTORY, URL_OPLOG, URL_MONGOD_LOG);
        ArrayList<MongoDBTransaction> transactions = history.transactions;

        int readOnly = 0;
        for (MongoDBTransaction txn : transactions) {
//            System.out.println(txn);
            if (txn.writeKeySet.isEmpty()) {
                readOnly++;
                System.out.println(txn);
            } else {
//                System.out.println(txn);
            }
        }
        System.out.println(readOnly);

    }
}
