package reader;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONReader;
import history.History;
import history.Session;
import history.transaction.HybridLogicalClock;
import history.transaction.OpType;
import history.transaction.Operation;
import history.transaction.Transaction;
import org.apache.commons.lang3.tuple.Pair;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

public class JSONFileReader implements Reader<Long, Long> {
    @Override
    public Pair<History<Long, Long>, Boolean> read(String filepath, boolean equalVIS) throws RuntimeException {
        long start = System.currentTimeMillis();
        ArrayList<Transaction<Long, Long>> transactions = null;
        HashMap<String, Session<Long, Long>> sessionsMap = new HashMap<>(41);
        boolean isINT = true;
        long maxKey = 0L;
        try {
            JSONReader jsonReader = new JSONReader(new FileReader(filepath));
            JSONArray jsonArray = (JSONArray) jsonReader.readObject();
            int size = jsonArray.size();
            transactions = new ArrayList<>(size + 1);
            transactions.add(new Transaction<>(null, null, null, null, null));
            for (int i = 0; i < size; i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                String sessionId = jsonObject.getString("sid");
                String transactionId = jsonObject.getString("tid");
                JSONObject jsonStartTs = jsonObject.getJSONObject("sts");
                HybridLogicalClock startTs = new HybridLogicalClock(jsonStartTs.getLong("p"), jsonStartTs.getLong("l"));
                JSONObject jsonCommitTs = jsonObject.getJSONObject("cts");
                HybridLogicalClock commitTs = new HybridLogicalClock(jsonCommitTs.getLong("p"), jsonCommitTs.getLong("l"));
                JSONArray jsonOperations = jsonObject.getJSONArray("ops");
                ArrayList<Operation<Long, Long>> operations = new ArrayList<>(jsonOperations.size());
                HashMap<Long, Operation<Long, Long>> lastWriteKeysMap
                        = new HashMap<>(jsonOperations.size() * 4 / 3 + 1);
                HashMap<Long, Operation<Long, Long>> firstReadKeysMap
                        = new HashMap<>(jsonOperations.size() * 4 / 3 + 1);
                HashMap<Long, Operation<Long, Long>> lastOperationKeysMap
                        = new HashMap<>(jsonOperations.size() * 4 / 3 + 1);
                for (Object objOperation : jsonOperations) {
                    JSONObject jsonOperation = (JSONObject) objOperation;
                    String type = jsonOperation.getString("t");
                    Long key = jsonOperation.getLong("k");
                    maxKey = (maxKey >= key) ? maxKey : key;
                    Long value = jsonOperation.getLong("v");
                    if ("w".equalsIgnoreCase(type) || "write".equalsIgnoreCase(type)) {
                        Operation<Long, Long> operation = new Operation<>(OpType.write, key, value);
                        operations.add(operation);
                        lastWriteKeysMap.put(key, operation);
                        lastOperationKeysMap.put(key, operation);
                    } else if ("r".equalsIgnoreCase(type) || "read".equalsIgnoreCase(type)) {
                        Operation<Long, Long> operation = new Operation<>(OpType.read, key, value);
                        operations.add(operation);
                        if (!lastOperationKeysMap.containsKey(key)) {
                            firstReadKeysMap.put(key, operation);
                        } else if (!Objects.equals(value, lastOperationKeysMap.get(key).getValue())) {
                            isINT = false;
                        }
                        lastOperationKeysMap.put(key, operation);
                    } else {
                        throw new RuntimeException("Unknown operation type.");
                    }
                }
                Session<Long, Long> session;
                if (sessionsMap.containsKey(sessionId)) {
                    session = sessionsMap.get(sessionId);
                } else {
                    session = new Session<>(sessionId);
                    sessionsMap.put(sessionId, session);
                }
                Transaction<Long, Long> txn = new Transaction<>(transactionId, operations, startTs, commitTs, session);
                txn.setLastWriteKeysMap(lastWriteKeysMap);
                txn.setFirstReadKeysMap(firstReadKeysMap);
                transactions.add(txn);
                session.getTransactions().add(txn);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        Pair<Transaction<Long, Long>, Session<Long, Long>> initialTxn = createInitialTxn(maxKey);
        assert transactions != null;
        transactions.set(0, initialTxn.getLeft());
        sessionsMap.put("initial", initialTxn.getRight());
        long end = System.currentTimeMillis();
        System.out.println("Checking INT and reading history: " + (end - start) / 1000.0 + "s");
        return Pair.of(new History<>(transactions, sessionsMap, equalVIS), isINT);
    }

    private Pair<Transaction<Long, Long>, Session<Long, Long>> createInitialTxn(long maxKey) {
        String transactionId = "initial";
        int opSize = (int) maxKey + 1;
        ArrayList<Operation<Long, Long>> operations = new ArrayList<>(opSize);
        HashMap<Long, Operation<Long, Long>> lastWriteKeysMap = new HashMap<>(opSize * 4 / 3 + 1);
        for (long key = 0; key <= maxKey; key++) {
            Operation<Long, Long> operation = new Operation<>(OpType.write, key, null);
            operations.add(operation);
            lastWriteKeysMap.put(key, operation);
        }
        HybridLogicalClock startTimestamp = new HybridLogicalClock(0L, 0L);
        HybridLogicalClock commitTimestamp = new HybridLogicalClock(0L, 0L);
        Session<Long, Long> session = new Session<>("initial");
        Transaction<Long, Long> transaction = new Transaction<>(transactionId, operations,
                startTimestamp, commitTimestamp, session);
        transaction.setLastWriteKeysMap(lastWriteKeysMap);
        transaction.setFirstReadKeysMap(new HashMap<>(1));
        session.getTransactions().add(transaction);
        return Pair.of(transaction, session);
    }
}
