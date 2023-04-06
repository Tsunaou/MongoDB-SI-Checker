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
import java.util.HashSet;

public class JSONFileReader implements Reader<Long, Long> {
    @Override
    public History<Long, Long> read(String filepath) throws RuntimeException {
        ArrayList<Transaction<Long, Long>> transactions = null;
        HashMap<String, Session<Long, Long>> sessionsMap = new HashMap<>(41);
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
                Long physicalStartTs = jsonStartTs.getLong("p");
                Long logicalStartTs = jsonStartTs.getLong("l");
                HybridLogicalClock startTimestamp = new HybridLogicalClock(physicalStartTs, logicalStartTs);
                JSONObject jsonCommitTs = jsonObject.getJSONObject("cts");
                Long physicalCommitTs = jsonCommitTs.getLong("p");
                Long logicalCommitTs = jsonCommitTs.getLong("l");
                HybridLogicalClock commitTimestamp = new HybridLogicalClock(physicalCommitTs, logicalCommitTs);
                JSONArray jsonOperations = jsonObject.getJSONArray("ops");
                ArrayList<Operation<Long, Long>> operations = new ArrayList<>(jsonOperations.size());
                for (Object objOperation : jsonOperations) {
                    JSONObject jsonOperation = (JSONObject) objOperation;
                    String type = jsonOperation.getString("t");
                    Long key = jsonOperation.getLong("k");
                    maxKey = (maxKey >= key) ? maxKey : key;
                    Long value = jsonOperation.getLong("v");
                    if ("w".equalsIgnoreCase(type) || "write".equalsIgnoreCase(type)) {
                        operations.add(new Operation<>(OpType.write, key, value));
                    } else if ("r".equalsIgnoreCase(type) || "read".equalsIgnoreCase(type)) {
                        operations.add(new Operation<>(OpType.read, key, value));
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
                Transaction<Long, Long> txn = new Transaction<>(transactionId, operations,
                        startTimestamp, commitTimestamp, session);
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
        return new History<>(transactions, sessionsMap);
    }

    private Pair<Transaction<Long, Long>, Session<Long, Long>> createInitialTxn(long maxKey) {
        String transactionId = "initial";
        ArrayList<Operation<Long, Long>> operations = new ArrayList<>((int) maxKey + 1);
        for (long key = 0; key <= maxKey; key++) {
            operations.add(new Operation<>(OpType.write, key, null));
        }
        HybridLogicalClock startTimestamp = new HybridLogicalClock(0L, 0L);
        HybridLogicalClock commitTimestamp = new HybridLogicalClock(0L, 0L);
        Session<Long, Long> session = new Session<>("initial");
        Transaction<Long, Long> transaction = new Transaction<>(transactionId, operations,
                startTimestamp, commitTimestamp, session);
        session.getTransactions().add(transaction);
        return Pair.of(transaction, session);
    }
}
