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

public class JSONFileReader implements Reader<Long, Long> {
    @Override
    public History<Long, Long> read(String filepath) throws RuntimeException {
        ArrayList<Transaction<Long, Long>> transactions = new ArrayList<>();
        ArrayList<Session<Long, Long>> sessions = new ArrayList<>();
        Pair<Transaction<Long, Long>, Session<Long, Long>> initialTxn = createInitialTxn();
        transactions.add(initialTxn.getLeft());
        sessions.add(initialTxn.getRight());
        try {
            JSONReader jsonReader = new JSONReader(new FileReader(filepath));
            jsonReader.startArray();
            while (jsonReader.hasNext()) {
                JSONObject jsonObject = (JSONObject) jsonReader.readObject();
                String sessionId = jsonObject.getString("sessionId");
                String transactionId = jsonObject.getString("transactionId");
                JSONObject jsonStartTs = jsonObject.getJSONObject("startTimestamp");
                Long physicalStartTs = jsonStartTs.getLong("physical");
                Long logicalStartTs = jsonStartTs.getLong("logical");
                HybridLogicalClock startTimestamp = new HybridLogicalClock(physicalStartTs, logicalStartTs);
                JSONObject jsonCommitTs = jsonObject.getJSONObject("commitTimestamp");
                Long physicalCommitTs = jsonCommitTs.getLong("physical");
                Long logicalCommitTs = jsonCommitTs.getLong("logical");
                HybridLogicalClock commitTimestamp = new HybridLogicalClock(physicalCommitTs, logicalCommitTs);
                JSONArray jsonOperations = jsonObject.getJSONArray("operations");
                ArrayList<Operation<Long, Long>> operations = new ArrayList<>();
                for (Object objOperation : jsonOperations) {
                    JSONObject jsonOperation = (JSONObject) objOperation;
                    String type = jsonOperation.getString("type");
                    Long key = jsonOperation.getLong("key");
                    Long value = jsonOperation.getLong("value");
                    if ("write".equalsIgnoreCase(type) || "w".equalsIgnoreCase(type)) {
                        operations.add(new Operation<>(OpType.write, key, value));
                    } else if ("read".equalsIgnoreCase(type) || "r".equalsIgnoreCase(type)) {
                        operations.add(new Operation<>(OpType.read, key, value));
                    } else {
                        throw new RuntimeException("Unknown operation type.");
                    }
                }
                Session<Long, Long> session = new Session<>(sessionId);
                if (sessions.contains(session)) {
                    session = sessions.get(sessions.indexOf(session));
                } else {
                    sessions.add(session);
                }
                Transaction<Long, Long> txn = new Transaction<>(transactionId, operations,
                        startTimestamp, commitTimestamp, session);
                transactions.add(txn);
                session.getTransactions().add(txn);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return new History<>(transactions, sessions);
    }

    private Pair<Transaction<Long, Long>, Session<Long, Long>> createInitialTxn() {
        String transactionId = "initial";
        ArrayList<Operation<Long, Long>> operations = new ArrayList<>();
        for (long key = 0; key < 1000L; key++) {
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
