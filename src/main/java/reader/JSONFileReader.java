package reader;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONReader;
import history.Session;
import history.transaction.HybridLogicalClock;
import history.transaction.OpType;
import history.transaction.Operation;
import history.transaction.Transaction;
import org.apache.commons.lang3.tuple.Pair;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;

public class JSONFileReader extends Reader {
    @Override
    public Pair<ArrayList<Transaction<Long, Long>>, ArrayList<Session<Long, Long>>> read(String filepath)
            throws RuntimeException {
        ArrayList<Transaction<Long, Long>> transactions = new ArrayList<>();
        ArrayList<Session<Long, Long>> sessions = new ArrayList<>();
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
        return Pair.of(transactions, sessions);
    }
}
