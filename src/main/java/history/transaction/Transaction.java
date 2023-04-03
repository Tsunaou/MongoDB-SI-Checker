package history.transaction;

import history.Session;

import java.util.ArrayList;

public class Transaction<KeyType, ValueType> {
    private final String transactionId;
    private final ArrayList<Operation<KeyType, ValueType>> operations;
    private final HybridLogicalClock startTimestamp;
    private final HybridLogicalClock commitTimestamp;

    public String getTransactionId() {
        return transactionId;
    }

    public ArrayList<Operation<KeyType, ValueType>> getOperations() {
        return operations;
    }

    public HybridLogicalClock getStartTimestamp() {
        return startTimestamp;
    }

    public HybridLogicalClock getCommitTimestamp() {
        return commitTimestamp;
    }

    public final Session<KeyType, ValueType> session;

    public Transaction(String transactionId, ArrayList<Operation<KeyType, ValueType>> operations,
                       HybridLogicalClock startTimestamp, HybridLogicalClock commitTimestamp,
                       Session<KeyType, ValueType> session) {
        this.transactionId = transactionId;
        this.operations = operations;
        this.startTimestamp = startTimestamp;
        this.commitTimestamp = commitTimestamp;
        this.session = session;
    }
}
