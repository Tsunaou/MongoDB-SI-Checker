package history.transaction;

import history.Session;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

public class Transaction<KeyType, ValueType> {
    private final String transactionId;
    private final ArrayList<Operation<KeyType, ValueType>> operations;
    private final HybridLogicalClock startTimestamp;
    private final HybridLogicalClock commitTimestamp;

    public final Session<KeyType, ValueType> session;

    private HashMap<KeyType, Operation<KeyType, ValueType>> lastWriteKeysMap;
    private HashMap<KeyType, Operation<KeyType, ValueType>> firstReadKeysMap;

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

    public HashMap<KeyType, Operation<KeyType, ValueType>> getLastWriteKeysMap() {
        return lastWriteKeysMap;
    }

    public void setLastWriteKeysMap(HashMap<KeyType, Operation<KeyType, ValueType>> lastWriteKeysMap) {
        this.lastWriteKeysMap = lastWriteKeysMap;
    }

    public HashMap<KeyType, Operation<KeyType, ValueType>> getFirstReadKeysMap() {
        return firstReadKeysMap;
    }

    public void setFirstReadKeysMap(HashMap<KeyType, Operation<KeyType, ValueType>> firstReadKeysMap) {
        this.firstReadKeysMap = firstReadKeysMap;
    }

    public Transaction(String transactionId, ArrayList<Operation<KeyType, ValueType>> operations,
                       HybridLogicalClock startTimestamp, HybridLogicalClock commitTimestamp,
                       Session<KeyType, ValueType> session) {
        this.transactionId = transactionId;
        this.operations = operations;
        this.startTimestamp = startTimestamp;
        this.commitTimestamp = commitTimestamp;
        this.session = session;
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Transaction<?, ?>) {
            Transaction<?, ?> txn = (Transaction<?, ?>) obj;
            return this == txn || this.transactionId.equals(txn.transactionId);
        }
        return false;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "transactionId='" + transactionId + '\'' +
                ", operations=" + operations +
                ", startTimestamp=" + startTimestamp +
                ", commitTimestamp=" + commitTimestamp +
                '}';
    }
}
