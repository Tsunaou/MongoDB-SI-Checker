package history;

import history.transaction.Transaction;

import java.util.ArrayList;

public class Session<KeyType, ValueType> {
    public final String sessionId;
    public final ArrayList<Transaction<KeyType, ValueType>> transactions;

    public ArrayList<Transaction<KeyType, ValueType>> getTransactions() {
        return transactions;
    }

    public Session(String sessionId, ArrayList<Transaction<KeyType, ValueType>> transactions) {
        this.sessionId = sessionId;
        this.transactions = transactions;
    }
}
