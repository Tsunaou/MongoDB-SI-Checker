package history;

import history.transaction.Transaction;

import java.util.ArrayList;
import java.util.Objects;

public class Session<KeyType, ValueType> {
    private final String sessionId;
    private final ArrayList<Transaction<KeyType, ValueType>> transactions = new ArrayList<>(100);

    public ArrayList<Transaction<KeyType, ValueType>> getTransactions() {
        return transactions;
    }

    public Session(String sessionId) {
        this.sessionId = sessionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Session<?, ?>) {
            Session<?, ?> session = (Session<?, ?>) obj;
            return this == session || this.sessionId.equals(session.sessionId);
        }
        return false;
    }
}
