package axiom;

import history.Edge;
import history.History;
import history.transaction.Transaction;

public class PREFIX<KeyType, ValueType> {
    private final History<KeyType, ValueType> history;

    public PREFIX(History<KeyType, ValueType> history) {
        this.history = history;
    }

    public boolean check() {
        for (Edge<Transaction<KeyType, ValueType>> arEdge : history.getAR()) {
            Transaction<KeyType, ValueType> txn1 = arEdge.getFrom();
            Transaction<KeyType, ValueType> txn2 = arEdge.getTo();
            for (Edge<Transaction<KeyType, ValueType>> visEdge : history.getVisByTxn().get(txn2)) {
                if (!history.getVIS().contains(new Edge<>(txn1, visEdge.getTo()))) {
                    return false;
                }
            }
        }
        return true;
    }
}
