package axiom;

import history.Edge;
import history.History;
import history.transaction.Transaction;

public class PREFIX {
    public static <KeyType, ValueType> boolean check(History<KeyType, ValueType> history) {
        for (Edge<Transaction<KeyType, ValueType>> arEdge : history.getAR()) {
            Transaction<KeyType, ValueType> txn1 = arEdge.getFrom();
            Transaction<KeyType, ValueType> txn2 = arEdge.getTo();
            for (Transaction<KeyType, ValueType> txn3 : history.getVisByTxn().get(txn2)) {
                if (!history.getVIS().contains(new Edge<>(txn1, txn3))) {
                    return false;
                }
            }
        }
        return true;
    }
}
