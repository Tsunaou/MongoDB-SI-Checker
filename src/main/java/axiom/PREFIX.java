package axiom;

import history.Edge;
import history.History;
import history.transaction.Transaction;

public class PREFIX {
    public static <KeyType, ValueType> boolean check(History<KeyType, ValueType> history) {
        for (Edge<Transaction<KeyType, ValueType>> arEdge : history.getAR()) {
            for (Transaction<KeyType, ValueType> txn3 : history.getVisByTxn().get(arEdge.getTo())) {
                if (!history.getVIS().contains(new Edge<>(arEdge.getFrom(), txn3))) {
                    return false;
                }
            }
        }
        return true;
    }
}
