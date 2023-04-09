package axiom;

import history.Edge;
import history.History;
import history.transaction.Transaction;

public class SESSION {
    public static <KeyType, ValueType> boolean check(History<KeyType, ValueType> history) {
        return history.getSO().parallelStream().allMatch(soEdge -> history.getVIS().contains(soEdge));
    }

    public static <KeyType, ValueType> boolean checkSingle(History<KeyType, ValueType> history) {
        for (Edge<Transaction<KeyType, ValueType>> soEdge : history.getSO()) {
            if (!history.getVIS().contains(soEdge)) {
                return false;
            }
        }
        return true;
    }
}
