package axiom;

import history.Edge;
import history.History;
import history.transaction.Transaction;

public class SESSION<KeyType, ValueType> {
    private final History<KeyType, ValueType> history;

    public SESSION(History<KeyType, ValueType> history) {
        this.history = history;
    }

    public boolean check() {
        for (Edge<Transaction<KeyType, ValueType>> soEdge : history.getSO()) {
            if (!history.getVIS().contains(soEdge)) {
                return false;
            }
        }
        return true;
    }
}
