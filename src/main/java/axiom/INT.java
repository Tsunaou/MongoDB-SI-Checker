package axiom;

import history.History;
import history.transaction.OpType;
import history.transaction.Operation;
import history.transaction.Transaction;

import java.util.ArrayList;
import java.util.Objects;

public class INT<KeyType, ValueType> {
    private final History<KeyType, ValueType> history;

    public INT(History<KeyType, ValueType> history) {
        this.history = history;
    }

    public boolean check() {
        for (Transaction<KeyType, ValueType> txn : history.getTransactions()) {
            ArrayList<Operation<KeyType, ValueType>> operations = txn.getOperations();
            for (int i = operations.size() - 1; i > 0; i--) {
                Operation<KeyType, ValueType> e = operations.get(i);
                if (e.getType() != OpType.read) {
                    continue;
                }
                for (int j = i - 1; j >= 0; j--) {
                    Operation<KeyType, ValueType> f = operations.get(j);
                    if (Objects.equals(e.getKey(), f.getKey())) {
                        if (Objects.equals(e.getValue(), f.getValue())) {
                            break;
                        } else {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }
}
