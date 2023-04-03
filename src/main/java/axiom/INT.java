package axiom;

import history.History;
import history.transaction.OpType;
import history.transaction.Operation;
import history.transaction.Transaction;

import java.util.ArrayList;

public class INT<KeyType, ValueType> {
    public History<KeyType, ValueType> history;

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
                    if (e.getKey().equals(f.getKey())) {
                        if (e.getValue().equals(f.getValue())) {
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
