package axiom;

import history.Edge;
import history.History;
import history.transaction.OpType;
import history.transaction.Operation;
import history.transaction.Transaction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;

public class EXT<KeyType, ValueType> {
    private final History<KeyType, ValueType> history;

    public EXT(History<KeyType, ValueType> history) {
        this.history = history;
    }

    public boolean check() {
        for (Transaction<KeyType, ValueType> txn : history.getTransactions()) {
            HashSet<KeyType> firstReadKeys = new HashSet<>();
            for (Operation<KeyType, ValueType> operation : txn.getOperations()) {
                KeyType key = operation.getKey();
                if (operation.getType() != OpType.read || firstReadKeys.contains(key)) {
                    continue;
                }
                firstReadKeys.add(key);
                ArrayList<Transaction<KeyType, ValueType>> fromTxns = new ArrayList<>();
                for (Edge<Transaction<KeyType, ValueType>> e : history.getVisInvByTxn().get(txn)) {
                    fromTxns.add(e.getFrom());
                }
                fromTxns.sort(Comparator.comparing(Transaction::getCommitTimestamp));
                boolean found = false;
                for (int i = fromTxns.size() - 1; i >= 0; i--) {
                    ArrayList<Operation<KeyType, ValueType>> formerOperations = fromTxns.get(i).getOperations();
                    for (int j = formerOperations.size() - 1; j >= 0; j--) {
                        Operation<KeyType, ValueType> formerOperation = formerOperations.get(j);
                        if (formerOperation.getType() == OpType.write && formerOperation.getKey().equals(key)) {
                            if (!formerOperation.getValue().equals(operation.getValue())) {
                                return false;
                            } else {
                                found = true;
                                break;
                            }
                        }
                    }
                    if (found) {
                        break;
                    }
                }
                if (!found) {
                    return false;
                }
            }
        }
        return true;
    }
}
