package axiom;

import history.History;
import history.transaction.OpType;
import history.transaction.Operation;
import history.transaction.Transaction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Objects;

public class EXT {
    public static <KeyType, ValueType> boolean check(History<KeyType, ValueType> history) {
        for (Transaction<KeyType, ValueType> txn : history.getTransactions()) {
            HashSet<KeyType> firstReadKeys = new HashSet<>();
            HashSet<KeyType> writeKeys = new HashSet<>();
            for (Operation<KeyType, ValueType> operation : txn.getOperations()) {
                KeyType key = operation.getKey();
                if (operation.getType() == OpType.write) {
                    writeKeys.add(key);
                    continue;
                } else if (firstReadKeys.contains(key) || writeKeys.contains(key)) {
                    continue;
                }
                firstReadKeys.add(key);
                ArrayList<Transaction<KeyType, ValueType>> fromTxns =
                        new ArrayList<>(history.getVisInvByTxn().get(txn));
                fromTxns.sort(Comparator.comparing(Transaction::getCommitTimestamp));
                boolean found = false;
                for (int i = fromTxns.size() - 1; i >= 0; i--) {
                    ArrayList<Operation<KeyType, ValueType>> formerOps = fromTxns.get(i).getOperations();
                    for (int j = formerOps.size() - 1; j >= 0; j--) {
                        Operation<KeyType, ValueType> formerOp = formerOps.get(j);
                        if (formerOp.getType() == OpType.write && Objects.equals(formerOp.getKey(), key)) {
                            if (!Objects.equals(formerOp.getValue(), operation.getValue())) {
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
