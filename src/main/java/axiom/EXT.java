package axiom;

import history.History;
import history.transaction.Operation;
import history.transaction.Transaction;

import java.util.*;

public class EXT {
    public static <KeyType, ValueType> boolean check(History<KeyType, ValueType> history) {
        for (Transaction<KeyType, ValueType> txn : history.getTransactions()) {
            for (Operation<KeyType, ValueType> operation : txn.getFirstReadKeysMap().values()) {
                KeyType key = operation.getKey();
                ArrayList<Transaction<KeyType, ValueType>> fromTxns =
                        new ArrayList<>(history.getVisInvByTxn().get(txn));
                fromTxns.sort(Comparator.comparing(Transaction::getCommitTimestamp));
                boolean found = false;
                for (int i = fromTxns.size() - 1; i >= 0; i--) {
                    HashMap<KeyType, Operation<KeyType, ValueType>> lastWriteKeysMap
                            = fromTxns.get(i).getLastWriteKeysMap();
                    if (lastWriteKeysMap.containsKey(key)) {
                        if (!Objects.equals(lastWriteKeysMap.get(key).getValue(), operation.getValue())) {
                            return false;
                        }
                        found = true;
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
