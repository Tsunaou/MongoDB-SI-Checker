package axiom;

import history.History;
import history.transaction.Operation;
import history.transaction.Transaction;

import java.util.*;

public class EXT {
    public static <KeyType, ValueType> boolean check(History<KeyType, ValueType> history) {
        return history.getTransactions().parallelStream()
                .allMatch(txn -> txn.getFirstReadKeysMap().entrySet().parallelStream()
                        .allMatch(opMap -> history.getVisInvByTxn().get(txn) >= 0 && history.getTransactions()
                                .parallelStream()
                                .limit(history.getVisInvByTxn().get(txn) + 1)
                                .filter(fromTxn -> fromTxn.getLastWriteKeysMap().containsKey(opMap.getKey()))
                                .map(fromTxn -> fromTxn.getLastWriteKeysMap().get(opMap.getKey()))
                                .reduce((first, second) -> second)
                                .filter(operation -> Objects.equals(operation.getValue(), opMap.getValue().getValue()))
                                .isPresent()));
    }

    public static <KeyType, ValueType> boolean checkSingle(History<KeyType, ValueType> history) {
        for (Transaction<KeyType, ValueType> txn : history.getTransactions()) {
            for (Operation<KeyType, ValueType> operation : txn.getFirstReadKeysMap().values()) {
                KeyType key = operation.getKey();
                boolean found = false;
                for (int i = history.getVisInvByTxn().get(txn); i >= 0; i--) {
                    Transaction<KeyType, ValueType> fromTxn = history.getTransactions().get(i);
                    HashMap<KeyType, Operation<KeyType, ValueType>> lastWriteKeysMap = fromTxn.getLastWriteKeysMap();
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
