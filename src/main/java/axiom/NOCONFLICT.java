package axiom;

import history.Edge;
import history.History;
import history.transaction.Operation;
import history.transaction.Transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.IntStream;

public class NOCONFLICT {
    public static <KeyType, ValueType> boolean check(History<KeyType, ValueType> history) {
        return IntStream.range(0, history.getTransactions().size() - 1).parallel()
                .allMatch(idx -> {
                    Transaction<KeyType, ValueType> txn1 = history.getTransactions().get(idx);
                    return history.getTransactions().parallelStream()
                            .skip(idx + 1)
                            .allMatch(txn2 -> txn1.getLastWriteKeysMap().entrySet().parallelStream()
                                    .allMatch(e1Map -> !txn2.getLastWriteKeysMap().containsKey(e1Map.getKey())
                                            || history.getVIS().contains(new Edge<>(txn1, txn2))
                                            || history.getVIS().contains(new Edge<>(txn2, txn1))));
                });
    }

    public static <KeyType, ValueType> boolean checkSingle(History<KeyType, ValueType> history) {
        ArrayList<Transaction<KeyType, ValueType>> transactions = history.getTransactions();
        HashSet<Edge<Transaction<KeyType, ValueType>>> VIS = history.getVIS();
        for (int i = 0; i < transactions.size() - 1; i++) {
            Transaction<KeyType, ValueType> txn1 = transactions.get(i);
            HashMap<KeyType, Operation<KeyType, ValueType>> lastWriteKeysMap1 = txn1.getLastWriteKeysMap();
            for (int j = i + 1; j < transactions.size(); j++) {
                Transaction<KeyType, ValueType> txn2 = transactions.get(j);
                HashMap<KeyType, Operation<KeyType, ValueType>> lastWriteKeysMap2 = txn2.getLastWriteKeysMap();
                for (Map.Entry<KeyType, Operation<KeyType, ValueType>> e1 : lastWriteKeysMap1.entrySet()) {
                    if (lastWriteKeysMap2.containsKey(e1.getKey()) &&
                            !VIS.contains(new Edge<>(txn1, txn2)) && !VIS.contains(new Edge<>(txn2, txn1))) {
                        return false;
                    }
                }
            }
        }
        return true;
    }
}
