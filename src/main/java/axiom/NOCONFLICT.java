package axiom;

import history.Edge;
import history.History;
import history.transaction.Operation;
import history.transaction.Transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class NOCONFLICT {
    public static <KeyType, ValueType> boolean check(History<KeyType, ValueType> history) {
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
