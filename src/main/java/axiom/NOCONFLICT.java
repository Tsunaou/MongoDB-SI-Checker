package axiom;

import history.Edge;
import history.History;
import history.transaction.OpType;
import history.transaction.Operation;
import history.transaction.Transaction;

import java.util.ArrayList;
import java.util.HashSet;

public class NOCONFLICT {
    public static <KeyType, ValueType> boolean check(History<KeyType, ValueType> history) {
        ArrayList<Transaction<KeyType, ValueType>> transactions = history.getTransactions();
        HashSet<Edge<Transaction<KeyType, ValueType>>> VIS = history.getVIS();
        for (int i = 0; i < transactions.size() - 1; i++) {
            Transaction<KeyType, ValueType> txn1 = transactions.get(i);
            HashSet<KeyType> lastWriteKeys = new HashSet<>();
            ArrayList<Operation<KeyType, ValueType>> txn1Operations = txn1.getOperations();
            for (int j = txn1Operations.size() - 1; j >= 0; j--) {
                Operation<KeyType, ValueType> txn1Operation = txn1Operations.get(j);
                KeyType key = txn1Operation.getKey();
                if (txn1Operation.getType() != OpType.write || lastWriteKeys.contains(key)) {
                    continue;
                }
                lastWriteKeys.add(key);
            }
            for (int j = i + 1; j < transactions.size(); j++) {
                Transaction<KeyType, ValueType> txn2 = transactions.get(j);
                for (Operation<KeyType, ValueType> txn2Operation : txn2.getOperations()) {
                    if (txn2Operation.getType() == OpType.write && lastWriteKeys.contains(txn2Operation.getKey())) {
                        if (!VIS.contains(new Edge<>(txn1, txn2)) && !VIS.contains(new Edge<>(txn2, txn1))) {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }
}
