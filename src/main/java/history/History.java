package history;

import history.transaction.Transaction;

import java.util.*;

public class History<KeyType, ValueType> {
    private final ArrayList<Transaction<KeyType, ValueType>> transactions;

    private final HashSet<Edge<Transaction<KeyType, ValueType>>> SO;

    private final HashMap<Transaction<KeyType, ValueType>, Integer> visInvByTxn;

    public History(ArrayList<Transaction<KeyType, ValueType>> transactions,
                   HashMap<String, Session<KeyType, ValueType>> sessionsMap,
                   boolean equalVIS) throws RuntimeException {
        long start = System.currentTimeMillis();

        this.transactions = transactions;
        this.transactions.sort(Comparator.comparing(Transaction::getCommitTimestamp));

        int txnSize = this.transactions.size();
        int sessionSize = sessionsMap.size();

        visInvByTxn = new HashMap<>(txnSize * 4 / 3 + 1);

        SO = new HashSet<>(2 * txnSize * (txnSize - sessionSize) / (3 * sessionSize) + 1);
        buildSO(sessionsMap);

        buildVIS(equalVIS);

        long end = System.currentTimeMillis();
        System.out.println("Building history: " + (end - start) / 1000.0 + "s");
    }

    private void buildSO(HashMap<String, Session<KeyType, ValueType>> sessionsMap) {
        for (Session<KeyType, ValueType> session : sessionsMap.values()) {
            ArrayList<Transaction<KeyType, ValueType>> txns = session.getTransactions();
            for (int i = 0; i < txns.size() - 1; i++) {
                Transaction<KeyType, ValueType> from = txns.get(i);
                for (int j = i + 1; j < txns.size(); j++) {
                    SO.add(new Edge<>(from, txns.get(j)));
                }
            }
        }
    }

    private void buildVIS(boolean equalVIS) {
        if (equalVIS) {
            buildEqualVIS();
        } else {
            buildNonEqualVIS();
        }
    }

    private void buildNonEqualVIS() {
        int txnSize = transactions.size();
        for (int i = 0; i < txnSize; i++) {
            Transaction<KeyType, ValueType> to = transactions.get(i);
            boolean found = false;
            for (int j = i - 1; j >= 0; j--) {
                if (transactions.get(j).getCommitTimestamp().compareTo(to.getStartTimestamp()) < 0) {
                    visInvByTxn.put(to, j);
                    found = true;
                    break;
                }
            }
            if (!found) {
                visInvByTxn.put(to, -1);
            }
        }
    }

    private void buildEqualVIS() {
        int txnSize = transactions.size();
        for (int i = 0; i < txnSize; i++) {
            Transaction<KeyType, ValueType> to = transactions.get(i);
            boolean found = false;
            for (int j = i - 1; j >= 0; j--) {
                if (transactions.get(j).getCommitTimestamp().compareTo(to.getStartTimestamp()) <= 0) {
                    visInvByTxn.put(to, j);
                    found = true;
                    break;
                }
            }
            if (!found) {
                visInvByTxn.put(to, -1);
            }
        }
    }

    public ArrayList<Transaction<KeyType, ValueType>> getTransactions() {
        return transactions;
    }

    public HashSet<Edge<Transaction<KeyType, ValueType>>> getSO() {
        return SO;
    }

    public HashMap<Transaction<KeyType, ValueType>, Integer> getVisInvByTxn() {
        return visInvByTxn;
    }
}
