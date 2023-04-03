package history;

import history.transaction.Transaction;

import java.util.*;

public class History<KeyType, ValueType> {
    private final ArrayList<Transaction<KeyType, ValueType>> transactions;

    private final HashSet<Edge<Transaction<KeyType, ValueType>>> SO = new HashSet<>();

    private final HashSet<Edge<Transaction<KeyType, ValueType>>> VIS = new HashSet<>();
    private final HashSet<Edge<Transaction<KeyType, ValueType>>> AR = new HashSet<>();

    public History(ArrayList<Transaction<KeyType, ValueType>> transactions,
                   ArrayList<Session<KeyType, ValueType>> sessions) throws RuntimeException {
        this.transactions = transactions;
        this.transactions.sort(Comparator.comparing(Transaction::getCommitTimestamp));

        buildSO(sessions);

        buildVIS();
        buildAR();

        if (!isValid()) {
            throw new RuntimeException("Invalid history.");
        }
    }

    private void buildSO(ArrayList<Session<KeyType, ValueType>> sessions) {
        for (Session<KeyType, ValueType> session : sessions) {
            ArrayList<Transaction<KeyType, ValueType>> txns = session.getTransactions();
            txns.sort(Comparator.comparing(Transaction::getCommitTimestamp));
            for (int i = 0; i < txns.size() - 1; i++) {
                Transaction<KeyType, ValueType> from = txns.get(i);
                for (int j = i + 1; j < txns.size(); j++) {
                    Transaction<KeyType, ValueType> to = txns.get(j);
                    SO.add(new Edge<>(from, to));
                }
            }
        }
    }

    private void buildVIS() {
        for (int i = 0; i < transactions.size() - 1; i++) {
            Transaction<KeyType, ValueType> from = transactions.get(i);
            for (int j = i + 1; j < transactions.size(); j++) {
                Transaction<KeyType, ValueType> to = transactions.get(j);
                if (to.getStartTimestamp().compareTo(from.getCommitTimestamp()) > 0) {
                    VIS.add(new Edge<>(from, to));
                }
            }
        }
    }

    private void buildAR() {
        for (int i = 0; i < transactions.size() - 1; i++) {
            Transaction<KeyType, ValueType> from = transactions.get(i);
            for (int j = i + 1; j < transactions.size(); j++) {
                Transaction<KeyType, ValueType> to = transactions.get(j);
                AR.add(new Edge<>(from, to));
            }
        }
    }

    private boolean isValid() {
        // check whether VIS is a subset of AR
        for (Edge<Transaction<KeyType, ValueType>> visEdge : VIS) {
            if (!AR.contains(visEdge)) {
                return false;
            }
        }
        // check whether AR is a strict total order
        for (Transaction<KeyType, ValueType> txn1 : transactions) {
            for (Transaction<KeyType, ValueType> txn2 : transactions) {
                if (txn1 == txn2 && AR.contains(new Edge<>(txn1, txn2))) {
                    return false;
                } else if (!AR.contains(new Edge<>(txn1, txn2)) && !AR.contains(new Edge<>(txn2, txn1))
                        || AR.contains(new Edge<>(txn1, txn2)) && AR.contains(new Edge<>(txn2, txn1))) {
                    return false;
                }
            }
        }
        for (Transaction<KeyType, ValueType> txn1 : transactions) {
            for (Transaction<KeyType, ValueType> txn2 : transactions) {
                if (!AR.contains(new Edge<>(txn1, txn2))) {
                    continue;
                }
                for (Transaction<KeyType, ValueType> txn3 : transactions) {
                    if (!AR.contains(new Edge<>(txn2, txn3))) {
                        continue;
                    }
                    if (!AR.contains(new Edge<>(txn1, txn3))) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public ArrayList<Transaction<KeyType, ValueType>> getTransactions() {
        return transactions;
    }

    public HashSet<Edge<Transaction<KeyType, ValueType>>> getSO() {
        return SO;
    }

    public HashSet<Edge<Transaction<KeyType, ValueType>>> getVIS() {
        return VIS;
    }

    public HashSet<Edge<Transaction<KeyType, ValueType>>> getAR() {
        return AR;
    }
}
