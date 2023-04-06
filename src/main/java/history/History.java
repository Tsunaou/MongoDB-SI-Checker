package history;

import history.transaction.Transaction;

import java.util.*;

public class History<KeyType, ValueType> {
    private final ArrayList<Transaction<KeyType, ValueType>> transactions;

    private final HashSet<Edge<Transaction<KeyType, ValueType>>> SO;

    private final HashSet<Edge<Transaction<KeyType, ValueType>>> VIS;
    private final HashSet<Edge<Transaction<KeyType, ValueType>>> AR;

    private final HashMap<Transaction<KeyType, ValueType>, HashSet<Transaction<KeyType, ValueType>>> visByTxn;
    private final HashMap<Transaction<KeyType, ValueType>, HashSet<Transaction<KeyType, ValueType>>> arByTxn;
    private final HashMap<Transaction<KeyType, ValueType>, HashSet<Transaction<KeyType, ValueType>>> visInvByTxn;

    public History(ArrayList<Transaction<KeyType, ValueType>> transactions,
                   HashMap<String, Session<KeyType, ValueType>> sessionsMap) throws RuntimeException {
        this.transactions = transactions;
        this.transactions.sort(Comparator.comparing(Transaction::getCommitTimestamp));

        int txnSize = this.transactions.size();
        int sessionSize = sessionsMap.size();

        visByTxn = new HashMap<>(txnSize * 4 / 3 + 1);
        arByTxn = new HashMap<>(txnSize * 4 / 3 + 1);
        visInvByTxn = new HashMap<>(txnSize * 4 / 3 + 1);

        SO = new HashSet<>(2 * txnSize * (txnSize - sessionSize) / (3 * sessionSize) + 1);
        buildSO(sessionsMap);

        AR = new HashSet<>(2 * txnSize * (txnSize - 1) / 3 + 1);
        buildAR();

        VIS = new HashSet<>(AR.size() * 4 / 3 + 1);
        buildVIS();

        if (!isValid()) {
            throw new RuntimeException("Invalid history.");
        }
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

    private void buildVIS() {
        int txnSize = transactions.size();
        for (int i = 0; i < txnSize; i++) {
            visInvByTxn.put(transactions.get(i), new HashSet<>(i * 4 / 3 + 1));
        }
        for (int i = 0; i < txnSize - 1; i++) {
            Transaction<KeyType, ValueType> from = transactions.get(i);
            visByTxn.put(from, new HashSet<>((txnSize - 1 - i) * 4 / 3 + 1));
            for (int j = i + 1; j < transactions.size(); j++) {
                Transaction<KeyType, ValueType> to = transactions.get(j);
                if (to.getStartTimestamp().compareTo(from.getCommitTimestamp()) > 0) {
                    VIS.add(new Edge<>(from, to));
                    visByTxn.get(from).add(to);
                    visInvByTxn.get(to).add(from);
                }
            }
        }
        visByTxn.put(transactions.get(txnSize - 1), new HashSet<>(1));
    }

    private void buildAR() {
        int txnSize = transactions.size();
        for (int i = 0; i < txnSize - 1; i++) {
            Transaction<KeyType, ValueType> from = transactions.get(i);
            arByTxn.put(from, new HashSet<>((txnSize - 1 - i) * 4 / 3 + 1));
            for (int j = i + 1; j < transactions.size(); j++) {
                Transaction<KeyType, ValueType> to = transactions.get(j);
                AR.add(new Edge<>(from, to));
                arByTxn.get(from).add(to);
            }
        }
        arByTxn.put(transactions.get(txnSize - 1), new HashSet<>(1));
    }

    private boolean isValid() {
        // check whether VIS is a subset of AR
        for (Edge<Transaction<KeyType, ValueType>> visEdge : VIS) {
            if (!AR.contains(visEdge)) {
                return false;
            }
        }
        // check whether AR is a strict total order
        for (int i = 0; i < transactions.size(); i++) {
            Transaction<KeyType, ValueType> txn1 = transactions.get(i);
            for (int j = i; j < transactions.size(); j++) {
                Transaction<KeyType, ValueType> txn2 = transactions.get(j);
                boolean t1ToT2 = AR.contains(new Edge<>(txn1, txn2));
                boolean t2ToT1 = AR.contains(new Edge<>(txn2, txn1));
                if (i == j) {
                    if (t1ToT2) {
                        return false;
                    } else {
                        continue;
                    }
                } else if (t1ToT2 && t2ToT1 || !t1ToT2 && !t2ToT1) {
                    return false;
                }
                if (t2ToT1) {
                    txn1 = transactions.get(j);
                    txn2 = transactions.get(i);
                }
                for (Transaction<KeyType, ValueType> txn3 : arByTxn.get(txn2)) {
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

    public HashMap<Transaction<KeyType, ValueType>, HashSet<Transaction<KeyType, ValueType>>> getVisByTxn() {
        return visByTxn;
    }

    public HashMap<Transaction<KeyType, ValueType>, HashSet<Transaction<KeyType, ValueType>>> getVisInvByTxn() {
        return visInvByTxn;
    }
}
