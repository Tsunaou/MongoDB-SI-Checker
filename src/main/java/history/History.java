package history;

import history.transaction.Transaction;

import java.util.*;

public class History<KeyType, ValueType> {
    private final ArrayList<Transaction<KeyType, ValueType>> transactions;

    private final HashSet<Edge<Transaction<KeyType, ValueType>>> SO = new HashSet<>();

    private final HashSet<Edge<Transaction<KeyType, ValueType>>> VIS = new HashSet<>();
    private final HashSet<Edge<Transaction<KeyType, ValueType>>> AR = new HashSet<>();

    private final HashMap<Transaction<KeyType, ValueType>,
            HashSet<Transaction<KeyType, ValueType>>> visByTxn = new HashMap<>();
    private final HashMap<Transaction<KeyType, ValueType>,
            HashSet<Transaction<KeyType, ValueType>>> arByTxn = new HashMap<>();
    private final HashMap<Transaction<KeyType, ValueType>,
            HashSet<Transaction<KeyType, ValueType>>> visInvByTxn = new HashMap<>();

    public History(ArrayList<Transaction<KeyType, ValueType>> transactions,
                   HashSet<Session<KeyType, ValueType>> sessions) throws RuntimeException {
        this.transactions = transactions;
        this.transactions.sort(Comparator.comparing(Transaction::getCommitTimestamp));

        for (Transaction<KeyType, ValueType> txn : this.transactions) {
            visByTxn.put(txn, new HashSet<>());
            arByTxn.put(txn, new HashSet<>());
            visInvByTxn.put(txn, new HashSet<>());
        }

        buildSO(sessions);

        buildVIS();
        buildAR();

        if (!isValid()) {
            throw new RuntimeException("Invalid history.");
        }
    }

    private void buildSO(HashSet<Session<KeyType, ValueType>> sessions) {
        for (Session<KeyType, ValueType> session : sessions) {
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
        for (int i = 0; i < transactions.size() - 1; i++) {
            Transaction<KeyType, ValueType> from = transactions.get(i);
            for (int j = i + 1; j < transactions.size(); j++) {
                Transaction<KeyType, ValueType> to = transactions.get(j);
                if (to.getStartTimestamp().compareTo(from.getCommitTimestamp()) > 0) {
                    VIS.add(new Edge<>(from, to));
                    visByTxn.get(from).add(to);
                    visInvByTxn.get(to).add(from);
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
                arByTxn.get(from).add(to);
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
