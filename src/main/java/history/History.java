package history;

import history.transaction.Transaction;

import java.util.*;

public class History<KeyType, ValueType> {
    private final ArrayList<Transaction<KeyType, ValueType>> transactions;

    private final HashSet<Edge<Transaction<KeyType, ValueType>>> SO = new HashSet<>();

    private final HashSet<Edge<Transaction<KeyType, ValueType>>> VIS = new HashSet<>();
    private final HashSet<Edge<Transaction<KeyType, ValueType>>> AR = new HashSet<>();

    private final HashMap<Transaction<KeyType, ValueType>,
            HashSet<Edge<Transaction<KeyType, ValueType>>>> soByTxn = new HashMap<>();
    private final HashMap<Transaction<KeyType, ValueType>,
            HashSet<Edge<Transaction<KeyType, ValueType>>>> visByTxn = new HashMap<>();
    private final HashMap<Transaction<KeyType, ValueType>,
            HashSet<Edge<Transaction<KeyType, ValueType>>>> arByTxn = new HashMap<>();
    private final HashMap<Transaction<KeyType, ValueType>,
            HashSet<Edge<Transaction<KeyType, ValueType>>>> soInvByTxn = new HashMap<>();
    private final HashMap<Transaction<KeyType, ValueType>,
            HashSet<Edge<Transaction<KeyType, ValueType>>>> visInvByTxn = new HashMap<>();
    private final HashMap<Transaction<KeyType, ValueType>,
            HashSet<Edge<Transaction<KeyType, ValueType>>>> arInvByTxn = new HashMap<>();

    public History(ArrayList<Transaction<KeyType, ValueType>> transactions,
                   ArrayList<Session<KeyType, ValueType>> sessions) throws RuntimeException {
        this.transactions = transactions;
        this.transactions.sort(Comparator.comparing(Transaction::getCommitTimestamp));

        for (Transaction<KeyType, ValueType> txn : this.transactions) {
            soByTxn.put(txn, new HashSet<>());
            visByTxn.put(txn, new HashSet<>());
            arByTxn.put(txn, new HashSet<>());
            soInvByTxn.put(txn, new HashSet<>());
            visInvByTxn.put(txn, new HashSet<>());
            arInvByTxn.put(txn, new HashSet<>());
        }

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
                    Edge<Transaction<KeyType, ValueType>> edge = new Edge<>(from, to);
                    SO.add(edge);
                    soByTxn.get(from).add(edge);
                    soInvByTxn.get(to).add(edge);
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
                    Edge<Transaction<KeyType, ValueType>> edge = new Edge<>(from, to);
                    VIS.add(edge);
                    visByTxn.get(from).add(edge);
                    visInvByTxn.get(to).add(edge);
                }
            }
        }
    }

    private void buildAR() {
        for (int i = 0; i < transactions.size() - 1; i++) {
            Transaction<KeyType, ValueType> from = transactions.get(i);
            for (int j = i + 1; j < transactions.size(); j++) {
                Transaction<KeyType, ValueType> to = transactions.get(j);
                Edge<Transaction<KeyType, ValueType>> edge = new Edge<>(from, to);
                AR.add(edge);
                arByTxn.get(from).add(edge);
                arInvByTxn.get(to).add(edge);
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
                for (Edge<Transaction<KeyType, ValueType>> e : arByTxn.get(txn2)) {
                    if (!AR.contains(new Edge<>(txn1, e.getTo()))) {
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

    public HashMap<Transaction<KeyType, ValueType>, HashSet<Edge<Transaction<KeyType, ValueType>>>> getVisByTxn() {
        return visByTxn;
    }

    public HashMap<Transaction<KeyType, ValueType>, HashSet<Edge<Transaction<KeyType, ValueType>>>> getVisInvByTxn() {
        return visInvByTxn;
    }
}
