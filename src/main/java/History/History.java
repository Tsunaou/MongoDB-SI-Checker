package History;


import Const.Const;
import Exceptions.HistoryInvalidException;
import History.WiredTiger.WiredTigerTransaction;

import java.util.*;

public class History<Txn extends Transaction> {
    public ArrayList<Txn> transactions;
    public ArrayList<Txn> readOnlyTransactions;
    public ArrayList<Txn> writeTransactions;

    public HashMap<Long, ArrayList<Txn>> keyWritesMap; // key: key, value: the transactions which have written into the key
    public HashMap<List<Integer>, ArrayList<Txn>> kvWritesMap;
    public HashMap<List<Integer>, ArrayList<Txn>> kvReadsMap;

    public History(ArrayList<Txn> transactions) throws HistoryInvalidException {
        this.transactions = transactions;

        this.readOnlyTransactions = new ArrayList<>();
        this.writeTransactions = new ArrayList<>();
        this.keyWritesMap = new HashMap<>();
        this.kvWritesMap = new HashMap<>();
        this.kvReadsMap = new HashMap<>();

        // 1. Get read only histories
        for (Txn txn : transactions) {
            if (txn.writeKeySet.isEmpty()) {
                this.readOnlyTransactions.add(txn);
            } else {
                this.writeTransactions.add(txn);
            }
        }

        // 2. Sort transactions(Default by the commit timestamp)
        this.sortTransactions();

        // 3. Calculate keyWritesMap
        ArrayList<Txn> list;
        for (Txn txn : this.transactions) {
            for (Long key : txn.writeKeySet) {
                if (keyWritesMap.containsKey(key)) {
                    list = keyWritesMap.get(key);
                } else {
                    list = new ArrayList<>();
                    keyWritesMap.put(key, list);
                }
                list.add(txn);
            }
        }

        // 4. Set Transaction Index
        for (int i = 0; i < this.transactions.size(); i++) {
            this.transactions.get(i).index = i;
        }

        // 5. Calculate kvWritesMap and kvReadsMap
        List<Integer> tuple;
        for (Txn txn : this.transactions) {
            for (Operation op : txn.writes) {
                tuple = Arrays.asList(op.key.intValue(), op.value.intValue());
                if (kvWritesMap.containsKey(tuple)) {
                    list = kvWritesMap.get(tuple);
                } else {
                    list = new ArrayList<Txn>();
                    kvWritesMap.put(tuple, list);
                }
                list.add(txn);
            }

            for (Operation op : txn.reads) {
                if (op.value == Const.INIT_READ) {
                    continue;
                }
                tuple = Arrays.asList(op.key.intValue(), op.value.intValue());
                if (kvReadsMap.containsKey(tuple)) {
                    list = kvReadsMap.get(tuple);
                } else {
                    list = new ArrayList<Txn>();
                    kvReadsMap.put(tuple, list);
                }
                list.add(txn);
            }
        }

    }

    protected void sortTransactions() {
        this.transactions.sort(Comparator.comparing(o -> o.commitTimestamp));
        this.readOnlyTransactions.sort(Comparator.comparing(o -> o.commitTimestamp));
        this.writeTransactions.sort(Comparator.comparing(o -> o.commitTimestamp));
    }

    protected void checkHistoryValid() throws HistoryInvalidException {
        if (checkHistory()) {
            System.out.println("[INFO] History is valid");
        } else {
            System.out.println("[ERROR] History is invalid");
        }
    }

    protected boolean checkHistory() throws HistoryInvalidException {
        return false;
    }
}
