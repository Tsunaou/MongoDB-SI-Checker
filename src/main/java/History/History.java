package History;

import Const.Const;
import Exceptions.HistoryInvalidException;
import History.WiredTiger.WtLog;
import History.WiredTiger.WtOp;
import History.WiredTiger.WtTxn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class History {
    public ArrayList<Transaction> transactions;
    public ArrayList<Transaction> readOnlyTransactions;
    public ArrayList<Transaction> writeTransactions;
    public WtLog wtLog;

    public HashMap<Long, ArrayList<Transaction>> keyWritesMap; // key: key, value: the transactions which have written into the key
    public HashMap<List<Integer>, ArrayList<Transaction>> kvWritesMap = new HashMap<>();
    public HashMap<List<Integer>, ArrayList<Transaction>> kvReadsMap = new HashMap<>();

    public History(ArrayList<Transaction> transactions, WtLog wtLog) throws HistoryInvalidException {
        this.transactions = transactions;
        this.readOnlyTransactions = new ArrayList<>();
        this.writeTransactions = new ArrayList<>();
        this.wtLog = wtLog;

        this.keyWritesMap = new HashMap<>();
        this.kvWritesMap = new HashMap<>();
        this.kvReadsMap = new HashMap<>();

        // 1. Get read only histories
        for (Transaction txn : transactions) {
            if (txn.tid == Const.READONLY_TID) {
                this.readOnlyTransactions.add(txn);
            } else {
                this.writeTransactions.add(txn);
            }
        }

        // 2. Sort transactions
        this.transactions.sort((o1, o2) -> (int) (o1.tid - o2.tid));
        this.readOnlyTransactions.sort((o1, o2) -> (int) (o1.tid - o2.tid));
        this.writeTransactions.sort((o1, o2) -> (int) (o1.tid - o2.tid));
        this.wtLog.logs.sort((((o1, o2) -> (int) (o1.tid - o2.tid))));

        // 3. Check history with wiredtiger log
        if (checkHistory()) {
            System.out.println("[INFO] History is valid");
        } else {
            System.out.println("[ERROR] History is invalid");
        }

        // 4. Redefine tid : from 0, 1, 2... keep the order
        int n = this.writeTransactions.size();
        for (int i = 0; i < n; i++) {
            this.writeTransactions.get(i).tid = i;
            this.wtLog.logs.get(i).tid = i;
        }

        // 5. Calculate keyWritesMap
        ArrayList<Transaction> list;
        for (Transaction txn : this.transactions) {
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

        // 6. Set Transaction Index
        for (int i = 0; i < this.transactions.size(); i++) {
            this.transactions.get(i).index = i;
        }

        // 7. Calculate kvWritesMap and kvReadsMap
        List<Integer> tuple;
        for (Transaction txn : this.transactions) {
            for(Operation op: txn.writes){
                tuple = Arrays.asList(op.key.intValue(), op.value.intValue());
                if(kvWritesMap.containsKey(tuple)){
                    list = kvWritesMap.get(tuple);
                }else{
                    list = new ArrayList<Transaction>();
                    kvWritesMap.put(tuple, list);
                }
                list.add(txn);
            }

            for(Operation op: txn.reads){
                if(op.value == Const.INIT_READ){
                    continue;
                }
                tuple = Arrays.asList(op.key.intValue(), op.value.intValue());
                if(kvReadsMap.containsKey(tuple)){
                    list = kvReadsMap.get(tuple);
                }else{
                    list = new ArrayList<Transaction>();
                    kvReadsMap.put(tuple, list);
                }
                list.add(txn);
            }
        }
    }

    public boolean checkHistory() throws HistoryInvalidException {
        if (writeTransactions.size() != wtLog.logs.size()) {
            throw new HistoryInvalidException("[ERROR] writeTransactions.size() should equal to wtLog.logg.size()");
        }
        int n = writeTransactions.size();
        Transaction txn1;
        WtTxn txn2;
        Operation op1;
        WtOp op2;
        for (int i = 0; i < n; i++) {
            txn1 = writeTransactions.get(i);
            txn2 = wtLog.logs.get(i);
            if (txn1.tid != txn2.tid) {
                throw new HistoryInvalidException("");
            }
            if (txn1.writes.size() != txn2.ops.size()) {
                throw new HistoryInvalidException("");
            }
            int m = txn1.writes.size();
            for (int j = 0; j < m; j++) {
                op1 = txn1.writes.get(j);
                op2 = txn2.ops.get(j);
                if (op1.key != op2.key || op1.value != op2.value) {
                    throw new HistoryInvalidException("");
                }
            }
        }
        return true;
    }


}
