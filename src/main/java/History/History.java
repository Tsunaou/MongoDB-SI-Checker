package History;

import Exceptions.HistoryInvalidException;
import History.WiredTiger.WtLog;
import History.WiredTiger.WtOp;
import History.WiredTiger.WtTxn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class History {
    public ArrayList<Transaction> transactions;
    public ArrayList<Transaction> readOnlyTransactions;
    public ArrayList<Transaction> writeTransactions;
    public WtLog wtLog;

    public History(ArrayList<Transaction> transactions, WtLog wtLog) throws HistoryInvalidException {
        this.transactions = transactions;
        this.readOnlyTransactions = new ArrayList<>();
        this.writeTransactions = new ArrayList<>();
        this.wtLog = wtLog;

        // 1. Get read only histories
        for (Transaction txn : transactions) {
            if (txn.tid == -1) {
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
