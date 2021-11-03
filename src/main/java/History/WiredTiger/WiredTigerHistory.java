package History.WiredTiger;

import Const.Const;
import Exceptions.HistoryInvalidException;
import History.History;
import History.Operation;
import History.WiredTiger.LSN.WtLog;
import History.WiredTiger.LSN.WtOp;
import History.WiredTiger.LSN.WtTxn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class WiredTigerHistory extends History<WiredTigerTransaction> {
    public WtLog wtLog;

    public WiredTigerHistory(ArrayList<WiredTigerTransaction> transactions, WtLog wtLog) throws HistoryInvalidException {
        super(transactions);
        this.wtLog = wtLog;
        this.wtLog.logs.sort((((o1, o2) -> (int) (o1.tid - o2.tid))));

        // Redefine tid : from 0, 1, 2... keep the order
        int n = this.writeTransactions.size();
        for (int i = 0; i < n; i++) {
            this.writeTransactions.get(i).tid = i;
            this.wtLog.logs.get(i).tid = i;
        }

        // Check history
        this.checkHistoryValid();

    }

    @Override
    protected void sortTransactions() {
        super.sortTransactions();
        this.transactions.sort((o1, o2) -> (int) (o1.tid - o2.tid));
        this.readOnlyTransactions.sort((o1, o2) -> (int) (o1.tid - o2.tid));
        this.writeTransactions.sort((o1, o2) -> (int) (o1.tid - o2.tid));
    }

    @Override
    public boolean checkHistory() throws HistoryInvalidException {
        if (writeTransactions.size() != wtLog.logs.size()) {
            throw new HistoryInvalidException("[ERROR] writeTransactions.size() should equal to wtLog.logg.size()");
        }
        int n = writeTransactions.size();
        WiredTigerTransaction txn1;
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
