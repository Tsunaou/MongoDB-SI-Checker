package History.MongoDB;

import Exceptions.HistoryInvalidException;
import History.History;
import History.Operation;
import History.MongoDB.Oplog.OplogHistory;
import History.MongoDB.Oplog.OplogTxn;

import java.util.ArrayList;
import java.util.Comparator;

public class MongoDBHistory extends History<MongoDBTransaction> {
    public OplogHistory oplogHistory;

    public MongoDBHistory(ArrayList<MongoDBTransaction> transactions, OplogHistory oplogHistory) throws HistoryInvalidException {
        super(transactions);
        this.oplogHistory = oplogHistory;
        this.oplogHistory.txns.sort(Comparator.comparingLong(o -> o.commitTimestamp.getLongTime()));

        // Redefine tid : from 0, 1, 2... keep the order
        int n = this.writeTransactions.size();
        for (int i = 0; i < n; i++) {
            this.writeTransactions.get(i).index = i;
        }

        // Check hisotory
        this.checkHistoryValid();
    }


    @Override
    protected boolean checkHistory() throws HistoryInvalidException {
        if(writeTransactions.size() != oplogHistory.txns.size()){
            throw new HistoryInvalidException("[ERROR] writeTransactions.size() should equal to oplogHistory.logg.size()");
        }

        int n = writeTransactions.size();
        MongoDBTransaction txn1;
        OplogTxn txn2;
        Operation op1, op2;
        for(int i=0; i<n; i++){
            txn1 = writeTransactions.get(i);
            txn2 = oplogHistory.txns.get(i);

            if(txn1.commitClusterTime.getLongTime() == txn2.commitTimestamp.getLongTime()){
                // Transactions may have same commitTimestamp, so they are out of order
                continue;
            }

            if (txn1.writes.size() != txn2.ops.size()) {
                System.out.println(txn1);
                System.out.println(txn2);
                throw new HistoryInvalidException("");
            }

            int m = txn1.writes.size();
            for (int j = 0; j < m; j++) {
                op1 = txn1.writes.get(j);
                op2 = txn2.ops.get(j);
                if (!op1.key.equals(op2.key) || !op1.value.equals(op2.value)) {
                    throw new HistoryInvalidException("");
                }
            }
            System.out.println(txn1.writes);
            System.out.println(txn2.ops);
        }

        return true;
    }
}
