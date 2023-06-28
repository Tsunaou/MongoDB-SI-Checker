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
        this.oplogHistory.txns.sort(Comparator.comparing(t0 -> t0.commitTimestamp));

        // Redefine index : from 0, 1, 2... keep the order
        int n = this.transactions.size();
        for (int i = 0; i < n; i++) {
            this.transactions.get(i).index = i;
        }

        // Check hisotory
        this.checkHistoryValid();
    }


    @Override
    protected boolean checkHistory() throws HistoryInvalidException {
        if(writeTransactions.size() != oplogHistory.txns.size()){
            oplogHistory.txns.removeIf((OplogTxn txn)-> txn.ops.isEmpty());
            oplogHistory.txns.sort(Comparator.comparing((OplogTxn txn) -> txn.commitTimestamp));
            writeTransactions.sort(Comparator.comparing((MongoDBTransaction txn) -> txn.commitTimestamp));

            int debugGroupSize = 3;

            System.out.println("writeTransaction.size()="+writeTransactions.size());
            System.out.println("oplogHistory.txns.size()="+ oplogHistory.txns.size());

            int size = Math.max(writeTransactions.size(), oplogHistory.txns.size());
            for(int i=0; i<size; i++) {
                MongoDBTransaction t1 = writeTransactions.get(i);
                OplogTxn t2 = oplogHistory.txns.get(i);

                if(t1.txnNumber == 241 && t1.process == 3) {
                    System.out.println("target");
                    System.out.println(t1);
                    System.out.println("-----");
                }

                if(writeTransactions.get(i).commitTimestamp.compareTo(oplogHistory.txns.get(i).commitTimestamp) != 0) {
                    System.out.println(writeTransactions.get(i));
                    System.out.println(oplogHistory.txns.get(i));
                    System.out.println("-------");
                    if(debugGroupSize-- <= 0) {
                        break;
                    }
                }
            }

            // 讨论 [] 的 oplog
            int emptyOplogTxnOpsCnt = 0;
            for(int i=0; i<size; i++) {
                if(oplogHistory.txns.get(i).ops.isEmpty()) {
                    emptyOplogTxnOpsCnt++;
                    System.out.println(oplogHistory.txns.get(i));
                }
            }
            System.out.println("empty op oplog txn is " + emptyOplogTxnOpsCnt);

            throw new HistoryInvalidException("[ERROR] writeTransactions.size() should equal to oplogHistory.logg.size()");
        }

        int n = writeTransactions.size();
        MongoDBTransaction txn1;
        OplogTxn txn2;
        Operation op1, op2;
        for(int i=0; i<n; i++){
            txn1 = writeTransactions.get(i);
            txn2 = oplogHistory.txns.get(i);

            if(txn1.commitTimestamp.compareTo(txn2.commitTimestamp) == 0){
                // Transactions may have same commitTimestamp, so they are out of order
                continue;
            }

            if (txn1.writes.size() != txn2.ops.size()) {
                System.out.println(txn1);
                System.out.println(txn2);
                System.out.println(txn1.commitTimestamp.compareTo(txn2.commitTimestamp) == 0);
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
