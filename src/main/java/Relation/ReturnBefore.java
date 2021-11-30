package Relation;

import Exceptions.RelationInvalidException;
import History.History;
import History.MongoDB.LogicalClock;
import History.Transaction;


public class ReturnBefore<Txn extends Transaction> extends Relation<Txn> {

    public boolean fixme = false; // Whether we should use read-from to fix the visibility
    public LogicalClock scale;

    public ReturnBefore(int n) {
        super(n);
    }

    public ReturnBefore(int n, boolean fixme) {
        super(n);
        this.fixme = fixme;
        this.scale = new LogicalClock(0, 0);
    }


    @Override
    public void calculateRelation(History<Txn> history) throws RelationInvalidException {
        super.calculateRelation(history);

        Txn txn1;
        Txn txn2;

        ReadFrom<Txn> RF = new ReadFrom<Txn>(n);
        if (fixme) {
            RF.calculateRelation(history);
        }

        int count = 0;
        int n = history.transactions.size();
        for (int i = 0; i < n; i++) {
            txn1 = history.transactions.get(i);
            for (int j = i + 1; j < n; j++) {
                txn2 = history.transactions.get(j);
                if (txn1.commitTimestamp.compareTo(txn2.startTimestamp) <= 0) {
                    addRelation(i, j);
                } else if (fixme && RF.relation.get(i, j)) {
                    count++;
                    addRelation(i, j);
                    LogicalClock diff = txn1.commitTimestamp.getAdvance(txn2.startTimestamp);
                    if (scale.compareTo(diff) < 0) {
                        scale = diff;
                    }
                }
            }
        }

        if(fixme){
            System.out.println("[INFO] " + count + " times of fixing in " + history.transactions.size() + " transactions");
            System.out.println("[INFO] The error scale is " + scale.toSecond() + "seconds");
        }


    }
}
