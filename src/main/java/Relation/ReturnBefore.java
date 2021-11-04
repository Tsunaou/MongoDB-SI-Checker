package Relation;

import Exceptions.RelationInvalidException;
import History.History;
import History.Transaction;
import History.WiredTiger.WiredTigerHistory;
import History.WiredTiger.WiredTigerTransaction;

import java.util.Comparator;

public class ReturnBefore extends Relation<WiredTigerTransaction> {
    public ReturnBefore(int n) {
        super(n);
    }

    @Override
    public void calculateRelation(History<WiredTigerTransaction> history) throws RelationInvalidException {
        super.calculateRelation(history);

        WiredTigerTransaction txn1;
        WiredTigerTransaction txn2;

        int n = history.transactions.size();
        for(int i=0; i<n; i++){
            txn1 = history.transactions.get(i);
            for(int j=i+1; j<n; j++){
                txn2 = history.transactions.get(j);
                if(txn1.commitTimestamp.compareTo(txn2.startTimestamp) < 0){
                    addRelation(i, j);
                }
            }
        }

    }
}
