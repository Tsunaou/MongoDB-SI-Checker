package Relation;

import Exceptions.RelationInvalidException;
import History.WiredTiger.WiredTigerHistory;
import History.WiredTiger.WiredTigerTransaction;

public class ReturnBefore extends Relation {
    public ReturnBefore(int n) {
        super(n);
    }

    @Override
    public void calculateRelation(WiredTigerHistory history) throws RelationInvalidException {
        super.calculateRelation(history);

        WiredTigerTransaction txn1;
        WiredTigerTransaction txn2;

        int n = history.transactions.size();
        for(int i=0; i<n; i++){
            txn1 = history.transactions.get(i);
            for(int j=i+1; j<n; j++){
                txn2 = history.transactions.get(j);
                if(txn1.commitTimestamp < txn2.startTimestamp){
                    addRelation(i, j);
                }
            }
        }

    }
}
