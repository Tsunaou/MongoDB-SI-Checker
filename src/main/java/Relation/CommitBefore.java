package Relation;

import Exceptions.RelationInvalidException;
import History.*;

public class CommitBefore extends Relation{
    public CommitBefore(int n) {
        super(n);
    }

    @Override
    public void calculateRelation(History history) throws RelationInvalidException {
        super.calculateRelation(history);

        Transaction txn1;
        Transaction txn2;

        int n = history.transactions.size();
        for(int i=0; i<n; i++){
            txn1 = history.transactions.get(i);
            for(int j=i+1; j<n; j++){
                txn2 = history.transactions.get(j);
                if(txn1.commitTimestamp < txn2.commitTimestamp){
                    addRelation(i, j);
                }
            }
        }
    }
}
