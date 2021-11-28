package Relation;

import Exceptions.RelationInvalidException;
import History.History;
import History.Transaction;


public class ReturnBefore<Txn extends Transaction> extends Relation<Txn> {
    public ReturnBefore(int n) {
        super(n);
    }

    @Override
    public void calculateRelation(History<Txn> history) throws RelationInvalidException {
        super.calculateRelation(history);

        Txn txn1;
        Txn txn2;

        int n = history.transactions.size();
        for(int i=0; i<n; i++){
            txn1 = history.transactions.get(i);
            for(int j=i+1; j<n; j++){
                txn2 = history.transactions.get(j);
                if(txn1.commitTimestamp.compareTo(txn2.startTimestamp) <= 0){
                    addRelation(i, j);
                }
            }
        }

    }
}
