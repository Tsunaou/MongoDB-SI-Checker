package Relation;

import Exceptions.RelationInvalidException;
import History.History;
import History.Transaction;
import History.WiredTiger.WiredTigerHistory;
import History.WiredTiger.WiredTigerTransaction;

import java.util.*;


public class ReadFrom<Txn extends Transaction> extends Relation<Txn> {
    public ReadFrom(int n) {
        super(n);
    }

    @Override
    public void calculateRelation(History<Txn> history) throws RelationInvalidException {
        super.calculateRelation(history);

        List<Integer> kvTuple;
        ArrayList<Txn> writes;
        int wIdx;
        int rIdx;
        for (Map.Entry<List<Integer>, ArrayList<Txn>> entry : history.kvReadsMap.entrySet()) {
            kvTuple = entry.getKey();
            if (history.kvWritesMap.containsKey(kvTuple)) {
                writes = history.kvWritesMap.get(kvTuple);
                if (writes.size() != 1) {
                    throw new RelationInvalidException("Not Differentiated History");
                }
                wIdx = (int) writes.get(0).index;
                for (Txn read : entry.getValue()) {
                    rIdx = (int) read.index;
                    if (wIdx != rIdx) {
                        addRelation(wIdx, (int) read.index);
                        System.out.println("Read from:");
                        System.out.println(history.transactions.get(wIdx));
                        System.out.println(history.transactions.get(rIdx));
                    }
                }
            } else {
                throw new RelationInvalidException("Thin Air Read when calculating Read From Relation " + kvTuple);
            }
        }

    }
}
