package Relation;

import Exceptions.RelationInvalidException;
import History.*;

import java.util.*;


public class ReadFrom extends Relation {
    public ReadFrom(int n) {
        super(n);
    }

    @Override
    public void calculateRelation(History history) throws RelationInvalidException {
        super.calculateRelation(history);

        List<Integer> kvTuple;
        ArrayList<Transaction> writes;
        int wIdx;
        int rIdx;
        for (Map.Entry<List<Integer>, ArrayList<Transaction>> entry : history.kvReadsMap.entrySet()) {
            kvTuple = entry.getKey();
            if (history.kvWritesMap.containsKey(kvTuple)) {
                writes = history.kvWritesMap.get(kvTuple);
                if (writes.size() != 1) {
                    throw new RelationInvalidException("Not Differentiated History");
                }
                wIdx = (int) writes.get(0).tid;
                for (Transaction read : entry.getValue()) {
                    rIdx = (int) read.index;
                    if (wIdx != rIdx) {
                        addRelation(wIdx, (int) read.index);
                    }
                }
            } else {
                throw new RelationInvalidException("Thin Air Read when calculating Read From Relation " + kvTuple);
            }
        }

    }
}
