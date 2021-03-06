package Relation;

import Exceptions.RelationInvalidException;
import History.History;
import History.WiredTiger.WiredTigerHistory;
import History.WiredTiger.WiredTigerTransaction;

import java.util.ArrayList;
import java.util.Map;

public class TidBefore extends Relation<WiredTigerTransaction> {
    public TidBefore(int n) {
        super(n);
    }

    @Override
    public void calculateRelation(History<WiredTigerTransaction> history) throws RelationInvalidException {
        super.calculateRelation(history);

        ArrayList<WiredTigerTransaction> list;
        WiredTigerTransaction txn1;
        WiredTigerTransaction txn2;
        for (Map.Entry<Long, ArrayList<WiredTigerTransaction>> entry : history.keyWritesMap.entrySet()) {
            list = entry.getValue();
            int n = list.size();
            for (int i = 0; i < n; i++) {
                txn1 = list.get(i);
                for (int j = i + 1; j < n; j++) {
                    txn2 = list.get(j);
                    if (txn1.tid < txn2.tid) {
                        addRelation((int) txn1.tid, (int) txn2.tid);
                    } else {
                        addRelation((int) txn2.tid, (int) txn1.tid);
                    }
                    if ((txn1.tid < txn2.tid) ^ (txn1.commitTimestamp.compareTo(txn2.commitTimestamp) < 0)) {
                        // xor
                        System.out.println(txn1);
                        System.out.println(txn2);
                        throw new RelationInvalidException("Conflict with Lemma 10");
                    }
                }
            }
        }

    }
}
