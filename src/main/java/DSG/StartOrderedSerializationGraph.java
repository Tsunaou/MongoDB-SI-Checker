package DSG;

import Exceptions.DSGInvalidException;
import Exceptions.HistoryInvalidException;
import Exceptions.RelationInvalidException;
import History.History;
import History.Transaction;
import History.WiredTiger.WiredTigerHistory;
import History.WiredTiger.WiredTigerHistoryReader;
import History.WiredTiger.WiredTigerTransaction;
import Relation.CommitBefore;
import Relation.Relation;
import TestUtil.Finals;

public class StartOrderedSerializationGraph<Txn extends Transaction> extends DirectSerializationGraph<Txn> {

    public CommitBefore<Txn> CB;

    public StartOrderedSerializationGraph(History<Txn> history) throws RelationInvalidException, DSGInvalidException {
        super(history);
        CB = new CommitBefore<Txn>(history.transactions.size());
    }

    /**
     * G-SI:
     * G-SI-a: Interference. A history H exhibits PHENOMENON G-SIa if SSG(H) contains a
     * read/write-dependency(ww or wr) edge from T_i to T_j without there also being a start-dependency edge from T_i to T_j
     * G-SI-b: Missed Effects. A history H exhibits phenomenon G-SIb if SSG(H) contains a directed cycle
     * with exactly one anti-dependency edges(rw).
     *
     * @return whether H exhibits G-SI
     */
    public boolean containsGSI() throws RelationInvalidException {
        // Contains G-SIa
        // Actually, it wat (ww[i][j] | wr[i][j]) & CB[j][i]
        int n = history.transactions.size();
        CB.calculateRelation(history);
        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                if ((ww.relation[i][j] | wr.relation[i][j]) & CB.relation[j][i]) {
                    return true;
                }
                if ((ww.relation[j][i] | wr.relation[j][i]) & CB.relation[i][j]) {
                    return true;
                }
            }
        }

        // Contains G-SIb
        Relation<Txn> R = new Relation<Txn>(n);
        R.union(ww);
        R.union(wr);
        R.union(CB);

        // exactly one rw edge
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                if (i == j) {
                    continue;
                }
            }
        }

        return false;
    }

    public static void main(String[] args) throws RelationInvalidException, HistoryInvalidException, DSGInvalidException {
        String URLHistory = Finals.URLHistory;
        String URLWTLog = Finals.URLWTLog;

        WiredTigerHistory history = WiredTigerHistoryReader.readHistory(URLHistory, URLWTLog);
        StartOrderedSerializationGraph<WiredTigerTransaction> ssg = new StartOrderedSerializationGraph<WiredTigerTransaction>(history);

        System.out.println(ssg.containsGSI());
    }
}
