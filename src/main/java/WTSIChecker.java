import CycleChecker.CycleChecker;
import Exceptions.HistoryInvalidException;
import Exceptions.RelationInvalidException;
import History.History;
import Relation.*;
import TestUtil.Finals;

import History.*;

public class WTSIChecker {
    public static void main(String[] args) throws HistoryInvalidException, RelationInvalidException {
        String URLHistory = Finals.URLHistory;
        String URLWTLog = Finals.URLWTLog;

        History history = HistoryReader.readHistory(URLHistory, URLWTLog);
        int nTransaction = history.transactions.size();

        ReturnBefore RB = new ReturnBefore(nTransaction);
        CommitBefore CB = new CommitBefore(nTransaction);
        ReadFrom RF = new ReadFrom(nTransaction);
        TidBefore TB = new TidBefore(nTransaction);

        RB.calculateRelation(history);
        CB.calculateRelation(history);
        RF.calculateRelation(history);
        TB.calculateRelation(history);

        Relation R = new Relation(nTransaction);
        R.union(RB);
        R.union(CB);
        R.union(RF);
        R.union(TB);

        if(CycleChecker.topoCycleChecker(R.relation)){
            System.out.println("The Relation if Cyclic");
        }else{
            System.out.println("The Relation if StrongSI");
        }
    }
}
