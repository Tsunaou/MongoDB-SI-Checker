import CycleChecker.CycleChecker;
import Exceptions.HistoryInvalidException;
import Exceptions.RelationInvalidException;
import History.WiredTiger.WiredTigerHistory;
import History.WiredTiger.WiredTigerHistoryReader;
import Relation.*;

public class WTSIChecker {
    public static void main(String[] args) throws HistoryInvalidException, RelationInvalidException {
        for(int i=0 ; i<1 ;i++){
            System.out.println("========== Testing History " + i + "===========");
            String BASE = "/media/young/Education/Programs/Java-Programs/Snapshot-Isolation-Checker-Java/src/main/resources/data-1022/" + i + "/";
            String URLHistory = BASE + "history.edn";
            String URLWTLog = BASE + "wiredtiger.log";

            WiredTigerHistory history = WiredTigerHistoryReader.readHistory(URLHistory, URLWTLog);
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
}
