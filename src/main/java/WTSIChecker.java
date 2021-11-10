import CycleChecker.CycleChecker;
import DSG.DirectSerializationGraph;
import Exceptions.DSGInvalidException;
import Exceptions.HistoryInvalidException;
import Exceptions.RelationInvalidException;
import History.WiredTiger.WiredTigerHistory;
import History.WiredTiger.WiredTigerTransaction;
import History.WiredTiger.WiredTigerHistoryReader;
import Relation.*;

public class WTSIChecker {
    public static void main(String[] args) throws HistoryInvalidException, RelationInvalidException, DSGInvalidException {
        for(int i=0 ; i<10 ;i++){
            System.out.println("========== Testing History " + i + "===========");
            String BASE = "/media/young/Education/Programs/Java-Programs/Snapshot-Isolation-Checker-Java/src/main/resources/data-1022/" + i + "/";
            String URLHistory = BASE + "history.edn";
            String URLWTLog = BASE + "wiredtiger.log";

            WiredTigerHistory history = WiredTigerHistoryReader.readHistory(URLHistory, URLWTLog);
            DirectSerializationGraph<WiredTigerTransaction> dsg = new DirectSerializationGraph<WiredTigerTransaction>(history);
            dsg.checkSI("StrongSI");
            int nTransaction = history.transactions.size();

            ReturnBefore RB = new ReturnBefore(nTransaction);
            CommitBefore<WiredTigerTransaction> CB = new CommitBefore<WiredTigerTransaction>(nTransaction);
            ReadFrom<WiredTigerTransaction> RF = new ReadFrom<WiredTigerTransaction>(nTransaction);
            TidBefore TB = new TidBefore(nTransaction);

            RB.calculateRelation(history);
            CB.calculateRelation(history);
            RF.calculateRelation(history);
            TB.calculateRelation(history);

            Relation<WiredTigerTransaction> R = new Relation<WiredTigerTransaction>(nTransaction);
            R.union(RB);
            R.union(CB);
            R.union(RF);
            R.union(TB);

            if(CycleChecker.topoCycleChecker(R.relation)){
                System.out.println("The Relation is Cyclic");
            }else{
                System.out.println("The Relation is StrongSI");
            }
        }

    }
}
