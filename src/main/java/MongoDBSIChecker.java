import CycleChecker.CycleChecker;
import Exceptions.HistoryInvalidException;
import Exceptions.RelationInvalidException;
import History.MongoDB.MongoDBHistory;
import History.MongoDB.MongoDBHistoryReader;
import History.MongoDB.MongoDBTransaction;
import Relation.*;
import TestUtil.Finals;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class MongoDBSIChecker {
    public static void main(String[] args) throws HistoryInvalidException, RelationInvalidException {
        String URLHistory = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/latest/history.edn";
        String URLOplog = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/latest/txns.json";

        MongoDBHistory history = MongoDBHistoryReader.readHistory(URLHistory, URLOplog);
        int nTransaction = history.transactions.size();


        CommitBefore<MongoDBTransaction> CB = new CommitBefore<MongoDBTransaction>(nTransaction);
        ReadFrom<MongoDBTransaction> RF = new ReadFrom<MongoDBTransaction>(nTransaction);

        CB.calculateRelation(history);
        RF.calculateRelation(history);

        Relation<MongoDBTransaction> R = new Relation<MongoDBTransaction>(nTransaction);
        R.union(CB);
        R.union(RF);

        if (CycleChecker.topoCycleChecker(R.relation)) {
            System.out.println("The Relation is Cyclic");
            List<Integer> cycles = CycleChecker.printCycle(R.relation);
            int n = cycles.size();
            System.out.println(history.transactions.get(cycles.get(0)));
            for (int i = 1; i < n; i++) {
                if(CB.relation[cycles.get(i-1)][cycles.get(i)]){
                    System.out.println("Commit Before");
                }
                if(RF.relation[cycles.get(i-1)][cycles.get(i)]){
                    System.out.println("Read From");
                }
                System.out.println(history.transactions.get(cycles.get(i)));
            }
        } else {
            System.out.println("The Relation is RealtimeSI/SessionSI");
        }
    }
}
