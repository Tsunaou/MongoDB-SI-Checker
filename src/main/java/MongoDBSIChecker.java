import CycleChecker.CycleChecker;
import Exceptions.HistoryInvalidException;
import Exceptions.RelationInvalidException;
import History.MongoDB.MongoDBHistory;
import History.MongoDB.MongoDBHistoryReader;
import History.MongoDB.MongoDBTransaction;
import Relation.*;
import TestUtil.Finals;

import java.util.ArrayList;

public class MongoDBSIChecker {
    public static void main(String[] args) throws HistoryInvalidException, RelationInvalidException {
        String URLHistory = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/latest/history.edn";
        String URLOplog = "/home/young/Programs/Jepsen-Mongo-Txn/logs/txns.json";

        MongoDBHistory history = MongoDBHistoryReader.readHistory(URLHistory, URLOplog);
        int nTransaction = history.transactions.size();


        CommitBefore<MongoDBTransaction> CB = new CommitBefore<MongoDBTransaction>(nTransaction);
        ReadFrom<MongoDBTransaction> RF = new ReadFrom<MongoDBTransaction>(nTransaction);

        CB.calculateRelation(history);
        RF.calculateRelation(history);

        Relation<MongoDBTransaction> R = new Relation<MongoDBTransaction>(nTransaction);
        R.union(CB);
        R.union(RF);

        if(CycleChecker.topoCycleChecker(R.relation)){
            System.out.println("The Relation is Cyclic");
        }else{
            System.out.println("The Relation is SessionSI");
        }
    }
}
