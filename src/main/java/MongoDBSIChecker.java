import CycleChecker.CycleChecker;
import Exceptions.HistoryInvalidException;
import Exceptions.RelationInvalidException;
import History.MongoDB.MongoDBHistory;
import History.MongoDB.MongoDBHistoryReader;
import History.MongoDB.MongoDBTransaction;
import Relation.*;
import TestUtil.Finals;

import java.io.File;
import java.util.*;

public class MongoDBSIChecker {

    public static void checkSI(String urlHistory, String urlOplog, String SIVariant) throws HistoryInvalidException, RelationInvalidException {
        System.out.println("---------------------------------------------------------------------------------");
        System.out.println("Checking history for " + SIVariant + " at " + urlHistory);
        MongoDBHistory history = MongoDBHistoryReader.readHistory(urlHistory, urlOplog);
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
                if (CB.relation[cycles.get(i - 1)][cycles.get(i)]) {
                    System.out.println("Commit Before");
                }
                if (RF.relation[cycles.get(i - 1)][cycles.get(i)]) {
                    System.out.println("Read From");
                }
                System.out.println(history.transactions.get(cycles.get(i)));
            }
        } else {
            System.out.println("The Relation is " + SIVariant);
        }
    }

    public static void main(String[] args) throws HistoryInvalidException, RelationInvalidException {
        String URLHistory;
        String URLOplog;

//        checkSI(URLHistory, URLOplog);
        String base = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store";
        File store = new File(base);
        HashMap<String, String> keyVariant = new HashMap<>();
        keyVariant.put("sharded", "Session-SI");
        keyVariant.put("replica", "Realtime-SI");

        for (File file : Objects.requireNonNull(store.listFiles())) {
            for(Map.Entry<String, String> entry: keyVariant.entrySet()){
                String keyword = entry.getKey();
                String variant = entry.getValue();
                if (file.isDirectory() && file.getPath().contains(keyword)) {
                    for (File data : Objects.requireNonNull(file.listFiles())) {
                        if (data.isDirectory() && !data.getPath().contains("latest")) {
                            URLHistory = data.getPath() + "/history.edn";
                            URLOplog = data.getPath() + "/txns.json";

                            if (new File(URLHistory).exists() && new File(URLOplog).exists()) {
                                checkSI(URLHistory, URLOplog, variant);
                            }
                        }
                    }
                }
            }
        }



    }
}
