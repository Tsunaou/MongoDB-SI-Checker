import CycleChecker.CycleChecker;
import DSG.DirectSerializationGraph;
import Exceptions.DSGInvalidException;
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

    public static void checkSI(String urlHistory, String urlOplog, String urlMongodLog, String SIVariant) throws HistoryInvalidException, RelationInvalidException, DSGInvalidException, NullPointerException {
        System.out.println("---------------------------------------------------------------------------------");
        System.out.println("Checking history for " + SIVariant + " at " + urlHistory);
        MongoDBHistory history = MongoDBHistoryReader.readHistory(urlHistory, urlOplog, urlMongodLog);
        DirectSerializationGraph<MongoDBTransaction> dsg = new DirectSerializationGraph<MongoDBTransaction>(history);
        dsg.checkSI(SIVariant);
        int nTransaction = history.transactions.size();


        CommitBefore<MongoDBTransaction> CB = new CommitBefore<MongoDBTransaction>(nTransaction);
        ReturnBefore<MongoDBTransaction> RB = new ReturnBefore<MongoDBTransaction>(nTransaction);
        ReadFrom<MongoDBTransaction> RF = new ReadFrom<MongoDBTransaction>(nTransaction);

        CB.calculateRelation(history);
        RB.calculateRelation(history);
        RF.calculateRelation(history);

        Relation<MongoDBTransaction> R = new Relation<MongoDBTransaction>(nTransaction);
        R.union(CB);
        R.union(RB);
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

    public static void checkAll() throws RelationInvalidException, DSGInvalidException, HistoryInvalidException {
        String URLHistory;
        String URLOplog;
        String URLMongodLog;

        String base = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store";
        File store = new File(base);
        HashMap<String, String> keyVariant = new HashMap<>();
        keyVariant.put("sharded", "Session-SI");
        keyVariant.put("replica", "Realtime-SI");

        for (File file : Objects.requireNonNull(store.listFiles())) {
            for (Map.Entry<String, String> entry : keyVariant.entrySet()) {
                String keyword = entry.getKey();
                String variant = entry.getValue();
                if (file.isDirectory() && file.getPath().contains(keyword)) {
                    for (File data : Objects.requireNonNull(file.listFiles())) {
                        if (data.isDirectory() && !data.getPath().contains("latest")) {
                            URLHistory = data.getPath() + "/history.edn";
                            URLOplog = data.getPath() + "/txns.json";
                            URLMongodLog = data.getPath() + "/mongod.json";

                            if (new File(URLHistory).exists() && new File(URLOplog).exists()) {
                                try {
                                    checkSI(URLHistory, URLOplog, URLMongodLog, variant);
                                } catch (NullPointerException | HistoryInvalidException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }
            }
        }

    }

    public static void checkLatest() throws RelationInvalidException, DSGInvalidException, HistoryInvalidException {
        String URLHistory = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/latest/history.edn";
        String URLOplog = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/latest/txns.json";
        String URLMongodLog = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/latest/mongod.json";
        checkSI(URLHistory, URLOplog, URLMongodLog, "Session-SI");
    }

    public static void checkSample() throws RelationInvalidException, DSGInvalidException, HistoryInvalidException {
        String base = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/mongodb wr sharded-cluster w:majority r:majority tw:majority tr:snapshot partition/20211111T072636.000Z/";
        String URLHistory = base + "history.edn";
        String URLOplog = base + "txns.json";
        String URLMongodLog = base + "mongod.json";
        if (new File(URLHistory).exists() && new File(URLOplog).exists()) {
            try {
                checkSI(URLHistory, URLOplog, URLMongodLog, "Session-SI");
            } catch (NullPointerException | HistoryInvalidException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws HistoryInvalidException, RelationInvalidException, DSGInvalidException {
//        checkLatest();
        checkAll();
//        checkSample();
    }
}
