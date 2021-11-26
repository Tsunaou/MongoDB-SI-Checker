import CycleChecker.CycleChecker;
import DSG.DirectSerializationGraph;
import Exceptions.DSGInvalidException;
import Exceptions.HistoryInvalidException;
import Exceptions.RelationInvalidException;
import History.MongoDB.MongoDBHistory;
import History.MongoDB.MongoDBHistoryReader;
import History.MongoDB.MongoDBTransaction;
import History.ResultReader;
import Relation.*;
import TestUtil.Finals;

import java.io.File;
import java.util.*;

public class MongoDBSIChecker {

    public static void checkSI(String urlHistory, String urlOplog, String urlMongodLog, String urlResults, String SIVariant) throws HistoryInvalidException, RelationInvalidException, DSGInvalidException, NullPointerException {
        long begin = System.currentTimeMillis();
        System.out.println("---------------------------------------------------------------------------------");
        System.out.println("Checking history for " + SIVariant + " at " + urlHistory);
        MongoDBHistory history = MongoDBHistoryReader.readHistory(urlHistory, urlOplog, urlMongodLog);

        int nTransaction = history.transactions.size();
        Relation<MongoDBTransaction> R = new Relation<MongoDBTransaction>(nTransaction);

        CommitBefore<MongoDBTransaction> CB = new CommitBefore<MongoDBTransaction>(nTransaction);
        CB.calculateRelation(history);
        R.union(CB);

//        DirectSerializationGraph<MongoDBTransaction> dsg = new DirectSerializationGraph<MongoDBTransaction>(history, CB);
//        dsg.checkSI(SIVariant);

        ReturnBefore<MongoDBTransaction> RB = new ReturnBefore<MongoDBTransaction>(nTransaction);
        RB.calculateRelation(history);
        R.union(RB);

        ReadFrom<MongoDBTransaction> RF = new ReadFrom<MongoDBTransaction>(nTransaction);
        RF.calculateRelation(history);
        R.union(RF);


        if (CycleChecker.topoCycleChecker(R.relation)) {
            System.out.println("The Relation is Cyclic");
            List<Integer> cycles = CycleChecker.printCycle(R.relation);
            int n = cycles.size();
            System.out.println(history.transactions.get(cycles.get(0)));
            for (int i = 1; i < n; i++) {
                if (CB.relation.get(cycles.get(i - 1), cycles.get(i))) {
                    System.out.println("Commit Before");
                }
                if (RF.relation.get(cycles.get(i - 1), cycles.get(i))) {
                    System.out.println("Read From");
                }
                System.out.println(history.transactions.get(cycles.get(i)));
            }
        } else {
            System.out.println("The Relation is " + SIVariant);
        }
        long end = System.currentTimeMillis();
        System.out.println("Cost " + (end - begin) + " ms");
    }

    public static void checkAll() throws RelationInvalidException, DSGInvalidException, HistoryInvalidException {
        checkAll("/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store");
    }

    public static void checkAll(String base) throws RelationInvalidException, DSGInvalidException, HistoryInvalidException {
        String URLHistory;
        String URLOplog;
        String URLMongodLog;
        String URLResults;

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
                            URLResults = data.getPath() + "/results.edn";

                            if (new File(URLHistory).exists() && new File(URLOplog).exists()) {
                                try {
                                    checkSI(URLHistory, URLOplog, URLMongodLog, URLResults, variant);
                                } catch (NullPointerException | HistoryInvalidException e) {
                                    e.printStackTrace();
                                }
                            }
                            if (new File(URLResults).exists()) {
                                ResultReader.report(URLResults);
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
        String URLResults = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/latest/results.json";
        checkSI(URLHistory, URLOplog, URLMongodLog, URLResults, "Session-SI");
    }

    public static void checkSample() throws RelationInvalidException, DSGInvalidException, HistoryInvalidException {
        String base = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/latest/";
        String URLHistory = base + "history.edn";
        String URLOplog = base + "txns.json";
        String URLMongodLog = base + "mongod.json";
        String URLResults = base + "results.json";
        if (new File(URLHistory).exists() && new File(URLOplog).exists()) {
            try {
                checkSI(URLHistory, URLOplog, URLMongodLog, URLResults, "Session-SI");
            } catch (NullPointerException | HistoryInvalidException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws HistoryInvalidException, RelationInvalidException, DSGInvalidException {
        if (args.length == 0) {
            checkAll();
        } else {
            System.out.println("args is " + Arrays.toString(args));
            String base = args[0];
            checkAll(base);
        }
    }
}
