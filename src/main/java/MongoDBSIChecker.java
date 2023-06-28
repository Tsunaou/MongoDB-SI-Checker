import CycleChecker.CycleChecker;
import DSG.DirectSerializationGraph;
import Exceptions.DSGInvalidException;
import Exceptions.HistoryInvalidException;
import Exceptions.RelationInvalidException;
import History.MongoDB.MongoDBHistory;
import History.MongoDB.MongoDBHistoryReader;
import History.MongoDB.MongoDBTransaction;
import History.ResultReader;
import History.WiredTiger.WiredTigerTransaction;
import IntExt.EXTChecker;
import IntExt.INTChecker;
import Relation.*;
import TestUtil.Finals;

import java.io.File;
import java.util.*;

public class MongoDBSIChecker {

    public static void checkHistoryValid(String urlHistory, String urlOplog, String urlMongodLog,
                                         String urlResults, String urlRoOplog, String SIVariant)
            throws HistoryInvalidException, RelationInvalidException, NullPointerException {
        System.out.println("---------------------------------------------------------------------------------");
        System.out.println("Checking history valid for " + SIVariant + " at " + urlHistory);
        MongoDBHistory history = MongoDBHistoryReader.readHistory(urlHistory, urlOplog, urlMongodLog, urlRoOplog, SIVariant);
    }

    public static void checkSIIntExt(String urlHistory, String urlOplog, String urlMongodLog,
                                     String urlResults, String urlRoOplog, String SIVariant)
            throws HistoryInvalidException, RelationInvalidException, DSGInvalidException, NullPointerException{
        long begin = System.currentTimeMillis();
        System.out.println("---------------------------------------------------------------------------------");
        System.out.println("Checking history for " + SIVariant + " at " + urlHistory);
        MongoDBHistory history = MongoDBHistoryReader.readHistory(urlHistory, urlOplog, urlMongodLog, urlRoOplog, SIVariant);

        INTChecker<MongoDBTransaction> intChecker = new INTChecker<MongoDBTransaction>();
        if (intChecker.checkINT(history)) {
            System.out.println("[INFO] Checking INT Successfully");
        } else {
            System.out.println("[ERROR] Checking INT Failed");
        }

        EXTChecker<MongoDBTransaction> extChecker = new EXTChecker<MongoDBTransaction>();
        if (extChecker.checkEXT(history)) {
            System.out.println("[INFO] Checking EXT Successfully");
        } else {
            System.out.println("[ERROR] Checking EXT Failed");
        }

        long end = System.currentTimeMillis();
        System.out.println("Cost " + (end - begin) + " ms");
    }

    public static void checkSI(String urlHistory, String urlOplog, String urlMongodLog,
                               String urlResults, String urlRoOplog, String SIVariant)
            throws HistoryInvalidException, RelationInvalidException, DSGInvalidException, NullPointerException {
        long begin = System.currentTimeMillis();
        System.out.println("---------------------------------------------------------------------------------");
        System.out.println("Checking history for " + SIVariant + " at " + urlHistory);
        MongoDBHistory history = MongoDBHistoryReader.readHistory(urlHistory, urlOplog, urlMongodLog, urlRoOplog, SIVariant);

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
//        checkAll("/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store");
        checkAll("/Users/ouyanghongrong/github-projects/MongoDB-SI-Checker/src/main/resources/store-20221029");
    }

    public static void checkAll(String base) throws RelationInvalidException, DSGInvalidException, HistoryInvalidException {
        String urlHistory;
        String urlOplog;
        String urlMongodLog;
        String urlResults;
        String urlRoOplog;

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
                            urlHistory = data.getPath() + "/history.edn";
                            urlOplog = data.getPath() + "/txns.json";
                            urlMongodLog = data.getPath() + "/mongod.json";
                            urlResults = data.getPath() + "/results.edn";
                            urlRoOplog = data.getPath() + "/ro_txns.json";

                            if (new File(urlHistory).exists() && new File(urlOplog).exists()) {
                                try {
//                                    checkSI(URLHistory, URLOplog, URLMongodLog, URLResults, variant);
                                    checkSIIntExt(urlHistory, urlOplog, urlMongodLog, urlResults, urlRoOplog, variant);
//                                    checkHistoryValid(URLHistory, URLOplog, URLMongodLog, URLResults, variant);
                                } catch (NullPointerException | HistoryInvalidException e) {
                                    e.printStackTrace();
                                }
                            }
                            if (new File(urlResults).exists()) {
                                ResultReader.report(urlResults);
                            }
                        }
                    }
                }
            }
        }

    }

    public static void checkLatest() throws RelationInvalidException, DSGInvalidException, HistoryInvalidException {
        String urlHistory = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/latest/history.edn";
        String urlOplog = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/latest/txns.json";
        String urlMongodLog = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/latest/mongod.json";
        String urlResults = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/latest/results.json";
        String urlRoOplog = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/latest/ro_txns.json";
        checkSI(urlHistory, urlOplog, urlMongodLog, urlResults, urlRoOplog, "Session-SI");
    }

    public static void checkSample() throws RelationInvalidException, DSGInvalidException, HistoryInvalidException {
        String base = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/latest/";
        String urlHistory = base + "history.edn";
        String urlOplog = base + "txns.json";
        String urlMongodLog = base + "mongod.json";
        String urlResults = base + "results.json";
        String urlRoOplog = base + "ro_txns.json";
        if (new File(urlHistory).exists() && new File(urlOplog).exists()) {
            try {
                checkSI(urlHistory, urlOplog, urlMongodLog, urlResults, urlRoOplog, "Session-SI");
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
