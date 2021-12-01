import CycleChecker.CycleChecker;
import Exceptions.DSGInvalidException;
import Exceptions.HistoryInvalidException;
import Exceptions.RelationInvalidException;
import History.ResultReader;
import History.WiredTiger.WiredTigerHistory;
import History.WiredTiger.WiredTigerTransaction;
import History.WiredTiger.WiredTigerHistoryReader;
import IntExt.EXTChecker;
import IntExt.INTChecker;
import Relation.*;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class WTSIChecker {

    public static void checkSIIntExt(String urlHistory, String urlWtLog) throws HistoryInvalidException, DSGInvalidException, RelationInvalidException {
        long begin = System.currentTimeMillis();
        System.out.println("---------------------------------------------------------------------------------");
        System.out.println("Checking history for Strong-SI at " + urlHistory);
        WiredTigerHistory history = WiredTigerHistoryReader.readHistory(urlHistory, urlWtLog);

        INTChecker<WiredTigerTransaction> intChecker = new INTChecker<WiredTigerTransaction>();
        if (intChecker.checkINT(history)) {
            System.out.println("[INFO] Checking INT Successfully");
        } else {
            System.out.println("[ERROR] Checking INT Failed");
        }

        EXTChecker<WiredTigerTransaction> extChecker = new EXTChecker<WiredTigerTransaction>();
        if (extChecker.checkEXT(history, true)) {
            System.out.println("[INFO] Checking EXT Successfully");
        } else {
            System.out.println("[ERROR] Checking EXT Failed");
            try{
                TidBefore TB = new TidBefore(history.transactions.size());
                TB.calculateRelation(history);
            }catch (RelationInvalidException e){
                System.out.println(e);
            }
        }

        long end = System.currentTimeMillis();
        System.out.println("Cost " + (end - begin) + " ms");
    }

    public static void checkSI(String urlHistory, String urlWtLog) throws HistoryInvalidException, DSGInvalidException, RelationInvalidException {
        long begin = System.currentTimeMillis();
        System.out.println("---------------------------------------------------------------------------------");
        System.out.println("Checking history for Strong-SI at " + urlHistory);
        WiredTigerHistory history = WiredTigerHistoryReader.readHistory(urlHistory, urlWtLog);

        int nTransaction = history.transactions.size();
        Relation<WiredTigerTransaction> R = new Relation<WiredTigerTransaction>(nTransaction);

        CommitBefore<WiredTigerTransaction> CB = new CommitBefore<WiredTigerTransaction>(nTransaction);
        CB.calculateRelation(history);
        R.union(CB);

        ReturnBefore<WiredTigerTransaction> RB = new ReturnBefore<WiredTigerTransaction>(nTransaction);
        RB.calculateRelation(history);
        R.union(RB);

        ReadFrom<WiredTigerTransaction> RF = new ReadFrom<WiredTigerTransaction>(nTransaction);
        RF.calculateRelation(history);
        R.union(RF);

        TidBefore TB = new TidBefore(nTransaction);
        TB.calculateRelation(history);
        R.union(TB);
//        DirectSerializationGraph<WiredTigerTransaction> dsg = new DirectSerializationGraph<WiredTigerTransaction>(history, CB);
//        dsg.checkSI("StrongSI");

        if (CycleChecker.topoCycleChecker(R.relation)) {
            System.out.println("The Relation is Cyclic");
        } else {
            System.out.println("The Relation is StrongSI");
        }
        long end = System.currentTimeMillis();
        System.out.println("Cost " + (end - begin) + " ms");
    }

    public static void checkResource(int n) throws RelationInvalidException, DSGInvalidException, HistoryInvalidException {
        String resources = Objects.requireNonNull(WTSIChecker.class.getResource("/")).getPath();
        System.out.println(resources);
        for (int i = 0; i < n; i++) {
            System.out.println("========== Testing History " + i + "===========");
            String BASE = resources + "data-1022/" + i + "/";
            String URLHistory = BASE + "history.edn";
            String URLWTLog = BASE + "wiredtiger.log";
//            CheckSI(URLHistory, URLWTLog);
            checkSIIntExt(URLHistory, URLWTLog);
        }
    }

    public static void checkAll() throws RelationInvalidException, DSGInvalidException, HistoryInvalidException{
        String base = "/home/young/DisAlg/jepsen.wiredtiger/store";
        checkAll(base);
    }

    public static void checkAll(String base) throws RelationInvalidException, DSGInvalidException, HistoryInvalidException {

        String URLHistory;
        String URLWTLog;
        String URLResults;

        File store = new File(base);
        HashMap<String, String> keyVariant = new HashMap<>();
        keyVariant.put("register", "Strong-SI");

        for (File file : Objects.requireNonNull(store.listFiles())) {
            for (Map.Entry<String, String> entry : keyVariant.entrySet()) {
                String keyword = entry.getKey();
                String variant = entry.getValue();
                if (file.isDirectory() && file.getPath().contains(keyword)) {
                    for (File data : Objects.requireNonNull(file.listFiles())) {
                        if (data.isDirectory() && !data.getPath().contains("latest")) {
                            URLHistory = data.getPath() + "/history.edn";
                            URLWTLog = data.getPath() + "/wiredtiger.log";
                            URLResults = data.getPath() + "/results.edn";

                            if (new File(URLHistory).exists() && new File(URLWTLog).exists()) {
                                try {
                                    checkSIIntExt(URLHistory, URLWTLog);
                                } catch (RelationInvalidException e) {
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

    public static void checkSample() throws RelationInvalidException, DSGInvalidException, HistoryInvalidException {
        String BASE = "/home/young/DisAlg/jepsen.wiredtiger/store/wiredtiger:rw-register/20211115T194949.000+0800/";
        String URLHistory = BASE + "history.edn";
        String URLWTLog = BASE + "wiredtiger.log";
        try {
            checkSI(URLHistory, URLWTLog);
        } catch (RelationInvalidException e) {
            e.printStackTrace();
        }
    }

    public static void checkLatest() throws RelationInvalidException, DSGInvalidException, HistoryInvalidException {
        String BASE = "/home/young/DisAlg/jepsen.wiredtiger/store/latest/";
        String URLHistory = BASE + "history.edn";
        String URLWTLog = BASE + "wiredtiger.log";
        try {
            checkSI(URLHistory, URLWTLog);
        } catch (RelationInvalidException e) {
            e.printStackTrace();
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
