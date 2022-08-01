package IntExt;

import Exceptions.HistoryInvalidException;
import History.*;
import History.MongoDB.MongoDBHistory;
import History.MongoDB.MongoDBHistoryReader;
import History.MongoDB.MongoDBTransaction;
import History.WiredTiger.WiredTigerHistory;
import History.WiredTiger.WiredTigerHistoryReader;
import History.WiredTiger.WiredTigerTransaction;

import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;

public class INTChecker<Txn extends Transaction> {

    public boolean checkINT(History<Txn> history) {
        // Checker INT
        ArrayList<Operation> ops;
        Operation op, pre;
        int n;

        for (Txn transaction : history.transactions) {
            for (Map.Entry<Long, ArrayList<Operation>> entry : transaction.operationsByKey.entrySet()) {
                ops = entry.getValue();
                n = ops.size();
                for (int i = 1; i < n; i++) {
                    op = ops.get(i);
                    if (op.type != OPType.read) {
                        continue;
                    }
                    pre = ops.get(i - 1);
                    if (!op.value.equals(pre.value)) {
                        System.out.println("[ERROR] Conflict with INT in key " + op.key);
                        System.out.println(transaction);
                        System.out.println(ops);
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public static void checkWiredTigerResource() throws HistoryInvalidException{
        String resources = Objects.requireNonNull(INTChecker.class.getResource("/")).getPath();
        INTChecker<WiredTigerTransaction> checker = new INTChecker<WiredTigerTransaction>();
        for (int i = 0; i < 10; i++) {
            System.out.println("------------------------------------------------------------");
            String BASE = resources + "data-1022/" + i + "/";
            String urlHistory = BASE + "history.edn";
            String urlWtLog = BASE + "wiredtiger.log";
            WiredTigerHistory history = WiredTigerHistoryReader.readHistory(urlHistory, urlWtLog);
            if (checker.checkINT(history)) {
                System.out.println("[INFO] Checking INT Successfully");
            } else {
                System.out.println("[ERROR] Checking INT Failed");
            }
        }
    }

    public static void main(String[] args) throws HistoryInvalidException {
        String base = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/mongodb wr sharded-cluster w:majority time:300 timeout-txn:30 txn-len:8 r:majority tw:majority tr:snapshot partition/20211129T091546.000Z/";
        String urlHistory = base + "history.edn";
        String urlOplog = base + "txns.json";
        String urlMongodLog = base + "mongod.json";
        String urlRoOplog = base + "ro_txns.json";
        MongoDBHistory history = MongoDBHistoryReader.readHistory(urlHistory, urlOplog, urlMongodLog, urlRoOplog);
        INTChecker<MongoDBTransaction> checker = new INTChecker<MongoDBTransaction>();
        long begin = System.currentTimeMillis();
        if (checker.checkINT(history)) {
            System.out.println("[INFO] Checking EXT Successfully");
        } else {
            System.out.println("[ERROR] Checking EXT Failed");
        }
        long end = System.currentTimeMillis();
        System.out.println("Cost " + (end - begin) + " ms");
    }
}
