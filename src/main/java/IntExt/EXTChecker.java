package IntExt;

import Exceptions.HistoryInvalidException;
import Exceptions.RelationInvalidException;
import History.*;
import History.MongoDB.MongoDBHistory;
import History.MongoDB.MongoDBHistoryReader;
import History.MongoDB.MongoDBTransaction;
import History.WiredTiger.WiredTigerHistory;
import History.WiredTiger.WiredTigerHistoryReader;
import History.WiredTiger.WiredTigerTransaction;
import Relation.*;
import java.util.*;

public class EXTChecker<Txn extends Transaction> {

    public boolean checkEXT(History<Txn> history) throws RelationInvalidException {
        return checkEXT(history, false);
    }

    public boolean checkEXT(History<Txn> history, boolean fixme) throws RelationInvalidException {
        // Checking EXT
        for (Txn transaction : history.transactions) {
            transaction.calculateRelationsForEXT();
        }

        int nTransaction = history.transactions.size();

        CommitBefore<Txn> AR = new CommitBefore<Txn>(nTransaction);
        AR.calculateRelation(history);

        ReturnBefore<Txn> VIS = new ReturnBefore<Txn>(nTransaction, fixme);
        VIS.calculateRelation(history);

        // All transactions write into key
        HashMap<Long, ArrayList<Txn>> keyWritesMap = history.keyWritesMap;
        HashMap<Long, HashSet<Txn>> writeTxnByKey = new HashMap<>();
        for (long key : keyWritesMap.keySet()) {
            writeTxnByKey.put(key, new HashSet<>(keyWritesMap.get(key)));
        }

        for (Txn txn : history.transactions) {
            for (long key : txn.firstReadyByKey.keySet()) {
                Operation read = txn.firstReadyByKey.get(key);  // T \vdash read(key, val)
//                System.out.println("Checking for read " + read + " in " + txn);
                if (read.value == 0 && !writeTxnByKey.containsKey(key)) {
                    // Init read: have not written
                    continue;
                }
                HashSet<Txn> writeTxn = writeTxnByKey.get(key);

                HashSet<Txn> visTxn = new HashSet<>();
                for (Txn t : writeTxn) {
                    if (VIS.relation.get(t.index, txn.index)) {
                        visTxn.add(t);
                    }
                }
                if (visTxn.isEmpty()) {
                    continue;
                }
                ArrayList<Txn> visWrite = new ArrayList<Txn>(visTxn);
                /**
                 * TODO: Consider equal commitTimestamp
                 *  Actually, thanks to data sharding, 2 txns with the same commit timestamp will not cover same key
                 */
                visWrite.sort(Comparator.comparing(o -> o.commitTimestamp));
//                for(Txn t: visWrite){
//                    System.out.println(t);
//                }
                int n = visWrite.size();
                Operation last = visWrite.get(n - 1).lastWriteByKey.get(key);
                if (!read.value.equals(last.value)) {
                    return false;
                }
            }
        }


        return true;
    }

    public static void checkWiredTigerResource(int n) throws HistoryInvalidException, RelationInvalidException {
        String resources = Objects.requireNonNull(INTChecker.class.getResource("/")).getPath();
        EXTChecker<WiredTigerTransaction> checker = new EXTChecker<WiredTigerTransaction>();
        for (int i = 0; i < n; i++) {
            System.out.println("------------------------------------------------------------");
            String BASE = resources + "data-1022/" + i + "/";
            String urlHistory = BASE + "history.edn";
            String urlWtLog = BASE + "wiredtiger.log";
            WiredTigerHistory history = WiredTigerHistoryReader.readHistory(urlHistory, urlWtLog);
            long begin = System.currentTimeMillis();

            if (checker.checkEXT(history, true)) {
                System.out.println("[INFO] Checking EXT Successfully");
            } else {
                System.out.println("[ERROR] Checking EXT Failed");
            }
            long end = System.currentTimeMillis();
            System.out.println("Cost " + (end - begin) + " ms");
        }
    }

    public static void checkMongoDBSample() throws HistoryInvalidException, RelationInvalidException {
        String base = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/mongodb wr sharded-cluster w:majority time:120 timeout-txn:30 txn-len:12 r:majority tw:majority tr:snapshot partition/20211128T042110.000Z/";
        String urlHistory = base + "history.edn";
        String urlOplog = base + "txns.json";
        String urlMongodLog = base + "mongod.json";
        MongoDBHistory history = MongoDBHistoryReader.readHistory(urlHistory, urlOplog, urlMongodLog);

        INTChecker<MongoDBTransaction> intChecker = new INTChecker<MongoDBTransaction>();
        intChecker.checkINT(history);

        EXTChecker<MongoDBTransaction> checker = new EXTChecker<MongoDBTransaction>();
        long begin = System.currentTimeMillis();
        if (checker.checkEXT(history)) {
            System.out.println("[INFO] Checking EXT Successfully");
        } else {
            System.out.println("[ERROR] Checking EXT Failed");
        }
        long end = System.currentTimeMillis();
        System.out.println("Cost " + (end - begin) + " ms");
    }

    public static void main(String[] args) throws HistoryInvalidException, RelationInvalidException {
        checkWiredTigerResource(10);
    }
}
