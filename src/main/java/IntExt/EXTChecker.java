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
        // Checking EXT
        for (Txn transaction : history.transactions) {
            transaction.calculateRelationsForEXT();
        }

        int nTransaction = history.transactions.size();

        CommitBefore<Txn> AR = new CommitBefore<Txn>(nTransaction);
        AR.calculateRelation(history);

        ReturnBefore<Txn> VIS = new ReturnBefore<Txn>(nTransaction);
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
                HashSet<Txn> visTxn = VIS.relationLeft(txn);
//                System.out.println("writeTxn: " + writeTxn);
//                System.out.println("visTxn: " + visTxn);
                visTxn.retainAll(writeTxn); // Intersection
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

    void checkWiredTigerResource() throws HistoryInvalidException, RelationInvalidException {
        String resources = Objects.requireNonNull(INTChecker.class.getResource("/")).getPath();
        EXTChecker<WiredTigerTransaction> checker = new EXTChecker<WiredTigerTransaction>();
        for (int i = 0; i < 10; i++) {
            System.out.println("------------------------------------------------------------");
            String BASE = resources + "data-1022/" + i + "/";
            String urlHistory = BASE + "history.edn";
            String urlWtLog = BASE + "wiredtiger.log";
            WiredTigerHistory history = WiredTigerHistoryReader.readHistory(urlHistory, urlWtLog);
            if (checker.checkEXT(history)) {
                System.out.println("[INFO] Checking EXT Successfully");
            } else {
                System.out.println("[ERROR] Checking EXT Failed");
            }
        }
    }

    public static void main(String[] args) throws HistoryInvalidException, RelationInvalidException {
        String base = "D:\\Education\\Programs\\Java\\MongoDB-SI-Checker\\src\\main\\resources\\store-1127\\replica\\20211126T140432.000Z\\";
        String urlHistory = base + "history.edn";
        String urlOplog = base + "txns.json";
        String urlMongodLog = base + "mongod.json";
        MongoDBHistory history = MongoDBHistoryReader.readHistory(urlHistory, urlOplog, urlMongodLog);
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
}
