package DSG;

import CycleChecker.CycleChecker;
import Exceptions.DSGInvalidException;
import Exceptions.HistoryInvalidException;
import Exceptions.RelationInvalidException;
import History.History;
import History.MongoDB.MongoDBHistory;
import History.MongoDB.MongoDBHistoryReader;
import History.MongoDB.MongoDBTransaction;
import History.Transaction;
import History.Operation;
import History.WiredTiger.WiredTigerHistory;
import History.WiredTiger.WiredTigerHistoryReader;
import History.WiredTiger.WiredTigerTransaction;
import Relation.Relation;
import Relation.CommitBefore;
import TestUtil.Finals;

import java.lang.reflect.ParameterizedType;
import java.util.*;

/**
 * DSG: Direct Serialization Graph from ICDE00 Generalized Isolation Level Definitions by Adya. et. al.
 */
public class DirectSerializationGraph<Txn extends Transaction> {
    public Relation<Txn> ww; // write-depends: T_i installs x_i and T_j installs x's next version
    public Relation<Txn> wr; // read-depends: T_i installs x_i, T_j reads x_i
    public Relation<Txn> rw; // anti-depends: T_i reads x_i and T_j install x's next version

    public History<Txn> history;

    public HashMap<Long, Register<Txn>> registerByKey;
    public HashMap<List<Long>, Register<Txn>> kvToRegister;

    public CommitBefore<Txn> AR;

    public DirectSerializationGraph(History<Txn> history, CommitBefore<Txn> AR) throws RelationInvalidException, DSGInvalidException {
        this.history = history;
        int n = history.transactions.size();

        ww = new Relation<Txn>(n);
        wr = new Relation<Txn>(n);
        rw = new Relation<Txn>(n);

        // Construct DSG
        // 1. Get the keySet
        HashSet<Long> keySet = new HashSet<>();
        for (Txn txn : history.transactions) {
            keySet.addAll(txn.writeKeySet);
        }
        // 2 Divide transactions by key
        HashMap<Long, ArrayList<Txn>> txnsByKey = new HashMap<>();
        for (long key : keySet) {
            txnsByKey.put(key, new ArrayList<Txn>());
        }


        for (Txn txn : history.transactions) {
            for (long key : txn.writeKeySet) {
                txnsByKey.get(key).add(txn);
            }
        }

        // 3. Get total order
        this.AR = AR;
        if(AR == null){
            AR = new CommitBefore<Txn>(n);
            AR.calculateRelation(history);
        }

        // 4. Sort transactions on each key by arbitration order
        // TODO: now we use commitTimestamp as arbitration order
        for (ArrayList<Txn> txns : txnsByKey.values()) {
            txns.sort(Comparator.comparing(t -> t.commitTimestamp));
        }

        // 5. Construct version order
        registerByKey = new HashMap<>();
        kvToRegister = new HashMap<>();

        for (Map.Entry<Long, ArrayList<Txn>> entry : txnsByKey.entrySet()) {
            long key = entry.getKey();
            Register<Txn> register = new Register<>(key, null, null);
            registerByKey.put(entry.getKey(), register);
            for (Txn txn : entry.getValue()) {
                if (txn.writeKeySet.contains(key)) {
                    for (Operation write : txn.writesByKey.get(key)) {
                        Register<Txn> reg = new Register<>(key, write.value, txn);
                        kvToRegister.put(Arrays.asList(key, write.value), reg);
                        register.insert(reg);
                    }
                }
            }
        }

        // Construct DSG
        for (Txn txn : history.transactions) {
            for (Operation op : txn.writes) {
                List<Long> kv = Arrays.asList(op.key, op.value);
                Register<Txn> ver = kvToRegister.get(kv); // txn write op.key with version ver
                Register<Txn> next = ver.nextVersion; // next write next version
                if (next != null && txn != next.installer) {
                    ww.addRelation(txn.index, next.installer.index);
                }
            }

            for (Operation op : txn.reads) {
                List<Long> kv = Arrays.asList(op.key, op.value);
                if (kvToRegister.containsKey(kv)) {
                    Register<Txn> ver = kvToRegister.get(kv); // txn read op.key which ver write
                    Register<Txn> next = ver.nextVersion; // next write op.key with next version

                    if (next != null && txn != next.installer) {
                        rw.addRelation(txn.index, next.installer.index);
                    }

                    if (txn != ver.installer) { // TODO: the same transaction
                        wr.addRelation(ver.installer.index, txn.index);
                    }
                } else {
                    if (op.value != 0) {
                        throw new DSGInvalidException("Only read 0 operation does not have version");
                    }
                }
            }
        }

    }

    /**
     * G0:  Write Cycles. A history H exhibits phenomenon G0 is DSG(H) contains a directed cycle consisting
     *      entirely of write-dependency(ww) edges.
     *
     * @return whether H exhibits G0
     */
    boolean containsG0() throws RelationInvalidException {
        System.out.println("Checking GO: Write Cycles(ww).");
        int n = history.transactions.size();
        Relation<Txn> R = new Relation<Txn>(n);
        R.union(ww);

        if (CycleChecker.topoCycleChecker(R.relation)) {
            System.out.println("This history exhibits phenomenon G0");
            List<Integer> cycles = CycleChecker.printCycle(R.relation);
            n = cycles.size();
            System.out.println(history.transactions.get(cycles.get(0)));
            for (int i = 1; i < n; i++) {
                if (ww.relation.get(cycles.get(i - 1), cycles.get(i))) {
                    System.out.println("W W");
                }
                System.out.println(history.transactions.get(cycles.get(i)));
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * G1: Now we only check G1c;
     * G1a: Aborted Reads. TODO: each read should read from some write, it should be check
     * G1b: Intermediate Reads. TODO: each read in txn1 should read form the last write in txn2 with some key $k$
     * G1c: Circular Information Flow. A history H exhibits phenomenon G1c is DSG(H) contains a directed cycle consisting
     *      entirely of dependency(ww or wr) edges.
     *
     * @return whether H exhibits G1
     */
    boolean containsG1() throws RelationInvalidException {
        System.out.println("Checking G1c: Circular Information Flow(ww or wr).");
        int n = history.transactions.size();
        Relation<Txn> R = new Relation<Txn>(n);
        R.union(ww);
        R.union(wr);

        if (CycleChecker.topoCycleChecker(R.relation)) {
            System.out.println("This history exhibits phenomenon G1");
            List<Integer> cycles = CycleChecker.printCycle(R.relation);
            n = cycles.size();
            System.out.println(history.transactions.get(cycles.get(0)));
            for (int i = 1; i < n; i++) {
                if (ww.relation.get(cycles.get(i - 1), cycles.get(i))) {
                    System.out.println("W W");
                }
                if (wr.relation.get(cycles.get(i - 1), cycles.get(i))) {
                    System.out.println("W R");
                }
                System.out.println(history.transactions.get(cycles.get(i)));
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * G2:  Write Cycles. A history H exhibits phenomenon G2 is DSG(H) contains a directed cycle
     *      with one or more anti-dependency(rw) edges;
     *
     * @return
     */
    boolean containsG2() throws RelationInvalidException {
        System.out.println("Checking G2: Write Cycles(rw).");
        int n = history.transactions.size();
        Relation<Txn> R = new Relation<Txn>(n);
        R.union(ww);
        R.union(wr);
        R.union(rw);

        if (CycleChecker.topoCycleChecker(R.relation)) {
            System.out.println("This history exhibits phenomenon G2");
            List<Integer> cycles = CycleChecker.printCycle(R.relation);
            n = cycles.size();
            System.out.println(history.transactions.get(cycles.get(0)));
            for (int i = 1; i < n; i++) {
                if (ww.relation.get(cycles.get(i - 1), cycles.get(i))) {
                    System.out.println("W W");
                }
                if (wr.relation.get(cycles.get(i - 1), cycles.get(i))) {
                    System.out.println("W R");
                }
                if (rw.relation.get(cycles.get(i - 1), cycles.get(i))) {
                    System.out.println("R W");
                }
                System.out.println(history.transactions.get(cycles.get(i)));
            }
            return true;
        } else {
            return false;
        }

    }

    /**
     * G-Single: Single Anti-dependency Cycles. A history H exhibits phenomenon G-single if DSG(H) contains a directed cycle
     *           with exactly one anti-dependency edge(rw).
     *
     * @return whether H exhibits G-Single
     */
    boolean containsGSingle(){
        return false;
    }

    public void checkSI() throws RelationInvalidException{
        checkSI("SI");
    }

    public void checkSI(String SIVariant) throws RelationInvalidException{
        boolean isG0 = containsG0();
        boolean isG1 = containsG1();

        if (!isG0 && !isG1) {
            System.out.println("The history is " + SIVariant + " but not serializable by checking DSG");
        }
    }

    public void checkDSG() throws RelationInvalidException{
        checkDSG("SI");
    }

    public void checkDSG(String SIVariant) throws RelationInvalidException {
        boolean isG0 = containsG0();
        boolean isG1 = containsG1();
        boolean isG2 = containsG2();

        if (!isG0 && !isG1) {
            if (!isG2) {
                System.out.println("The history is not only " + SIVariant + " but also by serializable checking DSG");
            } else {
                System.out.println("The history is " + SIVariant + " but not serializable by checking DSG");
            }
        }
    }


    public void printVersionOrder() {
        for (Map.Entry<Long, Register<Txn>> entry : registerByKey.entrySet()) {
            Register<Txn> guide = entry.getValue();
            System.out.println(guide.key);
            Register<Txn> reg = guide.nextVersion;
            while (reg.nextVersion != null) {
                System.out.print(reg.value + " ");
                reg = reg.nextVersion;
            }
            System.out.println();
        }
    }

    public static void WTExample() throws HistoryInvalidException, DSGInvalidException, RelationInvalidException {
        String URLHistory = Finals.URLHistory;
        String URLWTLog = Finals.URLWTLog;

        WiredTigerHistory history = WiredTigerHistoryReader.readHistory(URLHistory, URLWTLog);
        DirectSerializationGraph<WiredTigerTransaction> dsg = new DirectSerializationGraph<WiredTigerTransaction>(history, null);

        dsg.checkSI("Strong-SI");
    }

    public static void MongoDBExample() throws HistoryInvalidException, DSGInvalidException, RelationInvalidException {
        String base = "/home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/mongodb wr replica-set w:majority r:majority tw:majority tr:snapshot partition/20211104T135432.000Z/";
        String urlHistory = base + "history.edn";
        String urlOplog = base + "txns.json";
        String urlMongodLog = base + "mongod.json";
        String urlRoOplog = base + "ro_txns.json";
        MongoDBHistory history = MongoDBHistoryReader.readHistory(urlHistory, urlOplog, urlMongodLog, urlRoOplog, "Realtime-SI");
        DirectSerializationGraph<MongoDBTransaction> dsg = new DirectSerializationGraph<MongoDBTransaction>(history, null);
        dsg.checkSI("Realtime-SI/Session-SI");
    }


    public static void main(String[] args) throws HistoryInvalidException, RelationInvalidException, DSGInvalidException {
        WTExample();
//        MongoDBExample();
    }
}
