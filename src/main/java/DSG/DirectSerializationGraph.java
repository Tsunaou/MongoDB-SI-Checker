package DSG;

import CycleChecker.CycleChecker;
import Exceptions.DSGInvalidException;
import Exceptions.HistoryInvalidException;
import Exceptions.RelationInvalidException;
import History.History;
import History.Transaction;
import History.Operation;
import History.WiredTiger.WiredTigerHistory;
import History.WiredTiger.WiredTigerHistoryReader;
import History.WiredTiger.WiredTigerTransaction;
import Relation.Relation;
import Relation.CommitBefore;
import TestUtil.Finals;

import java.util.*;

/**
 * DSG: Direct Serialization Graph from ICDE00 Generalized Isolation Level Definitions by Adya. et. al.
 */
public class DirectSerializationGraph<Txn extends Transaction> {
    Relation<Txn> ww; // write-depends: T_i installs x_i and T_j installs x's next version
    Relation<Txn> wr; // read-depends: T_i installs x_i, T_j reads x_i
    Relation<Txn> rw; // anti-depends: T_i reads x_i and T_j install x's next version

    History<Txn> history;

    public DirectSerializationGraph(History<Txn> history) throws RelationInvalidException, DSGInvalidException {
        this.history = history;
        int n = history.transactions.size();

        ww = new Relation<Txn>(n);
        wr = new Relation<Txn>(n);
        rw = new Relation<Txn>(n);

        // Construct DSG
        // 1. Get the keySet
        HashSet<Long> keySet = new HashSet<>();
        for (Txn txn : history.transactions) {
            keySet.addAll(txn.keySet);
        }
        // 2 Divide transactions by key
        HashMap<Long, ArrayList<Txn>> txnsByKey = new HashMap<>();
        for (long key : keySet) {
            txnsByKey.put(key, new ArrayList<Txn>());
        }


        for (Txn txn : history.transactions) {
            for (long key : txn.keySet) {
                txnsByKey.get(key).add(txn);
            }
        }

        // 3. Get total order
        CommitBefore<Txn> AR = new CommitBefore<Txn>(n);
        AR.calculateRelation(history);

        // 4. Sort transactions on each key by arbitration order
        // TODO: now we use commitTimestamp as arbitration order
        for (ArrayList<Txn> txns : txnsByKey.values()) {
            txns.sort(Comparator.comparing(t -> t.commitTimestamp));
        }

        // 5. Construct version order
        HashMap<Long, Register<Txn>> registerByKey = new HashMap<>();
        HashMap<List<Long>, Register<Txn>> kvToRegister = new HashMap<>();

        for (Map.Entry<Long, ArrayList<Txn>> entry : txnsByKey.entrySet()) {
            long key = entry.getKey();
            Register<Txn> register = new Register<>(key, null, null);
            registerByKey.put(entry.getKey(), register);
            for (Txn txn : entry.getValue()) {
                // TODO: writeByKey may be a good choice
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

        for(Txn txn: history.transactions){
            for(Operation op: txn.writes){
                List<Long> kv = Arrays.asList(op.key, op.value);
                Register<Txn> ver = kvToRegister.get(kv);
                Register<Txn> next = ver.nextVersion;
                if(next != null && txn != next.installer){
                    ww.addRelation(txn.index, next.installer.index);
                }
            }

            for(Operation op: txn.reads){
                List<Long> kv = Arrays.asList(op.key, op.value);
                if(kvToRegister.containsKey(kv)){
                    Register<Txn> ver = kvToRegister.get(kv);
                    Register<Txn> next = ver.nextVersion;
                    Register<Txn> prev = ver.preVersion;

                    if(next != null && txn != next.installer){
                        rw.addRelation(txn.index, next.installer.index);
                    }

                    if(prev != null && txn != prev.installer){
                        wr.addRelation(txn.index, ver.installer.index);
                    }
                }else{
                    if(op.value != 0){
                        throw new DSGInvalidException("Only read 0 operation does not have version");
                    }
                }
            }
        }


        Relation<Txn> R = new Relation<Txn>(n);
        R.union(ww);
//        R.union(wr);
//        R.union(rw);

        if (CycleChecker.topoCycleChecker(R.relation)) {
            System.out.println("The DSG is Cyclic");
        } else {
            System.out.println("The DSG is Good");
        }

    }

    public static void main(String[] args) throws HistoryInvalidException, RelationInvalidException, DSGInvalidException {
        String URLHistory = Finals.URLHistory;
        String URLWTLog = Finals.URLWTLog;

        WiredTigerHistory history = WiredTigerHistoryReader.readHistory(URLHistory, URLWTLog);
        DirectSerializationGraph<WiredTigerTransaction> dsg = new DirectSerializationGraph<WiredTigerTransaction>(history);


    }
}
