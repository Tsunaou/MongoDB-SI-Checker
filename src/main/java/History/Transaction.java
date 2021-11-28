package History;

import History.MongoDB.LogicalClock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class Transaction {
    public long process;
    public ArrayList<Operation> operations;
    public LogicalClock commitTimestamp;
    public LogicalClock startTimestamp;
    public int index; // Special to readOnly transactions, to indicate the index in the array list of transactions

    public ArrayList<Operation> writes;
    public ArrayList<Operation> reads;

    public HashSet<Long> writeKeySet;
    public HashSet<Long> readKeySet;
    public HashSet<Long> keySet;

    public HashMap<Long, ArrayList<Operation>> writesByKey;
    public HashMap<Long, ArrayList<Operation>> readsByKey;
    public HashMap<Long, ArrayList<Operation>> operationsByKey;

    public HashMap<Long, Operation> firstReadyByKey;
    public HashMap<Long, Operation> lastWriteByKey;

    public Transaction(long process) {
        this.process = process;

        this.operations = new ArrayList<>();
        this.writes = new ArrayList<>();
        this.reads = new ArrayList<>();

        this.writeKeySet = new HashSet<>();
        this.readKeySet = new HashSet<>();
        this.keySet = new HashSet<>();

        this.commitTimestamp = new LogicalClock(Long.MAX_VALUE, Long.MAX_VALUE);

        this.writesByKey = new HashMap<>();
        this.readsByKey = new HashMap<>();
        this.operationsByKey = new HashMap<>();

        this.firstReadyByKey = new HashMap<>();
        this.lastWriteByKey = new HashMap<>();
    }

    public void calculateRelationsForEXT() {
        long key;
        ArrayList<Operation> ops;
        Operation first, last;
        // Calculate the last write for key in this transaction
        for (Map.Entry<Long, ArrayList<Operation>> entry : writesByKey.entrySet()) {
            key = entry.getKey();
            ops = entry.getValue();
            if (ops.isEmpty()) {
                continue;
            }
            last = ops.get(ops.size() - 1);
            this.lastWriteByKey.put(key, last);
        }

        // Calculate the first read for key before write
        for (Map.Entry<Long, ArrayList<Operation>> entry : operationsByKey.entrySet()) {
            key = entry.getKey();
            ops = entry.getValue();
            first = ops.get(0);
            if(first.type == OPType.write){
                continue;
            }
            this.firstReadyByKey.put(key, first);
        }

    }

    public void addOpToMapByKey(HashMap<Long, ArrayList<Operation>> maps, Operation op) {
        if (!maps.containsKey(op.key)) {
            maps.put(op.key, new ArrayList<>());
        }
        maps.get(op.key).add(op);
    }

    public void addOperation(Operation op) {
        operations.add(op);
        keySet.add(op.key);
        this.addOpToMapByKey(operationsByKey, op);
        switch (op.type) {
            case read:
                reads.add(op);
                readKeySet.add(op.key);
                this.addOpToMapByKey(readsByKey, op);
                break;
            case write:
                writes.add(op);
                writeKeySet.add(op.key);
                this.addOpToMapByKey(writesByKey, op);
                break;
            default:
                System.out.println("[ERROR] TYPE ERROR: Add operation into list failed");
        }
    }

}
