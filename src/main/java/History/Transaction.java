package History;

import History.MongoDB.LogicalClock;

import java.util.ArrayList;
import java.util.HashSet;

public class Transaction {
    public long process;
    public ArrayList<Operation> operations;
    public LogicalClock commitTimestamp;
    public long index; // Special to readOnly transactions, to indicate the index in the array list of transactions

    public ArrayList<Operation> writes;
    public ArrayList<Operation> reads;

    public HashSet<Long> writeKeySet;
    public HashSet<Long> readKeySet;
    public HashSet<Long> keySet;

    public Transaction(long process) {
        this.process = process;

        this.operations = new ArrayList<Operation>();
        this.writes = new ArrayList<Operation>();
        this.reads = new ArrayList<Operation>();

        this.writeKeySet = new HashSet<>();
        this.readKeySet = new HashSet<>();
        this.keySet = new HashSet<>();

        this.commitTimestamp = new LogicalClock(Long.MAX_VALUE,Long.MAX_VALUE);
    }

    public void addOperation(Operation op){
        operations.add(op);
        keySet.add(op.key);
        switch (op.type){
            case read:
                reads.add(op);
                readKeySet.add(op.key);
                break;
            case write:
                writes.add(op);
                writeKeySet.add(op.key);
                break;
            default:
                System.out.println("[ERROR] TYPE ERROR: Add operation into list failed");
        }
    }

    public void setCommitTimestamp(LogicalClock commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }
}
