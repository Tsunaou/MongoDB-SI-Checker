package History;

import java.util.ArrayList;
import java.util.HashSet;

public class Transaction {

    public long tid;
    public long process;
    public ArrayList<Operation> operations;
    public long startTimestamp;
    public long commitTimestamp;

    public ArrayList<Operation> writes;
    public ArrayList<Operation> reads;

    public HashSet<Long> writeKeySet;
    public HashSet<Long> readKeySet;
    public HashSet<Long> keySet;

    public long index; // Special to readOnly transactions, to indicate the index in the array list of transactions

    public Transaction(long tid, long process, long startTimestamp, long commitTimestamp) {
        this.tid = tid;
        this.process = process;
        this.startTimestamp = startTimestamp;
        this.commitTimestamp = commitTimestamp;

        this.operations = new ArrayList<Operation>();
        this.writes = new ArrayList<Operation>();
        this.reads = new ArrayList<Operation>();

        this.writeKeySet = new HashSet<>();
        this.readKeySet = new HashSet<>();
        this.keySet = new HashSet<>();

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

    @Override
    public String toString() {
        return "[" +
                "tid=" + tid +
                ", process=" + process +
                ", operations=" + operations +
                ", startTimestamp=" + startTimestamp +
                ", commitTimestamp=" + commitTimestamp +
                ';';
    }

    public static void main(String[] args) {

    }
}
