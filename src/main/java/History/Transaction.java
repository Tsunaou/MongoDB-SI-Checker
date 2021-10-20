package History;

import java.util.ArrayList;

public class Transaction {

    public long tid;
    public long process;
    public ArrayList<Operation> operations;
    public long startTimestamp;
    public long commitTimestamp;

    public ArrayList<Operation> writes;
    public ArrayList<Operation> reads;

    public Transaction(long tid, long process, long startTimestamp, long commitTimestamp) {
        this.tid = tid;
        this.process = process;
        this.startTimestamp = startTimestamp;
        this.commitTimestamp = commitTimestamp;

        this.operations = new ArrayList<Operation>();
        this.writes = new ArrayList<Operation>();
        this.reads = new ArrayList<Operation>();

    }

    public void addOperation(Operation op){
        operations.add(op);
        switch (op.type){
            case read: reads.add(op); break;
            case write:writes.add(op); break;
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
