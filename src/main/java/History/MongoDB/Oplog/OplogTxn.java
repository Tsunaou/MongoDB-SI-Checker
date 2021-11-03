package History.MongoDB.Oplog;

import History.MongoDB.LogicalClock;
import History.Operation;
import History.WiredTiger.LSN.WtOp;

import java.util.ArrayList;

public class OplogTxn {
    public LogicalClock commitTimestamp;
    public ArrayList<Operation> ops;
    public TxnType type;
    public ArrayList<String> participants;

    public OplogTxn(LogicalClock commitTimestamp, TxnType type, ArrayList<String> participants) {
        this.ops = new ArrayList<>();
        this.commitTimestamp = commitTimestamp;
        this.type = type;
        this.participants = participants;
    }

    public void add(Operation op) {
        ops.add(op);
    }

    @Override
    public String toString() {
        return "OplogTxn{" +
                "commitTimestamp=" + commitTimestamp +
                ", ops=" + ops +
                ", type=" + type +
                ", participants=" + participants +
                '}';
    }
}
