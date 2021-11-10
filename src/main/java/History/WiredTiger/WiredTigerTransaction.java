package History.WiredTiger;

import History.MongoDB.LogicalClock;
import History.Operation;
import History.Transaction;

import java.util.ArrayList;
import java.util.HashSet;

public class WiredTigerTransaction extends Transaction {

    public long tid;

    public WiredTigerTransaction(long tid, long process, LogicalClock startTimestamp, LogicalClock commitTimestamp) {
        super(process);
        this.tid = tid;
        this.startTimestamp = startTimestamp;
        this.commitTimestamp = commitTimestamp;
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
