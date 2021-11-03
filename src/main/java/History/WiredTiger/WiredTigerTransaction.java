package History.WiredTiger;

import History.Operation;
import History.Transaction;

import java.util.ArrayList;
import java.util.HashSet;

public class WiredTigerTransaction extends Transaction {

    public long tid;
    public long startTimestamp;

    public WiredTigerTransaction(long tid, long process, long startTimestamp, long commitTimestamp) {
        super(process);
        this.setCommitTimestamp(commitTimestamp);
        this.tid = tid;
        this.startTimestamp = startTimestamp;
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
