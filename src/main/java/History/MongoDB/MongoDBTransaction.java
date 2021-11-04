package History.MongoDB;

import History.MongoDB.Oplog.TxnType;
import History.Operation;
import History.Transaction;

import java.util.ArrayList;

public class MongoDBTransaction extends Transaction {

    public TxnType txnType;
    public LogicalClock commitClusterTime;
    public ArrayList<String> participants;

    public MongoDBTransaction(long process) {
        super(process);
    }

    public void setCommitClusterTime(LogicalClock commitClusterTime) {
        this.commitClusterTime = commitClusterTime;
        this.setCommitTimestamp(commitClusterTime.getLongTime());
    }

    @Override
    public String toString() {
        return "{" +
                "type=" + txnType +
                ", commitTs=" + commitClusterTime +
                ", shards=" + participants +
                ", ops=" + operations +
                '}';
    }
}