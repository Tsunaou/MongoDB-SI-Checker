package History.MongoDB;

import History.MongoDB.Oplog.TxnType;
import History.Operation;
import History.Transaction;

import java.util.ArrayList;

public class MongoDBTransaction extends Transaction {

    public TxnType txnType;
    public ArrayList<String> participants;

    public MongoDBTransaction(long process) {
        super(process);
    }

    public void setCommitClusterTime(LogicalClock commitClusterTime) {
        this.setCommitTimestamp(commitClusterTime);
    }

    @Override
    public String toString() {
        return "{" +
                "type=" + txnType +
                ", commitTs=" + commitTimestamp +
                ", shards=" + participants +
                ", ops=" + operations +
                '}';
    }
}