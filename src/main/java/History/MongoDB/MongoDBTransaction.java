package History.MongoDB;

import History.MongoDB.Oplog.TxnType;
import History.Operation;
import History.Transaction;

import java.util.ArrayList;

public class MongoDBTransaction extends Transaction {

    public TxnType txnType;
    public ArrayList<String> participants;
    public long txnNumber;
    public String uuid;

    public MongoDBTransaction(long process) {
        super(process);
    }

    public void setCommitClusterTime(LogicalClock commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }



    public void setReadTimestamp(LogicalClock readTimestamp){
        this.startTimestamp = readTimestamp;
    }

    @Override
    public String toString() {
        return "{" +
                "type=" + txnType +
                ", commitTs=" + commitTimestamp +
                ", readTs=" + startTimestamp +
                ", txnNumber=" + txnNumber +
                ", shards=" + participants +
                ", ops=" + operations +
                ", uuid=" + uuid +
                '}';
    }
}