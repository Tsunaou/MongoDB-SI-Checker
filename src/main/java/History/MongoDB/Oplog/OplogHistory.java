package History.MongoDB.Oplog;

import java.util.ArrayList;

public class OplogHistory {
    public ArrayList<OplogTxn> txns;

    public OplogHistory(){
        this.txns = new ArrayList<>();
    }

    public void add(OplogTxn txn){
        this.txns.add(txn);
    }
}
