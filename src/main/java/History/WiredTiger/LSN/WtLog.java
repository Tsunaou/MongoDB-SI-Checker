package History.WiredTiger.LSN;

import java.util.ArrayList;

public class WtLog {
    public ArrayList<WtTxn> logs;

    public WtLog() {
        this.logs = new ArrayList<>();
    }

    public void add(WtTxn txn){
        this.logs.add(txn);
    }

}
