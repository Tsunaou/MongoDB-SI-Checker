package History.WiredTiger;

import java.util.ArrayList;

public class WtTxn {
    public long tid;
    public ArrayList<WtOp> ops;

    public WtTxn(long tid) {
        this.ops = new ArrayList<>();
        this.tid = tid;
    }

    public void add(WtOp op){
        ops.add(op);
    }
}
