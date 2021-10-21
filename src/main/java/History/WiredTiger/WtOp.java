package History.WiredTiger;

public class WtOp {
    public long tid;
    public long key;
    public long value;

    public WtOp(long tid, long key, long value) {
        this.tid = tid;
        this.key = key;
        this.value = value;
    }
}
