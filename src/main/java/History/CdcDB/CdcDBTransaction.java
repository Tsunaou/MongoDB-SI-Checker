package History.CdcDB;

import Exceptions.HistoryInvalidException;
import History.MongoDB.LogicalClock;
import History.Transaction;

public class CdcDBTransaction extends Transaction {
    // TiDB: 虽然 TiDB 不是用的混合逻辑始终，但是 TiDB 的时间戳精度被混合逻辑时钟覆盖。
    public CdcDBTransaction(long process) {
        super(process);
    }

    @Override
    public String toString() {
        return "[" +
                "process=" + process +
                ", operations=" + operations +
                ", startTimestamp=" + startTimestamp +
                ", commitTimestamp=" + commitTimestamp +
                ';';
    }

    public void setTimestamp(long startTs, long commitTs) throws HistoryInvalidException {
        if(hasSetTimestamp()) {
//            if(startTs != startTimestamp.time || commitTs != commitTimestamp.time) {
//                System.out.println(startTs);
//                System.out.println(commitTs);
//                System.out.println(this);
//                throw new HistoryInvalidException("Invalid Timestamp");
//            }
            if(startTs < startTimestamp.time) {
                startTimestamp.time = startTs;
            }
            if(commitTs > commitTimestamp.time) {
                commitTimestamp.time = commitTs;
            }
        } else {
            startTimestamp = new LogicalClock(startTs, 0);
            commitTimestamp = new LogicalClock(commitTs, 0);
        }
    }

    public boolean hasSetTimestamp() {
        return startTimestamp != null;
    }

}
