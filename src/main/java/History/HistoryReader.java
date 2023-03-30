package History;

import us.bpsm.edn.Keyword;

import static us.bpsm.edn.Keyword.newKeyword;

public class HistoryReader {
    public static Keyword type = newKeyword("type");
    public static Keyword ok = newKeyword("ok");
    public static Keyword invoke = newKeyword("invoke");
    public static Keyword process = newKeyword("process");
    public static Keyword value = newKeyword("value");
    public static Keyword startTimestamp = newKeyword("start-timestamp");
    public static Keyword commitTimestamp = newKeyword("commit-timestamp");
    public static Keyword w = newKeyword("w");
    public static Keyword txnNumber = newKeyword("txn-number");
    public static Keyword sessionInfo = newKeyword("session-info");
    public static Keyword uuid = newKeyword("uuid");

    // For results.edn
    public static Keyword stats = newKeyword("stats");
    public static Keyword count = newKeyword("count");
    public static Keyword okCount = newKeyword("ok-count");
    public static Keyword failCount = newKeyword("fail-count");
    public static Keyword infoCount = newKeyword("info-count");

}
